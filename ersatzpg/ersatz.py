from collections import defaultdict
from itertools import izip
from cStringIO import StringIO
import psycopg2 as psycopg
import re, imp, csv, sys, time, select
from utffile import utffile
from create_partitions import permutation_tuple_generator
import new


def _debug_try(univ_conf, function, *args, **kwargs):
    #Wrapper that catches exceptions and goes into debugger if debug is on, passes exception otherwise
    try:
        return function(*args,**kwargs)
    except Exception as error:
        if univ_conf['debug']:
            import traceback; print traceback.format_exc()
            import pdb; pdb.set_trace()
        else:
            raise error

def new_process_config(universal_config):
    """Import config dictionary and set some sensible defaults
    universal_config is a dict that must contain a 'tables' entry with
    a dict entry that maps table names to table definition dicts.
    Each table definition dict must contain at least a 'columns' entry
    (defined in the help for new_process_columns), and a 'filename'
    entry indicating the file from which to load data for this table
    possible universal config entries:
        reformat_path - The location of the reformat functions module
        debug - Whether to drop into pdb when errors are raised
        use_utf - try to automatically detect encoding in source files and convert to utf-8
        testonly - rollback all work when done
        parallel_load - the tables to load simultaneously along with a dict of key names mapped to key sources, used for loading multiple tables from the same file
        key_sources - a dict of key source names to starting integers for sequential keys used in parallel load
    possible table config entries:
        copy_every - how many lines to process before copying into database
        format - just csv for now. May be used for fixed width later
        field_sep - the field separator for delimited files
        quotechar - the quote chracter for delimited files
        force_not_null - An iterable of columns in the table to copy as default blank characters rather than null
        udcs - A dict of column names to default values to import for those columns
        partitions - An OrderedDict of column names to a tuple of values on which to partition
        partition_table_pattern - the pattern of partition table names to use in finding the correct partition table for a given row

    returns:
        universal_config_dict - universal config with defaults set
        table_config_dict - a dict of table names to table config dicts with defaults set
        parallel_config_tuple - a tuple of table names to process in parallel
        keys - a dict of key names to key source names

    """
    universal_config_dict = {'reformat_path':None, 'debug': False, 'use_utf': False, 'testonly': False, 'parallel_load': ()}
    table_config_dict = defaultdict(lambda: {'dict_reader':False,'copy_every':100000, 'format':'csv','field_sep':',','quotechar':'"'})
    universal_config_dict.update(universal_config)
    for t in universal_config_dict['tables']:
        table_config_dict[t].update(universal_config_dict['tables'][t])
        if not table_config_dict[t].has_key('table') or not table_config_dict[t].has_key('filename') or not table_config_dict[t].has_key('columns'):
            if universal_config_dict['debug']:
                import pdb;pdb.set_trace()
            else:
                raise Exception('table config must contain table, filename, and columns')
    parallel_config_tuple = universal_config_dict['parallel_load']
    keys = {}
    for p in universal_config_dict['parallel_load']:
        p['tables'] = dict([(k, table_config_dict.pop(k)) for k in p['tables']])
        keys.update(p['keys'])
    return universal_config_dict, table_config_dict, parallel_config_tuple, keys

def db_connect(config):
    """Create a psycopg connection object given a config dict
    required config dict entries:
        db - the database name
        user - the user name
        pw - the password
    option config dict entries:
        host - remote host name
        port - port to connect on
        sslmode - whether or not to use ssl
    returns a psycopg connection object

    """
    connstr = []
    if config.has_key('host'):
        connstr.append("host={host}".format(config['host']))
    if config.has_key('port'):
        connstr.append("port={port}".format(config['port']))
    if config.has_key('sslmode'):
        connstr.append("sslmode={ssl}".format(config['sslmode']))
    connstr.append("dbname={db} user={user} password={pw}".format(**config))
    return psycopg.connect(' '.join(connstr))

def new_process_columns(table_conf):
    """convert the column definitions in a table into lists of column definitions separated by type
    columns may be of 3 types:
        single columns - These are columns where the source is indicated by a column number in the source file
        transformed columns - These are columns where the source is a function applied to a collection of columns. One function may be specified, along with an arbitrary number of input columns specified as column numbers in the source file, default arguments to the function, and an arbitrary number of output columns, each of which will be populated by the function's return values (the number of specified output columns must match the number of function return values)
        key columns - These are columns where the source is a sequential key from a source defined in the universal config. These are only used in tables that are processed in parallel. (So that two tables loaded in parallel can share a key and be linked together). If you want a key for an indidivual table, a default value set to a sequence in the SQL table definition should suffice, but I am open to hearing use cases for this in ersatz.
    returns:
        single_columns - a list of 2-tuples of the form (name, column number)
        transformed_columns - a list of 4-tuples of the form (return value(s), function, argument columns, default arguments)
        udcs - a list of 2-tuples of the form (name, default value)
        key_columns - a list of tuples of the form (name, key_name)

    """
    single_columns = []
    transformed_columns = []
    key_columns = []
    columns = table_conf['columns']
    if table_conf['dict_reader']:
        for k,v in columns.iteritems():
            if type(v) == str and type(k) == str:
                single_columns.append((k,v))
            elif type(v) == dict and v.has_key('function') and (type(k) == str or type(k) == tuple):
                transformed_columns.append(((k,) if type(k) == str else k, v['function'], [s for s in v['columns']], v['defaults'] if v.has_key('defaults') else {}))
            elif type(v) == dict and v.has_key('key'):
                key_columns.append((k,v['key']))
            else:
                raise Exception('Invalid column definition in table {table}: key(s):{k} value:{v}'.format(table=table_conf['table'], k=k, v=v))
    else:
        for k,v in columns.iteritems():
            if type(v) == int and type(k) == str:
                single_columns.append((k,v-1))
            elif type(v) == dict and v.has_key('function') and (type(k) == str or type(k) == tuple):
                transformed_columns.append(((k,) if type(k) == str else k, v['function'], [i-1 for i in v['columns']], v['defaults'] if v.has_key('defaults') else {}))
            elif type(v) == dict and v.has_key('key'):
                key_columns.append((k,v['key']))
            else:
                raise Exception('Invalid column definition in table {table}: key(s):{k} value:{v}'.format(table=table_conf['table'], k=k, v=v))
    udcs = [(k,v) for k,v in table_conf['udcs'].iteritems()] if table_conf.has_key('udcs') else []
    return single_columns, transformed_columns, udcs, key_columns

def _process_data(row, single_columns, transformed_columns,udcs, key_values = [], dict_reader=False):
    #grab directly single columns from a split row from the input file, run the row through the column transformations and append default and key sourced values. A hook for reading columns via csv.DictReader exist here but TODO deal with opening files for parallel load with dict/number specified different ways
    if dict_reader:
        single = [row[h] for name, h in single_columns]
        transformed = [v for tr in transformed_columns for v in tr[1](*([row[h] for h in tr[2]] + (tr[3] if type(tr[3]) == list else [])), **(tr[3] if type(tr[3]) == dict else {}))]
        default = [h for name, h in udcs]
        return single + transformed + default + key_values
    else:
        single = [(row[i] if i < len(row) else None) for name,i in single_columns]
        transformed = [v for tr in transformed_columns for v in tr[1](*([(row[i] if i < len(row) else None) for i in tr[2]] + (tr[3] if type(tr[3]) == list else [])), **(tr[3] if type(tr[3]) == dict else {}) )]
        default = [i for name, i in udcs]
    return single + transformed + default + key_values

def _create_keys(used_keys, keys, sources):
    #Grab specified key values from key sources and increment sources. Has to happen all at once for all keys so sources only increment once per row
    key_values = {}
    for k in used_keys:
        key_values[k] = sources[keys[k]]
        sources[keys[k]]+=1
    return key_values

class Table:
    """Class for grouping together all the stuff that is processed for a given table. This can be applied regardless of whether the table is being loaded in parallel or individually. The internals here will handle the cases where the table is being loaded into partitions or straight into one table.
    Fields:
        from new_process_columns:
            single_columns
            transformed_columns
            udcs
            key_columns
        sql statement formatters:
            processed_data_columns - list of column names in order to place in copy statement (matches order of columns returned from new_process_columns
            table_def - '{table}({columns})'
            force_not_null - A sql statement to append to COPY listing the columns to force not null
            sql_pattern - the format of the copy statement
            table_def_pattern - like table_def but leaves the table to be set by the partitioning table choice (if partitioning the table)
        field_sep - the field separator to use in csv readers based on this table
        quote_char - the quote char to use in csv readers based on this table
        copy_every - the number of lines to process for this table before copying into the database
        ptime - the amount of processing time spent for this table
        ctime - the amount of copying time spent for this table
    If partitioned:
        pattern - the partition table name pattern (a python formattable string)
        processed_data_indexes - a dictionary of processed_data_columns to integers indicating their position in the list (i.e. {'column1':1,'column2':2} etc)
        buf - a dict of possible partition tables to StringIO objects
        csvw - a dict of possible partition tables to csv writers that write to the corresponding entries in buf
    Else:
        buf - a dict with one entry mapping the table name to a StringIO object
        csvw - a dict with one entry mapping the table name to a csv writer that writes to buf

    Each Table has a write method that is defined dynamically depending on whether partitions are being done. Parition write will find the correct partition table entry in csvw based on pattern and the data processed by process_data and write the processed row to it. Non-partition write will simply write the process_data return to csvw
    Each Table also carries a reference to a _sql_writer coroutine defined using the Table object. This coroutine handles running the write method and copying every copy_ever rows. It also handles writing remaining rows when the file is closed out.

    """
    def __init__(self, table_conf, univ_conf, cursor):
        """Create member fields and dynamically defined write method
        takes table config object, universal config object, and psycopg cursor object

        """
        self.single_columns, self.transformed_columns, self.udcs, self.key_columns = new_process_columns(table_conf)
        self.processed_data_columns = [name for name, i in self.single_columns]+[n for names, f, i, d in self.transformed_columns for n in names] + [name for name, t in self.udcs] + [name for name, t in self.key_columns]
        self.table_def = "{table}({columns})".format(table=table_conf['table'],columns=','.join(self.processed_data_columns))
        self.force_not_null = 'FORCE NOT NULL ' + ','.join(s.strip() for s in table_conf['force_not_null']) if table_conf.has_key('force_not_null') else ''
        self.sql_pattern = "COPY {{table_def}} from STDOUT WITH CSV {force_not_null}".format(force_not_null=self.force_not_null)
        self.table_def_pattern = "{{table}}({columns})".format(columns=','.join(self.processed_data_columns))
        self.field_sep = table_conf['field_sep']
        self.quote_char = table_conf['quotechar']
        self.copy_every = int(table_conf['copy_every'])
        self.dict_reader = table_conf['dict_reader']
        self.ptime = 0
        self.ctime = 0
        if table_conf.has_key('partitions'):
            self.pattern = table_conf['partition_table_pattern']
            self.processed_data_indexes = dict(zip(self.processed_data_columns, range(len(self.processed_data_columns))))
            self.buf = dict([(self.pattern.format(**perm), StringIO()) for perm in permutation_tuple_generator(table_conf['partitions'])])
            self.csvw = dict([(k, csv.writer(v)) for k,v in self.buf.iteritems()])
            def write(self,l,key_values=None):
                p = _process_data(l, self.single_columns, self.transformed_columns, self.udcs, [key_values[k] for n,k in self.key_columns],self.dict_reader)
                part = dict((k,p[v]) for k,v in self.processed_data_indexes.iteritems())
                key = self.pattern.format(**part)
                self.csvw[key].writerow(p)
        else:
            self.buf = {table_conf['table']:StringIO()}
            self.csvw = {table_conf['table']:csv.writer(self.buf[table_conf['table']])}
            def write(self,l,key_values=None):
                p = _process_data(l, self.single_columns, self.transformed_columns, self.udcs, [key_values[k] for n,k in self.key_columns], self.dict_reader)
                self.csvw[table_conf['table']].writerow(p)
        self.write = new.instancemethod(write,self,Table)
        self.writer = _sql_writer(self,univ_conf,cursor)
        self.writer.next()

    def copy_sql(self,key,cursor):
        """Function to run actual copy command
        takes a table name key and a psycopg cursor object
        runs psycopg's copy_expert command, records copy time, closes out the old buffer and csvw and creates a new buffer and csvw

        """
        sql_formatted = self.sql_pattern.format(table_def=self.table_def_pattern.format(table=key))
        b = self.buf[key]
        b.seek(0)
        self.ctime -= time.time()
        cursor.copy_expert(sql_formatted, b)
        self.ctime += time.time()
        b.close()
        self.buf[key] = StringIO()
        self.csvw[key] = csv.writer(self.buf[key])

def process_parallel(p_conf, keys, univ_conf, connection):
    """Process tables in parallel from a selection of files
    takes:
        p_conf - a dict with a 'tables' entry mapping the names of the tables to process in parallel to their table configs
        keys - a dict of key names to key sources
        univ_conf - the universal config
        connection - a psycopg connection object
    Iterates through the table configs creating Table objects, collecting the keys used by the tables, and opening the files needed by the tables. Each file is associated with a csv reader (TODO: figure out how to choose DictReader based on all tables associated with the file's needs) which is then placed in a generator that generates tuples of (filename, row). Some quick izip usage provides iteration through tuples of the form ((filename1,row1),(filename2,row2)...) which can then be converted to a dict providing each table with a way to access its file's row without having to go through each file sequentially. This could potentially be used to load different columns of a table from different files in parallel. Each Table grabs the row it needs and sends it to its _sql_writer coroutine

    """
    fs = {}
    csvr = {}
    generators = []
    used_keys = set()
    table_classes = {}
    writers = {}
    cursor = connection.cursor()
    try:
        for table, table_conf in p_conf['tables'].iteritems():
            table_class = Table(table_conf, univ_conf, cursor)
            table_classes[table] = table_class
            used_keys.update((v for (k,v) in table_classes[table].key_columns))
            if not fs.has_key(table_conf['filename']):
                fs[table_conf['filename']] = utffile(table_conf['filename'], 'rU') if univ_conf['use_utf'] else open(table_conf['filename'],'rU')
            if not csvr.has_key(table_conf['filename']):
                if table_conf['dict_reader']:
                    csvr[table_conf['filename']] = csv.DictReader(fs[table_conf['filename']],quotechar=table_class.quote_char, delimiter=table_class.field_sep)
                else:
                    csvr[table_conf['filename']] = csv.reader(fs[table_conf['filename']], quotechar=table_class.quote_char, delimiter=table_class.field_sep)
                    if table_conf.has_key('skip_head_lines'):
                        shl = int(table_conf['skip_head_lines'])
                        for i in range(shl):
                            csvr[table_conf['filename']].next()
                generators.append(((table_conf['filename'],l) for l in csvr[table_conf['filename']]))
            elif isinstance(csvr[table_conf['filename']], csv.DictReader) != table_conf['dict_reader']:
                raise Exception('Improper Configuration: {table} is configured to {not1} use csv.DictReader but another table loaded from the same file is configured to {not2} use csv.DictReader. (the default is to not use it, so it must be specified on every table config for that file)'.format(table=table_conf['table'],not1=('not' if table_conf['dict_reader'] else ''), not2=('' if table_conf['dict_reader'] else 'not')))

        for lines in izip(*generators):
            lines = dict(lines)
            key_values = _create_keys(used_keys, keys, univ_conf['key_sources'])
            for table, table_conf in p_conf['tables'].iteritems():
                table_classes[table].writer.send((lines[table_conf['filename']],key_values))
        for table in p_conf['tables']:
            table_classes[table].writer.close()
    finally:
        for f in fs.values():
            f.close()

def process_table(table_conf, univ_conf, connection):
    """The simple, non parallel table processor
    takes:
        table_conf - a table config dict
        univ_conf - the univesal config dict
        connection - a psycopg connection object
    Creates a Table from the table_conf, opens the file associated with this table, creates a csv reader for it and iterates through the file sending the rows to the Table's _sql_writer coroutine

    """
    cursor = connection.cursor()
    table_class = Table(table_conf,univ_conf, cursor)
    with utffile(table_conf['filename'],'rU') if univ_conf['use_utf'] else open(table_conf['filename'], 'rU') as f:
        if table_conf['dict_reader']:
            csvr = csv.DictReader(f,quotechar=table_class.quote_char,delimiter=table_class.field_sep)
        else:
            csvr = csv.reader(f, quotechar=table_class.quote_char, delimiter=table_class.field_sep)
            if table_conf.has_key('skip_head_lines'):
                shl = int(table_conf['skip_head_lines'])
                for i in range(shl):
                    csvr.next()
        for l in csvr:
            table_class.writer.send((l,None))
        table_class.writer.close()

def _sql_writer(table_class,univ_conf,cursor):
    #A coroutine that is created with a Table, takes rows from a csv, and runs that table's write and copy_sql methods on that row and the Table's buf(s) as appropriate. Runs the final copy_sql for uncopied processed rows when closed
    x = 0
    try:
        while True:
            l,key_values = (yield)
            table_class.ptime -= time.time()
            _debug_try(univ_conf, table_class.write, l, key_values)
            table_class.ptime += time.time()
            x+=1
            if x % table_class.copy_every == 0:
                print "Copying {copy_every} lines".format(copy_every=table_class.copy_every)
                for k,v in table_class.buf.iteritems():
                    _debug_try(univ_conf, table_class.copy_sql,k,cursor)
                print "Time spent on building buffer: {ptime}".format(ptime=table_class.ptime)
                print "Time spent copying: {ctime}".format(ctime=table_class.ctime)
    except GeneratorExit:
        print "Copying {copy_every} lines".format(copy_every=(x % table_class.copy_every))
        for k,v in table_class.buf.iteritems():
            _debug_try(univ_conf, table_class.copy_sql,k,cursor)
        print "Time spent on building buffer: {ptime}".format(ptime=table_class.ptime)
        print "Time spent copying: {ctime}".format(ctime=table_class.ctime)


def new_process_copies(config_module, connection=None, config_name='ERSATZPG_CONFIG'):
    """Takes a module containing a configuration dictionary and possibly a connection and copies the configured tables into the database
    takes:
        config_module - a python module containing a dictionary defining a universal config containing one or more table configs. See the help for new_process_config for a list of config fields.
        connection - an optional psycopg connection object. If NOT provided, this function will create its own connection based on connection data in the config and commit or rollback depending on the 'testonly' field in the config. If provided, nothing will be committed by this function and it is the responsibility of the connection provider to commit changes.
        config_name - a string indicating the name of the config dict in the config module. 'ERSATZPG_CONFIG' is the default if no config_name is provided.

    """
    universal_conf, table_confs, parallel_confs, keys = new_process_config(config_module.__dict__[config_name])
    local_connection = False
    if not connection:
        local_connection = True
        connection = db_connect(universal_conf)
    try:
        for table in table_confs:
            print "Processing table {table}".format(table=table)
            process_table(table_confs[table], universal_conf, connection)
        for p_dict in parallel_confs:
            print "Processing tables {tables} in parallel".format(tables=', '.join(p_dict['tables']))
            process_parallel(p_dict, keys, universal_conf, connection)
    except Exception, error:
        if universal_conf['debug']:
            import traceback; print traceback.format_exc()
            import pdb; pdb.set_trace()
        else:
            raise error
    finally:
        if universal_conf['testonly']:
            if local_connection:
                connection.rollback()
        else:
            connection.commit()
        if local_connection:
            connection.close()

if __name__ == "__main__":
    new_process_copies(imp.load_source('config',sys.argv[1]))
