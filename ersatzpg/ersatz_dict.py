from collections import defaultdict
from itertools import izip
from cStringIO import StringIO
import psycopg2 as psycopg
import re, imp, csv, sys, time, select
from utffile import utffile
from create_partitions import permutation_tuple_generator
import new


def debug_try(univ_conf, function, *args, **kwargs):
    try:
        return function(*args,**kwargs)
    except Exception as error:
        if univ_conf['debug']:
            import traceback; print traceback.format_exc()
            import pdb; pdb.set_trace()
        else:
            raise error

def new_process_config(universal_config):
    universal_config_dict = {'reformat_path':None, 'debug': False, 'use_utf': False, 'testonly': False, 'parallel_load': ()}
    table_config_dict = defaultdict(lambda: {'copy_every':100000, 'format':'csv','field_sep':',','quotechar':'"'})
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
    numbered_columns = []
    transformed_columns = []
    key_columns = []
    columns = table_conf['columns']
    dict_reader = table_conf.has_key['dict_reader'] and table_conf['dict_reader']
    for k,v in columns.iteritems():
        if type(v) == int and type(k) == str:
            numbered_columns.append((k,v-1))
        elif type(v) == str and type(k) == str:
            numbered_columns.append((k,v))
            dict_reader = True
        elif type(v) == dict and v.has_key('function') and (type(k) == str or type(k) == tuple):
            transformed_columns.append(((k,) if type(k) == str else k, v['function'], [(i-1 if type(i)==int else i) for i in v['columns']], v['defaults'] if v.has_key('defaults') else {}))
            if any(map(lambda i: type(i)==str, v['columns'])):
                dict_reader = True
        elif type(v) == dict and v.has_key('key'):
            key_columns.append((k,v['key']))
        else:
            raise Exception('Invalid column definition in table {table}: key(s):{k} value:{v}'.format(table=table_conf['table'], k=k, v=v))
    udcs = [(k,v) for k,v in table_conf['udcs'].iteritems()] if table_conf.has_key('udcs') else []
    return numbered_columns, transformed_columns, udcs, key_columns, dict_reader

def process_data(row, numbered_columns, transformed_columns,udcs, key_values = [], field_names=None):
    if field_names:
        numbered = [(row[h] if type(h)==str else (row[field_names[h]] if h < len(field_names) else None)) for name, h in numbered_columns]
        transformed = [v for tr in transformed_columns for v in tr[1](*([(row[h] if type(h) == str else (row[field_names[h]] if h < len(field_names) else None)) for h in tr[2]] + (tr[3] if type(tr[3]) == list else [])), **(tr[3] if type(tr[3]) == dict else {}))]
        default = [h for name, h in udcs]
        return numbered + transformed + default + key_values
    else:
        numbered = [(row[i] if i < len(row) else None) for name,i in numbered_columns]
        transformed = [v for tr in transformed_columns for v in tr[1](*([(row[i] if i < len(row) else None) for i in tr[2]] + (tr[3] if type(tr[3]) == list else [])), **(tr[3] if type(tr[3]) == dict else {}) )]
        default = [i for name, i in udcs]
    return numbered + transformed + default + key_values

def create_keys(used_keys, keys, sources):
    key_values = {}
    for k in used_keys:
        key_values[k] = sources[keys[k]]
        sources[keys[k]]+=1
    return key_values

class Table:
    def __init__(self, table_conf, univ_conf, cursor):
        self.numbered_columns, self.transformed_columns, self.udcs, self.key_columns, self.dict_reader = new_process_columns(table_conf)
        self.processed_data_columns = [name for name, i in self.numbered_columns]+[n for names, f, i, d in self.transformed_columns for n in names] + [name for name, t in self.udcs] + [name for name, t in self.key_columns]
        self.table_def = "{table}({columns})".format(table=table_conf['table'],columns=','.join(self.processed_data_columns))
        self.force_not_null = 'FORCE NOT NULL ' + ','.join(s.strip() for s in table_conf['force_not_null']) if table_conf.has_key('force_not_null') else ''
        self.sql_pattern = "COPY {{table_def}} from STDOUT WITH CSV {force_not_null}".format(force_not_null=self.force_not_null)
        self.table_def_pattern = "{{table}}({columns})".format(columns=','.join(self.processed_data_columns))
        self.field_sep = table_conf['field_sep']
        self.quote_char = table_conf['quotechar']
        self.copy_every = int(table_conf['copy_every'])
        self.ptime = 0
        self.ctime = 0
        if table_conf.has_key('partitions'):
            self.pattern = table_conf['partition_table_pattern']
            self.processed_data_indexes = dict(zip(self.processed_data_columns, range(len(self.processed_data_columns))))
            self.buf = dict([(self.pattern.format(**perm), StringIO()) for perm in permutation_tuple_generator(table_conf['partitions'])])
            self.csvw = dict([(k, csv.writer(v)) for k,v in self.buf.iteritems()])
            def write(self,l,key_values=None):
                p = process_data(l, self.numbered_columns, self.transformed_columns, self.udcs, [key_values[k] for n,k in self.key_columns])
                part = dict((k,p[v]) for k,v in self.processed_data_indexes.iteritems())
                key = self.pattern.format(**part)
                self.csvw[key].writerow(p)
        else:
            self.buf = {table_conf['table']:StringIO()}
            self.csvw = {table_conf['table']:csv.writer(self.buf[table_conf['table']])}
            def write(self,l,key_values=None):
                p = process_data(l, self.numbered_columns, self.transformed_columns, self.udcs, [key_values[k] for n,k in self.key_columns])
                self.csvw[table_conf['table']].writerow(p)
        self.write = new.instancemethod(write,self,Table)
        self.writer = sql_writer(self,univ_conf,cursor)
        self.writer.next()

    def copy_sql(self,key,cursor):
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
                csvr[table_conf['filename']] = csv.reader(fs[table_conf['filename']], quotechar=table_class.quote_char, delimiter=table_class.field_sep)
                if table_conf.has_key('skip_head_lines'):
                    shl = int(table_conf['skip_head_lines'])
                    for i in range(shl):
                        csvr[table_conf['filename']].next()
                generators.append(((table_conf['filename'],l) for l in csvr[table_conf['filename']]))
        for lines in izip(*generators):
            lines = dict(lines)
            key_values = create_keys(used_keys, keys, univ_conf['key_sources'])
            for table, table_conf in p_conf['tables'].iteritems():
                table_classes[table].writer.send((lines[table_conf['filename']],key_values))
        for table in p_conf['tables']:
            table_classes[table].writer.close()
    finally:
        for f in fs.values():
            f.close()

def process_table(table_conf, univ_conf, connection):
    cursor = connection.cursor()
    table_class = Table(table_conf,univ_conf, cursor)
    with utffile(table_conf['filename'],'rU') if univ_conf['use_utf'] else open(table_conf['filename'], 'rU') as f:
        csvr = csv.reader(f, quotechar=table_class.quote_char, delimiter=table_class.field_sep)
        if table_conf.has_key('skip_head_lines'):
            shl = int(table_conf['skip_head_lines'])
            for i in range(shl):
                csvr.next()
        for l in csvr:
            table_class.writer.send((l,None))
        table_class.writer.close()

def sql_writer(table_class,univ_conf,cursor):
    x = 0
    try:
        while True:
            l,key_values = (yield)
            table_class.ptime -= time.time()
            debug_try(univ_conf, table_class.write, l, key_values)
            table_class.ptime += time.time()
            x+=1
            if x % table_class.copy_every == 0:
                print "Copying {copy_every} lines".format(copy_every=table_class.copy_every)
                for k,v in table_class.buf.iteritems():
                    debug_try(univ_conf, table_class.copy_sql,k,cursor)
                print "Time spent on building buffer: {ptime}".format(ptime=table_class.ptime)
                print "Time spent copying: {ctime}".format(ctime=table_class.ctime)
    except GeneratorExit:
        print "Copying {copy_every} lines".format(copy_every=(x % table_class.copy_every))
        for k,v in table_class.buf.iteritems():
            debug_try(univ_conf, table_class.copy_sql,k,cursor)
        print "Time spent on building buffer: {ptime}".format(ptime=table_class.ptime)
        print "Time spent copying: {ctime}".format(ctime=table_class.ctime)


def new_process_copies(config_module, connection=None, config_name='ERSATZPG_CONFIG'):
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
