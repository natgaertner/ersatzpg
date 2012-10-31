from collections import defaultdict
from itertools import izip
from cStringIO import StringIO
import psycopg2 as psycopg
import re, imp, csv, sys, time, select
from utffile import utffile
from create_partitions import permutation_tuple_generator

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
        connstr.append("host=%s" % config['host'])
    if config.has_key('port'):
        connstr.append("port=%s" % config['port'])
    if config.has_key('sslmode'):
        connstr.append("sslmode=%s" % config['sslmode'])
    connstr.append("dbname=%s user=%s password=%s" % (config['db'], config['user'], config['pw']))
    return psycopg.connect(' '.join(connstr))

def new_process_columns(table_conf):
    numbered_columns = []
    transformed_columns = []
    key_columns = []
    columns = table_conf['columns']
    for k,v in columns.iteritems():
        if type(v) == int and type(k) == str:
            numbered_columns.append((k,v-1))
        elif type(v) == dict and v.has_key('function') and (type(k) == str or type(k) == tuple):
            transformed_columns.append(((k,) if type(k) == str else k, v['function'], [i-1 for i in v['columns']], v['defaults'] if v.has_key('defaults') else {}))
        elif type(v) == dict and v.has_key('key'):
            key_columns.append((k,v['key']))
        else:
            raise Exception('Invalid column definition in table %s: key(s):%s value:%s' % (table_conf['table'], str(k), str(v)))

    udcs = [(k,v) for k,v in table_conf['udcs'].iteritems()] if table_conf.has_key('udcs') else []
    return numbered_columns, transformed_columns, udcs, key_columns

def function_lookup(module_name, func_name, reformat_path):
    module = imp.load_module(module_name, *imp.find_module(module_name, reformat_path))
    return module.__dict__[func_name]

def process_data(row, numbered_columns, transformed_columns,udcs, key_values = []):
    return [(row[i] if i < len(row) else None) for name,i in numbered_columns] + [v for tr in transformed_columns for v in tr[1](*([(row[i] if i < len(row) else None) for i in tr[2]] + (tr[3] if type(tr[3]) == list else [])), **(tr[3] if type(tr[3]) == dict else {}) )] + [i for name, i in udcs] + key_values

def create_keys(used_keys, keys, sources):
    key_values = {}
    for k in used_keys:
        key_values[k] = sources[keys[k]]
        sources[keys[k]]+=1
    return key_values

def process_parallel(p_conf, keys, univ_conf, connection):
    numbered_columns, transformed_columns, udcs, key_columns = {},{},{},{}
    table_def = {}
    processed_data_columns = {}
    sql_pattern = {}
    table_def_pattern = {}
    force_not_null = {}
    sql = {}
    field_sep = {}
    quote_char = {}
    copy_every = {}
    fs = {}
    buf = {}
    csvr = {}
    csvw = {}
    lines = {}
    write_dict = {}
    copy_sql_dict = {}
    pattern = {}
    processed_data_indexes = {}
    generators = []
    used_keys = set()
    for table, table_conf in p_conf['tables'].iteritems():
        numbered_columns[table], transformed_columns[table], udcs[table], key_columns[table] = new_process_columns(table_conf)
        used_keys.update((v for (k,v) in key_columns[table]))
        processed_data_columns[table] = [name for name, i in numbered_columns[table]]+[n for names, f, i, d in transformed_columns[table] for n in names] + [name for name, t in udcs[table]] + [name for name, t in key_columns[table]]
        table_def[table] = "%s(%s)" % (table_conf['table'],','.join(processed_data_columns[table]))
        #table_def[table] = "%s(%s)" % (table_conf['table'],','.join([name for name, i in numbered_columns[table]]+[n for names, f, i, d in transformed_columns[table] for n in names] + [name for name, t in udcs[table]] + [name for name, t in key_columns[table]]))
        force_not_null[table] = 'FORCE NOT NULL ' + ','.join(s.strip() for s in table_conf['force_not_null']) if table_conf.has_key('force_not_null') else ''
        sql[table] = "COPY %s from STDOUT WITH CSV %s" % (table_def[table], force_not_null[table])
        sql_pattern[table] = "COPY {{table_def}} from STDOUT WITH CSV {force_not_null}".format(force_not_null=force_not_null[table])
        table_def_pattern[table] = "{{table}}({columns})".format(columns=','.join(processed_data_columns[table]))
        field_sep[table] = table_conf['field_sep']
        quote_char[table] = table_conf['quotechar']
        copy_every[table] = int(table_conf['copy_every'])
    cursor = connection.cursor()
    try:
        for table, table_conf in p_conf['tables'].iteritems():
            if not fs.has_key(table_conf['filename']):
                fs[table_conf['filename']] = utffile(table_conf['filename'], 'rU') if univ_conf['use_utf'] else open(table_conf['filename'],'rU')
            #buf[table] = StringIO()
            if not csvr.has_key(table_conf['filename']):
                csvr[table_conf['filename']] = csv.reader(fs[table_conf['filename']], quotechar=quote_char[table], delimiter=field_sep[table])
                if table_conf.has_key('skip_head_lines'):
                    shl = int(table_conf['skip_head_lines'])
                    for i in range(shl):
                        csvr[table_conf['filename']].next()
                generators.append(((table_conf['filename'],l) for l in csvr[table_conf['filename']]))
            #csvw[table] = csv.writer(buf[table])
            bufs = None
            x = 0
            ptime = dict([(t,0) for t in p_conf['tables']])
            ctime = dict([(t,0) for t in p_conf['tables']])
            if table_conf.has_key('partitions'):
                pattern[table] = table_conf['partition_table_pattern']
                processed_data_indexes[table] = dict(zip(processed_data_columns[table], range(len(processed_data_columns[table]))))
                bufs = dict([(pattern[table].format(**perm), StringIO()) for perm in permutation_tuple_generator(table_conf['partitions'])])
                buf[table] = bufs
                csvws = dict([(k, csv.writer(v)) for k,v in buf[table].iteritems()])
                csvw[table] = csvws
                def write(l, table):
                    p = process_data(l, numbered_columns[table], transformed_columns[table], udcs[table], [key_values[k] for n,k in key_columns[table]])
                    part = dict((k,p[v]) for k,v in processed_data_indexes[table].iteritems())
                    csvw[table][pattern[table].format(**part)].writerow(p)
                def copy_sql(key,table,ct):
                    sql_formatted = sql_pattern[table].format(table_def=table_def_pattern[table].format(table=key))
                    b = buf[table][key]
                    b.seek(0)
                    ct -= time.time()
                    cursor.copy_expert(sql_formatted, b)
                    ct += time.time()
                    b.close()
                    buf[table][key] = StringIO()
                    csvw[table][key] = csv.writer(buf[table][key])
                    return ct
            else:
                b = StringIO()
                bufs = {table_conf['table']:b}
                buf[table] = bufs
                csvw[table] = csv.writer(b)
                def write(l, table):
                    csvw[table].writerow(process_data(l, numbered_columns[table], transformed_columns[table], udcs[table], [key_values[k] for n,k in key_columns[table]]))
                def copy_sql(key,table,ct):
                    b = buf[table][key]
                    b.seek(0)
                    ct -= time.time()
                    cursor.copy_expert(sql[table], b)
                    ct += time.time()
                    b.close()
                    buf[table][key] = StringIO()
                    csvw[table] = csv.writer(buf[table][key])
                    return ct

            write_dict[table] = write
            copy_sql_dict[table] = copy_sql

        for lines in izip(*generators):
            lines = dict(lines)
            key_values = create_keys(used_keys, keys, univ_conf['key_sources'])
            x+=1
            for table, table_conf in p_conf['tables'].iteritems():
                ptime[table] -= time.time()
                l = lines[table_conf['filename']]
                try:
                    write_dict[table](l, table)
                    #csvw[table].writerow(process_data(l, numbered_columns[table], transformed_columns[table], udcs[table], [key_values[k] for n,k in key_columns[table]]))
                except Exception, error:
                    if univ_conf['debug']:
                        import traceback; print traceback.format_exc()
                        import pdb; pdb.set_trace()
                    else:
                        raise error
                ptime[table] += time.time()
                if x % copy_every[table] == 0:
                    print "Copying {num} lines in table {table}".format(num=copy_every[table],table=table)
                    for k,v in buf[table].iteritems():
                        try:
                            ctime[table] = copy_sql_dict[table](k, table,ctime[table])
                        except Exception, error:
                            if univ_conf['debug']:
                                import traceback; print traceback.format_exc()
                                import pdb; pdb.set_trace()
                            else:
                                raise error
                    """
                    buf[table].seek(0)
                    try:
                        ctime -= time.time()
                        cursor.copy_expert(sql[table], buf[table])
                        ctime += time.time()
                    except Exception, error:
                        if univ_conf['debug']:
                            import traceback; print traceback.format_exc()
                            import pdb; pdb.set_trace()
                        else:
                            raise error
                    buf[table].close()
                    buf[table] = StringIO()
                    csvw[table] = csv.writer(buf[table])
                    """
                    print "Time spent on building buffer for table {table}: {num}".format(table=table, num=ptime[table])
                    print "Time spent copying table {table}: {num}".format(table=table, num=ctime[table])
                    #ctime = 0
                    #ptime[table] = 0
        for t in buf:
            print "Copying {num} lines in table {table}".format(num=(x % copy_every[t]), table=t)
            for k,v in buf[t].iteritems():
                try:
                    ctime[t] = copy_sql_dict[table](k,t,ctime[t])
                except Exception, error:
                    if univ_conf['debug']:
                        import traceback; print traceback.format_exc()
                        import pdb; pdb.set_trace()
                    else:
                        raise error

            """
            buf[t].seek(0)
            try:
                ctime -= time.time()
                cursor.copy_expert(sql[t], buf[t])
                ctime += time.time()
            except Exception, error:
                if univ_conf['debug']:
                    import traceback; print traceback.format_exc()
                    import pdb; pdb.set_trace()
                else:
                    raise error
            buf[t].close()
            """
            print "Time spent on building buffer: %s" % ptime[t]
            print "Time spent copying: %s" % ctime[t]
            #ctime = 0
    finally:
        for f in fs.values():
            f.close()

def process_table(table_conf, univ_conf, connection):
    numbered_columns, transformed_columns, udcs, keys = new_process_columns(table_conf)
    processed_data_columns = [name for name, i in numbered_columns]+[n for names, f, i, d in transformed_columns for n in names] + [name for name, t in udcs]
    table_def = "%s(%s)" % (table_conf['table'],','.join(processed_data_columns))
    force_not_null = 'FORCE NOT NULL ' + ','.join(s.strip() for s in table_conf['force_not_null']) if table_conf.has_key('force_not_null') else ''
    sql = "COPY %s from STDOUT WITH CSV %s" % (table_def, force_not_null)
    sql_pattern = "COPY {{table_def}} from STDOUT WITH CSV {force_not_null}".format(force_not_null=force_not_null)
    table_def_pattern = "{{table}}({columns})".format(columns=','.join(processed_data_columns))
    field_sep = table_conf['field_sep']
    quote_char = table_conf['quotechar']
    copy_every = int(table_conf['copy_every'])
    cursor = connection.cursor()
    with utffile(table_conf['filename'],'rU') if univ_conf['use_utf'] else open(table_conf['filename'], 'rU') as f:
        bufs = None
        x = 0
        ptime = 0
        ctime = 0
        if table_conf.has_key('partitions'):
            pattern = table_conf['partition_table_pattern']
            processed_data_indexes = dict(zip(processed_data_columns, range(len(processed_data_columns))))
            bufs = dict([(pattern.format(**perm), StringIO()) for perm in permutation_tuple_generator(table_conf['partitions'])])
            csvws = dict([(k, csv.writer(v)) for k,v in bufs.iteritems()])
            def write(l):
                p = process_data(l, numbered_columns, transformed_columns, udcs)
                part = dict((k,p[v]) for k,v in processed_data_indexes.iteritems())
                csvws[pattern.format(**part)].writerow(p)
            def copy_sql(key,ctime):
                sql_formatted = sql_pattern.format(table_def=table_def_pattern.format(table=key))
                b = bufs[key]
                b.seek(0)
                ctime -= time.time()
                cursor.copy_expert(sql_formatted, b)
                ctime += time.time()
                b.close()
                bufs[key] = StringIO()
                csvws[key] = csv.writer(bufs[key])
                return ctime
        else:
            buf = StringIO()
            bufs = {table_conf['table']:buf}
            csvw = csv.writer(bufs[table_conf['table']])
            def write(l):
                p = process_data(l, numbered_columns, transformed_columns, udcs)
                csvw.writerow(p)
            def copy_sql(key,ctime):
                bufs[table_conf['table']].seek(0)
                ctime -= time.time()
                cursor.copy_expert(sql, bufs[table_conf['table']])
                ctime += time.time()
                bufs[table_conf['table']].close()
                bufs[table_conf['table']] = StringIO()
                #csvw = csv.writer(bufs[table_conf['table']])
                return ctime

        csvr = csv.reader(f, quotechar=quote_char, delimiter=field_sep)
        if table_conf.has_key('skip_head_lines'):
            shl = int(table_conf['skip_head_lines'])
            for i in range(shl):
                csvr.next()
        for l  in csvr:
            ptime -= time.time()
            try:
                write(l)
            except Exception, error:
                if univ_conf['debug']:
                    import traceback; print traceback.format_exc()
                    import pdb; pdb.set_trace()
                else:
                    raise error
            ptime += time.time()
            x+=1
            if x % copy_every == 0:
                print "Copying %s lines" % copy_every
                for k,v in bufs.iteritems():
                    try:
                        ctime = copy_sql(k, ctime)
                        csvw = csv.writer(bufs[table_conf['table']])
                    except Exception, error:
                        if univ_conf['debug']:
                            import traceback; print traceback.format_exc()
                            import pdb; pdb.set_trace()
                        else:
                            raise error
                """
                buf.seek(0)
                try:
                    ctime -= time.time()
                    cursor.copy_expert(sql, buf)
                    ctime += time.time()
                except Exception, error:
                    if univ_conf['debug']:
                        import traceback; print traceback.format_exc()
                        import pdb; pdb.set_trace()
                    else:
                        raise error
                buf.close()
                buf = StringIO()
                csvw = csv.writer(buf)
                """
                print "Time spent on building buffer: %s" % ptime
                print "Time spent copying: %s" % ctime
                ptime = 0
                ctime = 0
        print "Copying %s lines" % (x % copy_every)
        for k,v in bufs.iteritems():
            try:
                ctime = copy_sql(k,ctime)
            except Exception, error:
                if univ_conf['debug']:
                    import traceback; print traceback.format_exc()
                    import pdb; pdb.set_trace()
                else:
                    raise error
        """
        buf.seek(0)
        try:
            ctime -= time.time()
            cursor.copy_expert(sql, buf)
            ctime += time.time()
        except Exception, error:
            if univ_conf['debug']:
                import traceback; print traceback.format_exc()
                import pdb; pdb.set_trace()
            else:
                raise error
        buf.close()
        """
        print "Time spent on building buffer: %s" % ptime
        print "Time spent copying: %s" % ctime

def new_process_copies(config_module, connection=None):
    universal_conf, table_confs, parallel_confs, keys = new_process_config(config_module.ERSATZPG_CONFIG)
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
