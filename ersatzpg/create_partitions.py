from collections import OrderedDict

def create_discrete_partitions(table_names, partition_values, cursor, prev_children=[], prev_values=[]):
    pv = OrderedDict(partition_values)
    if len(pv) == 0:
        return
    else:
        k = pv.keys()[0]
        v_list = pv.pop(k)
        for t in table_names:
            for v in v_list:
                drop_sql = "DROP TABLE IF EXISTS {parent}_{value} CASCADE;".format(parent=t,value=v)
                create_sql = ("CREATE TABLE {parent}_{value} (CHECK (" + ' AND '.join("{child} = '{value}'".format(child=c, value=v) for c,v in zip(prev_children + [k], prev_values + [v])) +")) INHERITS ({parent});").format(parent=t, value=v)
                print drop_sql
                print create_sql
                cursor.execute(drop_sql)
                cursor.execute(create_sql)
                create_discrete_partitions([t+'_'+str(v)], pv, cursor, prev_children + [k], prev_values + [v])
            function_sql = ("CREATE OR REPLACE FUNCTION {parent}_insert_trigger() RETURNS TRIGGER AS $$ BEGIN IF "+ ' ELSEIF '.join("NEW.{child} = '{value}' THEN INSERT INTO {parent}_{value} VALUES (NEW.*);".format(parent=t,child=k, value=v) for v in v_list) + " ELSE RAISE EXCEPTION 'NO SUCH {child} IN DATABASE'; END IF; RETURN NULL; END; $$ LANGUAGE plpgsql;").format(parent=t, child=k)
            print function_sql
            cursor.execute(function_sql)
            trigger_sql = 'CREATE TRIGGER insert_{parent}_trigger BEFORE INSERT on {parent} FOR EACH ROW EXECUTE PROCEDURE {parent}_insert_trigger();'.format(parent=t)
            print trigger_sql
            cursor.execute(trigger_sql)


def permutation_tuple_generator(partition_values, dict_so_far = {}):
    pv = dict(partition_values)
    if len(pv) == 0:
        yield dict_so_far
    else:
        k = pv.keys()[0]
        v_list = pv.pop(k)
        for v in v_list:
            d= dict(dict_so_far)
            d.update({k:v})
            for i in permutation_tuple_generator(pv, d):
                yield i
