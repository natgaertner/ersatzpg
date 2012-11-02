import os,sys
from collections import OrderedDict
os.chdir('..')
sys.path.append('.')
from ersatzpg import ersatz,ersatz_threaded, create_partitions as cp
from examples import test_part
conf = test_part.ERSATZPG_CONFIG
connection = ersatz.db_connect(conf)
#create our tables into which we want to import
connection.cursor().execute("drop table if exists part_test cascade")
connection.cursor().execute("drop table if exists part_test2 cascade")
connection.cursor().execute("create table part_test(column1 int, column2 int, column3 int, column4 int, column5 int);")
connection.cursor().execute("create table part_test2(column6 int, column7 int, column8 int, column9 int, column10 int);")

#create table partitions into which we want to import
cp.create_discrete_partitions(['part_test'], conf['tables']['part_test']['partitions'], connection.cursor())
od = OrderedDict([
        ('column6',(0,1,2,3,4,5,6,7,8,9)),
        ('column7',(0,1,2,3,4,5,6,7,8,9)),
        ])
cp.create_discrete_partitions(['part_test2'], od, connection.cursor())
connection.commit()
connection.close()

#run the import
#ersatz.new_process_copies(test_part)
ersatz_threaded.new_process_copies(test_part)
