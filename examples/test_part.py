from data.univ_settings import ERSATZPG_CONFIG
from collections import OrderedDict
DEFAULT_TABLE = {
        'skip_head_lines':0,
        'udcs':{
            },
        }

PART_TEST = dict(DEFAULT_TABLE)
PART_TEST.update({
    'table':'part_test',
    'partitions':
    OrderedDict([
            ('column1',(0,1,2,3,4,5,6,7,8,9)),
            ('column2',(0,1,2,3,4,5,6,7,8,9)),
            ]),
    'partition_table_pattern':'part_test_{column1}_{column2}',
    'filename':'/home/gaertner/code/ersatzpg/examples/randoms_small.csv',
    'field_sep':',',
    'columns':{
        'column1':1,
        'column2':2,
        'column3':3,
        'column4':4,
        'column5':5,
        },
    })

PART_TEST2 = dict(DEFAULT_TABLE)
PART_TEST2.update({
    'table':'part_test2',
    #'partitions':
    #OrderedDict([
    #        ('column6',(0,1,2,3,4,5,6,7,8,9)),
    #        ('column7',(0,1,2,3,4,5,6,7,8,9)),
    #        ]),
    #'partition_table_pattern':'part_test2_{column6}_{column7}',
    'filename':'/home/gaertner/code/ersatzpg/examples/randoms_small.csv',
    'field_sep':',',
    'columns':{
        'column6':6,
        'column7':7,
        'column8':8,
        'column9':9,
        'column10':{'key':'test_key'},
        },
    })
ERSATZPG_CONFIG.update({
    'tables':{
        'part_test':PART_TEST,
        'part_test2':PART_TEST2,
        },
    'parallel_load':({'tables':('part_test','part_test2'),'keys':{'test_key':'test_key_source'}},),
    'key_sources':{'test_key_source':1},
    })

