from state_abbr import states
from data import univ_settings
univ_settings = reload(univ_settings)
reformat = univ_settings.table_functions
try:
    ss = reload(state_specific)
except:
    from data import state_specific as ss
VIP_SOURCE_POSSIBLES = [s+'VIP' for s in states]
VF_SOURCE_POSSIBLES = [s+'VF' for s in states]
CANDIDATE_SOURCE_POSSIBLES = [s+'Candidates' for s in states]
ELECTION_POSSIBLES = ['2012']
DEFAULT_TABLE = {
        'skip_head_lines':1,
        'format':'csv',
        'field_sep':',',
        'quotechar':'"',
        'copy_every':100000,
        'udcs':{
            'election_key':ss.ELECTION,
            'source':ss.VIP_SOURCE
            },
        'sources':VIP_SOURCE_POSSIBLES,
        'elections':ELECTION_POSSIBLES,
        }

DEFAULT_VF_TABLE = dict(DEFAULT_TABLE)
DEFAULT_VF_TABLE.update({
    'filename':ss.VOTER_FILE_LOCATION,
    'field_sep':'\t',
    'udcs':{
        'source':ss.VF_SOURCE,
        'election_key':ss.ELECTION,
        },
        'sources':VF_SOURCE_POSSIBLES,
    })

DEFAULT_CANDIDATE_TABLE = dict(DEFAULT_TABLE)
DEFAULT_CANDIDATE_TABLE.update({
    'filename':ss.CANDIDATE_FILE_LOCATION,
    'udcs':{
        'source':ss.CANDIDATE_SOURCE,
        'election_key':ss.ELECTION,
        },
        'sources':CANDIDATE_SOURCE_POSSIBLES,
    })

DEFAULT_ACTUAL_TABLE = {
        'long_fields':(),
        'long_from':(),
        'long_to':(),
        }
