from data import univ_settings
from data.table_defaults import *
import os
STATE = 'AL'
VIP_SOURCE = STATE+'VIP'
VF_SOURCE = STATE+'VF'
VIP_TABLES = {
        'election_administration':'full',
        'election_official':'full',
        'election':'full',
        'locality':'full',
        'polling_location':'full',
        'precinct_polling_location':'full',
        'precinct':'full',
        'source':'full',
        'state':'full',
        'street_segment':'full',
        'electoral_district':None, #'full',part_cat','part_col','part_cat_col',
        'candidiate':None #'full',part_cat','part_col','part_cat_col',
        }
VIP_FEED_LOCATION = '/tmp/temp'
VOTER_FILE_LOCATION = '/home/gaertner/bip-data/data/voterfiles/al/TS_Google_Geo_AL_20111227.txt'
VOTER_FILE_SCHEMA = '/home/gaertner/bip-data/schema/ts_voter_file.sql'

VOTER_FILE = dict(DEFAULT_TABLE)
VOTER_FILE.update({
        'table':'voter_file',
        'filename':VOTER_FILE_LOCATION,
        'field_sep':'\t',
        'udcs':{
            'source':VF_SOURCE,
            'election_key':univ_settings.ELECTION,
            'residential_country':'USA',
            'mailing_country':'USA'
            },
        'columns':{
            'sos_voterid':1,
            'county_number':21,
            'county_id':22,
            ('residential_address1', 'residential_secondary_addr'):{'function':reformat.create_vf_address,'columns':(80,81,82,83,84,85,86)},
            'residential_city':76,
            'residential_state':77,
            'residential_zip':78,
            'residential_zip_plus4':79,
            #'residential_postalcode':18,
            ('mailing_address1', 'mailing_secondary_address'):{'function':reformat.create_vf_address,'columns':(96,97,98,99,100,101,102)},
            'mailing_city':92,
            'mailing_state':93,
            'mailing_zip':94,
            'mailing_zip_plus4':95,
            #'mailing_postal_code':26,
            'county_council':30,
            'city_council':31,
            'municipal_district':32,
            'school_district':33,
            'judicial_district':34,
            'congressional_district':23,
            'precinct_name':29,
            'precinct_code':28,
            'state_representative_district':25,
            'state_senate_district':24,
            'township':26,
            #'village':44,
            'ward':27
            },
        'force_not_null':('sos_voterid','county_number'),
        })
ERSATZPG_CONFIG['tables'].update({'voter_file':VOTER_FILE})

VOTER_FILE_DISTRICTS = (
'county_council',
#'city_council',
#'municipal_district',
#'school_district',
'judicial_district',
'congressional_district',
'state_representative_district',
'state_senate_district',
#'township',
#'ward'
)

ELECTION_ADMINISTRATION_LONG.update({'filename':os.path.join(VIP_FEED_LOCATION,'election_administration.txt'), 'udcs':{'source':VIP_SOURCE,'election_key':univ_settings.ELECTION}})
ELECTION_OFFICIAL_LONG.update({'filename':os.path.join(VIP_FEED_LOCATION,'election_official.txt'), 'udcs':{'source':VIP_SOURCE,'election_key':univ_settings.ELECTION}})
ELECTION_LONG.update({'filename':os.path.join(VIP_FEED_LOCATION,'election.txt'), 'udcs':{'source':VIP_SOURCE,'election_key':univ_settings.ELECTION}})
POLLING_LOCATION_LONG.update({'filename':os.path.join(VIP_FEED_LOCATION,'polling_location.txt'), 'udcs':{'source':VIP_SOURCE,'election_key':univ_settings.ELECTION}})
PRECINCT_POLLING_LOCATION_LONG.update({'filename':os.path.join(VIP_FEED_LOCATION,'precinct_polling_location.txt')})
PRECINCT_LONG.update({'filename':os.path.join(VIP_FEED_LOCATION,'precinct.txt'), 'udcs':{'source':VIP_SOURCE,'election_key':univ_settings.ELECTION}})
SOURCE_LONG.update({'filename':os.path.join(VIP_FEED_LOCATION,'source.txt'), 'udcs':{'source':VIP_SOURCE,'election_key':univ_settings.ELECTION}})
STATE_LONG.update({'filename':os.path.join(VIP_FEED_LOCATION,'state.txt'), 'udcs':{'source':VIP_SOURCE}})
STREET_SEGMENT_LONG.update({'filename':os.path.join(VIP_FEED_LOCATION,'street_segment.txt'), 'udcs':{'source':VIP_SOURCE,'election_key':univ_settings.ELECTION}})
GEO_ADDRESS_LONG_POLLING_LOCATION.update({'filename':os.path.join(VIP_FEED_LOCATION,'polling_location.txt')})
GEO_ADDRESS_LONG_STREET_SEGMENT.update({'filename':os.path.join(VIP_FEED_LOCATION,'street_segment.txt')})
GEO_ADDRESS_LONG_ELECTION_ADMINISTRATION_PHYSICAL_ADDRESS.update({'filename':os.path.join(VIP_FEED_LOCATION,'election_administration.txt')})
GEO_ADDRESS_LONG_ELECTION_ADMINISTRATION_MAILING_ADDRESS.update({'filename':os.path.join(VIP_FEED_LOCATION,'election_administration.txt')})
