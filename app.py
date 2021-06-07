###############################################################################
# Fuzzy Units Auto Mapper
# version: v0.0
# main.py
###############################################################################

# Importing the Libraries:-
from Connections.connections import CreateConnection
from fuzzyUnitMapper.fuzzyUnitMapper import FuzzyAutoMapper
from UnitsDataDecoupler.UnitsDataDecoupler import CoupleDecouple
from configobj import ConfigObj

# Config Object
conf = ConfigObj('config.ini')

# Initialize the Config Parameters:-
domain = conf['domain']
local_db_name = conf['local_db_name']
scsql = conf['scsql']
GSHEET_ID = conf['GSHEET_ID']
migration_phase = conf['migration_phase']
process_flow = conf['process_flow']

left = [conf['left'].split(",") if type(conf['left']) == str else conf['left']][0]
right = [conf['right'].split(",") if type(conf['right']) == str else conf['right']][0]

print(", ".join(left))


# Main Loop:-
if __name__ == '__main__':
    
    # Instantiate the Connection class Object:-
    con = CreateConnection(domain, local_db_name)

    # Initialize the Database Connections:-
    local = con.local_engine_connection()
    stage = con.stage_engine_connection()
    prod = con.prod_engine_connection()

    # Instantiate the Fuzzy Unit Mapper Class Object:-
    fm = FuzzyAutoMapper(migration_phase, local, stage, prod, scsql, left, right)

    # Start Fuzzy Mapping:-
    df_matcher = fm.fuzzyUnitResolver()

    # Instantiate the Gsheet Writer Class Object:-
    cd = CoupleDecouple(domain, GSHEET_ID, process_flow, migration_phase, stage, local, df_matcher, left, right)
    cd.unit_distinct_duplicator()
    cd.write2gsheet()
    cd.readgsheet()
    cd.folio_resolver()


