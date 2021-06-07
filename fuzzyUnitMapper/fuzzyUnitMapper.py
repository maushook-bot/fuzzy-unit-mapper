##########################################################################################
# fuzzyUnitMapper.py : Extracts Units Mappings between souurce and Track using fuzzy logic
##########################################################################################

# Importing the Modules:-
from fuzzymatcher import fuzzy_left_join
import pandas as pd


class FuzzyAutoMapper:
    def __init__(self, migration_phase, local, stage, prod, scsql, left, right):
        
        print("\n@@@ Inside FuzzyAutoMapper class @@@")
        self.migration_phase = migration_phase
        self.scsql = scsql
        self.left = left
        self.right = right
        self.local = local
        self.stage = stage
        self.prod = prod

        self.tcsql = '''  
                        SELECT u.name as name_trk, u.short_name as short_name_trk, 
                        u.unit_code as unit_code_trk, u.id as cabin_id 
                        FROM units u;
                    '''

    def fuzzyUnitResolver(self):
        
        print("$ Start Fuzzy Unit Mapping $")
        # Read the Source sql:-
        df_source = pd.read_sql(self.scsql, con = self.stage)
        print('Read the Source SQL')

        # Read the Track Dataset:- 
        df_track = pd.read_sql(self.tcsql, con = self.prod)
        print('Read the TRACK SQL')

        # Fuzzy Mapping of Source and Track data set:-
        matcher = fuzzy_left_join(df_source, df_track, self.left, self.right)

        # Load Fuzzy Matched Data ---> Stage:-
        #matcher.to_sql(name=f"tia_{self.migration_phase}_unit_processing", con=self.stage, if_exists='replace', index=False)
        matcher.to_sql(name=f"tia_{self.migration_phase}_unit_processing", con=self.local, if_exists='replace', index=False)

        print("$ Fuzzy Unit Mapping Complete $")
        return matcher
