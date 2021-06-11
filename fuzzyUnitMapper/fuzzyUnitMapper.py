##########################################################################################
# fuzzyUnitMapper.py : Extracts Units Mappings between souurce and Track using fuzzy logic
##########################################################################################

# Importing the Modules:-
from fuzzymatcher import fuzzy_left_join
import pandas as pd
import random


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
                        SELECT u.name as unit_name_trk, u.short_name as short_name_trk, 
                        u.unit_code as unit_code_trk, u.id as cabin_id 
                        FROM units u;
                    '''

    def fuzzyUnitResolver(self):
        
        print("$ Start Fuzzy Unit Mapping $")
        # Read the Source sql:-
        df_source = pd.read_sql(self.scsql, con = self.stage)
        print('Read Source SQL')

        # Read the Track Dataset:- 
        df_track = pd.read_sql(self.tcsql, con = self.prod)
        print('Read TRACK SQL')

        # Fuzzy Mapping of Source and Track data set:-
        matcher = fuzzy_left_join(df_source, df_track, self.left, self.right)

        print("$ Fuzzy Unit Mapping: Complete $")
        return matcher

    def fuzzyUnitProcessor(self, matcher):

        print(f"$ Preparing fuzzy unit processing table: tia_{self.migration_phase}_unit_processing $")
        data_dict = {'unit_stats_id': pd.Series([], dtype='int'),'folio': pd.Series([], dtype='str'), 
                     'unit_name_src': pd.Series([], dtype='str'), 'unit_code_src': pd.Series([], dtype='str'), 
                     'unit_name_trk': pd.Series([], dtype='str'), 'unit_code_trk': pd.Series([], dtype='str'), 
                     'short_name_trk': pd.Series([], dtype='str'), 'cabin_id': pd.Series([], dtype='int')}
        df_out = pd.DataFrame(data_dict)
        df_out.to_sql(name=f"tia_{self.migration_phase}_unit_processing", con=self.local, if_exists='replace', index=False)
        df_out.to_sql(name=f"tia_{self.migration_phase}_unit_processing", con=self.stage, if_exists='replace', index=False)
        
        for col in self.left:
            data_dict[col] = matcher[col]

        data_dict['unit_stats_id'] = random.randint(1, 100000)
        data_dict['folio'] = matcher['folio']
        data_dict['unit_name_trk'] = matcher['unit_name_trk']
        data_dict['unit_code_trk'] = matcher['unit_code_trk']
        data_dict['short_name_trk'] = matcher['short_name_trk']
        data_dict['cabin_id'] = matcher['cabin_id']
        df_out = pd.DataFrame(data_dict)

        df_out.to_sql(name=f"tia_{self.migration_phase}_unit_processing", con=self.local, if_exists='append', index=False)
        df_out.to_sql(name=f"tia_{self.migration_phase}_unit_processing", con=self.stage, if_exists='append', index=False)
        print("$ Fuzzy Unit Processing: Complete $")

