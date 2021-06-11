'''
df to Gsheet API Class
@@ 1. Units API dataframe ---> Client's Gsheet @@
@@ 2. Distinct Units -----> Folio
'''

# Import the Libraries:-
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from df2gspread import df2gspread as d2g
from df2gspread import gspread2df as g2d


class CoupleDecouple:
    def __init__(self, domain, GSHEET_ID, process_flow, migration_phase, stage, local):
        
        print("\n@@ Inside Units-Decoupler API Class @@")
        self.domain = domain
        self.process_flow = process_flow
        self.migration_phase = migration_phase
        self.stage = stage
        self.local = local
        self.left = ""
        self.right = ""

        self.left_str = ", ".join(self.left)
        self.right_str = ", ".join(self.right)

        # Service and creds Initialization:-
        self.SCOPES = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                       "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        self.CREDS = ServiceAccountCredentials.from_json_keyfile_name("UnitsDataDecoupler/creds.json", self.SCOPES)

        # Authorize the Service Creds and Get the Spreadsheet Metadata :-
        self.client = gspread.authorize(self.CREDS)
        self.GSHEET_META_ID = GSHEET_ID

        # Intialize the data dict:-
        self.data_dict = {'unit_stats_id': pd.Series([], dtype='int'),'folio': pd.Series([], dtype='str'), 
                     'unit_name_src': pd.Series([], dtype='str'), 'unit_code_src': pd.Series([], dtype='str'), 
                     'unit_name_trk': pd.Series([], dtype='str'), 'unit_code_trk': pd.Series([], dtype='str'), 
                     'short_name_trk': pd.Series([], dtype='str'), 'cabin_id': pd.Series([], dtype='int')}

    def unit_distinct_duplicator(self):
        
        if self.process_flow == 'write_to_gsheet':
            print("Duplicating the API Output data: distinct")
            sql = f"""
                    SELECT DISTINCT um.unit_name_src, um.unit_code_src, 
                    um.unit_name_trk, um.short_name_trk, um.unit_code_trk, um.cabin_id
                    FROM tia_{self.migration_phase}_unit_processing um;
                """
            df = pd.read_sql(sql, con=self.local)
            df.to_sql(f"tia_{self.migration_phase}_distinct_unit_processing", con=self.local, index=False, if_exists="replace")
            df.to_sql(f"tia_{self.migration_phase}_distinct_unit_processing", con=self.stage, index=False, if_exists="replace")

            c_sql = f""" 
                    SELECT COUNT(1) FROM tia_{self.migration_phase}_distinct_unit_processing;
            """ 
            df_count = pd.read_sql(c_sql, con=self.local)
            print("Count: ", df_count['COUNT(1)'][0])

        else:
            print("Duplicating API Output data: Disabled")

    def write2gsheet(self):
        if self.process_flow == 'write_to_gsheet':
            print("$$$ Exec Process Flow: <1> Writing Contents -----> Gsheet $$$")
            print("$ 1. Moving Matched Units to Gsheet $")
            sql = f"""
                    SELECT DISTINCT
                    um.unit_name_src, um.unit_code_src, 
                    um.unit_name_trk, um.short_name_trk, um.unit_code_trk, um.cabin_id
                    FROM tia_{self.migration_phase}_unit_processing um
                    WHERE coalesce(um.unit_code_src, 0) = coalesce(um.unit_code_trk, 0)
                    OR coalesce(um.unit_name_src, 0) = coalesce(um.unit_name_trk, 0)
                    OR coalesce(um.unit_code_src, 0) = coalesce(um.unit_name_trk, 0)
                    OR coalesce(um.unit_name_src, 0) = coalesce(um.unit_code_trk, 0)
                    OR coalesce(um.unit_code_src, 0) = coalesce(um.short_name_trk, 0)
                    OR coalesce(um.unit_name_src, 0) = coalesce(um.short_name_trk, 0);
            """
            c_sql = f"""
                    SELECT COUNT(1) FROM tia_{self.migration_phase}_distinct_unit_processing um
                    WHERE coalesce(um.unit_code_src, 0) = coalesce(um.unit_code_trk, 0)
                    OR coalesce(um.unit_name_src, 0) = coalesce(um.unit_name_trk, 0)
                    OR coalesce(um.unit_code_src, 0) = coalesce(um.unit_name_trk, 0)
                    OR coalesce(um.unit_name_src, 0) = coalesce(um.unit_code_trk, 0)
                    OR coalesce(um.unit_code_src, 0) = coalesce(um.short_name_trk, 0)
                    OR coalesce(um.unit_name_src, 0) = coalesce(um.short_name_trk, 0);
            """ 

            df_out = pd.read_sql(sql, con=self.local)
            df_count = pd.read_sql(c_sql, con=self.local)
            print("Count: ", df_count['COUNT(1)'][0])
            try:
                d2g.upload(df_out, self.GSHEET_META_ID, wks_name=f"{df_count['COUNT(1)'][0]} Matched Units",
                           credentials=self.CREDS, row_names=True)
            except ValueError:
                print("No Matched Units in table: Skip")

            print("$ -1. Moving Missing Units to Gsheet $")
            sql = f"""
                    SELECT DISTINCT
                    um.unit_name_src, um.unit_code_src,
                    um.unit_name_trk, um.short_name_trk, um.unit_code_trk, um.cabin_id
                    FROM tia_{self.migration_phase}_unit_processing um
                    WHERE (coalesce(um.unit_code_src, 0) <> coalesce(um.unit_code_trk, 0)
                    and coalesce(um.unit_name_src, 0) <> coalesce(um.unit_name_trk, 0)
                    and coalesce(um.unit_code_src, 0) <> coalesce(um.unit_name_trk, 0)
                    and coalesce(um.unit_name_src, 0) <> coalesce(um.unit_code_trk, 0)
                    and coalesce(um.unit_code_src, 0) <> coalesce(um.short_name_trk, 0)
                    and coalesce(um.unit_name_src, 0) <> coalesce(um.short_name_trk, 0)
                    )
                    or (um.cabin_id IS NULL);
            """
            c_sql = f"""
                    SELECT COUNT(1) FROM tia_{self.migration_phase}_distinct_unit_processing um
                    WHERE (coalesce(um.unit_code_src, 0) <> coalesce(um.unit_code_trk, 0)
                    and coalesce(um.unit_name_src, 0) <> coalesce(um.unit_name_trk, 0)
                    and coalesce(um.unit_code_src, 0) <> coalesce(um.unit_name_trk, 0)
                    and coalesce(um.unit_name_src, 0) <> coalesce(um.unit_code_trk, 0)
                    and coalesce(um.unit_code_src, 0) <> coalesce(um.short_name_trk, 0)
                    and coalesce(um.unit_name_src, 0) <> coalesce(um.short_name_trk, 0)
                    )
                    or (um.cabin_id IS NULL);
            """

            df_out = pd.read_sql(sql, con=self.local)
            df_count = pd.read_sql(c_sql, con=self.local)
            print("Count: ", df_count['COUNT(1)'][0])
            try:
                d2g.upload(df_out, self.GSHEET_META_ID, wks_name=f"{df_count['COUNT(1)'][0]} Missing Units",
                           credentials=self.CREDS, row_names=True)
            except ValueError:
                print("No Missing Units in table: Skip")


    def readgsheet(self):
        
        if self.process_flow == "read_gsheet":
            
            print("$$$ Exec Process Flow: <2> Reading Contents from Gsheet $$$")
            print("$ 1. Moving Matched Units <-- Gsheet $")
            c_sql = f"""
                    SELECT COUNT(1) FROM tia_{self.migration_phase}_distinct_unit_processing um
                    WHERE coalesce(um.unit_code_src, 0) = coalesce(um.unit_code_trk, 0)
                    OR coalesce(um.unit_name_src, 0) = coalesce(um.unit_name_trk, 0)
                    OR coalesce(um.unit_code_src, 0) = coalesce(um.unit_name_trk, 0)
                    OR coalesce(um.unit_name_src, 0) = coalesce(um.unit_code_trk, 0)
                    OR coalesce(um.unit_code_src, 0) = coalesce(um.short_name_trk, 0)
                    OR coalesce(um.unit_name_src, 0) = coalesce(um.short_name_trk, 0);
            """ 

            df_count = pd.read_sql(c_sql, con=self.local)
            try:
                df_1 = g2d.download(self.GSHEET_META_ID, wks_name=f"{df_count['COUNT(1)'][0]} Matched Units",
                                    credentials=self.CREDS, row_names=True, col_names=True)
            except ValueError:
                df_1 = pd.DataFrame(self.data_dict)
                print("VALUE ERROR: No Matched Units in table: Skip")
            except RuntimeError:
                df_1 = pd.DataFrame(self.data_dict)
                print("RUNTIME ERROR: <NON EXISTENT TABLE> No Matched Units in table: Skip")

            print("$ -1. Moving Missing Units <-- Gsheet $")
            c_sql = f"""
                    SELECT COUNT(1) FROM tia_{self.migration_phase}_distinct_unit_processing um
                    WHERE (coalesce(um.unit_code_src, 0) <> coalesce(um.unit_code_trk, 0)
                    and coalesce(um.unit_name_src, 0) <> coalesce(um.unit_name_trk, 0)
                    and coalesce(um.unit_code_src, 0) <> coalesce(um.unit_name_trk, 0)
                    and coalesce(um.unit_name_src, 0) <> coalesce(um.unit_code_trk, 0)
                    and coalesce(um.unit_code_src, 0) <> coalesce(um.short_name_trk, 0)
                    and coalesce(um.unit_name_src, 0) <> coalesce(um.short_name_trk, 0)
                    )
                    or (um.cabin_id IS NULL);
            """

            df_count = pd.read_sql(c_sql, con=self.local)

            try:
                df_minus_1 = g2d.download(self.GSHEET_META_ID, wks_name=f"{df_count['COUNT(1)'][0]} Missing Units",
                                    credentials=self.CREDS, row_names=True, col_names=True)
            except ValueError:
                df_minus_1 = pd.DataFrame(self.data_dict)
                print("VALUE ERROR: No Missing Units in table: Skip")
            except RuntimeError:
                df_minus_1 = pd.DataFrame(self.data_dict)
                print("RUNTIME ERROR: <NON EXISTENT TABLE> No Missing Units in table: Skip")

            
            print("## Merging the Data frames ---> Master DF")
            df_master = pd.concat([df_1, df_minus_1])
            df_master.to_sql(f"src_{self.migration_phase}_units_map", con=self.local, index=False, if_exists="replace")
            df_master.to_sql(f"src_{self.migration_phase}_units_map", con=self.stage, index=False, if_exists="replace")
            print("Data Loaded: Local/Stage")

        else:
            print("Process: Read from Gsheet #Disabled")

    def data_cleaner(self):

        if self.process_flow == "read_gsheet":
            unit_name_src = \
                f"""
                UPDATE src_{self.migration_phase}_units_map um
                SET um.unit_name_src = NULL
                WHERE (um.unit_name_src = 'None' or um.unit_name_src = '') ;
                """
            unit_code_src = \
                f"""
                UPDATE src_{self.migration_phase}_units_map um
                SET um.unit_code_src = NULL
                WHERE (um.unit_code_src = 'None' or um.unit_code_src = '');
                """
            unit_name_trk = \
                f"""
                UPDATE src_{self.migration_phase}_units_map um
                SET um.unit_name_trk = NULL
                WHERE (um.unit_name_trk = 'None' or um.unit_name_trk = '');
                """
            unit_code_trk = \
                f"""
                UPDATE src_{self.migration_phase}_units_map um
                SET um.unit_code_trk = NULL
                WHERE (um.unit_code_trk = 'None' or um.unit_code_trk = '');
                """
            short_name_trk = \
                f"""
                UPDATE src_{self.migration_phase}_units_map um
                SET um.short_name_trk = NULL
                WHERE (um.short_name_trk = 'None' OR um.short_name_trk = '');
                """
            cabin_id = \
                f"""
                UPDATE src_{self.migration_phase}_units_map um
                SET um.cabin_id = NULL
                WHERE (um.cabin_id = 'None' OR um.cabin_id = 'nan' OR um.cabin_id = '');
                """
           
            self.local.execute(unit_name_src)
            self.local.execute(unit_code_src)
            self.local.execute(unit_name_trk)
            self.local.execute(unit_code_trk)
            self.local.execute(short_name_trk)
            self.local.execute(cabin_id)

            self.stage.execute(unit_name_src)
            self.stage.execute(unit_code_src)
            self.stage.execute(unit_name_trk)
            self.stage.execute(unit_code_trk)
            self.stage.execute(short_name_trk)
            self.stage.execute(cabin_id)
            
            print("Data Cleaning Complete: Stage/Local")
        else:
            print("Process: Data Cleaner fx #Disabled")

    def folio_resolver(self):
        if self.process_flow == "read_gsheet":
            data_dict = {'folio': pd.Series([], dtype='str'),
                         'cabin_id': pd.Series([], dtype='int')}
            df = pd.DataFrame(data_dict)
            df.to_sql(f"tia_{self.migration_phase}_unit_map", con=self.local, index=False, if_exists="replace")
            df.to_sql(f"tia_{self.migration_phase}_unit_map", con=self.stage, index=False, if_exists="replace")

            print("Units Map table: Initialized")
            print("Mapping Folio and cabin ID")
            sql = \
                f"""
                    SELECT 
                    um.folio, b.cabin_id
                    FROM tia_{self.migration_phase}_unit_processing um
                    JOIN src_{self.migration_phase}_units_map b
                    ON coalesce(um.unit_code_src, 0) = coalesce(b.unit_code_src, 0)
                    AND coalesce(um.unit_name_src, 0) = coalesce(b.unit_name_src, 0)   
            """
            df = pd.read_sql(sql, con=self.local)
            df.to_sql(f"tia_{self.migration_phase}_unit_map", con=self.local, index=False, if_exists="append")
            df.to_sql(f"tia_{self.migration_phase}_unit_map", con=self.stage, index=False, if_exists="append")
            print("Unit Map table: Created\n")
        else:
            print("Process: Folio Resolver fx #Disabled\n")
