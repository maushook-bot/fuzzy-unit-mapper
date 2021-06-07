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
    def __init__(self, domain, GSHEET_ID, process_flow, migration_phase, stage, local, df_matcher, left, right):
        
        print("\n@@ Inside Gsheets API Class @@")
        self.domain = domain
        self.process_flow = process_flow
        self.migration_phase = migration_phase
        self.stage = stage
        self.local = local
        self.df_matcher = df_matcher
        self.left = left
        self.right = right

        self.left_str = ", ".join(self.left)
        self.right_str = ", ".join(self.right)

        # Service and creds Initialization:-
        self.SCOPES = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                       "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        self.CREDS = ServiceAccountCredentials.from_json_keyfile_name("UnitsDataDecoupler/creds.json", self.SCOPES)

        # Authorize the Service Creds and Get the Spreadsheet Metadata :-
        self.client = gspread.authorize(self.CREDS)
        self.GSHEET_META_ID = GSHEET_ID

    def unit_distinct_duplicator(self):
        if self.process_flow == 'write_to_gsheet':
            print("Duplicating the API Output data: distinct")
            sql = f"""
                    SELECT DISTINCT {self.left_str}, 
                    um.name_trk, um.short_name_trk, um.unit_code_trk, um.cabin_id
                    FROM tia_{self.migration_phase}_unit_processing um;
                """
            df = pd.read_sql(sql, con=self.local)
            df.to_sql(f"tia_{self.migration_phase}_distinct_unit_processing", con=self.local, index=False, if_exists="replace")
        else:
            print("Duplicating API Output data: Disabled")

    def write2gsheet(self):
        if self.process_flow == 'write_to_gsheet':
            print("$$$ Exec Process Flow: <1> Writing Contents -----> Gsheet $$$")
            print("$ 1. Moving Matched Units to Gsheet $")
            sql = f"""
                    SELECT DISTINCT
                    {self.left_str}, 
                    um.name_trk, um.short_name_trk, um.unit_code_trk, um.cabin_id
                    FROM tia_{self.migration_phase}_unit_processing um
                    WHERE um.unit_code_src = um.unit_code_trk
                    OR um.unit_name_src = um.name_trk
                    OR um.unit_code_src = um.name_trk
                    OR um.unit_name_src = um.unit_code_trk;
            """
            c_sql = f"""
                    SELECT COUNT(1) FROM tia_{self.migration_phase}_distinct_unit_processing um
                    WHERE um.unit_code_src = um.unit_code_trk
                    OR um.unit_name_src = um.name_trk
                    OR um.unit_code_src = um.name_trk
                    OR um.unit_name_src = um.unit_code_trk;
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
                    {self.left_str}, 
                    um.name_trk, um.short_name_trk, um.unit_code_trk, um.cabin_id
                    FROM tia_{self.migration_phase}_unit_processing um
                    WHERE um.unit_code_src <> um.unit_code_trk
                    and um.unit_name_src <> um.name_trk
                    and um.unit_code_src <> um.name_trk
                    and um.unit_name_src <> um.unit_code_trk
                    or um.unit_code_trk IS NULL;
            """
            c_sql = f"""
                    SELECT COUNT(1) FROM tia_{self.migration_phase}_distinct_unit_processing um
                    WHERE um.unit_code_src <> um.unit_code_trk
                    and um.unit_name_src <> um.name_trk
                    and um.unit_code_src <> um.name_trk
                    and um.unit_name_src <> um.unit_code_trk
                    or um.unit_code_trk IS NULL;
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
                    WHERE um.unit_code_src = um.unit_code_trk
                    OR um.unit_name_src = um.name_trk
                    OR um.unit_code_src = um.name_trk
                    OR um.unit_name_src = um.unit_code_trk;
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
                    WHERE um.unit_code_src <> um.unit_code_trk
                    and um.unit_name_src <> um.name_trk
                    and um.unit_code_src <> um.name_trk
                    and um.unit_name_src <> um.unit_code_trk
                    or um.unit_code_trk IS NULL;
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
            #df_master.to_sql(f"src_{self.migration_phase}_units_map", con=self.stage, index=False, if_exists="append")
            print("Data Loaded: Local/Stage")

        else:
            print("Process: Read from Gsheet #Disabled")

    def data_cleaner(self, stage, local):
        if self.process_flow == "read_gsheet":
            email_address = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.email_address = NULL
                WHERE cm.email_address = 'None';
                """
            email_address_2 = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.email_address_2 = NULL
                WHERE cm.email_address_2 = 'None';
                """
            home_phone = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.home_phone = NULL
                WHERE cm.home_phone = 'None';
                """
            work_phone = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.work_phone = NULL
                WHERE cm.work_phone = 'None';
                """
            cell_phone = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.cell_phone = NULL
                WHERE cm.cell_phone = 'None';
                """
            other_phone = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.other_phone = NULL
                WHERE cm.other_phone = 'None';
                """
            address1 = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.address1 = NULL
                WHERE cm.address1 = 'None';
                """
            address2 = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.address2 = NULL
                WHERE cm.address2 = 'None';
                """
            city = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.city = NULL
                WHERE cm.city = 'None';
                """
            state = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.state = NULL
                WHERE cm.state = 'None';
                """
            zip = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.zip = NULL
                WHERE cm.zip = 'None';
                """
            country = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.country = NULL
                WHERE cm.country = 'None';
                """
            first_name_trk = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.first_name_trk = NULL
                WHERE cm.first_name_trk = 'None';
                """
            last_name_trk = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.last_name_trk = NULL
                WHERE cm.last_name_trk = 'None';
                """
            email_address_trk = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.email_address_trk = NULL
                WHERE cm.email_address_trk = 'None';
                """
            email_address_2_trk = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.email_address_2_trk = NULL
                WHERE cm.email_address_2_trk = 'None';
                """
            home_phone_trk = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.home_phone_trk = NULL
                WHERE cm.home_phone_trk = 'None';
                """
            cell_phone_trk = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.cell_phone_trk = NULL
                WHERE cm.cell_phone_trk = 'None';
                """
            other_phone_trk = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.other_phone_trk = NULL
                WHERE cm.other_phone_trk = 'None';
                """
            no_identity = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.no_identity = NULL
                WHERE cm.no_identity = 'None';
                """
            customer_id = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.customer_id = NULL
                WHERE cm.customer_id = 'None';
                """
            api_response = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.api_response = NULL
                WHERE cm.api_response = 'None';
                """
            Remarks = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.Remarks = NULL
                WHERE cm.Remarks = 'None';
                """
            fax = \
                f"""
                UPDATE src_{self.migration_phase}_contacts_map cm
                SET cm.fax = NULL
                WHERE cm.fax = 'None';
                """
            stage.execute(email_address)
            stage.execute(email_address_2)
            stage.execute(home_phone)
            stage.execute(work_phone)
            stage.execute(cell_phone)
            stage.execute(other_phone)
            stage.execute(fax)
            stage.execute(address1)
            stage.execute(address2)
            stage.execute(city)
            stage.execute(state)
            stage.execute(zip)
            stage.execute(country)
            stage.execute(first_name_trk)
            stage.execute(last_name_trk)
            stage.execute(email_address_trk)
            stage.execute(email_address_2_trk)
            stage.execute(home_phone_trk)
            stage.execute(cell_phone_trk)
            stage.execute(other_phone_trk)
            stage.execute(no_identity)
            stage.execute(customer_id)
            stage.execute(api_response)
            stage.execute(Remarks)

            local.execute(email_address)
            local.execute(email_address_2)
            local.execute(home_phone)
            local.execute(work_phone)
            local.execute(cell_phone)
            local.execute(other_phone)
            local.execute(fax)
            local.execute(address1)
            local.execute(address2)
            local.execute(city)
            local.execute(state)
            local.execute(zip)
            local.execute(country)
            local.execute(first_name_trk)
            local.execute(last_name_trk)
            local.execute(email_address_trk)
            local.execute(email_address_2_trk)
            local.execute(home_phone_trk)
            local.execute(cell_phone_trk)
            local.execute(other_phone_trk)
            local.execute(no_identity)
            local.execute(customer_id)
            local.execute(api_response)
            local.execute(Remarks)
            print("Data Cleaning Complete: Stage/Local")
        else:
            print("Process: Data Cleaner fx #Disabled")

    def folio_resolver(self):
        if self.process_flow == "read_gsheet":
            data_dict = {'folio': pd.Series([], dtype='int'),
                         'cabin_id': pd.Series([], dtype='int')}
            df = pd.DataFrame(data_dict)
            df.to_sql(f"tia_{self.migration_phase}_unit_map", con=self.local, index=False, if_exists="replace")

            print("Units Map table: Initialized")
            print("Mapping Folio and cabin ID")
            sql = \
                f"""
                    SELECT 
                    um.folio, b.cabin_id
                    FROM tia_{self.migration_phase}_unit_processing um
                    JOIN src_{self.migration_phase}_units_map b
                    ON um.unit_code_src = b.unit_code_src
                    AND um.unit_name_src = b.unit_name_src   
            """
            df = pd.read_sql(sql, con=self.local)
            df.to_sql(f"tia_{self.migration_phase}_unit_map", con=self.local, index=False, if_exists="append")
            print("Unit Map table: Created")
        else:
            print("Process: Folio Resolver fx #Disabled")
