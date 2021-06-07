##################################################################################
# connections.py : Extracts a domain's staging and production DB parameters ######
##################################################################################

# Importing the Modules:-
from sqlalchemy import create_engine
from decouple import config
import pandas as pd


class CreateConnection:
    def __init__(self, domain, local_DB):
        print("\n@@@ Inside Connections Class @@@")
        self.domain = domain
        self.local_DB = local_DB

        # Decode the Environment variables: DB Connection Strings:-
        ### Staging ###
        self.MYSQL_USER_TIA = config('MYSQL_USER_TIA')
        self.PASSWORD_STAGE = config('MYSQL_PASSWORD_TIA')
        self.HOST_STAGE = config('HOST_TIA')

        ### Sandbox/ Stage3 ###
        self.MYSQL_USER_TRACK_STAGE = config('MYSQL_USER_TRACK_STAGE')
        self.PASSWORD_SANDBOX = config('MYSQL_PASSWORD_TRACK_STAGE')
        self.HOST_SANDBOX = config('HOST_TRACK_STAGE')

        ### Prod ###
        self.MYSQL_USER_TRACK_PROD = config('MYSQL_USER_TRACK_PROD')
        self.PASSWORD_PROD = config('MYSQL_PASSWORD_TRACK_PROD')
        self.HOST_PROD = config('HOST_TRACK_PROD')

        ### LOCAL ###
        self.LOCAL_USER = config('LOCAL_USER')
        self.LOCAL_PASSWORD = config('LOCAL_PASSWORD')
        self.LOCAL_HOST = config('LOCAL_HOST')

        # DB Engine Parameters:-
        # Get DB Params Initialized
        app_core = create_engine(f"mysql+pymysql://{self.MYSQL_USER_TIA}:{self.PASSWORD_STAGE}@{self.HOST_STAGE}/app_core")
        self.uuid = pd.read_sql(f"SELECT c.tia_uuid FROM customer c WHERE c.domain = '{self.domain}';",
                                con=app_core);
        self.prod_db = pd.read_sql(f"SELECT c.db_name_prod FROM customer c WHERE c.domain = '{self.domain}';",
                                   con=app_core);
        print("App Core DB: Connection Established!")
        self.uuid = self.uuid['tia_uuid'][0]
        self.prod_db = self.prod_db['db_name_prod'][0]
        print('UUID:', self.uuid)
        print('Track DB:', self.prod_db)

    def local_engine_connection(self):
        print("Extracting Local Database Engine Parameter")
        local = create_engine(f"mysql+pymysql://{self.LOCAL_USER}:{self.LOCAL_PASSWORD}@{self.LOCAL_HOST}/{self.local_DB}")
        print("Local DB: Connection Established!")
        return local

    def stage_engine_connection(self):
        print("Extracting stage Database Engine Parameter")
        stage = create_engine(f"mysql+pymysql://{self.MYSQL_USER_TIA}:{self.PASSWORD_STAGE}@{self.HOST_STAGE}/{self.uuid}")
        print("Staging DB: Connection Established!")
        return stage

    def sandbox_engine_connection(self):
        print("Extracting sandbox Database Engine Parameter")
        sandbox = create_engine(
            f"mysql+pymysql://{self.MYSQL_USER_TRACK_STAGE}:{self.PASSWORD_SANDBOX}@{self.HOST_SANDBOX}/track_imports_sandbox")
        print("Sandbox DB: Connection Established!")
        return sandbox

    def prod_engine_connection(self):
        print("Extracting production Database Engine Parameter")
        prod = create_engine(
            f"mysql+pymysql://{self.MYSQL_USER_TRACK_PROD}:{self.PASSWORD_PROD}@{self.HOST_PROD}/{self.prod_db}")
        print("Production DB: Connection Established!")
        return prod
