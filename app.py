###############################################################################
# @@@ FLASK FRAMEWORK : Fuzzy Units Auto Mapper @@@
# version: v2.0
# app.py
###############################################################################

# Importing the Libraries:-
from flask import Flask, jsonify, request
from flask_restful import Resource, Api
from Connections.connections import CreateConnection
from fuzzyUnitMapper.fuzzyUnitMapper import FuzzyAutoMapper
from UnitsDataDecoupler.UnitsDataDecoupler import CoupleDecouple
import json

# Initialize the Flask App:-
app = Flask(__name__)
app.secret_key = 'mash'
api = Api(app)


class FuzzyUnitMapper(Resource):
    def __init__(self):

        # APP Resource variable:-
        self.resource = \
            {
                "data": {
                    "domain": "",
                    "local_db_name": "",
                    "source_sql": "",
                    "left": "",
                    "right": "",
                    "migration_phase": ""
                },
                "url": "",
                "Status": "",
                "message": "",
                "ok": ""
            }
        print('Resource Initialized')

    def post(self):
        request_data = request.get_json()
        try:
            self.resource["data"]["domain"] = request_data["domain"]
            self.resource["data"]["local_db_name"] = request_data["local_db_name"]
            self.resource["data"]["source_sql"] = request_data["source_sql"]
            self.resource["data"]["left"] = request_data["left"]
            self.resource["data"]["right"] = request_data["right"]
            self.resource["data"]["migration_phase"] = request_data["migration_phase"]
            self.resource["Status"] = 200
            self.resource["ok"] = True
            self.resource["message"] = "Units API Mapper Resource Config Updated Succesfully!"
            self.resource["url"] = "http://127.0.0.1:7000/track/api/v1/units-api-mapper"

            # Update configuration --> settings.json file
            with open("settings.json", "w") as outfile:
                json.dump(self.resource["data"], outfile)
            return jsonify(self.resource)

        except KeyError:
            self.resource["Status"] = 422
            self.resource["ok"] = False
            self.resource["message"] = "Un-processable Entity: Check the config payload key data"
            self.resource["url"] = "http://127.0.0.1:7000/track/api/v1/units-api-mapper"
            return jsonify(self.resource)


    def get(self):

        # Read Configuration settings.json:-
        with open("settings.json") as json_file:
            config_data = json.load(json_file)

        # Initialize the Config Parameters:-
        domain = config_data['domain']
        local_db_name = config_data['local_db_name']
        scsql = config_data['source_sql']
        migration_phase = config_data['migration_phase']

        left = [config_data['left'].split(",") if type(config_data['left']) == str else config_data['left']][0]
        right = [config_data['right'].split(",") if type(config_data['right']) == str else config_data['right']][0]

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
        fm.fuzzyUnitProcessor(df_matcher)

        # Delete configuration --> settings.json file
        with open("settings.json", "w") as outfile:
            json.dump({"data": "Empty JSON"}, outfile)
        
        print("Configuration: Reset")
        return jsonify({"message": "Units API Mapper execution: Success",
                        "url": "http://127.0.0.1:7000/track/api/v1/units-api-mapper",
                        "Status": 200,
                        "ok": True})



class UnitDecoupler(Resource):
    
    def __init__(self):

        # APP Resource variable:-
        self.resource = \
            {
                "data": {
                    "domain": "",
                    "local_db_name": "",
                    "GSHEET_ID": "",
                    "migration_phase": "",
                    "process_flow": ""
                },
                "url": "",
                "Status": "",
                "message": "",
                "ok": ""
            }
        print('Resource Initialized')


    def post(self):
        
        request_data = request.get_json()
        try:
            self.resource["data"]["domain"] = request_data["domain"]
            self.resource["data"]["local_db_name"] = request_data["local_db_name"]
            self.resource["data"]["GSHEET_ID"] = request_data["GSHEET_ID"]
            self.resource["data"]["migration_phase"] = request_data["migration_phase"]
            self.resource["data"]["process_flow"] = request_data["process_flow"]
            self.resource["Status"] = 200
            self.resource["ok"] = True
            self.resource["message"] = "Units Decoupler API Resource Config Updated Succesfully!"
            self.resource["url"] = "http://127.0.0.1:7000/track/api/v1/units-decoupler"

            # Update configuration --> settings.json file
            with open("settings.json", "w") as outfile:
                json.dump(self.resource["data"], outfile)
            return jsonify(self.resource)

        except KeyError:
            self.resource["Status"] = 422
            self.resource["ok"] = False
            self.resource["message"] = "Un-processable Entity: Check the config payload key data"
            self.resource["url"] = "http://127.0.0.1:7000/track/api/v1/units-decoupler"
            return jsonify(self.resource)

    def get(self):
        
        # Read Configuration settings.json:-
        with open("settings.json") as json_file:
            config_data = json.load(json_file)

        # Input Config Parameters:-
        domain = config_data['domain']
        local_DB = config_data['local_db_name']
        GSHEET_ID = config_data['GSHEET_ID']
        process_flow = config_data['process_flow']
        migration_phase = config_data['migration_phase']

        # Main loop:-
        print('Start of Main Loop')
        con = CreateConnection(domain, local_DB)
        stage = con.stage_engine_connection()
        local = con.local_engine_connection()

        # Instantiate the Gsheet Writer Class Object:-
        cd = CoupleDecouple(domain, GSHEET_ID, process_flow, migration_phase, stage, local)
        cd.unit_distinct_duplicator()
        cd.write2gsheet()
        cd.readgsheet()
        cd.data_cleaner()
        cd.folio_resolver()
        print('End of Main Loop')

        # Delete configuration --> settings.json file
        with open("settings.json", "w") as outfile:
            json.dump({"data": "Empty JSON"}, outfile)

        print("Configuration: Reset")
        return jsonify({"message": "Units API Couple-Decoupler execution: Success",
                        "url": "http://127.0.0.1:7000/track/api/v1/units-decoupler",
                        "Status": 200,
                        "ok": True})


# Main Loop:-
if __name__ == '__main__':

    # Add resource endpoints to the class:-
    api.add_resource(FuzzyUnitMapper, '/track/api/v1/units-api-mapper')
    api.add_resource(UnitDecoupler, '/track/api/v1/units-decoupler')

    # Run the Flask App in the designated Port:-
    app.run(port=7000)

    with open("settings.json", "w") as outfile:
        json.dump({"data": "Empty JSON"}, outfile)

    print("Configuration: Reset")
    print("End of App")
