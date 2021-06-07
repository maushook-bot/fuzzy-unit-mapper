# Notes

## Fuzzy Auto Units Mapper
Business Requirements:-

During the Data Migration process, EDI needs to prepare the Pre-migration tables required for Data Migration. The Fuzzy Units Mapper maps the source units related information 
with the units information at the track side. 

## Introduction:-

The Reservation and folios table at the TRACK side requires the reservation_id to be mapped to the cabin_id. This is the end goal of unit mapping.
Fuzzy logic is employed which levenshtein distance measure to calculate the closeness of distance between two matching strings/units.
SQL Matching is used on top of Fuzzy Matched records to ensure the correctness of mapping.

## Configuration Params:-

#### Connection Parameters:-

domain = "villagerealty"

local_db_name = "villagerealty"

scsql = 

        SELECT f.`Folio Number` as folio, f.`Unit Name` AS unit_code_src, f.`Unit Address` AS unit_name_src 
        FROM src_initial_v12_folio_audit_report f 
        JOIN src_initial_v12_reservations_made_report r ON f.`Folio Number` = r.Folio;

left = "unit_name_src", "unit_code_src"

right = "name_trk", "unit_code_trk"


#### Read/Write contents <----> Gsheet:-

GSHEET_ID = "1AKQcUWS_mROr2v2SJpMT3da6BzBrO6DfIKjSYZucNcc"

process_flow = "read_gsheet"

migration_phase = "initial"