Parse patent grant and assignment info from USPTO and match with compustat data.

## File descriptions

* Analysis
  * analyze_patent.py: analyze just patent data
  * analyze_transfer.py: analyze transfer data with firm match
  * analyze_within.py: analyze firm data

* Name standardization
  * standardize.py: name standardization routines

* Parsing raw data files
  * parse_grants_all.py: parse all patent grant data (gen1,gen2,gen3) into patents.db
  * parse_grants_gen1.py: parse gen1 patent grant data into patents.db
  * parse_grants_gen2.py: parse gen2 patent grant data into patents.db
  * parse_grants_gen3.py: parse gen3 patent grant data into patents.db
  * parse_assign_all.py: parse all patent assignment data into patents.db
  * parse_assign_sax.py: parse sax patent assignment data into patents.db
  * parse_compustat.py: parse compustat data
  * parse_nber_grants.py: parse nber grant info
  * parse_nber_info.py: parse ownership info from nber
  * fix_patent_tables.py: fix various issues with patent data
  * find_assign_dups.py: flag assignments between the same entity

* Name matching and firm aggregation
  * patents_match_grants.py: match firms by name from patent grant data
  * patents_match_assign.py: same for assignment names
  * patents_match_compustat.py: same for compustat names
  * patents_match_merge.py: merge all of above into firmyear tables
  * patents_match_tokens.py: generate token frequency from grant names

* Pure compustat match
  * compustat_assign_match.py: match names from patent assignments to compustat names into transfers.db
  * compustat_grant_match.py: match names from patent grants to compustat names into transfers.db
  * compustat_merge.py: merge matched assignment data into compustat.db

## Workflow

* fetch patent grant files: grant_files/fetch_grants.py
* fix malformed XML in grant files: grant_files/fix_grants_gen2.py and grant_files/fix_grants_gen3.py
* fetch patent assignment files: assign_files/fetch_assignments.py
* patent grants parse: parse_grants_all.py
* patent assignments parse: parse_assign_all.py
* merge grants/assignments: fix_patent_tables.py
* flag redundant assignments: flag_assign_dups.py
* parse compustat: parse_compustat.py
* parse citation data: parse_cites_all.py
* parse maintenance data: parse_maint.py
* match grant data: patents_match_grants.py
* match assign data on top: patents_match_assign.py
* match compustat data on top: patents_match_compustat.py
* match citation data: patents_match_cites.py
* merge it all together: patents_match_merge.py

## Database layout

* patents.db:
  * patent: (patnum int, filedate text, grantdate text, classone int, classtwo int, owner text)
  * assignment: (patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text)

* compustat.db:
  * firmyear: (gvkey int, year int, income real, revenue real, rnd real) + (source_pnum int, dest_pnum int)
  * firmname: (gvkey int, name text)
  * firmkey: (gvkey int, idx int, keyword text, ntoks int)

* within.db:
  * grant_info
  * assignment_info
  * firmyear_info
  * firm

## Data sources

* assignment_files: patent reassignment data from Google/USPTO
* grant_files: patent grant data from Google/USPTO
* compustat_files: Compustat data since 1950 from WRDS
* other_files:
  * naics_co09.csv: concordance between US Patent Classes (USPC) and NAICS codes
  * sic_co08.csv: concordance between USPC and SIC codes

