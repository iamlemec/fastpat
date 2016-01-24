Parse patent grant and assignment info from USPTO and match with Compustat data.

## File descriptions

* Analysis
  * analyze_patent.py: analyze just patent data
  * analyze_transfer.py: analyze transfer data with firm match
  * analyze_within.py: analyze firm data

* Parsing raw data files
  * parse_grants_all.py: parse all patent grant data (gen1,gen2,gen3) into patents.db
  * parse_grants_gen1.py: parse gen1 patent grant data into patents.db
  * parse_grants_gen2.py: parse gen2 patent grant data into patents.db
  * parse_grants_gen3.py: parse gen3 patent grant data into patents.db
  * parse_assign_all.py: parse all patent assignment data into patents.db
  * parse_assign_etree.py: parse sax patent assignment data into patents.db
  * parse_cites_all.py: parse all citations data (using NBER for pre-2006)
  * parse_cites_gen3.py: parse cites from grant data into patents.db
  * parse_maint.py: parse patent maintenance data into patents.db
  * parse_compustat.py: parse compustat data
  * parse_nber_grants.py: parse nber grant info
  * parse_nber_info.py: parse ownership info from nber

* Cleaning patent data
  * process_patents.py: fix various issues with patent data
  * process_assign.py: flag assignments between the same entity
  * process_cites.py: resolve citations at firm level and find self-cites

* Name matching and firm aggregation
  * firm_cluster.py: match firms by name from all data sources
  * firm_merge.py: merge all of above into firmyear panel

## Workflow

* fetch patent grant files: grant_files/fetch_grants.py
* fix malformed XML in grant files: grant_files/fix_grants_gen2.py and grant_files/fix_grants_gen3.py
* fetch patent assignment files: assign_files/fetch_assignments.py
* patent grants parse: parse_grants_all.py
* patent assignments parse: parse_assign_all.py
* parse compustat: parse_compustat.py
* parse citation data: parse_cites_all.py
* parse maintenance data: parse_maint.py
* merge grants/assignments: process_patents.py
* flag redundant assignments: process_assign.py
* generate firm clusters: firm_cluster.py
* aggregate citation data: process_cites.py
* merge it all together: firm_merge.py

## Database layout

* patents.db:
  * patent: (patnum int, filedate text, grantdate text, classone int, classtwo int, owner text)
  * assignment: (patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text)
  * firmyear_info - final product, merged panel data
  * owner_firm (ownerid int, firm_num int) - owner corresponds to unique string, firms are collection of similarly named owners

## Data sources

* assignment_files: patent reassignment data from Google/USPTO
* grant_files: patent grant data from Google/USPTO
* compustat_files: Compustat data since 1950 from WRDS
* other_files:
  * naics_co09.csv: concordance between US Patent Classes (USPC) and NAICS codes
  * sic_co08.csv: concordance between USPC and SIC codes

## TODO
