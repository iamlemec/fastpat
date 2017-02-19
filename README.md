Parse patent grant and assignment info from USPTO and match with Compustat data.

## File descriptions

* Acquiring data
    * `fetch_grants.py`: fetch patent grant files
    * `fetch_assign.py`: fetch patent assignment files
    * batch unzip XML files: `ls *.zip | xargs -n 1 unzip`
* Parsing raw data files
    * `parse_grants.py`: parse patent grants (including citations)
    * `parse_assign.py`: parse patent assignments
    * `parse_maint.py`: parse patent maintenance events
    * `parse_compustat.py`: parse compustat data
* Cleaning patent data
    * `process_assign.py`: flag assignments between the same entity
* Name matching and firm aggregation
    * `firm_cluster.py`: match firms by name from all data sources
    * `process_cites.py`: resolve citations at firm level and find self-cites
    * `firm_merge.py`: merge all of above into firmyear panel
* Analysis
    * `analyze_patent.py`: analyze just patent data
    * `analyze_transfer.py`: analyze transfer data with firm match
    * `analyze_within.py`: analyze firm data
* Extra utilities:
  * `parse_nber_grants.py`: parse nber grant info
  * `parse_nber_info.py`: parse ownership info from nber

## Database layout

* `patent`: (patnum int, filedate text, grantdate text, classone int, classtwo int, owner text)
* `assignment`: (patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text)
* `firmyear_info`: final product, merged panel data
* `owner_firm` (ownerid int, firm_num int) - owner corresponds to unique string, firms are collection of similarly named owners

## Data sources

* `assign_files`: patent reassignment data from Google/USPTO
* `grant_files`: patent grant data from Google/USPTO
* `compustat_files`: Compustat data since 1950 from WRDS

## TODO
