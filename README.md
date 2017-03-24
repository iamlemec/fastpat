Parse patent grant and assignment info from USPTO and match with Compustat data. This handles all patent grant formats (dat, pgb, ipgb) and uses `sqlite3` as the storage backend.

Additionally, cluster patents by firm name. Uses locality-sensitive hashing as a first pass then find components in the graph induced by a Levenshtein distance threshhold.

## File descriptions

* Acquiring data
    * `fetch_grants.py`: fetch patent grant files
    * `fetch_assign.py`: fetch patent assignment files
    * batch unzip XML files: `ls *.zip | xargs -n 1 unzip`
* Parsing raw data files
    * `parse_grants.py`: parse patent grants (including citations), all data formats
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

* `patent`: (patnum int, filedate text, grantdate text, class text, ipc text, ipcver text, city text, state text, country text, owner text, claims int, title text, abstract text, gen int)
* `assign`: (assignid integer primary key, patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text, assignee_state text, assignee_country text)
* `maint`: (patnum int, ever_large int, last_maint int)

## Data sources

* `grant_files`: patent grant data from Google/USPTO
* `assign_files`: patent reassignment data from Google/USPTO
* `maint_files`: patent maintentance data from Google/USPTO
* `compustat_files`: Compustat data since 1950 from WRDS

