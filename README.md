Parse patent grant and assignment info from USPTO and match with Compustat data. This handles all patent grant formats (dat, pgb, ipgb) and uses `sqlite3` as the storage backend.

Additionally, cluster patents by firm name. Uses locality-sensitive hashing as a first pass then find components in the graph induced by a Levenshtein distance threshhold.

I also maintain a simplified repository with only the parsing code, which is kept roughly in sync with this, over at: [patents_simple](https://github.com/iamlemec/patents_simple). *Note that this can also parse Chinese patent data!*

You can also find some higher level analysis code, mostly using `pandas`, in the [patents_analyze](https://github.com/iamlemec/patents_analyze) repository.

## File descriptions

Below is the pipeline that you'll want to follow. There are many small design decisions I've made along the way, and you may want to tweak these to suit your own purposes.

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

## Database layout

The parsed data is stored and manipulated with `sqlite3` in a single file. I usually put these in `store`. All of the parse commands take a `--db` argument where you can specify the exact file name. The internal layout is:

* `patent`: (patnum int, filedate text, grantdate text, class text, ipc text, ipcver text, city text, state text, country text, owner text, claims int, title text, abstract text, gen int)
* `assign`: (assignid integer primary key, patnum int, execdate text, recdate text, conveyance text, assignor text, assignee text, assignee_state text, assignee_country text)
* `maint`: (patnum int, ever_large int, last_maint int)

## Data sources

The fetch commands use the following layout:

* `data/grant_files`: patent grant data from Google/USPTO
* `data/assign_files`: patent reassignment data from Google/USPTO
* `data/maint_files`: patent maintentance data from Google/USPTO
* `data/compustat_files`: Compustat data since 1950 from WRDS
