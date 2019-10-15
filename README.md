Parse patent grant and assignment info from USPTO and match with Compustat data. This handles all patent grant formats (dat, pgb, ipgb) and uses `sqlite3` as the storage backend.

Additionally, cluster patents by firm name. Uses locality-sensitive hashing as a first pass then find components in the graph induced by a Levenshtein distance threshhold.

I also maintain a simplified repository with only the parsing code, which is kept roughly in sync with this, over at: [patents_simple](https://github.com/iamlemec/patents_simple). *Note that this can also parse Chinese patent data!*

You can also find some higher level analysis code, mostly using `pandas`, in the [patents_analyze](https://github.com/iamlemec/patents_analyze) repository.

## Usage

Below is the pipeline that you'll want to follow. There are many small design decisions I've made along the way, and you may want to tweak these to suit your own purposes.

* Acquiring data
    * `fetch_apply.py`: fetch patent application files
    * `fetch_grant.py`: fetch patent grant files
    * `fetch_assign.py`: fetch patent assignment files
    * batch unzip XML files: `ls *.zip | xargs -n 1 unzip`
* Parsing raw data files
    * `parse_apply.py`: parse patent applications
    * `parse_grant.py`: parse patent grants (including citations), all data formats
    * `parse_assign.py`: parse patent assignments
    * `parse_maint.py`: parse patent maintenance events
    * `parse_compu.py`: parse compustat data
* Name matching and aggregation
    * `process_assign.py`: flag assignments between the same entity
    * `firm_cluster.py`: match firms by name from all data sources
    * `process_cites.py`: resolve citations at firm level and find self-cites
    * `firm_merge.py`: merge all of above into firmyear panel

The parsed data is stored and manipulated with `sqlite3` in a single file. I usually put these in `store`. All of the parse and aggregate commands take a `--db` argument where you can specify the exact file name.

## Data sources

The fetch commands use the following layout:

* `data/grant`: patent grant data from USPTO
* `data/apply`: patent application data from USPTO
* `data/assign`: patent reassignment data from USPTO
* `data/maint`: patent maintentance data from USPTO
* `data/compustat`: Compustat data since 1950 from WRDS
