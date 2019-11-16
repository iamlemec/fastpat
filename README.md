## USPTO Patent Parser

Parse patent application, grant, assignment, and maintenance info from [USPTO Bulk Data](https://bulkdata.uspto.gov/). This handles all patent formats and outputs to pure CSV. Clusters patents by firm name, first filtering using locality-sensitive hashing, then finding components induced by a Levenshtein distance threshhold.

### Requirements

For parsing, you'll need: `numpy`, `pandas`, and `lxml`. For firm clustering, you'll additionally need: `xxhash`, `editdistance`, `networkx`, and `Cython`. All of these are available through both `pip` and `conda`.

### Usage

Below is the pipeline that you'll want to follow. The entire process is designed to idempotent, meaning you can rerun any step to propagate updates.

#### Downloading Data

You can download and extract the raw XML data files using the `fetch_*.py` files. These look in the respective `meta/*_files.txt` for the list of files to download and store them in the corresponding `data/*`. The fetch scripts by default will not overwrite existing files and will only download those that are absent.

#### Parsing Data

Parsing is done using the `parse_*.py` scripts. In the case of `apply`, `grant`, `assign`, this will output to separate files in `parsed/*` so as to allow for multi-threaded parsing. Running `load_data.py` afterwards will combine these intermediate files into unified files in `tables`. Conversely, `parse_maint.py` and `parse_compustat.py` output directly to `tables`.

#### Firm Clustering

If you're parsing patent assignments, run `firm_assign.py` to flag assignments between the same entity. To cluster firms into common entities based on name similarity, run `firm_cluster.py`. You can use the `sources` option to control which names are included. Run `firm_cites.py` to aggregate citation information to the patent level and account for self citations.

Finally, run `firm_merge.py` to merge all of the above into a firm-year panel. This last step requires applications, grants, assignments, and maintenance events to be used (Compustat is optional).

### Example

Suppose you just want to parse patent grants. To do this, you would go through the following steps:

1. Fetch the grant data with `fetch_grant.py`.
2. Parse the grant data with `parse_grant.py`.
3. Load the grant data with `load_data.py grant`.
4. Cluster firm names with `firm_cluster.py grant`.
5. Process citations with `firm_cites.py`.
