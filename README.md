## Fastpat

Fetch and parse patent application, grant, assignment, and maintenance info from [USPTO Bulk Data](https://bulkdata.uspto.gov/). This handles all patent formats and outputs to pure CSV. Clusters patents by firm name, first filtering using locality-sensitive hashing, then finding components induced by a Levenshtein distance threshhold.

### Requirements

In general, you'll need the `fire` library. For parsing, you'll need: `numpy`, `pandas`, and `lxml`. For firm clustering, you'll additionally need: `xxhash`, `editdistance`, `networkx`, and `Cython`. All of these are available through both `pip` and `conda`. You can install all the requirements with `pip` by running: `pip install -r requirements.txt`.

### Usage

Most common tasks can be executed through the `fastpat` command. For more advanced usage, you can also directly call the functions in the library itself. When using `fastpat` you have to specify the data directory. You can either do this by passing the `--datadir` flag directly or by setting the environment variable `FASTPAT_DATADIR`. If you've cloned the repository locally, you have to run `python3 -m fastpat` instead of `fastpat`.

#### Downloading Data

The following USPTO data sources are supported
- `grant`: patent grants
- `apply`: patent applications
- `assign`: patent resassignments
- `maint`: patent maintenance events
- `tmapply`: trademark applications (preliminary)

To download the files for data source `SOURCE`, run the command
``` bash
fastpat fetch SOURCE
```

This library ships with a list of source files for each type, however this will become out of date over time. As such, you can also specify your own metadata path containing these files. You can do this by passing the `--metadir` flag directly or by setting the `FASTPAT_METADIR` environment variable. If you've cloned this repository locally, you can also update the files in `fastpat/meta`.

#### Parsing Data

Parsing works similarly to fetching. Simply run
``` bash
fastpat parse SOURCE
```
for one of the sources listed above.

#### Firm Clustering

This step is a bit more bespoke, and you may want to change things to suit your needs. But in general, there are four subcommands you can pass to `fastpat firms`: `assign` which eliminates duplicate or redundant patent transfers from the reassignment data, `cluster` which groups firm names into common entities using locality sensitive matching and Levenshtein distance, `cites` which aggregates citation data to the patent level, and `merge` which brings it all together into a firm-year panel. The simplest thing is to simply run these subcommands in order.

### Example

Suppose you just want to parse patent grants. To do this, you would go through the following steps:

0. Set up the environment with `export FASTPAT_DATADIR=data`
1. Fetch the grant data with `fastpat fetch grant`
2. Parse the grant data with `fastpat parse grant`
4. Cluster firm names with `fastpat firms cluster --sources grant`
5. Process citations with `fastpat firms cites`

If you want to work with applications, grants, reassignment, and maintenance, you can run the following

0. Set up the environment with `export FASTPAT_DATADIR=data`
1. Fetch all the data with `fastpat fetch SOURCE` for each of `SOURCE` in `apply`, `grant`, `assign`, `maint` (four separate commands)
2. Parse all the data with `fastpat parse SOURCE` for each of `SOURCE` in `apply`, `grant`, `assign`, `maint` (four separate commands)
3. Prune the resassignment data with `fastpat firms assign`
4. Cluster firm names with `fastpat firms cluster --sources apply,grant,assign,maint`
5. Process citations with `fastpat firms cites`
6. Merge into firm-year panel with `fastpat firms merge`

### Data Updates

Continual data updating works very well for applications and grants. Only new files will be downloaded and unzipped. The way the patent office constructs the assignment data means that you'll have to delete it and re-download it roughly once a year. Similarly, maintenance information is stored in a single file, so to update that, you'll need to delete the data file `raw/maint/MaintFeeEvents.zip` and rerun the fetch command.

The parsing code will also only parse new files. If you wish to rerun the parsing step for a given file, either delete its outputs (in the `parsed` data directory) or pass the `--overwrite` flag (this works for the fetching step too). The clustering and merging steps must be run for any update to propagate the changes throughout. These will take about the same amount of time even for small updates, as they are undertaking global computations. Every command is idempotent, meaning it can be rerun without breaking anything.

#### Migration

If you've been using older versions of this repository, the new data layout is slightly different. To avoid having to re-download everything, you can move the contents of your `data` directly to `data/raw` and use `data` as the data directory path that you pass to `fastpat`. It's probably best to then re-parse everything and remove the `parsed` and `tables` directories.
