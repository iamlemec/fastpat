## USPTO Patent Parser

Parse patent application, grant, assignment, and maintenance info from [USPTO Bulk Data](https://bulkdata.uspto.gov/). This handles all patent formats and outputs to pure CSV. Clusters patents by firm name, first filtering using locality-sensitive hashing, then finding components induced by a Levenshtein distance threshhold.

### Requirements

In general, you'll need the `fire` library. For parsing, you'll need: `numpy`, `pandas`, and `lxml`. For firm clustering, you'll additionally need: `xxhash`, `editdistance`, `networkx`, and `Cython`. All of these are available through both `pip` and `conda`.

### Usage

Most common tasks can be executed through the `patcmd` interface. For more advanced usage, you can also directly call the functions in the library itself. When using `patcmd` you have to specify the data directory. You can either do this by passing the `--datadir` flag directly or by setting the environment variable `PATENTS_DATADIR`.

#### Downloading Data

The following USPTO data sources are supported
- `grant`: patent grants
- `apply`: patent applications
- `assign`: patent resassignments
- `maint`: patent maintenance events
- `tmapply`: trademark applications

To download the files for data source `SOURCE`, run the command
``` bash
./patcmd fetch SOURCE
```

This library ships with a list of source files for each type, however this will become out of date over time. As such, you can also specify your own metadata path containing these files. You can do this by passing the `--metadir` flag directly or by setting the `PATENTS_METADIR` environment variable. If you've cloned this repository locally, you can also update the files in `patents/meta`.

#### Parsing Data

Parsing works similarly to fetching. Simply run
``` bash
./patcmd parse SOURCE
```
for one of the sources listed above.

#### Firm Clustering

This step is a bit more bespoke, and you may want to change things to suit your needs. But in general, there are four subcommands you can pass to `patcmd firms`: `assign` which eliminates duplicate or redundant patent transfers from the reassignment data, `cluster` which groups firm names into common entities using locality sensitive matching and Levenshtein distance, `cites` which aggregates citation data to the patent level, and `merge` which brings it all together into a firm-year panel. The simplest thing is to simply run these subcommands in order.

### Example

Suppose you just want to parse patent grants. To do this, you would go through the following steps:

0. Set up the environment with `export PATENTS_DATADIR=data`
1. Fetch the grant data with `./patcmd fetch grant`
2. Parse the grant data with `./patcmd parse grant`
4. Cluster firm names with `./patcmd firms cluster --sources grant`
5. Process citations with `./patcmd firms cites`

### Migration

If you've been using older versions of this repository, the new data layout is slightly different. To avoid having to re-download everything, you can move the contents of your `data` directly to `data/raw` and use `data` as the data directory path that you pass to `patcmd`. It's probably best to then re-parse everything and remove the `parsed` and `tables` directories.
