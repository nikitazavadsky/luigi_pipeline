## Task Definition

There is a [source](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi) with archives needed. Each dataset contains `.tar` archive with `g-zip` archieved files inside.

### Definition of done

1. Luigi pipeline for generic dataset processing is created
2. Dataset loader is generic (can accept any .txt.gz TARBALL dataset for download)
3. Folder structure for dataset files is created (DEBUG to check order of operations and final storage for pipeline output)

### Run guide (without pipenv)
* Launch command line
* Install packages: luigi, pandas, requests with command ```pip install luigi pandas requests```
* Go to tasks directory with command line ```cd tasks```
* Bash: ```PYTHONPATH='..' luigi --module tasks.load_to_storage_task LoadToStorageTask --local-scheduler```
* Possible arguments:
    * `--dataset-id` (string dataset id to extract. Default: GSE68849)
    * `--date` (string date in format YYYY-MM-DD. Default: today)

Example command: ```PYTHONPATH='..' luigi --module tasks.load_to_storage_task LoadToStorageTask --local-scheduler --dataset-id GSE68854``` 

#### Tested dataset IDs (taking into account only txt.gz TARBALL archives)

* GSE68849
* GSE68854
