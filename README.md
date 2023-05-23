## Task Definition

There is a [source](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi) with archieves needed. Each dataset contains `.tar` archieve with `g-zip` archieved files inside.

### Definition of done

1. Luigi pipeline for generic dataset processing is created
2. Dataset loader is generic (can accept any dataset for download)
3. Folder structure for dataset files is created

### Run guide

* Bash: ```PYTHONPATH='..' luigi --module tasks.download_archieve_task DownloadArchieveTask --local-scheduler```