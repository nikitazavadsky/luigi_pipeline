from datetime import date
from luigi import build, Task

from tasks.download_archive_task import DownloadArchieveTask
from tasks.process_archieve_files_task import ProcessArchieveFilesTask

from constants import ARCHIEVE_DEFAULT_DATASET



class Pipeline(Task):
    ''' Dummy task that triggers execution of a other tasks'''

    def requires(self):
        yield ProcessArchieveFilesTask(dataset_id=ARCHIEVE_DEFAULT_DATASET, date=date.today())