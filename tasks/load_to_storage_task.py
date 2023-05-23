import os
import shutil
from datetime import date

from luigi import DateParameter, LocalTarget, Parameter, Task

from constants import (
    ARCHIEVE_DEFAULT_DATASET,
    STORAGE_PATH,
)
from tasks.process_archive_files_task import ProcessArchiveFilesTask


class LoadToStorageTask(Task):
    """This task should load pipeline results to storage"""

    dataset_id = Parameter(default=ARCHIEVE_DEFAULT_DATASET)
    date = DateParameter(default=date.today())

    def requires(self):
        yield ProcessArchiveFilesTask(dataset_id=self.dataset_id, date=self.date)

    def output(self):
        output_directory = os.path.join(
            STORAGE_PATH,
            self.dataset_id
        )
        return LocalTarget(output_directory, format=None)

    def run(self):
        # Get the path of the previous task's output
        previous_output_path = self.input()[0].path

        storage_directory = os.path.join(
            STORAGE_PATH,
            self.dataset_id
        )

        os.makedirs(storage_directory, exist_ok=True)

        # Copy the folder from the previous task's output to storage
        shutil.copytree(previous_output_path, storage_directory, dirs_exist_ok=True)
