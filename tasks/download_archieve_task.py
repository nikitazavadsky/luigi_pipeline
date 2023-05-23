from datetime import date

from luigi import build, DateParameter, LocalTarget, Task, Parameter

import logging

import requests
import os
import gzip
import tarfile

from constants import (
    ARCHIEVE_BASE_URL,
    ARCHIEVE_SEARCH_PARAMETER,
    ARCHIEVE_DEFAULT_DATASET,
    EXTRACT_OUTPUT_PATH,
)


class DownloadArchieveTask(Task):
    """This task should download dataset archieve with appropriate folder structure"""

    dataset_id = Parameter(default=ARCHIEVE_DEFAULT_DATASET)
    date = DateParameter(default=date.today())

    def output(self):
        return LocalTarget(
            os.path.join(
                EXTRACT_OUTPUT_PATH,
                str(self.date.year),
                str(self.date.month),
                str(self.date.day),
                self.dataset_id,
            )
        )

    def run(self):
        response = requests.get(
            url=f"{ARCHIEVE_BASE_URL}",
            params={ARCHIEVE_SEARCH_PARAMETER: self.dataset_id, "format": "file"},
        )

        # Create the directory for the dataset
        output_dir = os.path.join(
            EXTRACT_OUTPUT_PATH,
            str(self.date.year),
            str(self.date.month),
            str(self.date.day),
            self.dataset_id,
        )

        tar_path = os.path.join(output_dir, "tar")
        untar_path = os.path.join(output_dir, "untar")

        os.makedirs(tar_path, exist_ok=True)
        os.makedirs(untar_path, exist_ok=True)

        # Extract the archive to the dataset directory
        archive_path = os.path.join(tar_path, f"{self.dataset_id}.tar")
        with open(archive_path, "wb") as archive_file:
            archive_file.write(response.content)

        with tarfile.open(archive_path, "r") as tar:
            tar.extractall(path=untar_path)

        for root, _, files in os.walk(untar_path):
            for file in files:
                if file.endswith(".txt.gz"):
                    gz_file_path = os.path.join(root, file)
                    txt_file_path = os.path.join(
                        root, file[:-3]
                    )  # Remove ".gz" extension
                    with gzip.open(gz_file_path, "rb") as gz_file, open(
                        txt_file_path, "wb"
                    ) as txt_file:
                        txt_file.write(gz_file.read())
                    os.remove(gz_file_path)
