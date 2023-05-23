import io
import os
import shutil
from datetime import date

import pandas as pd
from luigi import DateParameter, LocalTarget, Parameter, Task

from constants import (
    ARCHIEVE_DEFAULT_DATASET,
    TRANSFORM_INPUT_PATH,
    TRANSFORM_OUTPUT_PATH,
)
from tasks.download_archive_task import DownloadArchiveTask


class ProcessArchiveFilesTask(Task):
    """This task should process archieve files. It takes txt files and decompose them to separate .tsv files by table heading"""

    dataset_id = Parameter(default=ARCHIEVE_DEFAULT_DATASET)
    date = DateParameter(default=date.today())

    def requires(self):
        yield DownloadArchiveTask(dataset_id=self.dataset_id, date=self.date)

    def output(self):
        output_directory = os.path.join(
            TRANSFORM_OUTPUT_PATH,
            str(self.date.year),
            str(self.date.month),
            str(self.date.day),
            self.dataset_id,
        )
        return LocalTarget(output_directory, format=None)

    def run(self):
        # Get the path of the previous task's output
        previous_output_path = self.input()[0].path

        input_directory = os.path.join(
            TRANSFORM_INPUT_PATH,
            str(self.date.year),
            str(self.date.month),
            str(self.date.day),
            self.dataset_id,
        )

        # Copy the folder from the previous task's output to the current task's input
        shutil.copytree(previous_output_path, input_directory, dirs_exist_ok=True)

        # Create the dataset_id directory in TRANSFORM_OUTPUT_PATH
        output_directory = os.path.join(
            TRANSFORM_OUTPUT_PATH,
            str(self.date.year),
            str(self.date.month),
            str(self.date.day),
            self.dataset_id,
        )
        os.makedirs(output_directory, exist_ok=True)

        dfs = {}
        for root, _, files in os.walk(input_directory):
            for file in files:
                file_path = os.path.join(root, file)
                dfs = self._process_file(file_path, dfs)
                file_directory_path = os.path.join(
                    output_directory, os.path.splitext(file)[0]
                )
                os.makedirs(file_directory_path, exist_ok=True)
                # Write separate files per header in the output directory
                for mode in ["Full", "Lopped"]:
                    file_directory_mode_path = os.path.join(file_directory_path, mode)
                    os.makedirs(file_directory_mode_path, exist_ok=True)
                    for header, df in dfs.items():
                        output_file = os.path.join(
                            file_directory_mode_path, f"{header}.tsv"
                        )
                        self._process_dataframe(df, mode, header, output_file)

    def _process_file(self, file_path, dfs):
        with open(file_path) as f:
            write_key = None
            fio = io.StringIO()
            for l in f.readlines():
                if l.startswith("["):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == "Heading" else "infer"
                        dfs[write_key] = pd.read_csv(fio, sep="\t", header=header)
                    fio = io.StringIO()
                    write_key = l.strip("[]\n")
                    continue
                if write_key:
                    fio.write(l)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep="\t")
        return dfs

    @staticmethod
    def _process_dataframe(df, mode, header, output_path):
        if mode == "Lopped" and header == "Probes":
            # Remove list of columns from DataFrame
            columns_to_remove = [
                "Definition",
                "Ontology_Component",
                "Ontology_Process",
                "Ontology_Function",
                "Synonyms",
                "Obsolete_Probe_Id",
                "Probe_Sequence",
            ]
            if all(col in df.columns for col in columns_to_remove):
                df_copy = df.drop(columns=columns_to_remove, inplace=False)
                df_copy.to_csv(output_path, sep="\t", index=False)
        else:
            df.to_csv(output_path, sep="\t", index=False)
