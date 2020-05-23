import json
import logging
import os
import shelve
import shutil
from collections import namedtuple

from .. import Misc

SimulationResult = namedtuple("SimulationResult", ["parameters", "observables"])


class Simulation:

    # No extension needed - shelve will add one (which one is not guaranteed)
    FILENAME_OBSERVABLES = "observables"

    def __init__(self, name, storage_folder, parameters):
        self._name = name
        self._storage_folder = storage_folder
        self._parameters = parameters

    def get_name(self):
        return self._name

    def get_parameters(self):
        return self._parameters

    def _get_storage_folder(self):
        return self._storage_folder

    def _get_instance_storage_folder(self):
        return os.path.join(self._get_storage_folder(), self.get_name())

    def get_observables_relative_filepath(self):
        return os.path.join(
            self._get_instance_storage_folder(), self.FILENAME_OBSERVABLES
        )

    def get_results(self):
        return SimulationResult(
            parameters=self.get_parameters(), observables=self.get_observables()
        )

    def store_observables(self, observables):

        relative_filepath = self.get_observables_relative_filepath()
        Misc.create_directory_if_not_exists(os.path.dirname(relative_filepath))
        with shelve.open(relative_filepath) as storage:
            for key, value in observables.items():
                storage[key] = value

    def load_observables(self):
        relative_filepath = self.get_observables_relative_filepath()
        with shelve.open(relative_filepath) as storage:
            observables = {key: value for key, value in storage.items()}
        return observables

    def _delete_storage_folder_recursively(self):
        shutil.rmtree(os.path.dirname(self.get_observables_relative_filepath()))

    def delete_artifacts(self):
        self._delete_storage_folder_recursively()

    def get_observables(self):
        return self.load_observables()

    def run_async(self):
        try:
            print(f"Starting {self.get_parameters()}")
            observables = self.run(**self.get_parameters())

            self.store_observables(observables)
        except:
            logging.exception("Error in worker:")
            raise
