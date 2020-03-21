import itertools
import multiprocessing
import os
import shutil

from musk.core import Base
from musk.metas import ExperimentMeta


class Experiment(Base):
    _simulations = None

    @classmethod
    def load(cls, name, storage_folder):
        meta_base_path = cls._build_instance_storage_path(storage_folder, name)
        meta_file_path = cls._get_meta_file_path(meta_base_path)
        meta = ExperimentMeta.load(meta_file_path)
        return cls(meta, new=False)

    def __init__(self, meta, new=True):
        self.meta = meta
        self._new = new
        # if self._is_new():
        #     self.delete_experiment_folder()

    def _get_simulation_storage_path(self):
        return os.path.join(self._get_instance_storage_path(), Base.SIMULATIONS_FOLDER)

    def _initialize_simulations(self):
        if self._is_new():
            simulations = self._build_simulations()
        else:
            simulations = self._load_simulations()

        return simulations

    def _load_simulations(self):
        simulations = []
        simulation_storage_path = self._get_simulation_storage_path()
        for simulation_name in self.meta.simulation_names:
            simulation = SimplePercolationSimulation.load(
                simulation_name, simulation_storage_path
            )
            simulations.append(simulation)
        return simulations

    def delete_experiment_folder(self):

        experiment_folder = self._get_instance_storage_path()
        assert experiment_folder.endswith(self.meta.name)
        try:
            print(f"Deleting {experiment_folder} recursively in 5 seconds...")
            # shutil.rmtree(experiment_folder)
        except:
            print("Not deleting")
            pass

    def get_simulations_parameters(self):
        experiments_parameters = []
        parameter_combinations = itertools.product(
            *list(self.meta.parameter_range.values())
        )
        parameters_names = list(self.meta.parameter_range.keys())
        for combination in parameter_combinations:
            experiment_parameters = {
                parameters_names[index]: combination[index]
                for index in range(len(parameters_names))
            }
            experiments_parameters.append(experiment_parameters)
        return experiments_parameters

    def run(self, n_workers=multiprocessing.cpu_count() - 1):

        simulations = self.get_simulations()
        simulation_futures = []
        with multiprocessing.Pool(n_workers) as pool:
            for simulation in simulations:
                print(id(simulation))
                simulation_futures.append(pool.apply_async(simulation.run))

            for simulation_future in simulation_futures:
                simulation_future.get()

        if self._should_save():
            self.save()

    def get_simulations(self):
        if not self._simulations:
            self._simulations = self._initialize_simulations()

        return self._simulations

    def _get_simulation_name(self):
        return self._get_random_string(8)
