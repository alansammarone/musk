import itertools
import multiprocessing
import os
import shutil
import json

from .. import Misc


class Experiment:

    META_FILENAME = "meta.json"
    _simulations = []

    def __init__(
        self, simulation_class, parameter_range, storage_folder, delete_artifacts=True,
    ):
        self._simulation_class = simulation_class
        self._parameter_range = parameter_range
        self._storage_folder = storage_folder
        self._delete_artifacts = delete_artifacts
        self._simulation_names = []

    def get_parameter_sets(self, parameter_range):
        parameter_names = [*parameter_range]  # Equivalent to .keys()
        number_of_parameters = len(parameter_names)
        for parameter_set in itertools.product(*parameter_range.values()):
            yield {
                parameter_names[index]: parameter_set[index]
                for index in range(number_of_parameters)
            }

    def _get_simulations(self):
        if not self._simulations:
            self._simulations = self._instantiate_simulations()
        return self._simulations

    def _meta_file_exists(self):
        return os.path.isfile(self._get_meta_filepath())

    def _load_meta_file(self):
        with open(self._get_meta_filepath()) as meta_file:
            return json.loads(meta_file.read())

    def _check_loaded_meta_is_valid(self, loaded_meta):
        pass  # CHECK PARAMS, STORAGE_CLASS

    def _build_simulation_names(self, parameter_sets, should_load_from_meta):

        if should_load_from_meta is True:
            # AND META IS VALID - MIGHT HAVE CHANGED PARAMS?:
            meta = self._load_meta_file()
            self._check_loaded_meta_is_valid(meta)
            names = meta["simulation_names"]

        else:
            number_of_names = len(parameter_sets)
            names = [
                Misc.get_random_string(8, only_lowercase=True)
                for _ in range(number_of_names)
            ]
        return names

    def _instantiate_simulations(self):

        parameter_sets = list(self.get_parameter_sets(self._parameter_range))
        names = self._build_simulation_names(parameter_sets, self._meta_file_exists())

        assert len(parameter_sets) == len(names)

        simulations = []
        for index in range(len(names)):
            simulations.append(
                self._simulation_class(
                    names[index], self._storage_folder, parameter_sets[index]
                )
            )

        return simulations

    def _aggregate_simulations_by_parameter_set(self, simulations):
        """
            Aggregate simulations by simulation parameter set.
        """
        groups = {}
        for simulation in simulations:
            # We use a frozenset so that we can it the parameters as keys. ????
            parameters = frozenset(simulation.get_parameters().items())
            if parameters in groups:
                groups[parameters].append(simulation)
            else:
                groups[parameters] = [simulation]
        return groups

    def _get_group_representations(self, aggregated_simulations):
        """
            Given a dictionary of aggregated (by parameter set) simulations, 
            generate a more useful representation of the group
            to be used by the analysis methods 
        """
        for parameter_set, simulation_group in aggregated_simulations.items():
            group_representation = []
            for simulation in simulation_group:
                simulation_representation = {
                    "observables": simulation.get_observables(),
                    "parameters": simulation.get_parameters(),
                }
                group_representation.append(simulation_representation)

            yield dict(parameter_set), group_representation

    def do_delete_artifacts(self):
        for simulation in self._get_simulations():
            simulation.delete_artifacts()

        if self._meta_file_exists():
            os.remove(self._get_meta_filepath())

    def run_analysis(self):
        simulations = self._get_simulations()
        agg_simulations = self._aggregate_simulations_by_parameter_set(simulations)
        groups = self._get_group_representations(agg_simulations)
        self.analyze(groups)
        if self._delete_artifacts:
            self.do_delete_artifacts()

    def _get_meta_filepath(self):
        return os.path.join(self._storage_folder, self.META_FILENAME)

    def _get_meta(self):
        return {
            "parameter_range": self._parameter_range,
            "simulation_names": [
                simulation.get_name() for simulation in self._get_simulations()
            ],
        }

    def _store_meta(self):
        storage_file_path = self._get_meta_filepath()
        meta = self._get_meta()
        with open(storage_file_path, "wb") as storage_file:
            storage_file.write(json.dumps(meta, indent=4).encode("utf-8"))

    def _run_simulations_async(self, n_workers):
        futures = []
        with multiprocessing.Pool(n_workers) as pool:
            number_of_simulations = 0
            for simulation in self._get_simulations():
                number_of_simulations += 1
                futures.append(pool.apply_async(simulation.run_async))

            for index, future in enumerate(futures):
                future.get()
                print(f"Done with {index}/{number_of_simulations}")

    def run_simulations(self, n_workers=None, force_recompute=False):

        if n_workers is None:
            n_workers = multiprocessing.cpu_count() - 1

        if force_recompute or not self._meta_file_exists():

            self._run_simulations_async(n_workers)
            if self._delete_artifacts is False:
                self._store_meta()
