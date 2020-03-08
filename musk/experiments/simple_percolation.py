import os

from .base import Experiment
from ..metas import SimplePercolationSimulationMeta
from ..simulations import SimplePercolationSimulation


class SimplePercolationExperiment(Experiment):
    def _build_simulations(self):
        simulations = []
        for simulation_parameters in self.get_simulations_parameters():
            simulation_name = self._get_random_string(8)
            self.meta.simulation_names.append(simulation_name)

            if self._should_save():
                simulation_storage_path = self._get_simulation_storage_path()
            else:
                simulation_storage_path = None

            meta = SimplePercolationSimulationMeta(
                simulation_name,
                **simulation_parameters,
                storage_path=simulation_storage_path,
            )
            simulation = SimplePercolationSimulation(meta)

            simulations.append(simulation)

        return simulations
