import logging
import random

from ..lattices import Square2DLattice
from ..metas import LatticeMeta

from . import Simulation


class SimplePercolationSimulation(Simulation):

    logger = logging.getLogger("simulations.simplepercolation")

    def _build_lattices(self):
        # TODO move to base
        lattices = []
        self.meta.lattice_names = [self._get_random_string(8)]  # TODO looks funny
        for lattice_name in self.meta.lattice_names:
            lattice_meta = LatticeMeta(
                name=lattice_name,
                size=self.meta.lattice_size,
                storage_path=self._get_lattice_storage_path()
                if self._should_save()
                else None,
            )
            lattices.append(Square2DLattice(lattice_meta))

        return lattices

    def get_random_state_node(self, i, j):

        """
            Given a node position, 
            return a choice of state.
        """

        # Here the choice is independent of the node position
        if random.random() < self.meta.occupation_probability:
            state = 1
        else:
            state = 0

        return state

    def get_lattice(self):
        return self.get_lattices()[0]

    def set_state_at_node(self, state, i, j):
        return self.get_lattice().set_state_at_node(state, i, j)

    def get_clusters(self):
        return self.get_lattice().get_clusters_with_state(1)

    def run(self):
        print("Inside: " + str(id(self)))
        self.logger.info(
            f"Running SimplePercolationSimulation with "
            f"lattice_size {self.meta.lattice_size} and "
            f"percolation_probability {self.meta.occupation_probability:.2f}"
        )
        for i in range(self.meta.lattice_size):
            for j in range(self.meta.lattice_size):
                state = self.get_random_state_node(i, j)
                self.set_state_at_node(state, i, j)

        return {"clusters": self.get_clusters()}

        # self.clusters = self.get_clusters()
        # self.save_clusters()

        # if self._should_save():
        #     self.get_lattice().save()
        #     self.save()
