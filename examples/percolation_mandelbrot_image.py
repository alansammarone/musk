import matplotlib.pyplot as plt
import time
from musk import Experiment, Simulation, Square2DLattice, Misc, LatticeStateImage
from collections import Counter
from random import random, choices


class MandelbrotPercolationImageSimulation(Simulation):
    def run(self, lattice_size=None, n_divisions=None, p=None):

        # P(dead) = P(state = 1) = p
        lattice = Square2DLattice(lattice_size)
        lattice.fill_randomly([0, 1], state_weights=[1 - p, p])

        while n_divisions:
            lattice.divide()
            alive_indexes = list(lattice.get_nodes_with_state(0))

            n_alive = len(alive_indexes)

            new_states = choices([0, 1], weights=[1 - p, p], k=n_alive)

            for index, new_state in zip(alive_indexes, new_states):
                if new_state == 1:
                    lattice.set_state_at_node(1, *index)
            n_divisions -= 1

        return {"state": lattice.get_state_as_matrix()}


class MandelbrotPercolationImageExperiment(Experiment):
    def analyze(self, groups):

        # size = groups[0][0]["state"]

        groups = list(groups)
        n_divisions = groups[0][0]["n_divisions"]
        original_size = groups[0][0]["lattice_size"]

        part_final_size = original_size * 2 ** n_divisions
        state_top_left = groups[0][1][0]["observables"]["state"]
        state_top_right = groups[0][1][1]["observables"]["state"]
        state_bottom_left = groups[0][1][2]["observables"]["state"]
        state_bottom_right = groups[0][1][3]["observables"]["state"]

        top = [
            state_top_left[index] + state_top_right[index]
            for index in range(part_final_size)
        ]

        bottom = [
            state_bottom_left[index] + state_bottom_right[index]
            for index in range(part_final_size)
        ]

        final = top + bottom

        lattice = Square2DLattice(part_final_size * 2)
        lattice.set_state_from_matrix(final)
        LatticeStateImage(
            lattice, image_width=part_final_size * 2, image_height=part_final_size * 2
        ).save("mandelbrotpercolation.png")


if __name__ == "__main__":
    parameter_range = {
        "p": [0.15],
        "lattice_size": [4] * 4,
        "n_divisions": [10],
    }
    experiment = MandelbrotPercolationImageExperiment(
        MandelbrotPercolationImageSimulation,
        parameter_range=parameter_range,
        storage_folder="data/mandelbrot",
        delete_artifacts=True,
    )
    t1 = time.time()
    experiment.run_simulations(n_workers=4)
    experiment.run_analysis()
    t2 = time.time()
    took = t2 - t1
    print(f"Took: {took} ")
