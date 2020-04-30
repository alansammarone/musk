import matplotlib.pyplot as plt

from musk import Experiment, Simulation, Linear1DLattice, Misc
from collections import Counter


class Percolation1DSimulation(Simulation):
    def run(self, occupation_probability=None, lattice_size=None):

        lattice = Linear1DLattice(lattice_size)
        lattice.fill_randomly(
            [0, 1], state_weights=[1 - occupation_probability, occupation_probability]
        )
        clusters = lattice.get_clusters_with_state(1)
        return {"clusters": clusters}


class Percolation1DExperiment(Experiment):
    def analyze(self, groups):
        for parameter_set, simulation_group in groups:
            cluster_size_ratios = []
            for simulation in simulation_group:
                clusters = simulation["observables"]["clusters"]

                lattice_size = simulation["parameters"]["lattice_size"]
                cluster_sizes = [len(cluster) for cluster in clusters]

                cluster_sizes_ratio = [
                    int(cluster_size) for cluster_size in cluster_sizes
                ]
                cluster_size_ratios.extend(cluster_sizes_ratio)

            counter = Counter(cluster_size_ratios)
            values = counter.keys()

            counts = list(counter.values())
            frequencies = [count for count in counts]
            p = parameter_set["occupation_probability"]
            ex1 = 3 * p * (1 - p) ** 2 + 2 * p ** 2 * (1 - p)
            ex2 = 2 * p ** 2 * (1 - p)
            ex3 = p ** 3
            total = ex1 + ex2 + ex3
            n_simulations = len(simulation_group)
            print(n_simulations)
            print(f"1: {counter[1]/n_simulations}  - {ex1}")
            print(f"2: {counter[2]/n_simulations}  - {ex2}")
            print(f"3: {counter[3]/n_simulations}  - {ex3}")
            print(total)

            plt.bar(values, frequencies, align="center")

        plt.savefig("bar1png.png")


if __name__ == "__main__":
    parameter_range = {
        # "occupation_probability":  linspace(0.01, 0.99, 99),
        "occupation_probability": [0.32] * 20000,
        "lattice_size": [3],
    }
    experiment = Percolation1DExperiment(
        Percolation1DSimulation,
        parameter_range=parameter_range,
        storage_folder="data/percolation1d",
        delete_artifacts=True,
    )
    experiment.run_simulations(force_recompute=1)
    experiment.run_analysis()
