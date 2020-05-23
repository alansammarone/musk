import matplotlib.pyplot as plt

from musk import Experiment, Simulation, Linear1DLattice, Misc
from collections import Counter
import seaborn

seaborn.set()
plt.rc("text", usetex=True)
plt.rc("font", family="serif")
plt.rc("xtick", labelsize="x-small")
plt.rc("ytick", labelsize="x-small")


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
            frequencies = [count / sum(counts) for count in counts]
            p = parameter_set["occupation_probability"]
            n_simulations = len(simulation_group)

            plt.figure(figsize=(16, 9), dpi=200)
            plt.scatter(values, frequencies)
        plt.title(f"1D percolation (N={lattice_size})")
        plt.ylabel("Frequency")
        plt.xlabel("Cluster size")
        plt.savefig("perc_2d_prob.png")


if __name__ == "__main__":
    parameter_range = {
        # "occupation_probability":  linspace(0.01, 0.99, 99),
        "occupation_probability": [0.9] * 1000,
        "lattice_size": [1024],
    }
    experiment = Percolation1DExperiment(
        Percolation1DSimulation,
        parameter_range=parameter_range,
        storage_folder="data/percolation1d",
        delete_artifacts=False,
    )
    experiment.run_simulations()
    experiment.run_analysis()
