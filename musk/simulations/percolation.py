class Percolation2DSimulation(Simulation):
    def run(self, p=None, lattice_size=None):

        lattice = Square2DLattice(lattice_size)
        lattice.fill_randomly([0, 1], state_weights=[1 - p, p])
        clusters = lattice.get_clusters_with_state(1)
        return dict(clusters=clusters)
