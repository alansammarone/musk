from .base import Lattice


class Square2DLattice(Lattice):
    def __init__(self, size):
        self._size = size

    def get_neighbour_nodes(self, i, j):
        last_node_index = self.get_size() - 1
        neighbours = set()
        # Clock-wise, starting from top
        if i != 0:
            neighbours.add((i - 1, j))
        if j != last_node_index:
            neighbours.add((i, j + 1))
        if i != last_node_index:
            neighbours.add((i + 1, j))
        if j != 0:
            neighbours.add((i, j - 1))

        return neighbours

    def get_all_nodes(self):
        for i in range(self.get_size()):
            for j in range(self.get_size()):
                yield (i, j)

    def set_state_from_matrix(self, matrix):

        for i, row in enumerate(matrix):
            for j, entry in enumerate(row):
                self.set_state_at_node(entry, i, j)
