from .base import Lattice
import functools


class Square2DLattice(Lattice):

    _neighbours = {}

    def __init__(self, size):
        self._size = size
        super().__init__(size)

    def get_all_nodes(self):
        nodes = []
        for i in range(self.get_size()):
            for j in range(self.get_size()):
                yield ((i, j))

    def set_state_from_matrix(self, matrix):

        for i, row in enumerate(matrix):
            for j, entry in enumerate(row):
                self.set_state_at_node(entry, i, j)

    def get_number_of_nodes(self) -> int:
        return self.get_size() ** 2

    def get_state_as_matrix(self):
        matrix = [
            [None for column in range(self.get_size())]
            for row in range(self.get_size())
        ]

        for (i, j) in self.get_all_nodes():
            matrix[i][j] = self.get_state_at_node(i, j)
        return matrix

    def get_boundaries(self):
        size = self.get_size()
        range_ = range(size)
        top_boundary = frozenset({(0, j) for j in range_})
        bottom_boundary = frozenset({(size - 1, j) for j in range_})
        return frozenset({top_boundary, bottom_boundary})

    def get_max_distance(self) -> float:
        return 2 ** 0.5 * self.get_size()

    def divide(self):
        """
            Update state so that any node with a given state
            becomes 4 nodes with the same state
        """
        state = {}

        for i in range(self.get_size()):
            for j in range(self.get_size()):
                previous_state = self.get_state_at_node(i, j)
                for index in [
                    (2 * i, 2 * j),
                    (2 * i, 2 * j + 1),
                    (2 * i + 1, 2 * j),
                    (2 * i + 1, 2 * j + 1),
                ]:
                    node_key = self._get_node_key(*index)
                    state[node_key] = previous_state
        self._state = state
        self._size *= 2

    def get_neighbour_nodes(self, i, j):
        raise NotImplementedError


class Square2DPeriodicLattice(Square2DLattice):
    def get_neighbour_nodes(self, i, j):

        last_node_index = self.get_size() - 1

        return set(  # Clock-wise, starting from top
            [
                ((i - 1) % last_node_index, j),
                (i, (j + 1) % last_node_index),
                ((i + 1) % last_node_index, j),
                (i, (j - 1) % last_node_index),
            ]
        )


class Square2DFiniteLattice(Square2DLattice):
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
