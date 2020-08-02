import random

from .base import Lattice


class Linear1DLattice(Lattice):
    def __init__(self, size):
        self._size = size
        super().__init__(size)

    def get_neighbour_nodes(self, index):
        last_node_index = self.get_size() - 1
        neighbours = set()

        if index != 0:
            neighbours.add((index - 1,))

        if index != last_node_index:
            neighbours.add((index + 1,))

        return neighbours

    def get_all_nodes(self):
        return [(index,) for index in range(self.get_size())]

    def set_state_from_list(self, list_):
        for index, entry in enumerate(list_):
            self.set_state_at_node(entry, index)

    def get_number_of_nodes(self):
        return self.get_size()

    def get_boundaries(self):
        size = self.get_size()

        left_boundary = frozenset({(0,)})
        right_boundary = frozenset({(size - 1,)})
        return frozenset({left_boundary, right_boundary})
