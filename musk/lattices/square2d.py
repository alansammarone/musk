from .base import Lattice
import functools

from numba import njit
from numba.typed import Dict
from numba import types
import numba


class Square2DLattice(Lattice):

    _neighbours = {}
    # _state = Dict.empty(key_type=types.unicode_type, value_type=types.boolean)

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

        try:
            return self._neighbours[(i, j)]
        except:

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

            self._neighbours[(i, j)] = neighbours
            return self._neighbours[(i, j)]

    # @functools.lru_cache(maxsize=None)
    def get_all_nodes(self):
        nodes = []
        for i in range(self.get_size()):
            for j in range(self.get_size()):
                yield ((i, j))
        #         nodes.append((i, j))
        # return nodes

    def set_state_from_matrix(self, matrix):

        for i, row in enumerate(matrix):
            for j, entry in enumerate(row):
                self.set_state_at_node(entry, i, j)

    # @functools.lru_cache(maxsize=None)
    def get_number_of_nodes(self):
        return self.get_size() ** 2

    def get_state_as_matrix(self):
        matrix = [
            [None for column in range(self.get_size())]
            for row in range(self.get_size())
        ]

        for (i, j) in self.get_all_nodes():
            matrix[i][j] = self.get_state_at_node(i, j)
        return matrix

    # @functools.lru_cache(maxsize=None)
    def get_boundaries(self):
        size = self.get_size()
        range_ = range(size)
        top_boundary = frozenset({(0, j) for j in range_})
        bottom_boundary = frozenset({(size - 1, j) for j in range_})
        return frozenset({top_boundary, bottom_boundary})

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


#     def get_cluster(self, start_node):
#         return get_cluster(self._state, start_node[0], start_node[1], self.get_size())


# # @njit(numba.boolean(numba.pyobject, numba.int8, numba.int8))
# @njit
# def get_state_at_node(state, i, j):
#     return state[get_node_key(i, j)]


# @njit
# def get_neighbour_nodes(i, j, size):
#     last_node_index = size - 1
#     neighbours = set()
#     # Clock-wise, starting from top
#     if i != 0:
#         neighbours.add((i - 1, j))
#     if j != last_node_index:
#         neighbours.add((i, j + 1))
#     if i != last_node_index:
#         neighbours.add((i + 1, j))
#     if j != 0:
#         neighbours.add((i, j - 1))

#     return neighbours


# @njit(locals={"i": numba.int8, "j": numba.int8})
# def get_cluster(state, i_, j_, size):
#     start_node = (i_, j_)
#     start_node_state = 1
#     visited, stack = set(), set([start_node])
#     while stack:
#         i, j = stack.pop()
#         # if (i, j) in visited:
#         #     continue
#         # visited.add((i, j))
#         # neighbors = get_neighbour_nodes(i, j, size)
#         # stack.update(
#         #     filter(
#         #         lambda neighbor: get_state_at_node(state, i, j) == start_node_state,
#         #         neighbors,
#         #     )
#         # )

#     return frozenset(visited)


# @functools.lru_cache(maxsize=None)
# def get_node_key(index) -> str:
#     return "_".join(map(str, index))
