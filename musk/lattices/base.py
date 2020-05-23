import logging
import random

from ..exceptions import InvalidStateException


class Lattice:

    _size = None
    _state = None

    def get_size(self):
        return self._size

    def _get_node_key(self, *indexes):

        if len(indexes) == 0:
            raise ValueError("Empty indexes received")

        return "_".join(map(str, indexes))

    def get_nodes_with_state(self, state):
        for index in self.get_all_nodes():
            if self.get_state_at_node(*index) == state:
                yield index

    def get_state_at_node(self, *indexes):

        node_key = self._get_node_key(*indexes)

        try:
            return self._state[node_key]
        except KeyError:
            raise InvalidStateException(
                f"Node at position ({node_key}) is not initialized."
            )

    def set_state_at_node(self, state, *indexes):

        if not self._state:
            self._state = {}

        node_key = self._get_node_key(*indexes)
        self._state[node_key] = state

    def fill_randomly(self, state_choices, state_weights=None):

        states = random.choices(
            state_choices, weights=state_weights, k=self.get_number_of_nodes()
        )
        node_indexes = self.get_all_nodes()

        for node_index, state in zip(node_indexes, states):
            self.set_state_at_node(state, *node_index)

    def get_clusters_with_state(self, state):
        """
            Return all clusters present in the lattice
            whose state equals state.
        """

        clusters = set()
        for node in self.get_all_nodes():
            if self.get_state_at_node(*node) != state:
                continue

            cluster = []  # otherwise outer if breaks
            for cluster in clusters:
                if node in cluster:
                    break
            if node in cluster:
                continue

            clusters.add(self.get_cluster(node))

        return frozenset(clusters)

    def get_cluster(self, start_node):
        """ 
            Return the cluster start_node belongs to.
        """
        start_node_state = self.get_state_at_node(*start_node)
        visited, stack = set(), [start_node]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            neighbors = self.get_neighbour_nodes(*node)
            stack.extend(
                filter(
                    lambda neighbor: self.get_state_at_node(*neighbor)
                    == start_node_state,
                    neighbors,
                )
            )

        return frozenset(visited)
