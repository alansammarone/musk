import logging
import random
import functools

from ..exceptions import InvalidStateException


from typing import Dict, Tuple, Generator, List, FrozenSet, Set

from typing import Generator

Node1DIndex = int
NodeIndex = Tuple[Node1DIndex, ...]
State = int
Cluster = FrozenSet[NodeIndex]


class Lattice:

    _size: int
    _state: Dict

    def __init__(self, size):
        self._state = {}

    def get_all_nodes(self) -> List[NodeIndex]:
        raise NotImplemented

    def get_number_of_nodes(self) -> int:
        raise NotImplemented

    def get_neighbour_nodes(self, *node: Node1DIndex) -> Set[NodeIndex]:
        raise NotImplemented

    def get_size(self) -> int:
        return self._size

    def _get_node_key(self, *indexes) -> str:

        if len(indexes) == 0:
            raise ValueError("Empty indexes received")

        return "_".join(map(str, indexes))

    def get_nodes_with_state(self, state: State) -> Generator:
        for index in self.get_all_nodes():
            if self.get_state_at_node(*index) == state:
                yield index

    def get_state_at_node(self, *indexes: Node1DIndex) -> State:

        node_key = self._get_node_key(*indexes)

        try:
            return self._state[node_key]
        except KeyError:
            raise InvalidStateException(
                f"Node at position ({node_key}) is not initialized."
            )

    def set_state_at_node(self, state: State, *indexes: Node1DIndex):

        node_key = self._get_node_key(*indexes)
        self._state[node_key] = state

    def fill_randomly(self, state_choices: List[State], state_weights: list = []):

        states = random.choices(
            state_choices, weights=state_weights, k=self.get_number_of_nodes()
        )
        node_indexes = self.get_all_nodes()

        for node_index, state in zip(node_indexes, states):
            self.set_state_at_node(state, *node_index)

    def get_clusters_with_state(self, state: State) -> FrozenSet[Cluster]:
        """
            Return all clusters present in the lattice
            whose state equals state.
        """

        clusters: set = set()
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

    def get_cluster(self, start_node: NodeIndex) -> Cluster:
        """
            Return the cluster start_node belongs to.
        """
        start_node_state = self.get_state_at_node(*start_node)
        visited, stack = set(), set([start_node])
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            neighbors = self.get_neighbour_nodes(*node)
            stack.update(
                filter(
                    lambda neighbor: self.get_state_at_node(*neighbor)
                    == start_node_state,
                    neighbors,
                )
            )

        return frozenset(visited)

    # def __del__(self):
    #     self._state = None
    #     del self._state
