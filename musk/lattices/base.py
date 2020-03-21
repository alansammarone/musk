import logging

from musk.core import Base
from musk.metas import LatticeMeta
from ..exceptions import InvalidStateException


class Lattice(Base):

    STATE_FILE_NAME = "state.json"

    size = None
    _state = None

    def __init__(self, meta, new=True):
        self.meta = meta
        self._new = new
        self.logger = logging.getLogger("lattice")

    def get_size(self):
        return self.meta.size

    def initialize_state(self):
        if self._is_new():
            state = self._build_state()  # TODO - funny name?
        else:
            state = self._load_state()
        return state

    def _build_state(self):
        size = self.get_size()
        state = {}
        for i in range(size):
            row = {}
            for j in range(size):
                row[j] = None
            state[i] = row
        return state

    def get_state(self):

        if not self._state:
            self._state = self.initialize_state()

        return self._state

    def get_state_at_node(self, i, j):
        state = self.get_state()[i][j]
        if state is None:
            raise InvalidStateException(
                f"Node at position ({i},{j}) is not initialized."
            )
        return state

    def set_state_from_matrix(self, matrix):

        for i, row in enumerate(matrix):
            for j, entry in enumerate(row):
                self.set_state_at_node(entry, i, j)

    def set_state_at_node(self, state, i, j):

        if not self._state:
            self._state = self.initialize_state()

        self._state[i][j] = state

    def _get_state_storage_path(self):
        return os.path.join(self._get_instance_storage_path(), Lattice.STATE_FILE_NAME,)

    def _save_state(self):
        state_storage_path = self._get_state_storage_path()
        self._create_directory_if_not_exists(os.path.dirname(state_storage_path))

        state_file_contents = json.dumps(self.get_state(), indent=4).encode("utf8")
        with open(state_storage_path, "wb") as state_file:
            state_file.write(state_file_contents)

    def _load_state(self):
        state_storage_path = self._get_state_storage_path()
        with open(state_storage_path, "rb") as state_file:
            state = json.loads(state_file.read())
        return state

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

    def save(self):
        super().save()
        self._save_state()

    @classmethod
    def load(cls, name, storage_path):
        meta_base_path = cls._build_instance_storage_path(storage_path, name)
        meta_file_path = cls._get_meta_file_path(meta_base_path)
        meta = LatticeMeta.load(meta_file_path)
        return cls(meta, new=False)
