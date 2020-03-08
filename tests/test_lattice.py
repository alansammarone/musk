import unittest
from musk.lattices import Square2DLattice
from musk.metas import LatticeMeta


class TestSquare2DLattice(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.meta = LatticeMeta(
            name="test", size=None  # This will be set differently for each test
        )

    def test_setters_and_getters_work(cls):
        pass

    def test_clusters_are_computed_correctly(self):

        matrix_lattice_state = [
            [1, 0, 0],
            [0, 0, 1],
            [1, 0, 1],
        ]
        self.meta.size = 3
        lattice = Square2DLattice(self.meta)
        lattice.set_state_from_matrix(matrix_lattice_state)

        expected_cluster = {(0, 0)}
        actual_cluster = lattice.get_cluster((0, 0))
        self.assertEqual(expected_cluster, actual_cluster)

        expected_cluster = {(2, 0)}
        actual_cluster = lattice.get_cluster((2, 0))
        self.assertEqual(expected_cluster, actual_cluster)

        expected_cluster = {(1, 2), (2, 2)}
        for node in [(1, 2), (2, 2)]:
            actual_cluster = lattice.get_cluster(node)
            self.assertEqual(expected_cluster, actual_cluster)

        expected_cluster = {(0, 1), (0, 2), (1, 0), (1, 1), (2, 1)}
        for node in [(0, 1), (0, 2), (1, 0), (1, 1), (2, 1)]:
            actual_cluster = lattice.get_cluster(node)
            self.assertEqual(expected_cluster, actual_cluster)

    def test_neighbours_are_computed_correctly(self):
        self.meta.size = 3
        lattice = Square2DLattice(self.meta)

        # 4 Edges
        expected = {(0, 0), (1, 1), (0, 2)}
        actual = lattice.get_neighbour_nodes(0, 1)
        self.assertEqual(expected, actual)

        expected = {(0, 2), (1, 1), (2, 2)}
        actual = lattice.get_neighbour_nodes(1, 2)
        self.assertEqual(expected, actual)

        expected = {(2, 0), (1, 1), (2, 2)}
        actual = lattice.get_neighbour_nodes(2, 1)
        self.assertEqual(expected, actual)

        expected = {(0, 0), (1, 1), (2, 0)}
        actual = lattice.get_neighbour_nodes(1, 0)
        self.assertEqual(expected, actual)

        # 4 Corners
        expected = {(0, 1), (1, 0)}
        actual = lattice.get_neighbour_nodes(0, 0)
        self.assertEqual(expected, actual)

        expected = {(0, 1), (1, 2)}
        actual = lattice.get_neighbour_nodes(0, 2)
        self.assertEqual(expected, actual)

        expected = {(1, 2), (2, 1)}
        actual = lattice.get_neighbour_nodes(2, 2)
        self.assertEqual(expected, actual)

        expected = {(1, 0), (2, 1)}
        actual = lattice.get_neighbour_nodes(2, 0)
        self.assertEqual(expected, actual)

        # Center
        expected = {(0, 1), (1, 2), (2, 1), (1, 0)}
        actual = lattice.get_neighbour_nodes(1, 1)
        self.assertEqual(expected, actual)

    def test_get_clusters_with_state_works_correctly(self):
        self.meta.size = 3
        matrix_lattice_state = [
            [1, 0, 0],
            [0, 0, 1],
            [1, 0, 1],
        ]
        lattice = Square2DLattice(self.meta)
        lattice.set_state_from_matrix(matrix_lattice_state)
        expected_clusters = {
            frozenset({(0, 0)}),
            frozenset({(2, 0)}),
            frozenset({(1, 2), (2, 2)}),
        }
        actual_clusters = lattice.get_clusters_with_state(1)
        self.assertEqual(expected_clusters, actual_clusters)

        expected_clusters = {frozenset({(0, 1), (0, 2), (1, 0), (1, 1), (2, 1)})}
        actual_clusters = lattice.get_clusters_with_state(0)
        self.assertEqual(expected_clusters, actual_clusters)

    def test_set_state_from_matrix_works_correctly(self):
        self.meta.size = 3
        matrix_lattice_state = [
            [1, 0, 0],
            [0, 0, 1],
            [1, 0, 1],
        ]
        lattice = Square2DLattice(self.meta)
        lattice.set_state_from_matrix(matrix_lattice_state)
        for i, row in enumerate(matrix_lattice_state):
            for j, entry in enumerate(row):
                expected = entry
                actual = lattice.get_state_at_node(i, j)
                self.assertEqual(expected, actual)
