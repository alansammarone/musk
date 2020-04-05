import unittest
from musk import Linear1DLattice, Square2DLattice


class TestLinear1DLattice(unittest.TestCase):
    def test_set_state_from_list_results_in_correct_state(self):
        lattice_size = 5
        lattice = Linear1DLattice(lattice_size)
        state = [0, 0, 1, 1, 0]
        lattice.set_state_from_list(state)
        for index in lattice.get_all_nodes():
            expected = state[index[0]]
            actual = lattice.get_state_at_node(*index)
            self.assertEqual(expected, actual)

    def test_neighbours_are_computed_correctly(self):
        lattice_size = 5
        lattice = Linear1DLattice(lattice_size)

        expected = {(1,)}
        actual = lattice.get_neighbour_nodes(0)
        self.assertEqual(expected, actual)

        expected = {(0,), (2,)}
        actual = lattice.get_neighbour_nodes(1)
        self.assertEqual(expected, actual)

        expected = {(1,), (3,)}
        actual = lattice.get_neighbour_nodes(2)
        self.assertEqual(expected, actual)

        expected = {(2,), (4,)}
        actual = lattice.get_neighbour_nodes(3)
        self.assertEqual(expected, actual)

        expected = {(3,)}
        actual = lattice.get_neighbour_nodes(4)
        self.assertEqual(expected, actual)

    def test_clusters_are_computed_correctly(self):

        lattice_size = 5
        lattice = Linear1DLattice(lattice_size)
        state = [0, 0, 1, 1, 0]
        lattice.set_state_from_list(state)

        expected = frozenset({(0,), (1,)})
        actual = lattice.get_cluster((0,))
        self.assertEqual(expected, actual)

        expected = frozenset({(0,), (1,)})
        actual = lattice.get_cluster((0,))
        self.assertEqual(expected, actual)

        expected = frozenset({(2,), (3,)})
        actual = lattice.get_cluster((2,))
        self.assertEqual(expected, actual)

        expected = frozenset({(2,), (3,)})
        actual = lattice.get_cluster((3,))
        self.assertEqual(expected, actual)

        expected = frozenset({(4,)})
        actual = lattice.get_cluster((4,))
        self.assertEqual(expected, actual)

    def test_get_clusters_with_state_returns_correct_clusters(self):

        lattice_size = 5
        lattice = Linear1DLattice(lattice_size)
        state = [0, 0, 1, 1, 0]
        lattice.set_state_from_list(state)

        expected = frozenset({frozenset({(0,), (1,)}), frozenset({(4,)})})
        actual = lattice.get_clusters_with_state(0)
        self.assertEqual(expected, actual)

        expected = frozenset({frozenset({(2,), (3,)})})
        actual = lattice.get_clusters_with_state(1)
        self.assertEqual(expected, actual)


class TestSquare2DLattice(unittest.TestCase):
    def test_clusters_are_computed_correctly(self):

        matrix_lattice_state = [
            [1, 0, 0],
            [0, 0, 1],
            [1, 0, 1],
        ]
        lattice_size = 3
        lattice = Square2DLattice(lattice_size)
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
        lattice_size = 3
        lattice = Square2DLattice(lattice_size)

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

    def test_get_clusters_with_state_returns_correct_clusters(self):
        lattice_size = 3
        lattice = Square2DLattice(lattice_size)
        matrix_lattice_state = [
            [1, 0, 0],
            [0, 0, 1],
            [1, 0, 1],
        ]
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

    def test_set_state_from_matrix_results_in_correct_state(self):
        lattice_size = 3
        lattice = Square2DLattice(lattice_size)
        matrix_lattice_state = [
            [1, 0, 0],
            [0, 0, 1],
            [1, 0, 1],
        ]

        lattice.set_state_from_matrix(matrix_lattice_state)
        for i, row in enumerate(matrix_lattice_state):
            for j, entry in enumerate(row):
                expected = entry
                actual = lattice.get_state_at_node(i, j)
                self.assertEqual(expected, actual)
