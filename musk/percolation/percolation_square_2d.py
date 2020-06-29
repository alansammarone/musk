from musk.core.sqs import SQSQueue
from musk.lattices import Square2DLattice
from musk.percolation.base import (
    PercolationModel,
    PercolationProcessor,
    PercolationSimulation,
    PercolationStatsModel,
    PercolationStatsProcessor,
)


class P2SQueue(SQSQueue):
    name = "percolation_2d_square"


class P2SStatsQueue(SQSQueue):
    name = "percolation_2d_square_stats"


class P2SModel(PercolationModel):
    _tablename: str = "percolation_2d_square"


class P2SStatsModel(PercolationStatsModel):
    _tablename: str = "percolation_2d_square_stats"


class P2SSimulation(PercolationSimulation):
    model_class = P2SModel
    lattice_class = Square2DLattice


class P2SStatsProcessor(PercolationStatsProcessor):

    simulation_model_class = P2SModel
    stats_model_class = P2SStatsModel
    lattice_class = Square2DLattice


class P2SProcessor(PercolationProcessor):
    simulation_class = P2SSimulation
