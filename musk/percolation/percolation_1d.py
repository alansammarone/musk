from musk.core.sqs import SQSQueue
from musk.lattices import Linear1DLattice
from musk.percolation.base import (
    PercolationModel,
    PercolationProcessor,
    PercolationSimulation,
    PercolationStatsModel,
)

from musk.percolation.stats import PercolationStatsProcessor


class P1LQueue(SQSQueue):
    name = "percolation_1d_linear"


class P1LStatsQueue(SQSQueue):
    name = "percolation_1d_linear_stats"


class P1LModel(PercolationModel):
    _tablename: str = "percolation_1d_linear"


class P1LStatsModel(PercolationStatsModel):
    _tablename: str = "percolation_1d_linear_stats"


class P1LSimulation(PercolationSimulation):
    model_class = P1LModel
    lattice_class = Linear1DLattice


class P1LStatsProcessor(PercolationStatsProcessor):

    simulation_model_class = P1LModel
    stats_model_class = P1LStatsModel
    lattice_class = Linear1DLattice


class P1LProcessor(PercolationProcessor):
    simulation_class = P1LSimulation
