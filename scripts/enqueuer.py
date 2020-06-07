from musk.core.sqs import SQSQueue


class Percolation2DSquare(SQSQueue):
    name = "percolation_2d_square"


template = dict(parameters=dict(probability=0.59, size=128,), repeat=32)

Percolation2DSquare("dev").write([template] * 10)
