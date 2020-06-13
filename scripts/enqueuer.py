from musk.core.sqs import SQSQueue


class Percolation2DSquare(SQSQueue):
    name = "percolation_2d_square"


class Percolation2DSquareStatsQueue(SQSQueue):
    name = "percolation_2d_square_stats"


type_ = "simulation"
env = "prod"

if type_ == "simulation":

    for p in range(45, 75):
        template = dict(parameters=dict(probability=p / 100, size=512), repeat=16,)
        # template = dict(parameters=dict(probability=0.59, size=256,), repeat=128)
        Percolation2DSquare(env).write([template] * 4)

elif type_ == "stats":

    for p in range(45, 75):
        template = dict(parameters=dict(probability=p / 100, size=32))
        # template = dict(parameters=dict(probability=0.59, size=256,), repeat=128)
        Percolation2DSquareStatsQueue(env).write([template])
