from musk.core.sqs import SQSQueue


class Percolation2DSquare(SQSQueue):
    name = "percolation_2d_square"


class Percolation2DSquareStatsQueue(SQSQueue):
    name = "percolation_2d_square_stats"


type_ = "stats"
env = "prod"

if type_ == "simulation":

    for p in range(45, 75):
        template = dict(parameters=dict(probability=p / 100, size=512), repeat=16,)
        Percolation2DSquare(env).write([template] * 4)

elif type_ == "stats":

    for p in range(45, 75):
        # for size in [16, 32, 64]:
        # for size in [128, 256, 512]:
        for size in [192]:
            template = dict(parameters=dict(probability=p / 100, size=size))
            Percolation2DSquareStatsQueue(env).write([template])
