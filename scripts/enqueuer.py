from musk.core.sqs import SQSQueue
from musk.core.sql import MySQL


class Percolation2DSquare(SQSQueue):
    name = "percolation_2d_square"


class Percolation2DSquareStatsQueue(SQSQueue):
    name = "percolation_2d_square_stats"


def get_all_sizes_and_probabilities():
    query = "select distinct size, probability from percolation_2d_square;"
    mysql = MySQL()
    results = mysql.fetch(query)
    return results


def get_all_probabilities():
    query = "select distinct probability from percolation_2d_square;"
    mysql = MySQL()
    results = mysql.fetch(query)
    return results


type_ = "stats"
env = "dev"

if type_ == "simulation":
    for p in range(550, 650):
        # for p in range(45, 75):
        template = dict(parameters=dict(probability=p / 1000, size=512), repeat=32,)
        Percolation2DSquare(env).write([template] * 10)

elif type_ == "stats":

    combinations = list(get_all_probabilities())
    import random

    random.shuffle(combinations)
    for row in combinations:
        probability = row["probability"]
        template = dict(parameters=dict(probability=probability))
        print(f"Sending {template}")
        Percolation2DSquareStatsQueue(env).write([template])
