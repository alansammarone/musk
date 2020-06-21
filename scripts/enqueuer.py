from musk.core.sql import MySQL
from musk.percolation import PS2Queue, PS2StatsQueue


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
        template = dict(parameters=dict(probability=p / 1000, size=16), repeat=4,)
        PS2Queue(env).write([template] * 10)

elif type_ == "stats":

    combinations = list(get_all_probabilities())
    import random

    random.shuffle(combinations)
    for row in combinations:
        probability = row["probability"]
        template = dict(parameters=dict(probability=probability))
        print(f"Sending {template}")
        PS2StatsQueue(env).write([template])
