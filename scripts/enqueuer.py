import random

from musk.core.sql import MySQL
from musk.percolation import (
    P1LModel,
    P1LQueue,
    P1LStatsQueue,
    P2SModel,
    P2SQueue,
    P2SStatsQueue,
)


model = "linear_1d"
type_ = "stats"
env = "dev"

if model == "linear_1d":
    simulation_queue, stats_queue, simulation_model = (
        P1LQueue(env),
        P1LStatsQueue(env),
        P1LModel,
    )
elif model == "square_2d":
    simulation_queue, stats_queue, simulation_model = (
        PS2Queue(env),
        PS2StatsQueue(env),
        PS2Model,
    )


def get_all_sizes_and_probabilities():
    query = f"select distinct size, probability from {simulation_model._tablename}"
    mysql = MySQL()
    results = mysql.fetch(query)
    return results


def get_all_probabilities():
    query = f"select distinct probability from {simulation_model._tablename};"
    mysql = MySQL()
    results = mysql.fetch(query)
    return results


detailed_p_2d_range = [p / 1000 for p in range(550, 650)]
general_p_2d_range = [p / 100 for p in range(45, 75)]
coarse_p_1d_range = [0.25, 0.5, 0.75]


if type_ == "simulation":
    p_range = coarse_p_1d_range
    for p in p_range:
        # for p in range(45, 75):
        template = dict(parameters=dict(probability=p, size=16), repeat=4,)
        simulation_queue.write([template] * 4)

elif type_ == "stats":

    combinations = list(get_all_sizes_and_probabilities())
    random.shuffle(combinations)
    for row in combinations:
        probability = row["probability"]
        size = row["size"]
        template = dict(parameters=dict(probability=probability, size=size))
        print(f"Sending {template}")
        stats_queue.write([template])
    # template = dict(parameters=dict(size=512))
    # PS2StatsQueue(env).write([template])
