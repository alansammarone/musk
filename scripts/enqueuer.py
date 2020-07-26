import itertools
import random
import time
from musk.core.sql import MySQL
from musk.percolation import (
    P1LModel,
    P1LQueue,
    P1LStatsQueue,
    P2SModel,
    P2SQueue,
    P2SStatsQueue,
)


model = "square_2d"
type_ = "stats"
env = "prod"


print(f"Environment: {env.upper()}")
if env == "prod":
    time.sleep(2)


if model == "linear_1d":
    simulation_queue, stats_queue, simulation_model = (
        P1LQueue(env),
        P1LStatsQueue(env),
        P1LModel,
    )
elif model == "square_2d":
    simulation_queue, stats_queue, simulation_model = (
        P2SQueue(env),
        P2SStatsQueue(env),
        P2SModel,
    )


def get_all_sizes_and_probabilities():
    query = f"SELECT distinct size, probability FROM {simulation_model._tablename}"
    mysql = MySQL()
    results = mysql.fetch(query)
    return results


def get_all_probabilities():
    query = f"SELECT distinct probability FROM {simulation_model._tablename};"
    mysql = MySQL()
    results = mysql.fetch(query)
    return results


def get_all_sizes():
    query = f"SELECT distinct size FROM {simulation_model._tablename};"

    mysql = MySQL()
    results = mysql.fetch(query)
    return results


def get_all_ids_for_size_and_probability(size, probability, limit):
    query = f"""
        SELECT id FROM {simulation_model._tablename}
        WHERE size = {size} AND round(probability, 3) = {probability}
        LIMIT {limit}
    """
    mysql = MySQL()
    results = mysql.fetch(query)
    return results


def get_id_chunks(size, probability):

    limit = 8192
    chunk_size = 350
    ids = [
        row["id"]
        for row in get_all_ids_for_size_and_probability(size, probability, limit)
    ]
    return [
        ids[index : index + chunk_size]
        for index in range(0, min(limit, len(ids)), chunk_size)
    ]


detailed_p_2d_range = [p / 1000 for p in range(550, 650)]
general_p_2d_range = [p / 100 for p in range(45, 75)]
coarse_p_1d_range = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
extension_p_2d_range = [p / 100 for p in list(range(1, 45)) + list(range(75, 100))]


if type_ == "simulation":
    p_range = extension_p_2d_range
    sizes = [size["size"] for size in get_all_sizes()]

    combinations = list(itertools.product(p_range, sizes))
    random.shuffle(combinations)
    for p, size in combinations:
        if size == 512:
            repeat = 16
        elif size == 394:
            repeat = 56
        else:
            repeat = 128
        template = dict(parameters=dict(probability=p, size=size), repeat=repeat,)
        print(template)
        simulation_queue.write([template] * 10)

elif type_ == "stats":
    combinations = list(get_all_sizes_and_probabilities())

    stats = [
        # "has_percolated",
        # "cluster_size_histogram",
        # "mean_cluster_size",
        "correlation_function",
        # "percolating_cluster_strength",
    ]
    # size_filter = [16, 32, 96, 128, 192, 256, 294, 512]
    size_filter = [128]
    probability_filter = detailed_p_2d_range + extension_p_2d_range + general_p_2d_range
    random.shuffle(combinations)
    combinations = filter(lambda comb: comb["size"] in size_filter, combinations)
    combinations = filter(
        lambda comb: comb["probability"] in probability_filter, combinations
    )
    for row in combinations:

        probability = row["probability"]
        size = row["size"]
        id_chunks = get_id_chunks(size, probability)
        for id_chunk in id_chunks:
            template = dict(
                parameters=dict(probability=probability, size=size),
                ids=id_chunk,
                stats=stats,
            )
            print(
                f"Size: {size}, probability: {probability}, number of ids: {len(id_chunk)}"
            )
            stats_queue.write([template])
