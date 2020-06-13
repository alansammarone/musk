import mysql.connector

MySQLConnection = mysql.connector.connection.MySQLConnection


class MySQL:

    _config: MySQLConfig
    _connection: MySQLConnection

    def __init__(self):
        self._config = MySQLConfig
        self._connection = self._get_new_connection()

    def _get_connection_config(self) -> dict:

        return dict(
            host=self._config.HOST,
            port=self._config.PORT,
            user=self._config.USER,
            password=self._config.PASSWORD,
            database=self._config.DATABASE,
            connection_timeout=self._config.CONNECTION_TIMEOUT,
            use_pure=True,
        )

    def _get_new_connection(self) -> MySQLConnection:
        config = self._get_connection_config()
        connection = mysql.connector.connect(**config)
        return connection

    def fetch(self, query: str, parameters: tuple = ()):
        cursor = self._connection.cursor()
        cursor.execute(query, parameters)
        for row in cursor:
            row = dict(zip(cursor.column_names, row))
            yield row
        cursor.close()


# print(mysql.connector.connection.MySQLConnection)
mysql = MySQL()
query = "select * from percolation_2d_square where size = 512;"
import bz2
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SimulationExecution:

    id: str
    probability: float
    size: int
    observables: dict
    took: float
    created: datetime

    @classmethod
    def from_db_row(cls, row):
        return cls(**row)

    # probability=self.probability,
    # size=self.size,
    # observables=bz2.compress(
    #     bytes(
    #         json.dumps(self._observables, cls=PythonObjectEncoder).encode(
    #             "utf-8"
    #         )
    #     )
    # ),
    # took=self.took.total_seconds(),
    # created=self.created.strftime("%Y-%m-%d %H:%M:%S"),


import time

a = time.time()

rows = []
for row in mysql.fetch(query):
    # print(row.key)
    a = SimulationExecution(**row)
    print(len(a["observables"]))


b = time.time()
print(b - a)
