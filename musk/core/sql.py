import logging

import mysql.connector
from musk.config.config import MySQLConfig

MySQLConnection = mysql.connector.connection.MySQLConnection
import pymysql


class MySQL:

    _config: MySQLConfig
    _connection: MySQLConnection

    def __init__(self):
        self._config = MySQLConfig
        self._connection = self._get_new_connection()
        self._logger = logging.getLogger(__file__)

    def _get_connection_config(self) -> dict:
        return dict(
            host=self._config.HOST,
            port=self._config.PORT,
            user=self._config.USER,
            password=self._config.PASSWORD,
            database=self._config.DATABASE,
            connect_timeout=self._config.CONNECTION_TIMEOUT,
            autocommit=True,
        )
        return dict(
            host=self._config.HOST,
            port=self._config.PORT,
            user=self._config.USER,
            password=self._config.PASSWORD,
            database=self._config.DATABASE,
            connection_timeout=self._config.CONNECTION_TIMEOUT,
            use_pure=True,  # This ensures we use the C Extesion. Otherwise, we get SegFault
            autocommit=True,
        )

    def _get_new_connection(self) -> MySQLConnection:
        config = self._get_connection_config()
        connection = pymysql.connect(**config)
        return connection

    def fetch(self, query: str, parameters: tuple = ()):
        cursor = self._connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute(query, parameters)
        for row in cursor.fetchall_unbuffered():
            yield row
        cursor.close()

    def execute(self, query: str, parameters: dict):
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
            self._connection.commit()
        except:
            self._logger.debug(cursor._last_executed)
            self._logger.debug(parameters)
            self._connection.rollback()
            raise
        finally:
            cursor.close()
