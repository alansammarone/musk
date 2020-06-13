import logging
import mysql.connector

from musk.config.config import MySQLConfig

MySQLConnection = mysql.connector.connection.MySQLConnection

logger = logging.getLogger(__file__)


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
            use_pure=True,  # This ensures we use the C Extesion. Otherwise, we get SegFault
        )

    def _get_new_connection(self) -> MySQLConnection:
        config = self._get_connection_config()
        connection = mysql.connector.connect(**config)
        return connection

    def fetch(self, query: str, parameters: tuple = ()):
        cursor = self._connection.cursor(dictionary=True)
        cursor.execute(query, parameters)
        for row in cursor:
            yield row
        cursor.close()

    def execute(self, query: str, parameters: dict):
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters)
            self._connection.commit()
        except:
            print(cursor.statement)
            logger.debug(cursor.statement)
            self._connection.rollback()
            raise
        finally:
            cursor.close()
