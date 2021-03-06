from environs import Env


env = Env()


class Config:
    pass


class MySQLConfig(Config):
    with env.prefixed("MYSQL_"):
        HOST: str = env("HOST", "localhost")
        PORT: int = env.int("PORT", 3306)
        USER: str = env("USER", "root")
        PASSWORD: str = env("PASSWORD", None)
        DATABASE: str = env("DATABASE", "musk")
        CONNECTION_TIMEOUT: int = env.int("CONNECTION_TIMEOUT", 30)


class DequeuerConfig(Config):

    with env.prefixed("DEQUEUER_"):
        MESSAGES_PER_READ: int = env.int("MESSAGES_PER_READ", 1)
        PROCESS_COUNT: int = env.int("PROCESS_COUNT", 1)
        ENV: str = env("ENV", "dev")


class LoggingConfig(Config):

    with env.prefixed("LOGGING_"):
        CONFIG_FILEPATH: str = env("CONFIG_FILEPATH", "logging.yaml")
