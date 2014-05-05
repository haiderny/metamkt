import logging
import sqlalchemy


def get_connection():
    engine = sqlalchemy.create_engine('sqlite:////home/vishakh/dev/metamkt/metamkt/metamkt.sqlite')
    conn = engine.connect()
    return conn


def get_logger(script='Metamkt'):
    log = logging.getLogger(script)
    log.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(filename)s %(lineno)d - %(levelname)s - %(message)s"))
    log.addHandler(handler)
    return log