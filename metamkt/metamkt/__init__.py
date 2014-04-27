"""Main entry point
"""
from pyramid.config import Configurator

from sqlalchemy import engine_from_config
from models import initialize_sql

def main(global_config, **settings):
    config = Configurator(settings=settings)
    config.include("cornice")
    config.scan("metamkt.views")

    engine = engine_from_config(settings, 'sqlalchemy.')
    initialize_sql(engine)

    return config.make_wsgi_app()
