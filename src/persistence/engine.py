from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.config.postgres_settings import PostgresSettings
from contextlib import contextmanager


class DatabaseManager:

    def __init__(self, config: PostgresSettings):

        self.engine = create_engine(config.url)
        self.session_factory = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self):

        session = self.session_factory()
        try:
            yield session
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
