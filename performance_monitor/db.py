from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlmodel import SQLModel, create_engine, Session

SYNC_DATABASE_URL = "sqlite:///./monitoring.db"
ASYNC_DATABASE_URL = "sqlite+aiosqlite:///./monitoring.db"
async_engine = create_async_engine(ASYNC_DATABASE_URL, echo=True)


@asynccontextmanager
async def get_async_session() -> AsyncSession:
    async with async_engine.connect() as connection:
        async with AsyncSession(bind=connection) as session:
            yield session


sync_engine = create_engine(SYNC_DATABASE_URL)


def init_db():
    with sync_engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL"))
    SQLModel.metadata.create_all(sync_engine)


def get_session() -> Session:
    return Session(sync_engine)
