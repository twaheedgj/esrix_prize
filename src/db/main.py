from sshtunnel import SSHTunnelForwarder
from config import config
import asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, text
from sqlmodel.ext.asyncio.session import AsyncSession
from sshtunnel import SSHTunnelForwarder
from config import config

import os

def start_tunnel():
    ssh_key_path = os.path.abspath("bahawsuser/bahawsuser.pem")
    print(f"SSH Key Path: {ssh_key_path}")
    tunnel = SSHTunnelForwarder(
        (config.SSH_HOST, 22),
        ssh_username=config.SSH_USER,
        ssh_pkey="bahawsuser\\bahawsuser.pem", 
        remote_bind_address=(config.REMOTE_DB_HOST, config.REMOTE_DB_PORT),
        local_bind_address=("localhost", config.LOCAL_PORT)
    )
    tunnel.start()

async def create_engine_async() -> AsyncEngine:
    start_tunnel()
    return create_async_engine(
        f"postgresql+asyncpg://{config.DB_USER}:{config.DB_PASSWORD}@localhost:{config.LOCAL_PORT}/{config.DB_NAME}",
        echo=True,
        pool_size=30,        # Increase the pool size
        max_overflow=60,     # Allow extra connections
        pool_timeout=90,     # Increase timeout
        pool_recycle=2600 
    )

async def init_db():
    global engine
    engine = await create_engine_async()  # Await engine creation
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)  # Create tables

async def get_session() -> AsyncSession: # type: ignore
    Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as session:
        yield session