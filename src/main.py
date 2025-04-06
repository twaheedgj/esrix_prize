from fastapi import FastAPI
from contextlib import asynccontextmanager
from db.main import init_db
from routes.xprize import xprize_router
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    # Cleanup code can be added here if needed
app = FastAPI(
    lifespan=lifespan
)

app.include_router(xprize_router)