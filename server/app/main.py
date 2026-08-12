from fastapi import FastAPI

from . import config
from .routes import auth, files, fs, objects

app = FastAPI(title="CloudStreamFS Control Plane", version="0.1.0")

config.ensure_dirs()

app.include_router(objects.router, prefix="/api/v1")
app.include_router(auth.router, prefix="/api/v1")
app.include_router(fs.router, prefix="/api/v1")
app.include_router(files.router, prefix="/api/v1")


@app.get("/healthz")
def healthz():
    return {"ok": True, "storage": config.STORAGE_BACKEND}