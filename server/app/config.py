import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("CLOUDSTREAM_DATA", Path.home() / ".cloudstream" / "server"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "cloudstream.db"
OBJECTS_DIR = DATA_DIR / "objects"

JWT_SECRET = os.environ.get("CLOUDSTREAM_SECRET", "dev-only-change-me")
JWT_ALG = "HS256"
JWT_TTL_SECONDS = int(os.environ.get("CLOUDSTREAM_TOKEN_TTL", str(24 * 3600)))

CHUNK_SIZE = int(os.environ.get("CLOUDSTREAM_CHUNK_SIZE", str(64 * 1024 * 1024)))
PAR_TTL_SECONDS = int(os.environ.get("CLOUDSTREAM_PAR_TTL", str(15 * 60)))

# Storage backend: "local" (dev stand-in) or "oci" (production)
STORAGE_BACKEND = os.environ.get("CLOUDSTREAM_STORAGE", "local")

# Publicly reachable base URL of this API, used to build local-mode URLs
PUBLIC_BASE_URL = os.environ.get("CLOUDSTREAM_URL", "http://127.0.0.1:8000")

# OCI (used only when STORAGE_BACKEND=oci)
OCI_BUCKET = os.environ.get("CLOUDSTREAM_OCI_BUCKET", "cloudstream")
OCI_PROFILE = os.environ.get("CLOUDSTREAM_OCI_PROFILE", "DEFAULT")


def ensure_dirs() -> None:
    OBJECTS_DIR.mkdir(parents=True, exist_ok=True)