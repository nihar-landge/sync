import sqlite3
import threading
import uuid
from pathlib import PurePosixPath
from typing import Optional

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            TEXT PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS files (
    id          TEXT PRIMARY KEY,
    owner_id    TEXT NOT NULL,
    path        TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL,
    chunk_size  INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'uploading',  -- uploading | complete | error
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_files_owner_path ON files(owner_id, path);

CREATE TABLE IF NOT EXISTS chunks (
    file_id     TEXT NOT NULL REFERENCES files(id),
    chunk_index INTEGER NOT NULL,
    object_key  TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL,
    checksum    TEXT,
    uploaded    BOOLEAN NOT NULL DEFAULT 0,
    PRIMARY KEY (file_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS directories (
    id       TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    path     TEXT NOT NULL UNIQUE
);
"""

_local = threading.local()


def get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(config.DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_SCHEMA)
        conn.commit()
        _local.conn = conn
    return conn


# ---------------------------------------------------------------------------
# users
# ---------------------------------------------------------------------------

def create_user(username: str, password_hash: str) -> str:
    uid = str(uuid.uuid4())
    conn = get_conn()
    conn.execute(
        "INSERT INTO users (id, username, password_hash) VALUES (?, ?, ?)",
        (uid, username, password_hash),
    )
    conn.commit()
    return uid


def get_user(username: str) -> Optional[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM users WHERE username = ?", (username,)
    ).fetchone()


def get_user_by_id(user_id: str) -> Optional[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM users WHERE id = ?", (user_id,)
    ).fetchone()


# ---------------------------------------------------------------------------
# directories
# ---------------------------------------------------------------------------

def ensure_directory_tree(owner_id: str, path: str) -> None:
    """Create parent directories for a virtual path, e.g. /a/b/c -> /a, /a/b."""
    conn = get_conn()
    p = PurePosixPath(path)
    parts = []
    for part in p.parts[1:-1]:
        parts.append(part)
        cur = "/" + "/".join(parts)
        conn.execute(
            "INSERT OR IGNORE INTO directories (id, owner_id, path) VALUES (?, ?, ?)",
            (str(uuid.uuid4()), owner_id, cur),
        )
    conn.commit()


def create_directory(owner_id: str, path: str) -> None:
    ensure_directory_tree(owner_id, path)
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO directories (id, owner_id, path) VALUES (?, ?, ?)",
        (str(uuid.uuid4()), owner_id, path),
    )
    conn.commit()


def list_directory(owner_id: str, path: str) -> list:
    conn = get_conn()
    rows = conn.execute(
        "SELECT path FROM directories WHERE owner_id = ? AND path LIKE ? "
        "AND path NOT LIKE ?",
        (owner_id, path.rstrip("/") + "/%", path.rstrip("/") + "/%/%"),
    ).fetchall()

    files = conn.execute(
        "SELECT id, path, size_bytes, updated_at FROM files "
        "WHERE owner_id = ? AND status = 'complete' AND path LIKE ? "
        "AND path NOT LIKE ?",
        (owner_id, path.rstrip("/") + "/%", path.rstrip("/") + "/%/%"),
    ).fetchall()

    entries = []
    seen = {r["path"] for r in rows}
    for r in rows:
        name = PurePosixPath(r["path"]).name
        entries.append({"name": name, "type": "dir", "size": 0, "mtime": None})
    for r in files:
        name = PurePosixPath(r["path"]).name
        entries.append(
            {"id": r["id"], "name": name, "type": "file", "size": r["size_bytes"], "mtime": r["updated_at"]}
        )

    entries.sort(key=lambda e: (e["type"] != "dir", e["name"]))
    parent = "/" if path == "/" else str(PurePosixPath(path).parent)
    return {"path": path, "entries": entries, "parent": parent}


# ---------------------------------------------------------------------------
# files & chunks
# ---------------------------------------------------------------------------

def create_file(owner_id: str, path: str, size_bytes: int, chunk_size: int) -> dict:
    file_id = str(uuid.uuid4())
    chunk_count = (size_bytes + chunk_size - 1) // chunk_size
    if size_bytes == 0:
        chunk_count = 0
    conn = get_conn()
    conn.execute(
        "INSERT INTO files (id, owner_id, path, size_bytes, chunk_size, chunk_count, status) "
        "VALUES (?, ?, ?, ?, ?, ?, 'uploading')",
        (file_id, owner_id, path, size_bytes, chunk_size, chunk_count),
    )
    for i in range(chunk_count):
        last = i == chunk_count - 1
        chunk_bytes = size_bytes - i * chunk_size if last else chunk_size
        key = f"file_{file_id}/chunk_{i:06d}"
        conn.execute(
            "INSERT INTO chunks (file_id, chunk_index, object_key, size_bytes) "
            "VALUES (?, ?, ?, ?)",
            (file_id, i, key, chunk_bytes),
        )
    conn.commit()
    return {
        "file_id": file_id,
        "path": path,
        "size_bytes": size_bytes,
        "chunk_size": chunk_size,
        "chunk_count": chunk_count,
    }


def get_file(file_id: str, owner_id: str) -> Optional[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM files WHERE id = ? AND owner_id = ?", (file_id, owner_id)
    ).fetchone()


def get_file_by_path(owner_id: str, path: str) -> Optional[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM files WHERE owner_id = ? AND path = ?", (owner_id, path)
    ).fetchone()


def get_chunks(file_id: str) -> list:
    return get_conn().execute(
        "SELECT * FROM chunks WHERE file_id = ? ORDER BY chunk_index", (file_id,)
    ).fetchall()


def complete_chunk(file_id: str, chunk_index: int, checksum: str, size_bytes: int) -> None:
    conn = get_conn()
    conn.execute(
        "UPDATE chunks SET checksum = ?, size_bytes = ?, uploaded = 1 "
        "WHERE file_id = ? AND chunk_index = ?",
        (checksum, size_bytes, file_id, chunk_index),
    )
    if get_conn().execute(
        "SELECT COUNT(*) c FROM chunks WHERE file_id = ? AND uploaded = 0", (file_id,)
    ).fetchone()["c"] == 0:
        conn.execute(
            "UPDATE files SET status = 'complete', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (file_id,),
        )
    conn.commit()


def count_uploaded(file_id: str) -> int:
    return get_conn().execute(
        "SELECT COUNT(*) c FROM chunks WHERE file_id = ? AND uploaded = 1", (file_id,)
    ).fetchone()["c"]


def delete_file(file_id: str, owner_id: str) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT path FROM files WHERE id = ? AND owner_id = ?", (file_id, owner_id)
    ).fetchone()
    if row is None:
        return False
    conn.execute("DELETE FROM chunks WHERE file_id = ?", (file_id,))
    conn.execute("DELETE FROM files WHERE id = ?", (file_id,))
    conn.commit()
    return True


# ---------------------------------------------------------------------------
# virtual filesystem helpers
# ---------------------------------------------------------------------------

def resolve_path(owner_id: str, path: str):
    """Return (kind, row) where kind is 'file'|'dir'|None."""
    path = path.rstrip("/") or "/"
    if path != "/":
        row = get_file_by_path(owner_id, path)
        if row is not None:
            return "file", row
        conn = get_conn()
        if conn.execute(
            "SELECT 1 FROM directories WHERE owner_id = ? AND path = ?", (owner_id, path)
        ).fetchone():
            return "dir", None
        return None, None
    return "dir", None


def normalize(path: str) -> str:
    path = path.strip()
    if not path.startswith("/"):
        path = "/" + path
    return str(PurePosixPath(path)) if path != "/" else "/"