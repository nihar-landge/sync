from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import auth, config, db, storage

router = APIRouter(prefix="/files", tags=["files"])


class InitUploadRequest(BaseModel):
    path: str
    size_bytes: int = 0


@router.post("/init-upload")
def init_upload(req: InitUploadRequest, user=auth.UserDep):
    if req.size_bytes < 0:
        raise HTTPException(400, "size_bytes must be >= 0")
    path = db.normalize(req.path)
    if path == "/":
        raise HTTPException(400, "Invalid path")

    kind, _ = db.resolve_path(user["id"], path)
    if kind == "file":
        raise HTTPException(409, "A file already exists at this path")
    parent = db.normalize("/".join(path.split("/")[:-1]))
    if parent != "/":
        kind, _ = db.resolve_path(user["id"], parent)
        if kind != "dir":
            raise HTTPException(400, "Parent directory does not exist")

    db.ensure_directory_tree(user["id"], path)
    meta = db.create_file(user["id"], path, req.size_bytes, config.CHUNK_SIZE)
    return {
        "file_id": meta["file_id"],
        "chunk_size": meta["chunk_size"],
        "chunk_count": meta["chunk_count"],
        "size_bytes": meta["size_bytes"],
    }


@router.get("/{file_id}/chunks/{n}/upload-url")
def chunk_upload_url(file_id: str, n: int, user=auth.UserDep):
    row = _chunk_or_404(file_id, n, user)
    url, expires = storage.get_storage().upload_url(row["object_key"])
    return {"url": url, "expires_at": expires}


@router.get("/{file_id}/chunks/{n}/download-url")
def chunk_download_url(file_id: str, n: int, user=auth.UserDep):
    row = _chunk_or_404(file_id, n, user, require_uploaded=True)
    url, expires = storage.get_storage().download_url(row["object_key"])
    return {"url": url, "expires_at": expires}


class ChunkCompleteRequest(BaseModel):
    checksum: str | None = None
    size_bytes: int | None = None


@router.post("/{file_id}/chunks/{n}/complete")
def chunk_complete(file_id: str, n: int, req: ChunkCompleteRequest, user=auth.UserDep):
    row = _chunk_or_404(file_id, n, user)
    size = req.size_bytes if req.size_bytes is not None else row["size_bytes"]
    db.complete_chunk(file_id, n, req.checksum, size)
    return {"ok": True, "chunk_index": n, "uploaded": True}


@router.get("/{file_id}/meta")
def file_meta(file_id: str, user=auth.UserDep):
    row = db.get_file(file_id, user["id"])
    if row is None:
        raise HTTPException(404, "File not found")
    chunks = db.get_chunks(file_id)
    return {
        "id": row["id"],
        "path": row["path"],
        "size_bytes": row["size_bytes"],
        "chunk_size": row["chunk_size"],
        "chunk_count": row["chunk_count"],
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "chunks": [
            {
                "index": c["chunk_index"],
                "object_key": c["object_key"],
                "size_bytes": c["size_bytes"],
                "uploaded": bool(c["uploaded"]),
                "checksum": c["checksum"],
            }
            for c in chunks
        ],
    }


@router.delete("/{file_id}")
def file_delete(file_id: str, user=auth.UserDep):
    if not db.delete_file(file_id, user["id"]):
        raise HTTPException(404, "File not found")
    storage.get_storage().delete_file(file_id)
    return {"ok": True, "deleted": file_id}


def _chunk_or_404(file_id: str, n: int, user, require_uploaded: bool = False):
    row = db.get_file(file_id, user["id"])
    if row is None:
        raise HTTPException(404, "File not found")
    chunks = db.get_chunks(file_id)
    if not (0 <= n < len(chunks)):
        raise HTTPException(404, "Chunk index out of range")
    chunk = chunks[n]
    if require_uploaded and not chunk["uploaded"]:
        raise HTTPException(409, "Chunk has not been uploaded yet")
    return chunk