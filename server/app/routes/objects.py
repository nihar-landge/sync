from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

from .. import storage

# NOTE: This route exists only for the "local" dev storage backend, which is
# a stand-in for OCI Object Storage. In production (backend=oci) the client
# fetches bytes directly from objectstorage.<region>.oraclecloud.com and
# this endpoint is never called.
#
# No JWT is required here: the signed URL *is* the credential, exactly like
# an OCI PAR. The token is scoped to one object and expires.

router = APIRouter(prefix="/objects", tags=["objects"])


@router.get("/{object_key:path}")
def object_get(object_key: str, token: str = "", expires_at: str = ""):
    _validate(object_key, token, expires_at)
    path: Path = storage.get_storage()._object_path(object_key)
    if not path.is_file():
        raise HTTPException(404, "Object not found")
    return FileResponse(path, headers={"Accept-Ranges": "bytes"})


@router.put("/{object_key:path}")
async def object_put(request: Request, object_key: str, token: str = "", expires_at: str = ""):
    _validate(object_key, token, expires_at)

    parts = object_key.split("/")
    if len(parts) != 2 or not parts[0].startswith("file_") or not parts[1].startswith("chunk_"):
        raise HTTPException(400, "Invalid object key")
    file_id = parts[0][len("file_"):]

    path: Path = storage.get_storage()._object_path(object_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        async for body in request.stream():
            f.write(body)
    return {"ok": True, "object_key": object_key}


def _validate(object_key: str, token: str, expires_at: str) -> None:
    if not token or not expires_at:
        raise HTTPException(403, "Invalid or expired token")
    if not isinstance(storage.get_storage(), storage.LocalStorage):
        raise HTTPException(403, "Object fetch disabled for this backend")
    if not storage.get_storage().verify_token(object_key, token, expires_at):
        raise HTTPException(403, "Invalid or expired token")