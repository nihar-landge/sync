from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import auth, db

router = APIRouter(prefix="/fs", tags=["filesystem"])


@router.get("/list")
def list_dir(path: str = "/", user=auth.UserDep):
    path = db.normalize(path)
    kind, row = db.resolve_path(user["id"], path)
    if kind is None:
        raise HTTPException(404, "Path does not exist")
    if kind != "dir":
        raise HTTPException(400, "Not a directory")
    return db.list_directory(user["id"], path)


class MkdirRequest(BaseModel):
    path: str


@router.post("/mkdir")
def mkdir(req: MkdirRequest, user=auth.UserDep):
    path = db.normalize(req.path)
    if path == "/":
        raise HTTPException(400, "Cannot create root")
    kind, _ = db.resolve_path(user["id"], path)
    if kind is not None and kind == "dir":
        return {"ok": True, "already_exists": True}
    if kind == "file":
        raise HTTPException(409, "A file exists at this path")
    parent = db.normalize("/".join(path.split("/")[:-1]))
    if parent != "/":
        kind, _ = db.resolve_path(user["id"], parent)
        if kind != "dir":
            raise HTTPException(400, "Parent directory does not exist")
    db.create_directory(user["id"], path)
    return {"ok": True, "path": path}