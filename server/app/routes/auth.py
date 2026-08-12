from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import auth, db

router = APIRouter(prefix="/auth", tags=["auth"])


class Credentials(BaseModel):
    username: str
    password: str


@router.post("/register")
def register(creds: Credentials):
    if len(creds.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    if db.get_user(creds.username) is not None:
        raise HTTPException(409, "Username already exists")
    uid = db.create_user(creds.username, auth.hash_password(creds.password))
    return {"id": uid, "username": creds.username, "access_token": auth.issue_token(uid)}


@router.post("/login")
def login(creds: Credentials):
    user = db.get_user(creds.username)
    if user is None or not auth.verify_password(creds.password, user["password_hash"]):
        raise HTTPException(401, "Invalid username or password")
    return {"id": user["id"], "username": user["username"], "access_token": auth.issue_token(user["id"])}