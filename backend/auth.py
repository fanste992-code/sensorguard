"""
auth.py — FIX #3: Proper auth with bcrypt + JWT.

Replaces sha256 with bcrypt, custom tokens with python-jose JWT.
Same API surface — drop-in replacement.
"""
import os
import time
from typing import Optional

from fastapi import HTTPException, Header

# bcrypt for password hashing (salted, slow, secure)
from passlib.context import CryptContext
pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

# python-jose for standard JWT
from jose import jwt, JWTError

SECRET = os.getenv("JWT_SECRET", "sg-dev-secret-change-in-production")
ALGORITHM = "HS256"
EXPIRY = 86400 * 30   # 30 days


def hash_pw(password: str) -> str:
    """Hash password with bcrypt (salted, 12 rounds by default)."""
    return pwd_ctx.hash(password)


def verify_pw(password: str, hashed: str) -> bool:
    """Verify password against bcrypt hash."""
    return pwd_ctx.verify(password, hashed)


def make_token(user_id: int, email: str) -> str:
    """Create a standard HS256 JWT."""
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": int(time.time()) + EXPIRY,
    }
    return jwt.encode(payload, SECRET, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    """Decode and verify JWT. Returns payload or None."""
    try:
        payload = jwt.decode(token, SECRET, algorithms=[ALGORITHM])
        if payload.get("exp", 0) < time.time():
            return None
        payload["sub"] = int(payload["sub"])
        return payload
    except (JWTError, ValueError, KeyError):
        return None


def get_user_id(authorization: str = Header(None)) -> int:
    """FastAPI dependency: extract user_id from Authorization header."""
    if not authorization:
        raise HTTPException(401, "Missing authorization header")
    token = authorization.replace("Bearer ", "")
    payload = decode_token(token)
    if not payload:
        raise HTTPException(401, "Invalid or expired token")
    return payload["sub"]
