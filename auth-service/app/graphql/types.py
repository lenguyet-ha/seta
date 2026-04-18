
import strawberry
from typing import Optional
from datetime import datetime

@strawberry.type
class UserType:
    user_id: int
    username: str
    email: str
    role: str
    is_active: bool
    created_at: str
    
    @classmethod
    def from_db(cls, db_user):
        """Convert database user to GraphQL type"""
        if not db_user:
            return None
        return cls(
            user_id=db_user.user_id,
            username=db_user.username,
            email=db_user.email,
            role=db_user.role,
            is_active=db_user.is_active,
            created_at=db_user.created_at.isoformat() if db_user.created_at else None
        )

@strawberry.type
class TokenType:
    access_token: Optional[str] = None
    token_type: str = "bearer"
    user_id: Optional[int] = None
    role: Optional[str] = None
    valid: bool = True

@strawberry.input
class LoginInput:
    username: str
    password: str

@strawberry.input
class UserUpdateInput:
    username: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None

@strawberry.input
class ChangePasswordInput:
    old_password: str
    new_password: str