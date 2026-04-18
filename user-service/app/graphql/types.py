import strawberry
from typing import Optional

@strawberry.type
class UserType:
    id: int
    username: str
    email: str
    role: str
    is_active: bool
    created_at: str

    @classmethod
    def from_db(cls, db_user):
        if not db_user:
            return None
        return cls(
            id=db_user.id,
            username=db_user.username,
            email=db_user.email,
            role=db_user.role,
            is_active=db_user.is_active,
            created_at=db_user.created_at.isoformat() if db_user.created_at else None,
        )

@strawberry.input
class CreateUserInput:
    username: str
    email: str
    password: str
    role: str = "Member"

@strawberry.input
class UpdateUserInput:
    user_id: int
    username: Optional[str] = None
    role: Optional[str] = None

@strawberry.input
class QueryUserParams:
    page: int = 1
    limit: int = 10
    role: Optional[str] = None
    text_search: Optional[str] = None
