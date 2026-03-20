import httpx
from app.core.config import settings
from app.graphql.types import UserCreateInput, QueryUserParams, UpdateUserInput
from sqlalchemy.orm import Session
from app.models.user import User


async def create_user(db: Session, input_data: UserCreateInput) -> User:
    # 1. Check if user already exists in user-service
    db_user = (
        db.query(User)
        .filter(
            (User.username == input_data.username) | (User.email == input_data.email)
        )
        .first()
    )
    if db_user:
        raise Exception("Username or email already registered")

    # 2. Create user in user-service database
    new_user = User(
        username=input_data.username,
        email=input_data.email,
        role=input_data.role,
        is_active=True,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # 3. Call auth-service to register credentials
    try:
        async with httpx.AsyncClient() as client:
            mutation = """
            mutation Register($id: Int!, $email: String!, $password: String!) {
              register(input: {
                userId: $id,
                email: $email,
                password: $password
              }) {
                id
              }
            }
            """
            response = await client.post(
                settings.AUTH_SERVICE_URL,
                json={
                    "query": mutation,
                    "variables": {
                        "id": new_user.id,
                        "email": new_user.email,
                        "password": input_data.password,
                    },
                },
            )
            response.raise_for_status()
            result = response.json()
            if "errors" in result:
                raise Exception(f"Auth Service error: {result['errors'][0]['message']}")

    except Exception as e:
        db.delete(new_user)
        db.commit()
        raise Exception(f"Failed to register user in auth service: {str(e)}")

    return new_user


async def update_user(db: Session, input_data: UpdateUserInput) -> User:
    db_user = db.query(User).filter(User.id == input_data.user_id).first()
    if not db_user:
        raise Exception("User not found")

    if input_data.user_name:
        db_user.user_name = input_data.user_name
    if input_data.role:
        db_user.role = input_data.role

    db.commit()
    db.refresh(db_user)
    return db_user


async def get_user_by_id(db: Session, user_id: int) -> User:
    db_user = db.query(User).filter(User.id == user_id)
    if not db_user:
        raise Exception("User not found")
    return db_user


async def list_users(db: Session, params: QueryUserParams) -> list[User]:
    query = db.query(User).filter(User.is_active)
    if params.role:
        query = query.filter(User.role == params.role)
    if params.text_search:
        query = query.filter(
            User.username.ilike(f"%{params.text_search}%")
            | User.email.ilike(f"%{params.text_search}%")
        )
    query = query.offset((params.page - 1) * params.limit)
    return query.all()
