import httpx
from app.core.config import settings
from app.graphql.types import UserCreateInput, QueryUserParams, UpdateUserInput
from sqlalchemy.orm import Session
from app.models.user import User
from app.services.cache_service import (
    cache_service, 
    get_users_cache_key, 
    get_user_cache_key,
    get_users_list_pattern
)


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

    # 4. Invalidate users list cache
    await cache_service.delete_pattern(get_users_list_pattern())

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
    
    # Invalidate cache for this user and all list caches
    await cache_service.delete(get_user_cache_key(input_data.user_id))
    await cache_service.delete_pattern(get_users_list_pattern())
    
    return db_user


async def get_user_by_id(db: Session, user_id: int) -> User:
    # Try to get from cache first
    cache_key = get_user_cache_key(user_id)
    cached_user = await cache_service.get(cache_key)
    if cached_user:
        # Reconstruct User object from cached data
        user = User(**cached_user)
        return user
    
    # If not in cache, get from database
    db_user = db.query(User).filter(User.id == user_id).first()
    if not db_user:
        raise Exception("User not found")
    
    # Cache the result
    user_dict = {
        "id": db_user.id,
        "username": db_user.username,
        "email": db_user.email,
        "role": db_user.role,
        "is_active": db_user.is_active,
    }
    await cache_service.set(cache_key, user_dict)
    
    return db_user


async def list_users(db: Session, params: QueryUserParams) -> list[User]:
    # Generate cache key based on parameters
    cache_key = get_users_cache_key(
        role=params.role,
        page=params.page,
        limit=params.limit,
        text_search=params.text_search
    )
    
    # Try to get from cache first
    cached_users = await cache_service.get(cache_key)
    if cached_users:
        return [User(**user_data) for user_data in cached_users]
    
    # If not in cache, query database
    query = db.query(User).filter(User.is_active)
    if params.role:
        query = query.filter(User.role == params.role)
    if params.text_search:
        query = query.filter(
            User.username.ilike(f"%{params.text_search}%")
            | User.email.ilike(f"%{params.text_search}%")
        )
    query = query.offset((params.page - 1) * params.limit).limit(params.limit)
    users = query.all()
    
    # Cache the results
    users_data = [
        {
            "id": u.id,
            "username": u.username,
            "email": u.email,
            "role": u.role,
            "is_active": u.is_active,
        }
        for u in users
    ]
    await cache_service.set(cache_key, users_data)
    
    return users


async def delete_user(db: Session, user_id: int) -> bool:
    """Delete a user and invalidate all related caches"""
    db_user = db.query(User).filter(User.id == user_id).first()
    if not db_user:
        raise Exception("User not found")
    
    # Delete from database
    db.delete(db_user)
    db.commit()
    
    # Invalidate cache for this user and all list caches
    await cache_service.delete(get_user_cache_key(user_id))
    await cache_service.delete_pattern(get_users_list_pattern())
    
    return True
