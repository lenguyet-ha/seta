import strawberry
from strawberry.types import Info
from app.services import user_service
from app.graphql.types import UserType, QueryUserParams, CreateUserInput, UpdateUserInput

def get_db(info: Info):
    return info.context["db"]

async def resolve_list_users(params: QueryUserParams, info: Info) -> list[UserType]:
    db = get_db(info)
    users = await user_service.list_users(db, params)
    return [UserType.from_db(user) for user in users]

async def resolve_create_user(input: CreateUserInput, info: Info) -> UserType:
    db = get_db(info)
    user = await user_service.create_user(db, input)
    return UserType.from_db(user)

async def resolve_update_user(input: UpdateUserInput, info: Info) -> UserType:
    db = get_db(info)
    user = await user_service.update_user(db, input)
    return UserType.from_db(user)

async def resolve_user_by_id(user_id: int, info: Info) -> UserType:
    db = get_db(info)
    user = await user_service.get_user_by_id(db, user_id)
    return UserType.from_db(user)