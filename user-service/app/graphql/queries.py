import strawberry
from strawberry.types import Info
from app.graphql.types import UserType, QueryUserParams
from app.graphql.resolvers import resolve_user_by_id, resolve_list_users

@strawberry.type
class UserQuery:
    @strawberry.field
    async def user_by_id(self, user_id: int, info: Info) -> UserType:
        return await resolve_user_by_id(user_id, info)

    @strawberry.field
    async def list_users(self, params: QueryUserParams, info: Info) -> list[UserType]:
        return await resolve_list_users(params, info)