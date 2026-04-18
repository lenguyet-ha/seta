import strawberry
from strawberry.types import Info
from app.graphql.types import UserType, CreateUserInput, UpdateUserInput
from app.graphql.resolvers import resolve_create_user, resolve_update_user

@strawberry.type
class UserMutation:
    @strawberry.field
    async def create_user(self, input: CreateUserInput, info: Info) -> UserType:
        return await resolve_create_user(input, info)

    @strawberry.field
    async def update_user(self, input: UpdateUserInput, info: Info) -> UserType:
        return await resolve_update_user(input, info)