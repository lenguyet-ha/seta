import strawberry
from strawberry.types import Info
from app.graphql.types import UserType, CreateUserInput, UpdateUserInput
from app.graphql.resolvers import resolver_create_user, resolver_update_user

@strawberry.type
class UserMutation:
    @strawberry.field
    def create_user(self, input: CreateUserInput, info: Info) -> UserType:
        return resolver_create_user(input, info)

    @strawberry.field
    def update_user(self, input: UpdateUserInput, info: Info) -> UserType:
        return resolver_update_user(input, info)