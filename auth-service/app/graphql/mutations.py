import strawberry
from strawberry.types import Info
from app.graphql.types import UserType, TokenType, LoginInput, ChangePasswordInput
from app.graphql.resolvers import resolve_login, resolve_change_password
from seta_shared.graphql.directives import IsAuthenticated

@strawberry.type
class AuthMutation:
    @strawberry.mutation
    def login(self, info: Info, input: LoginInput) -> TokenType:
        return resolve_login(input, info)
    @strawberry.mutation(permission_classes=[IsAuthenticated])
    def change_password(self, info: Info, input: ChangePasswordInput) -> UserType:
        return resolve_change_password(input, info)
