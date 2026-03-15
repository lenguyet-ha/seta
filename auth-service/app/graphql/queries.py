import strawberry
from strawberry.types import Info
from app.graphql.types import UserType
from app.graphql.resolvers import resolve_me
from seta_shared.graphql.directives import IsAuthenticated

@strawberry.type
class AuthQuery:
    @strawberry.field(permission_classes=[IsAuthenticated])
    def me(self, info: Info) -> UserType:
        return resolve_me(info)
    