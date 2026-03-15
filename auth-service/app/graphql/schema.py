# pyre-ignore[21]
import strawberry
# pyre-ignore[21]
from app.graphql.queries import AuthQuery
# pyre-ignore[21]
from app.graphql.mutations import AuthMutation

@strawberry.type
class Query(AuthQuery):
    pass

@strawberry.type
class Mutation(AuthMutation):
    pass

schema = strawberry.Schema(query=Query, mutation=Mutation)
