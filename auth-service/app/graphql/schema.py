import strawberry
from app.graphql.queries import AuthQuery
from app.graphql.mutations import AuthMutation

@strawberry.type
class Query(AuthQuery):
    pass

@strawberry.type
class Mutation(AuthMutation):
    pass

schema = strawberry.Schema(query=Query, mutation=Mutation)
