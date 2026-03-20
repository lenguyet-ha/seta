import strawberry
from app.graphql.queries import UserQuery
from app.graphql.mutations import UserMutation


@strawberry.type
class Query(UserQuery):
    pass

@strawberry.type
class Mutation(UserMutation):
    pass

schema = strawberry.Schema(query=Query, mutation=Mutation)
