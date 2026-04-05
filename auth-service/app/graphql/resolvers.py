import strawberry
from strawberry.types import Info
from app.services import auth_service
from app.graphql.types import LoginInput, ChangePasswordInput, UserType, TokenType
from seta_shared.graphql.directives import get_auth_user_from_context

def get_db(info: Info):
    return info.context["db"]

def resolve_login(input: LoginInput, info: Info) -> TokenType:
    return auth_service.authenticate_user(get_db(info), input)

def resolve_change_password(input: ChangePasswordInput, info: Info) -> UserType:
    auth_user = get_auth_user_from_context(info)
    return auth_service.change_password(get_db(info), auth_user.user_id, input)

def resolve_me(info: Info) -> UserType:
    auth_user = get_auth_user_from_context(info)
    return auth_service.get_user_by_id(get_db(info), auth_user.user_id)
