import strawberry
from strawberry.permission import BasePermission
from strawberry.types import Info
from typing import Any, List, Optional, Union
from jose import jwt, JWTError
from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass
class AuthUser:
    user_id: int
    role: str
    exp: int
    
    
class TokenDecoder(ABC):
    @abstractmethod
    def decode(self, token: str) -> Optional[AuthUser]:
        pass


class JWTTokenDecoder(TokenDecoder):
    def __init__(self, secret_key: str, algorithm: str = "HS256"):
        self.secret_key = secret_key
        self.algorithm = algorithm
    
    def decode(self, token: str) -> Optional[AuthUser]:
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            user_id = payload.get("user_id")
            role = payload.get("role", "member")
            exp = payload.get("exp")
            
            if user_id is None:
                return None
                
            return AuthUser(
                user_id=int(user_id),
                role=role,
                exp=exp
            )
        except JWTError:
            return None


_token_decoder: Optional[TokenDecoder] = None


def configure_auth(decoder: TokenDecoder):
    global _token_decoder
    _token_decoder = decoder


def get_token_decoder() -> TokenDecoder:
    if _token_decoder is None:
        raise RuntimeError("Auth not configured. Call configure_auth() first.")
    return _token_decoder


def get_auth_user_from_context(info: Info) -> Optional[AuthUser]:
    return info.context.get("auth_user")


def get_token_from_request(info: Info) -> Optional[str]:
    request = info.context.get("request")
    if not request:
        return None
    
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        print(f"DEBUG: Found auth header: {auth_header}")
        return auth_header[7:]
    return None


class IsAuthenticated(BasePermission):
    """
    Permission to check if user is authenticated.
    
    Usage:
        @strawberry.field(permission_classes=[IsAuthenticated])
        def protected_field(self) -> str:
            return "secret"
    """
    message = "Not authenticated"
    
    def has_permission(self, source: Any, info: Info, **kwargs) -> bool:
        # Try to get from context first (if middleware already decoded)
        auth_user = get_auth_user_from_context(info)
        print(f"DEBUG: Auth user from context: {auth_user}")
        
        if auth_user is None:
            token = get_token_from_request(info)
            if token:
                decoder = get_token_decoder()
                auth_user = decoder.decode(token)
                if auth_user:
                    info.context["auth_user"] = auth_user
        
        return auth_user is not None


class HasRole(BasePermission):
    """
    Permission to check if user has required role(s).
    
    Usage:
        @strawberry.field(permission_classes=[HasRole])
        def admin_only(self) -> str:
            return "admin secret"
    
    Or with custom roles:
        class AdminOnly(HasRole):
            allowed_roles = ["admin"]
    """
    message = "Permission denied"
    allowed_roles: List[str] = []
    
    def has_permission(self, source: Any, info: Info, **kwargs) -> bool:
        # First check authentication
        auth_user = get_auth_user_from_context(info)
        
        if auth_user is None:
            token = get_token_from_request(info)
            if token:
                decoder = get_token_decoder()
                auth_user = decoder.decode(token)
                if auth_user:
                    info.context["auth_user"] = auth_user
        
        if auth_user is None:
            self.message = "Not authenticated"
            return False
        
        if not self.allowed_roles:
            return True  
            
        if auth_user.role not in self.allowed_roles:
            self.message = f"Role '{auth_user.role}' not allowed. Required: {self.allowed_roles}"
            return False
            
        return True


def require_roles(*roles: str):
    """
    Factory function to create role-based permission class.
    
    Usage:
        @strawberry.field(permission_classes=[require_roles("admin", "manager")])
        def manager_field(self) -> str:
            return "managers only"
    """
    class RolePermission(HasRole):
        allowed_roles = list(roles)
        message = f"Required roles: {list(roles)}"
    
    return RolePermission


class IsAdmin(HasRole):
    allowed_roles = ["admin"]
    message = "Admin access required"


class IsManager(HasRole):
    allowed_roles = ["admin", "manager"]
    message = "Manager access required"


class IsMember(HasRole):
    allowed_roles = ["admin", "manager", "member"]
    message = "Member access required"