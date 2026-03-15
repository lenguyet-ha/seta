from typing import Optional, Any
from dataclasses import dataclass
from .directives import AuthUser, get_token_decoder


@dataclass
class GraphQLContext:
    """Standard GraphQL context for all services"""
    request: Any
    auth_user: Optional[AuthUser] = None
    db: Optional[Any] = None
    
    @classmethod
    async def create(cls, request, db=None):
        """Create context with automatic token decoding"""
        auth_user = None
        
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            try:
                decoder = get_token_decoder()
                auth_user = decoder.decode(token)
            except RuntimeError:
                pass  # Auth not configured
        
        return cls(
            request=request,
            auth_user=auth_user,
            db=db,
        )