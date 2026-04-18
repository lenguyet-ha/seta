import re
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from app.core.config import settings

router = APIRouter()

ROUTING_MAP: dict[str, str] = {
    "login": settings.AUTH_SERVICE_URL,
    "me": settings.AUTH_SERVICE_URL,
    "changePassword": settings.AUTH_SERVICE_URL,
    
    "userById": settings.USER_SERVICE_URL,
    "listUsers": settings.USER_SERVICE_URL,
    "createUser": settings.USER_SERVICE_URL,
    "updateUser": settings.USER_SERVICE_URL,
}

FIELD_PATTERN = re.compile(
    r"(?:query|mutation|subscription)?\s*"
    r"(?:\w+\s*)?"
    r"\{\s*"
    r"(\w+)"
    r"\s*",
    re.IGNORECASE,
)

def _extract_first_field(query: str) -> str | None:
    match = FIELD_PATTERN.search(query)
    if match:
        field = match.group(1)
        return field
    return None

def _resolve_service_url(field_name: str) -> str:
    url = ROUTING_MAP.get(field_name)
    if url is None:
        raise HTTPException(status_code=400, detail=f"Unknown field '{field_name}' in query")
    return url

_http_client: httpx.AsyncClient | None = None

async def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=settings.PROXY_TIMEOUT)
    return _http_client

async def close_http_client():
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None
    
@router.post("/graphql")
async def proxy_graphql(request: Request) -> JSONResponse:
    body = await request.json()
    query = body.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Missing 'query' in request body")
    
    field_name = _extract_first_field(query)
    if not field_name:
        raise HTTPException(status_code=400, detail="Could not extract field from query")
    
    service_url = _resolve_service_url(field_name)
    target_url = f"{service_url}/graphql"
    
    forward_headers = {"Content-Type": "application/json"}
    auth_header = request.headers.get("Authorization")
    if auth_header:
        forward_headers["Authorization"] = auth_header
    
    try:
        client = await get_http_client()
        response = await client.post(
            target_url,
            json=body,
            headers=forward_headers,
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Error connecting to service: {str(e)}")
    
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Service returned error: {str(e)}")
    
    
    return JSONResponse(content=response.json(), status_code=response.status_code)