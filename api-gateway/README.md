# API Gateway — SETA Microservices

## Mục lục

- [1. Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
- [2. Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
- [3. Routing Table](#3-routing-table)
- [4. Source Code](#4-source-code)
- [5. Docker](#5-docker)
- [6. Cách chạy](#6-cách-chạy)
- [7. Ví dụ Request / Response](#7-ví-dụ-request--response)
- [8. Thêm Service mới](#8-thêm-service-mới)
- [9. Troubleshooting](#9-troubleshooting)

---

## 1. Tổng quan kiến trúc

```
                         ┌─────────────────────────┐
                         │        Client            │
                         │  (Web App / Mobile App)   │
                         └───────────┬───────────────┘
                                     │
                              POST /graphql
                                     │
                         ┌───────────▼───────────────┐
                         │      API Gateway          │
                         │     (FastAPI :8000)        │
                         │                           │
                         │  1. Nhận GraphQL request   │
                         │  2. Parse field names      │
                         │  3. Lookup routing map     │
                         │  4. Forward request (httpx)│
                         │  5. Trả response về client │
                         └─────┬─────────────┬───────┘
                               │             │
                 ┌─────────────▼──┐   ┌──────▼──────────────┐
                 │  auth-service  │   │   user-service      │
                 │  :8001/graphql │   │   :8002/graphql     │
                 │                │   │                     │
                 │  • login       │   │  • userById         │
                 │  • me          │   │  • listUsers        │
                 │  • changePwd   │   │  • createUser       │
                 │                │   │  • updateUser       │
                 └────────────────┘   └─────────────────────┘
```

### Flow chi tiết

1. **Client** gửi `POST /graphql` đến Gateway (port `8000`)
2. **Gateway** đọc `body.query`, dùng regex parse ra tên các **field** (VD: `login`, `me`, `listUsers`)
3. Tra **routing map** → xác định field đó thuộc service nào
4. **Forward** nguyên request (query + variables + headers) đến service backend qua `httpx.AsyncClient`
5. Service backend xử lý, trả JSON response
6. **Gateway** trả nguyên response về Client

> **Lưu ý**: Gateway là một **proxy thuần túy** — nó KHÔNG có GraphQL schema riêng,
> không validate query, không decode JWT. Mỗi service tự handle auth bằng `seta_shared.graphql.directives`.

---

## 2. Cấu trúc thư mục

```
api-gateway/
├── Dockerfile
├── requirements.txt
└── app/
    ├── __init__.py
    ├── main.py              # FastAPI app + mount routes
    └── core/
        ├── __init__.py
        ├── config.py         # Pydantic Settings (env vars)
        └── router.py         # Logic parse + route GraphQL
```

---

## 3. Routing Table

| GraphQL Field     | Type     | Route đến           | Mô tả                        |
|-------------------|----------|----------------------|-------------------------------|
| `login`           | Mutation | `auth-service:8001`  | Đăng nhập, trả JWT token     |
| `me`              | Query    | `auth-service:8001`  | Thông tin user đang đăng nhập |
| `changePassword`  | Mutation | `auth-service:8001`  | Đổi mật khẩu (cần auth)      |
| `userById`        | Query    | `user-service:8002`  | Lấy user theo ID              |
| `listUsers`       | Query    | `user-service:8002`  | Danh sách users (phân trang)  |
| `createUser`      | Mutation | `user-service:8002`  | Tạo user mới                  |
| `updateUser`      | Mutation | `user-service:8002`  | Cập nhật thông tin user       |

---

## 4. Source Code

### 4.1. `requirements.txt`

```txt
fastapi==0.104.1
uvicorn==0.24.0
httpx==0.25.1
pydantic-settings==2.1.0
```

---

### 4.2. `app/__init__.py`

```python
# empty
```

---

### 4.3. `app/core/__init__.py`

```python
# empty
```

---

### 4.4. `app/core/config.py`

```python
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Cấu hình API Gateway.
    Đọc từ environment variables, fallback về giá trị mặc định (dev).
    """

    PROJECT_NAME: str = "SETA API Gateway"

    # URL các backend services (dùng tên Docker container khi chạy trong Docker)
    AUTH_SERVICE_URL: str = "http://localhost:8001"
    USER_SERVICE_URL: str = "http://localhost:8002"

    # Timeout cho request tới backend (giây)
    PROXY_TIMEOUT: float = 30.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
```

**Environment Variables**:

| Variable           | Mặc định                   | Mô tả                          |
|--------------------|----------------------------|---------------------------------|
| `AUTH_SERVICE_URL`  | `http://localhost:8001`    | URL auth-service                |
| `USER_SERVICE_URL`  | `http://localhost:8002`    | URL user-service                |
| `PROXY_TIMEOUT`     | `30.0`                    | Timeout (giây) cho proxy request|

---

### 4.5. `app/core/router.py` ⭐ (file chính)

```python
"""
GraphQL Proxy Router
====================
Parse incoming GraphQL request → xác định service → forward request.
"""

import re
import httpx
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from app.core.config import settings

router = APIRouter()

# ============================================================
# ROUTING MAP
# Key   = tên GraphQL field (camelCase, đúng như trong schema)
# Value = base URL của service backend
# ============================================================
ROUTING_MAP: dict[str, str] = {
    # auth-service operations
    "login":          settings.AUTH_SERVICE_URL,
    "me":             settings.AUTH_SERVICE_URL,
    "changePassword": settings.AUTH_SERVICE_URL,

    # user-service operations
    "userById":       settings.USER_SERVICE_URL,
    "listUsers":      settings.USER_SERVICE_URL,
    "createUser":     settings.USER_SERVICE_URL,
    "updateUser":     settings.USER_SERVICE_URL,
}

# Regex để extract tên field từ GraphQL query string.
# Match pattern: query/mutation (optional name) { fieldName
# Ví dụ: "mutation Login { login(input: ...) { ... } }" → capture "login"
#         "{ me { userId } }"                            → capture "me"
#         "query { listUsers(...) { ... } }"             → capture "listUsers"
FIELD_PATTERN = re.compile(
    r"(?:query|mutation|subscription)?\s*"  # optional operation type
    r"(?:\w+\s*)?"                          # optional operation name
    r"\{\s*"                                # opening brace
    r"(\w+)",                               # ← capture first field name
    re.IGNORECASE,
)


def _extract_first_field(query: str) -> str | None:
    """
    Trích xuất tên field đầu tiên từ GraphQL query string.

    Ví dụ:
        "mutation { login(input: {...}) { accessToken } }"  → "login"
        "{ me { userId email } }"                           → "me"
        "query GetUsers { listUsers(params: {...}) { id } }" → "listUsers"
    """
    match = FIELD_PATTERN.search(query)
    if match:
        field = match.group(1)
        # Xử lý edge case: nếu capture được "query"/"mutation" thì thử lại
        if field.lower() in ("query", "mutation", "subscription"):
            # Tìm field thực ở sau
            rest = query[match.end():]
            inner_match = re.search(r"\{\s*(\w+)", rest)
            if inner_match:
                return inner_match.group(1)
        return field
    return None


def _resolve_service_url(field_name: str) -> str:
    """
    Tra routing map để tìm service URL từ field name.
    Raise 400 nếu field không có trong map.
    """
    url = ROUTING_MAP.get(field_name)
    if url is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "UNKNOWN_OPERATION",
                "message": f"Không tìm thấy service cho field '{field_name}'.",
                "known_fields": list(ROUTING_MAP.keys()),
            },
        )
    return url


# ============================================================
# Shared httpx client — reuse connection pool
# ============================================================
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


# ============================================================
# MAIN ENDPOINT
# ============================================================
@router.post("/graphql")
async def proxy_graphql(request: Request):
    """
    Entry point duy nhất cho mọi GraphQL request.

    Flow:
        1. Đọc JSON body (query, variables, operationName)
        2. Parse field name từ query string
        3. Tra routing map → service URL
        4. Forward request đến service backend
        5. Trả response về client
    """

    # --- 1. Parse request body ---

    
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    query = body.get("query", "")
    if not query:
        raise HTTPException(status_code=400, detail="Missing 'query' in request body")

    # --- 2. Extract field name ---
    field_name = _extract_first_field(query)
    if not field_name:
        raise HTTPException(
            status_code=400,
            detail="Không thể parse field name từ GraphQL query.",
        )

    # --- 3. Resolve service URL ---
    service_url = _resolve_service_url(field_name)
    target_url = f"{service_url}/graphql"

    # --- 4. Build headers (forward Authorization) ---
    forward_headers = {"Content-Type": "application/json"}
    auth_header = request.headers.get("Authorization")
    if auth_header:
        forward_headers["Authorization"] = auth_header

    # --- 5. Forward request ---
    try:
        client = await get_http_client()
        response = await client.post(
            target_url,
            json=body,
            headers=forward_headers,
        )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=502,
            detail=f"Không thể kết nối đến service: {service_url}",
        )
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail=f"Service timeout: {service_url}",
        )

    # --- 6. Return response ---
    return JSONResponse(
        content=response.json(),
        status_code=response.status_code,
    )


# ============================================================
# HEALTH CHECK
# ============================================================
@router.get("/health")
async def health_check():
    """Health check endpoint cho Docker / load balancer."""
    return {"status": "healthy", "service": "api-gateway"}
```

**Giải thích logic routing**:

```
Client gửi:
{
  "query": "mutation { login(input: {username: \"ha\", password: \"123\"}) { accessToken } }"
}

Gateway parse:
  1. Regex match → field_name = "login"
  2. ROUTING_MAP["login"] → "http://auth-service:8001"
  3. Forward POST đến http://auth-service:8001/graphql với body nguyên bản
  4. auth-service xử lý, trả JSON
  5. Gateway trả JSON đó về client
```

---

### 4.6. `app/main.py`

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.core.config import settings
from app.core.router import router, close_http_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown events."""
    yield
    # Cleanup httpx client khi shutdown
    await close_http_client()


app = FastAPI(
    title=settings.PROJECT_NAME,
    lifespan=lifespan,
)

# --- CORS (cho phép frontend gọi API) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # Production: đổi thành domain cụ thể
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Mount router ---
app.include_router(router)
```

---

## 5. Docker

### 5.1. `Dockerfile`

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### 5.2. `docker-compose.yml` (phần api-gateway)

```yaml
  api-gateway:
    build: ./api-gateway
    ports:
      - "8000:8000"
    environment:
      - AUTH_SERVICE_URL=http://auth-service:8001
      - USER_SERVICE_URL=http://user-service:8002
    depends_on:
      - auth-service
      - user-service
    networks:
      - microservices-net
    restart: unless-stopped
```

> Phần này đã có sẵn trong `docker-compose.yml` gốc, chỉ cần đảm bảo Dockerfile đúng.

---

## 6. Cách chạy

### 6.1. Chạy local (development)

```bash
# Từ thư mục api-gateway/
cd api-gateway

# Cài dependencies
pip install -r requirements.txt

# Set environment variables (nếu services chạy local)
export AUTH_SERVICE_URL=http://localhost:8001
export USER_SERVICE_URL=http://localhost:8002

# Chạy
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### 6.2. Chạy với Docker Compose (toàn bộ hệ thống)

```bash
# Từ thư mục root (seta/)
docker compose up --build
```

### 6.3. Chỉ build api-gateway

```bash
docker compose build api-gateway
docker compose up api-gateway
```

---

## 7. Ví dụ Request / Response

### 7.1. Login (→ auth-service)

**Request**:
```bash
curl -X POST http://localhost:8000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { login(input: { username: \"admin\", password: \"123456\" }) { accessToken tokenType userId role } }"
  }'
```

**Response**:
```json
{
  "data": {
    "login": {
      "accessToken": "eyJhbGciOiJIUzI1NiIs...",
      "tokenType": "bearer",
      "userId": 1,
      "role": "admin"
    }
  }
}
```

### 7.2. Get current user — `me` (→ auth-service, cần auth)

**Request**:
```bash
curl -X POST http://localhost:8000/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIs..." \
  -d '{
    "query": "{ me { userId username email role isActive } }"
  }'
```

**Response**:
```json
{
  "data": {
    "me": {
      "userId": 1,
      "username": "admin",
      "email": "admin@seta.com",
      "role": "admin",
      "isActive": true
    }
  }
}
```

### 7.3. List users (→ user-service)

**Request**:
```bash
curl -X POST http://localhost:8000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ listUsers(params: { page: 1, limit: 5 }) { id username email role } }"
  }'
```

**Response**:
```json
{
  "data": {
    "listUsers": [
      { "id": 1, "username": "admin", "email": "admin@seta.com", "role": "admin" },
      { "id": 2, "username": "user1", "email": "user1@seta.com", "role": "member" }
    ]
  }
}
```

### 7.4. Change password (→ auth-service, cần auth)

**Request**:
```bash
curl -X POST http://localhost:8000/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIs..." \
  -d '{
    "query": "mutation { changePassword(input: { oldPassword: \"123456\", newPassword: \"654321\" }) { userId username } }"
  }'
```

### 7.5. Health check

```bash
curl http://localhost:8000/health
# → {"status": "healthy", "service": "api-gateway"}
```

---

## 8. Thêm Service mới

Khi thêm một service mới (VD: `notification-service` ở port `8003`), chỉ cần **2 bước**:

### Bước 1: Thêm URL vào `config.py`

```python
class Settings(BaseSettings):
    # ... existing ...
    NOTIFICATION_SERVICE_URL: str = "http://notification-service:8003"
```

### Bước 2: Thêm fields vào `ROUTING_MAP` trong `router.py`

```python
ROUTING_MAP: dict[str, str] = {
    # ... existing ...

    # notification-service operations
    "sendNotification":  settings.NOTIFICATION_SERVICE_URL,
    "listNotifications": settings.NOTIFICATION_SERVICE_URL,
}
```

**Không cần sửa thêm gì khác.** Gateway sẽ tự động route.

### Bước 3: Thêm vào `docker-compose.yml`

```yaml
environment:
  - NOTIFICATION_SERVICE_URL=http://notification-service:8003
depends_on:
  - notification-service
```

---

## 9. Troubleshooting

### 9.1. Lỗi `UNKNOWN_OPERATION`

```json
{
  "detail": {
    "error": "UNKNOWN_OPERATION",
    "message": "Không tìm thấy service cho field 'xyz'.",
    "known_fields": ["login", "me", "changePassword", ...]
  }
}
```

**Nguyên nhân**: Field name trong query không có trong `ROUTING_MAP`.

**Xử lý**: Kiểm tra tên field có đúng camelCase, đúng chính tả không. Nếu là field mới → thêm vào `ROUTING_MAP`.

---

### 9.2. Lỗi `502 Bad Gateway`

```json
{ "detail": "Không thể kết nối đến service: http://auth-service:8001" }
```

**Nguyên nhân**: Service backend chưa start hoặc không reachable.

**Xử lý**:
```bash
# Kiểm tra service có đang chạy không
docker compose ps

# Xem logs service
docker compose logs auth-service

# Kiểm tra network
docker compose exec api-gateway ping auth-service
```

---

### 9.3. Lỗi `504 Gateway Timeout`

**Nguyên nhân**: Service backend xử lý quá lâu (> `PROXY_TIMEOUT`).

**Xử lý**: Tăng `PROXY_TIMEOUT` trong environment:
```yaml
environment:
  - PROXY_TIMEOUT=60.0
```

---

### 9.4. Authorization không hoạt động

**Kiểm tra**: Gateway có forward header `Authorization` không?

Thêm log tạm vào `router.py`:
```python
print(f"[GATEWAY] field={field_name} → {target_url}")
print(f"[GATEWAY] Authorization: {auth_header}")
```

Xem logs:
```bash
docker compose logs -f api-gateway
```

---

### 9.5. Bảng tóm tắt Error Codes

| HTTP Code | Ý nghĩa                    | Nguyên nhân thường gặp                    |
|-----------|-----------------------------|--------------------------------------------|
| `400`     | Bad Request                 | Query rỗng, JSON invalid, field không nhận diện được |
| `502`     | Bad Gateway                 | Backend service không kết nối được         |
| `504`     | Gateway Timeout             | Backend xử lý quá chậm                    |
| `401/403` | Unauthorized/Forbidden      | Do backend service trả về (JWT invalid/expired) |
