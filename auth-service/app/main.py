from fastapi import FastAPI, Depends, Request
from strawberry.fastapi import GraphQLRouter
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.database import Base, engine, get_db
from app.graphql.schema import schema
from seta_shared.graphql import configure_auth, JWTTokenDecoder

Base.metadata.create_all(bind=engine)

app = FastAPI(title=settings.PROJECT_NAME)

configure_auth(JWTTokenDecoder(
    secret_key=settings.SECRET_KEY,
    algorithm="HS256"
))

async def get_context(request: Request, db: Session = Depends(get_db)):
    return {
        "request": request,
        "db": db,
        "auth_user": None
    }

graphql_app = GraphQLRouter(schema, context_getter=get_context)
app.include_router(graphql_app, prefix="/graphql")

@app.get("/")
def read_root():
    return {"message": f"Welcome to {settings.PROJECT_NAME}"}