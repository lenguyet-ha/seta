from fastapi import FastAPI, Request
from strawberry.fastapi import GraphQLRouter
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.database import Base, engine, get_db
from app.graphql.schema import schema

Base.metadata.create_all(bind=engine)
app = FastAPI(title=settings.PROJECT_NAME)

async def get_context(request: Request, db: Session = Depends(get_db)):
    return {
        "request": request,
        "db": db,
        "auth_user": None
    }
    
graphql_app = GraphQLRouter(schema=schema, context_getter=get_context)
app.include_router(graphql_app, prefix="/graphql")


@app.get("/")
def read_root():
    return {"message": f"Welcome to {settings.PROJECT_NAME}"}
