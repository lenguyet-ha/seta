from fastapi import FastAPI, Request
from strawberry.fastapi import GraphQLRouter
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.database import Base, engine, get_db
from app.graphql.schema import schema
from seta_shared.kafka import(
    init_kafka_producer,
    KafkaConsumerService,
    Topics,
)
from contextlib import asynccontextmanager
import asyncio
from . import register_user_handlers

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    
    producer = init_kafka_producer(
        bootstrap_servers=settings.KafkaBootstrapServers,
        client_id="user-service-producer"
    )
    await producer.start()

    consumer = KafkaConsumerService(
        topics=[Topics.USER_EVENTS],
        bootstrap_servers=settings.KafkaBootstrapServers,
        group_id="user-service-group",
        client_id="user-service-consumer",
    )
    register_user_handlers(consumer)
    await consumer.start()
    
    consumer_task = asyncio.create_task(consumer.consume())
    yield
    
    await consumer.stop()
    consumer_task.cancel()
    await producer.stop()
    
app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)
    
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
