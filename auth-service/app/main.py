from fastapi import FastAPI, Depends, Request
from strawberry.fastapi import GraphQLRouter
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.database import Base, engine, get_db
from app.graphql.schema import schema
from seta_shared.graphql import configure_auth, JWTTokenDecoder
from seta_shared.kafka import (
    init_kafka_producer,
    KafkaConsumerService,
    Topics,
)
from contextlib import asynccontextmanager
import asyncio
from app.handlers.auth_handlers import register_auth_handlers


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    
    # Start Kafka producer
    producer = init_kafka_producer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        client_id="auth-service-producer"
    )
    await producer.start()

    # Start Kafka consumer to listen for user events
    consumer = KafkaConsumerService(
        topics=[Topics.USER_EVENTS],
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="auth-service-group",
        client_id="auth-service-consumer",
    )
    register_auth_handlers(consumer)
    await consumer.start()
    
    consumer_task = asyncio.create_task(consumer.consume())
    yield
    
    await consumer.stop()
    consumer_task.cancel()
    await producer.stop()

app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan)

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


@app.get("/health")
def health():
    return {"status": "ok"}