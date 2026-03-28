"""
================================================================================
SETA KAFKA SHARED PACKAGE - FULL CODE
================================================================================

This file contains all the code needed to implement Kafka in your microservices.

FOLDER STRUCTURE:
-----------------
shared/
└── seta_shared/
    └── kafka/
        ├── __init__.py
        ├── events.py
        ├── producer.py
        └── consumer.py

user-service/
└── app/
    ├── main.py
    ├── handlers/
    │   ├── __init__.py
    │   └── user_handlers.py
    └── services/
        └── user_service.py

================================================================================
"""


# ==============================================================================
# FILE: shared/seta_shared/kafka/__init__.py
# ==============================================================================

"""
from .producer import KafkaProducerService, kafka_producer, init_kafka_producer
from .consumer import KafkaConsumerService
from .events import Topics, UserEvents, AuthEvents

__all__ = [
    # Producer
    "KafkaProducerService",
    "kafka_producer",
    "init_kafka_producer",
    # Consumer
    "KafkaConsumerService",
    # Events
    "Topics",
    "UserEvents",
    "AuthEvents",
]
"""


# ==============================================================================
# FILE: shared/seta_shared/kafka/events.py
# ==============================================================================

"""
Kafka Topics and Event Types
Shared across all microservices
"""


class Topics:
    """Kafka topic names"""
    USER_EVENTS = "user-events"
    AUTH_EVENTS = "auth-events"
    LOGS = "logs"
    NOTIFICATIONS = "notifications"


class UserEvents:
    """User service event types"""
    # Command events (requests)
    CREATE_REQUESTED = "USER_CREATE_REQUESTED"
    UPDATE_REQUESTED = "USER_UPDATE_REQUESTED"
    DELETE_REQUESTED = "USER_DELETE_REQUESTED"
    
    # Result events (success)
    CREATED = "USER_CREATED"
    UPDATED = "USER_UPDATED"
    DELETED = "USER_DELETED"
    ACTIVATED = "USER_ACTIVATED"
    DEACTIVATED = "USER_DEACTIVATED"
    
    # Error events
    CREATE_FAILED = "USER_CREATE_FAILED"
    UPDATE_FAILED = "USER_UPDATE_FAILED"
    DELETE_FAILED = "USER_DELETE_FAILED"


class AuthEvents:
    """Auth service event types"""
    # Command events
    REGISTER_REQUESTED = "AUTH_REGISTER_REQUESTED"
    
    # Result events
    REGISTERED = "AUTH_REGISTERED"
    LOGIN = "USER_LOGIN"
    LOGOUT = "USER_LOGOUT"
    PASSWORD_CHANGED = "PASSWORD_CHANGED"
    TOKEN_REFRESHED = "TOKEN_REFRESHED"
    
    # Error events
    REGISTER_FAILED = "AUTH_REGISTER_FAILED"
    LOGIN_FAILED = "LOGIN_FAILED"


# ==============================================================================
# FILE: shared/seta_shared/kafka/producer.py
# ==============================================================================

"""
Kafka Producer Service
Async producer using aiokafka
"""

import json
import logging
from typing import Optional, Any
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaProducerService:
    """
    Async Kafka Producer wrapper
    
    Usage:
        producer = KafkaProducerService("kafka:9092")
        await producer.start()
        await producer.send_event("user-events", "USER_CREATED", {...})
        await producer.stop()
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        client_id: str = "seta-producer",
    ):
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._producer: Optional[AIOKafkaProducer] = None
        self._started = False

    @property
    def is_connected(self) -> bool:
        return self._started and self._producer is not None

    async def start(self) -> None:
        """Initialize and connect to Kafka broker"""
        if self._started:
            logger.warning("Producer already started")
            return

        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                client_id=self._client_id,
                value_serializer=self._serialize_value,
                key_serializer=self._serialize_key,
                acks="all",  # Wait for all replicas
                enable_idempotence=True,  # Exactly-once semantics
                max_batch_size=16384,
                linger_ms=10,  # Batch messages for 10ms
                compression_type="gzip",
            )
            await self._producer.start()
            self._started = True
            logger.info(f"Kafka Producer connected to {self._bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to start Kafka Producer: {e}")
            raise

    async def stop(self) -> None:
        """Gracefully shutdown producer"""
        if self._producer and self._started:
            await self._producer.stop()
            self._started = False
            logger.info("Kafka Producer stopped")

    async def send_event(
        self,
        topic: str,
        event_type: str,
        data: dict,
        key: Optional[str] = None,
        headers: Optional[dict] = None,
    ) -> None:
        """
        Send event to Kafka topic
        
        Args:
            topic: Topic name (e.g., "user-events")
            event_type: Event type string (e.g., "USER_CREATED")
            data: Event payload dictionary
            key: Optional partition key (messages with same key go to same partition)
            headers: Optional message headers
        
        Raises:
            RuntimeError: If producer not started
            KafkaError: If send fails
        """
        if not self.is_connected:
            raise RuntimeError("Kafka Producer not started. Call start() first.")

        message = self._build_message(event_type, data)
        kafka_headers = self._build_headers(headers)

        try:
            await self._producer.send_and_wait(
                topic=topic,
                value=message,
                key=key,
                headers=kafka_headers,
            )
            logger.info(f"[Kafka] Sent {event_type} to {topic}")
        except KafkaError as e:
            logger.error(f"[Kafka] Failed to send {event_type}: {e}")
            raise

    async def send_batch(
        self,
        topic: str,
        events: list[dict],
    ) -> None:
        """
        Send multiple events to same topic
        
        Args:
            topic: Topic name
            events: List of {"event_type": str, "data": dict, "key": str}
        """
        if not self.is_connected:
            raise RuntimeError("Kafka Producer not started")

        batch = self._producer.create_batch()
        
        for event in events:
            message = self._build_message(event["event_type"], event["data"])
            serialized_value = self._serialize_value(message)
            serialized_key = self._serialize_key(event.get("key"))
            
            metadata = batch.append(
                key=serialized_key,
                value=serialized_value,
                timestamp=None,
            )
            if metadata is None:
                # Batch is full, send and create new
                await self._producer.send_batch(batch, topic)
                batch = self._producer.create_batch()
                batch.append(key=serialized_key, value=serialized_value, timestamp=None)
        
        # Send remaining
        if batch.record_count() > 0:
            await self._producer.send_batch(batch, topic)
        
        logger.info(f"[Kafka] Sent batch of {len(events)} events to {topic}")

    def _build_message(self, event_type: str, data: dict) -> dict:
        """Build standardized message envelope"""
        return {
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": "1.0",
        }

    def _build_headers(self, headers: Optional[dict]) -> Optional[list]:
        """Convert dict headers to Kafka format"""
        if not headers:
            return None
        return [(k, v.encode("utf-8")) for k, v in headers.items()]

    @staticmethod
    def _serialize_value(value: Any) -> bytes:
        """JSON serialize message value"""
        return json.dumps(value, default=str, ensure_ascii=False).encode("utf-8")

    @staticmethod
    def _serialize_key(key: Optional[str]) -> Optional[bytes]:
        """Encode partition key"""
        return key.encode("utf-8") if key else None


# Global singleton instance
kafka_producer: Optional[KafkaProducerService] = None


def init_kafka_producer(
    bootstrap_servers: str,
    client_id: str = "seta-producer",
) -> KafkaProducerService:
    """
    Initialize global Kafka producer singleton
    
    Args:
        bootstrap_servers: Kafka broker addresses
        client_id: Client identifier
    
    Returns:
        Configured KafkaProducerService instance
    """
    global kafka_producer
    kafka_producer = KafkaProducerService(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
    )
    return kafka_producer


def get_kafka_producer() -> KafkaProducerService:
    """Get global producer instance, raise if not initialized"""
    if kafka_producer is None:
        raise RuntimeError("Kafka producer not initialized. Call init_kafka_producer() first.")
    return kafka_producer


# ==============================================================================
# FILE: shared/seta_shared/kafka/consumer.py
# ==============================================================================

"""
Kafka Consumer Service
Async consumer using aiokafka with handler registration
"""

import json
import logging
import asyncio
from typing import Optional, Callable, Dict, Any, Awaitable

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)

# Type alias for event handlers
EventHandler = Callable[[dict], Awaitable[None]]


class KafkaConsumerService:
    """
    Async Kafka Consumer with event handler registration
    
    Usage:
        consumer = KafkaConsumerService(
            topics=["user-events"],
            bootstrap_servers="kafka:9092",
            group_id="user-service",
        )
        consumer.register_handler("USER_CREATED", handle_user_created)
        await consumer.start()
        await consumer.consume()  # Blocking loop
        await consumer.stop()
    """
    
    def __init__(
        self,
        topics: list[str],
        bootstrap_servers: str = "kafka:9092",
        group_id: str = "default-group",
        client_id: str = "seta-consumer",
        auto_offset_reset: str = "earliest",
    ):
        self._topics = topics
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._client_id = client_id
        self._auto_offset_reset = auto_offset_reset
        
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._handlers: Dict[str, EventHandler] = {}
        self._running = False
        self._started = False

    @property
    def is_connected(self) -> bool:
        return self._started and self._consumer is not None

    def register_handler(self, event_type: str, handler: EventHandler) -> None:
        """
        Register async handler for event type
        
        Args:
            event_type: Event type to handle (e.g., "USER_CREATED")
            handler: Async function(data: dict) -> None
        """
        self._handlers[event_type] = handler
        logger.info(f"Registered handler for {event_type}")

    def register_handlers(self, handlers: Dict[str, EventHandler]) -> None:
        """Register multiple handlers at once"""
        for event_type, handler in handlers.items():
            self.register_handler(event_type, handler)

    async def start(self) -> None:
        """Initialize and connect to Kafka broker"""
        if self._started:
            logger.warning("Consumer already started")
            return

        try:
            self._consumer = AIOKafkaConsumer(
                *self._topics,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                client_id=self._client_id,
                auto_offset_reset=self._auto_offset_reset,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                value_deserializer=self._deserialize_value,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            await self._consumer.start()
            self._started = True
            self._running = True
            logger.info(
                f"Kafka Consumer started | Topics: {self._topics} | Group: {self._group_id}"
            )
        except KafkaError as e:
            logger.error(f"Failed to start Kafka Consumer: {e}")
            raise

    async def stop(self) -> None:
        """Gracefully shutdown consumer"""
        self._running = False
        if self._consumer and self._started:
            await self._consumer.stop()
            self._started = False
            logger.info("Kafka Consumer stopped")

    async def consume(self) -> None:
        """
        Main consume loop - runs until stop() is called
        This is a blocking operation, run in background task
        """
        if not self.is_connected:
            raise RuntimeError("Consumer not started. Call start() first.")

        logger.info("Starting consume loop...")
        
        try:
            async for message in self._consumer:
                if not self._running:
                    break

                await self._process_message(message)
                
        except asyncio.CancelledError:
            logger.info("Consume loop cancelled")
        except Exception as e:
            logger.error(f"Consume loop error: {e}")
            raise

    async def consume_one(self, timeout_ms: int = 1000) -> Optional[dict]:
        """
        Consume single message (for testing/manual processing)
        
        Returns:
            Message dict or None if timeout
        """
        if not self.is_connected:
            raise RuntimeError("Consumer not started")

        data = await self._consumer.getone()
        return data.value if data else None

    async def _process_message(self, message: Any) -> None:
        """Process single Kafka message"""
        try:
            event = message.value
            if not event:
                logger.warning(f"Empty message from {message.topic}")
                return

            event_type = event.get("event_type")
            data = event.get("data", {})
            timestamp = event.get("timestamp")

            logger.info(
                f"[Kafka] Received {event_type} from {message.topic} "
                f"(partition={message.partition}, offset={message.offset})"
            )

            # Find and call handler
            handler = self._handlers.get(event_type)
            if handler:
                try:
                    await handler(data)
                    logger.info(f"[Kafka] Processed {event_type}")
                except Exception as e:
                    logger.error(f"[Kafka] Handler error for {event_type}: {e}")
                    # TODO: Send to dead-letter queue
            else:
                logger.debug(f"[Kafka] No handler registered for {event_type}")

        except Exception as e:
            logger.error(f"[Kafka] Error processing message: {e}")

    @staticmethod
    def _deserialize_value(value: bytes) -> dict:
        """JSON deserialize message value"""
        return json.loads(value.decode("utf-8"))


class KafkaConsumerManager:
    """
    Manager for multiple consumers (if service needs to consume from multiple topic groups)
    """
    
    def __init__(self):
        self._consumers: Dict[str, KafkaConsumerService] = {}
        self._tasks: Dict[str, asyncio.Task] = {}

    def add_consumer(self, name: str, consumer: KafkaConsumerService) -> None:
        """Add consumer to manager"""
        self._consumers[name] = consumer

    async def start_all(self) -> None:
        """Start all consumers"""
        for name, consumer in self._consumers.items():
            await consumer.start()
            self._tasks[name] = asyncio.create_task(
                consumer.consume(),
                name=f"consumer-{name}",
            )
            logger.info(f"Started consumer: {name}")

    async def stop_all(self) -> None:
        """Stop all consumers"""
        for name, consumer in self._consumers.items():
            await consumer.stop()
            if name in self._tasks:
                self._tasks[name].cancel()
        self._consumers.clear()
        self._tasks.clear()
        logger.info("All consumers stopped")


# ==============================================================================
# FILE: shared/pyproject.toml
# ==============================================================================

"""
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "seta_shared"
version = "0.1.0"
description = "Shared utilities for SETA microservices"
requires-python = ">=3.9"
dependencies = [
    "strawberry-graphql[fastapi]>=0.220.0",
    "python-jose[cryptography]>=3.3.0",
    "pydantic>=2.0.0",
    "aiokafka>=0.10.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
]

[tool.setuptools.packages.find]
where = ["."]
include = ["seta_shared*", "constants*"]
"""


# ==============================================================================
# FILE: user-service/app/handlers/__init__.py
# ==============================================================================

"""
from .user_handlers import register_user_handlers

__all__ = ["register_user_handlers"]
"""


# ==============================================================================
# FILE: user-service/app/handlers/user_handlers.py
# ==============================================================================

"""
from sqlalchemy.orm import Session
from app.core.database import SessionLocal
from app.models.user import User
from seta_shared.kafka import (
    KafkaConsumerService,
    kafka_producer,
    Topics,
    UserEvents,
)


async def handle_create_user_requested(data: dict) -> None:
    '''Handle USER_CREATE_REQUESTED event'''
    db: Session = SessionLocal()
    request_id = data.get("request_id")
    
    try:
        # Check duplicate
        existing = db.query(User).filter(
            (User.username == data["username"]) |
            (User.email == data["email"])
        ).first()
        
        if existing:
            await kafka_producer.send_event(
                topic=Topics.USER_EVENTS,
                event_type=UserEvents.CREATE_FAILED,
                data={
                    "request_id": request_id,
                    "reason": "Username or email already exists",
                },
                key=data["email"],
            )
            return
        
        # Create user
        new_user = User(
            username=data["username"],
            email=data["email"],
            role=data.get("role", "user"),
            is_active=True,
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        
        # Emit success event
        await kafka_producer.send_event(
            topic=Topics.USER_EVENTS,
            event_type=UserEvents.CREATED,
            data={
                "request_id": request_id,
                "user_id": new_user.id,
                "username": new_user.username,
                "email": new_user.email,
                "role": new_user.role,
            },
            key=str(new_user.id),
        )
        
    except Exception as e:
        db.rollback()
        await kafka_producer.send_event(
            topic=Topics.USER_EVENTS,
            event_type=UserEvents.CREATE_FAILED,
            data={
                "request_id": request_id,
                "reason": str(e),
            },
        )
    finally:
        db.close()


async def handle_update_user_requested(data: dict) -> None:
    '''Handle USER_UPDATE_REQUESTED event'''
    db: Session = SessionLocal()
    request_id = data.get("request_id")
    user_id = data.get("user_id")
    
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            await kafka_producer.send_event(
                topic=Topics.USER_EVENTS,
                event_type=UserEvents.UPDATE_FAILED,
                data={"request_id": request_id, "reason": "User not found"},
            )
            return
        
        # Update fields
        if "username" in data:
            user.username = data["username"]
        if "role" in data:
            user.role = data["role"]
        if "is_active" in data:
            user.is_active = data["is_active"]
        
        db.commit()
        db.refresh(user)
        
        await kafka_producer.send_event(
            topic=Topics.USER_EVENTS,
            event_type=UserEvents.UPDATED,
            data={
                "request_id": request_id,
                "user_id": user.id,
                "username": user.username,
            },
            key=str(user.id),
        )
        
    except Exception as e:
        db.rollback()
        await kafka_producer.send_event(
            topic=Topics.USER_EVENTS,
            event_type=UserEvents.UPDATE_FAILED,
            data={"request_id": request_id, "reason": str(e)},
        )
    finally:
        db.close()


def register_user_handlers(consumer: KafkaConsumerService) -> None:
    '''Register all user event handlers'''
    consumer.register_handlers({
        UserEvents.CREATE_REQUESTED: handle_create_user_requested,
        UserEvents.UPDATE_REQUESTED: handle_update_user_requested,
    })
"""


# ==============================================================================
# FILE: user-service/app/main.py
# ==============================================================================

"""
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from seta_shared.kafka import (
    init_kafka_producer,
    KafkaConsumerService,
    Topics,
)
from app.core.config import settings
from app.core.database import engine, Base
from app.handlers import register_user_handlers


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    # Start Kafka Producer
    producer = init_kafka_producer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        client_id="user-service-producer",
    )
    await producer.start()
    
    # Start Kafka Consumer
    consumer = KafkaConsumerService(
        topics=[Topics.USER_EVENTS],
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="user-service-group",
        client_id="user-service-consumer",
    )
    register_user_handlers(consumer)
    await consumer.start()
    
    # Run consumer in background
    consumer_task = asyncio.create_task(consumer.consume())
    
    yield
    
    # Shutdown
    await consumer.stop()
    consumer_task.cancel()
    await producer.stop()


app = FastAPI(title="User Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "user-service"}
"""


# ==============================================================================
# FILE: user-service/app/services/user_service.py (API Layer - Emit Events)
# ==============================================================================

"""
import uuid
from seta_shared.kafka import kafka_producer, Topics, UserEvents
from app.graphql.types import CreateUserInput


async def request_create_user(input_data: CreateUserInput) -> dict:
    '''
    API layer - just emit event, don't touch DB
    '''
    request_id = str(uuid.uuid4())
    
    await kafka_producer.send_event(
        topic=Topics.USER_EVENTS,
        event_type=UserEvents.CREATE_REQUESTED,
        data={
            "request_id": request_id,
            "username": input_data.username,
            "email": input_data.email,
            "password": input_data.password,
            "role": input_data.role or "user",
        },
        key=input_data.email,
    )
    
    return {
        "request_id": request_id,
        "status": "PENDING",
        "message": "User creation request submitted",
    }


async def request_update_user(user_id: int, input_data: dict) -> dict:
    '''Emit update request'''
    request_id = str(uuid.uuid4())
    
    await kafka_producer.send_event(
        topic=Topics.USER_EVENTS,
        event_type=UserEvents.UPDATE_REQUESTED,
        data={
            "request_id": request_id,
            "user_id": user_id,
            **input_data,
        },
        key=str(user_id),
    )
    
    return {
        "request_id": request_id,
        "status": "PENDING",
    }
"""


# ==============================================================================
# FILE: user-service/Dockerfile
# ==============================================================================

"""
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install shared package FIRST
COPY shared/ /shared/
RUN pip install --no-cache-dir /shared/

# Copy and install service dependencies
COPY user-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY user-service/app ./app

EXPOSE 8002

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002"]
"""


# ==============================================================================
# FILE: docker-compose.yml (Updated build context)
# ==============================================================================

"""
# User Service
user-service:
  build:
    context: .                           # Root folder
    dockerfile: user-service/Dockerfile  # Specify dockerfile path
  ports:
    - "8002:8002"
  environment:
    - DATABASE_URL=postgresql://postgres:1@user-db:5432/user_db
    - REDIS_URL=redis://redis:6379
    - KAFKA_BOOTSTRAP_SERVERS=kafka:9093
  depends_on:
    user-db:
      condition: service_healthy
    redis:
      condition: service_healthy
    kafka:
      condition: service_healthy
  networks:
    - microservices-net
  restart: unless-stopped
"""


# ==============================================================================
# FILE: user-service/app/core/config.py (Add Kafka config)
# ==============================================================================

"""
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "User Service"
    DATABASE_URL: str = "postgresql://postgres@localhost:5434/postgres"
    AUTH_SERVICE_URL: str = "http://localhost:8001/graphql"
    REDIS_URL: str = "redis://localhost:6379"
    REDIS_CACHE_EXPIRE: int = 3600

    # Kafka Config
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
"""


# ==============================================================================
# QUICK START INSTRUCTIONS
# ==============================================================================

"""
1. Create folder structure:
   mkdir -p shared/seta_shared/kafka
   mkdir -p user-service/app/handlers

2. Copy each section above to its respective file

3. Update shared/pyproject.toml with aiokafka dependency

4. Update docker-compose.yml build context

5. Test locally:
   cd shared && pip install -e .
   cd ../user-service && pip install -r requirements.txt
   uvicorn app.main:app --reload --port 8002

6. Build with Docker:
   docker-compose up --build user-service
"""
