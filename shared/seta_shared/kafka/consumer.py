import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)

EventHandler = Callable[[Dict[str, Any]], Awaitable[None]]


class KafkaConsumerService:
    def __init__(
        self,
        topics: list[str],
        bootstrap_servers: str = "kafka:9092",
        group_id: str = "default-group",
        client_id: str = "seta-consumer",
        auto_offset_reset: str = "earliest",
    ) -> None:
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
        self._handlers[event_type] = handler
        logger.info("Registered handler for event_type=%s", event_type)

    def register_handlers(self, handlers: Dict[str, EventHandler]) -> None:
        for event_type, handler in handlers.items():
            self.register_handler(event_type, handler)

    async def start(self) -> None:
        if self._started:
            logger.warning("Consumer is already started")
            return

        try:
            self._consumer = AIOKafkaConsumer(
                *self._topics,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                client_id=self._client_id,
                auto_offset_reset=self._auto_offset_reset,
                value_deserializer=self._deserialize_value,
                enable_auto_commit=True,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000,
                auto_commit_interval_ms=5000,
            )
            await self._consumer.start()
            self._started = True
            self._running = True
            logger.info(
                "Kafka consumer started | topics=%s group_id=%s",
                self._topics,
                self._group_id,
            )
        except KafkaError as e:
            logger.error("Failed to start Kafka consumer: %s", e)
            raise

    async def stop(self) -> None:
        self._running = False
        if self._consumer and self._started:
            await self._consumer.stop()
            self._started = False
            logger.info("Kafka consumer stopped successfully")

    async def consume(self) -> None:
        if not self.is_connected:
            raise RuntimeError("Consumer is not connected")

        logger.info("Starting Kafka consume loop")

        try:
            async for msg in self._consumer:
                if not self._running:
                    break
                await self._process_message(msg)
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled")
        except KafkaError as e:
            logger.error("Error while consuming messages: %s", e)
            raise

    async def consume_one(self, timeout_ms: int = 1000) -> Optional[Dict[str, Any]]:
        if not self.is_connected:
            raise RuntimeError("Consumer is not connected")

        msg = await self._consumer.getone(timeout_ms=timeout_ms)
        return msg.value if msg else None

    async def _process_message(self, msg: Any) -> None:
        try:
            event = msg.value
            if not event:
                logger.warning("Received empty message, skipping")
                return

            event_type = event.get("event_type")
            data = event.get("data", {})
            timestamp = event.get("timestamp")

            logger.info(
                '[Kafka] Received event topic="%s" event_type="%s" partition=%s offset=%s timestamp=%s',
                msg.topic,
                event_type,
                msg.partition,
                msg.offset,
                timestamp,
            )

            handler = self._handlers.get(event_type)
            if handler:
                await handler(data)
            else:
                logger.warning("No handler registered for event_type=%s", event_type)

        except Exception as e:
            logger.error("Error processing message: %s", e)

    @staticmethod
    def _deserialize_value(value: bytes) -> Dict[str, Any]:
        return json.loads(value.decode("utf-8"))


class KafkaConsumerManager:
    def __init__(self) -> None:
        self._consumers: Dict[str, KafkaConsumerService] = {}
        self._tasks: Dict[str, asyncio.Task] = {}

    def add_consumer(self, name: str, consumer: KafkaConsumerService) -> None:
        self._consumers[name] = consumer

    async def start_all(self) -> None:
        for name, consumer in self._consumers.items():
            await consumer.start()
            self._tasks[name] = asyncio.create_task(
                consumer.consume(),
                name=f"kafka-consumer-{name}",
            )
            logger.info("Started consumer: %s", name)

    async def stop_all(self) -> None:
        for name, consumer in self._consumers.items():
            await consumer.stop()
            task = self._tasks.get(name)
            if task:
                task.cancel()
                logger.info("Stopped consumer task: %s", name)

        self._tasks.clear()
        self._consumers.clear()
        logger.info("All consumers stopped and cleared")
