import json
import logging
from typing import Optional, Any
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaProducerService:
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
        if self._started:
            logger.warning("Producer is already started")
            return

        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                client_id=self._client_id,
                value_serializer=self._serialize_value,
                key_serializer=self._serialize_key,
                acks="all",
                enable_idempotence=True,
                max_batch_size=16384,
                linger_ms=5,
                compression_type="gzip",
            )
            await self._producer.start()
            self._started = True
            logger.info("Kafka producer started successfully")

        except KafkaError as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            raise

    async def stop(self) -> None:
        if self._producer and self._started:
            await self._producer.stop()
            self._started = False
            logger.info("Kafka producer stopped successfully")

    async def send_event(
        self,
        topic: str,
        event_type: str,
        data: dict,
        key: Optional[Any] = None,
        headers: Optional[dict] = None,
    ) -> None:
        if not self.is_connected:
            raise RuntimeError(
                "Producer is not connected. Please start the producer before sending events."
            )

        message = self._build_message(event_type, data, headers)
        kafka_headers = self._build_headers(headers)

        try:
            await self._producer.send_and_wait(
                topic=topic,
                value=message,
                key=key,
                headers=kafka_headers,
            )
            logger.info(f'[Kafka] Sent event to topic "{topic}": {event_type}')

        except KafkaError as e:
            logger.error(f"Failed to send event to Kafka: {e}")
            raise

    async def send_batch(
        self,
        topic: str,
        events: list[dict],
    ) -> None:
        if not self.is_connected:
            raise RuntimeError(
                "Producer is not connected. Please start the producer before sending events."
            )

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
                await self._producer.send_batch(batch, topic)
                batch = self._producer.create_batch()
                batch.append(
                    key=serialized_key,
                    value=serialized_value,
                    timestamp=None,
                )

        if batch.size() > 0:
            await self._producer.send_batch(batch, topic)
        
        logger.info(f'[Kafka] Sent batch of {len(events)} events to topic "{topic}"')
        
    def _build_message(self, event_type: str, data: dict) -> dict:
        return{
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": "1.0",
        }
    
    def _build_headers(self, headers: Optional[dict]) -> Optional[list]:
        if not headers:
            return None
        return [(k, v.encode("utf-8")) for k, v in headers.items()]
    
    @staticmethod
    def _serialize_value(value: Any) -> bytes:
        return json.dumps(value, default=str, ensure_ascii=False).encode("utf-8")
    
    @staticmethod
    def _serialize_key(key: Optional[Any]) -> Optional[bytes]:
        return key.encode("utf-8") if key is not None else None
    
    
kafka_producer: Optional[KafkaProducerService] = None

def init_kafka_producer(
    bootstrap_servers: str,
    client_id: str = "seta-producer"
) -> KafkaProducerService:
    global kafka_producer
    kafka_producer = KafkaProducerService(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
    )   
    return kafka_producer   

def get_kafka_producer() -> KafkaProducerService:
    if kafka_producer is None:
        raise RuntimeError("Kafka producer is not initialized. Please call init_kafka_producer first.")
    return kafka_producer  
        