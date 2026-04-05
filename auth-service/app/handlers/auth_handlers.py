import logging
from sqlalchemy.orm import Session
from app.core.database import SessionLocal
from app.models.credential import Credential
from app.core.security import get_password_hash
from seta_shared.kafka import (
    KafkaConsumerService,
    kafka_producer,
    Topics,
    AuthEvents,
    UserEvents,
)

logger = logging.getLogger(__name__)


async def handle_user_created(data: dict) -> None:
    """
    Handles USER_CREATED events from user-service.
    Creates credentials for the newly created user.
    """
    db: Session = SessionLocal()
    user_id = data.get("user_id")
    
    try:
        # Check if credentials already exist for this user
        existing = db.query(Credential).filter(
            Credential.user_id == user_id
        ).first()
        
        if existing:
            logger.warning(f"Credentials already exist for user_id={user_id}, skipping")
            return
        
        # Hash the password and create credential record
        hashed_password = get_password_hash(data["password"])
        new_credential = Credential(
            user_id=user_id,
            username=data["username"],
            email=data["email"],
            password_hash=hashed_password,
        )
        db.add(new_credential)
        db.commit()
        db.refresh(new_credential)
        
        logger.info(f"Created credentials for user_id={user_id}")
        
        # Publish success event
    
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to create credentials for user_id={user_id}: {e}")
        
        await kafka_producer.send_event(
            topic=Topics.AUTH_EVENTS,
            event_type=AuthEvents.CREDENTIAL_CREATE_FAILED,
            data={
                "user_id": user_id,
                "reason": str(e),
            },
            key=str(user_id) if user_id else None,
        )
    finally:
        db.close()


def register_auth_handlers(consumer: KafkaConsumerService) -> None:
    consumer.register_handler(
        UserEvents.USER_CREATED, handle_user_created,
    )
