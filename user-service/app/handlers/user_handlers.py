from sqlalchemy.orm import Session
from app.core.database import SessionLocal
from app.models.user import User
from seta_shared.kafka import (
    KafkaConsumerService,
    kafka_producer,
    Topics,
    UserEvents,
)


async def handle_update_user_requested(data: dict) -> None:
    db: Session = SessionLocal()
    request_id = data.get("request_id")
    user_id = data.get("user_id")
    
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            await kafka_producer.send_event(
                topic=Topics.USER_EVENTS,
                event_type=UserEvents.USER_UPDATED_FAILED,
                data={
                    "request_id": request_id,
                    "reason": "User not found"
                },
            )
            return
        
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
            event_type=UserEvents.USER_UPDATED,
            data={
                "request_id": request_id,
                "user_id": user.id,
                "username": user.username,
                "role": user.role,
                "is_active": user.is_active,
            },
            key=str(user.id),
        )
    
    except Exception as e:
        db.rollback()
        await kafka_producer.send_event(
            topic=Topics.USER_EVENTS,
            event_type=UserEvents.USER_UPDATED_FAILED,
            data={
                "request_id": request_id,
                "reason": str(e)
            },
        )
    finally:
        db.close()
        

def register_user_handlers(consumer: KafkaConsumerService) -> None:
    consumer.register_handler(
        UserEvents.USER_UPDATE_REQUESTED, handle_update_user_requested,
    )