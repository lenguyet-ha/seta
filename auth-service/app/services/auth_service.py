from sqlalchemy.orm import Session
from app.models.credential import Credential
from app.core.security import get_password_hash, verify_password, create_access_token
from app.graphql.types import LoginInput, ChangePasswordInput, UserType, TokenType
from app.core.config import settings
import httpx

async def authenticate_user(db: Session, input_data: LoginInput) -> TokenType:
    credential = db.query(Credential).filter(Credential.email == input_data.email).first()
    if not credential or not verify_password(input_data.password, credential.password_hash):
        raise Exception("Incorrect email or password")
    
    try:
        async with httpx.AsyncClient() as client:
            query = """
            query GetUserById($id: Int!) {
              getUserById(userId: $id) {
                id
                role
              }
            }
            """
            response = await client.post(
                settings.USER_SERVICE_URL,
                json={
                    "query": query,
                    "variables": {"id": credential.user_id},
                },
            )
            response.raise_for_status()
            result = response.json()
            if "errors" in result:
                raise Exception(f"User Service error: {result['errors'][0]['message']}")
            user_data = result["data"]["getUserById"]
            if not user_data:
                raise Exception("User not found")
    except httpx.HTTPError as e:
        raise Exception(f"Failed to fetch user from User Service: {str(e)}")

    access_token = create_access_token(user_id=credential.user_id)
    return TokenType(
        access_token=access_token,
        token_type="bearer",
        user_id=credential.user_id,
        role=user_data["role"],
        valid=True
    )

async def change_password(db: Session, user_id: int, input_data: ChangePasswordInput) -> UserType:
    credential = db.query(Credential).filter(Credential.user_id == user_id).first()
    if not credential:
        raise Exception("User not found")
        
    if not verify_password(input_data.old_password, credential.password_hash):
        raise Exception("Incorrect old password")
        
    credential.password_hash = get_password_hash(input_data.new_password)
    db.commit()
    db.refresh(credential)
    return UserType.from_db(credential)

async def get_user_by_id(db: Session, user_id: int) -> UserType:
    credential = db.query(Credential).filter(Credential.user_id == user_id).first()
    if not credential:
        raise Exception("User not found")
    return UserType.from_db(credential)
