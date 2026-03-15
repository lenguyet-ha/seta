from sqlalchemy.orm import Session
from app.models.user import User
from app.core.security import get_password_hash, verify_password, create_access_token
from app.graphql.types import RegisterInput, LoginInput, ChangePasswordInput, UserType, TokenType

def register_user(db: Session, input_data: RegisterInput) -> UserType:
    db_user = db.query(User).filter(
        (User.username == input_data.username) | (User.email == input_data.email)
    ).first()
    if db_user:
        raise Exception("Username or email already registered")
    
    print(f"DEBUG: Password length: {input_data.password}")
    hashed_password = get_password_hash(input_data.password)
    new_user = User(
        username=input_data.username,
        email=input_data.email,
        password_hash=hashed_password,
        role=input_data.role
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return UserType.from_db(new_user)

def authenticate_user(db: Session, input_data: LoginInput) -> TokenType:
    user = db.query(User).filter(User.username == input_data.username).first()
    if not user or not verify_password(input_data.password, user.password_hash):
        raise Exception("Incorrect username or password")
    
    access_token = create_access_token(user_id=user.id)
    return TokenType(
        access_token=access_token,
        token_type="bearer",
        user_id=user.id,
        role=user.role,
        valid=True
    )

def change_password(db: Session, user_id: int, input_data: ChangePasswordInput) -> UserType:
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise Exception("User not found")
        
    if not verify_password(input_data.old_password, user.password_hash):
        raise Exception("Incorrect old password")
        
    user.password_hash = get_password_hash(input_data.new_password)
    db.commit()
    db.refresh(user)
    return UserType.from_db(user)

def get_user_by_id(db: Session, user_id: int) -> UserType:
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise Exception("User not found")
    return UserType.from_db(user)
