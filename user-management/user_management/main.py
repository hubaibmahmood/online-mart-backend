from jose import jwt, JWTError, ExpiredSignatureError
from fastapi import  FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from datetime import datetime, timedelta
from sqlmodel import SQLModel,Field, Session, create_engine, select
from typing import Annotated, Optional, AsyncGenerator
from user_management import settings
from contextlib import asynccontextmanager



ALGORITHM = "HS256"
SECRET_KEY = "A Secure Secret Key"




class UserBase(SQLModel):
    username: str = Field(index=True)
    password: str = Field(index=True)



class User(UserBase, table=True):
    id: Optional[int] = Field(primary_key=True)
    full_name: str = Field(index=True)
    email: str = Field(index=True)
    # token: str = Field(default=None, index=True)




class Admin(UserBase, table=True):
    id: Optional[int] = Field(primary_key=True)
    full_name: str = Field(index=True)
    email: str = Field(index=True)




connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)


engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300
)



def create_db_and_tables()->None:
    SQLModel.metadata.create_all(engine)




@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("Creating tables..")
    # loop.run_until_complete(consume_messages('todos', 'broker:19092'))
    create_db_and_tables()
    yield



app = FastAPI(lifespan=lifespan, title="User Management Microservice", 
    version="0.0.1",
    servers=[
        {
            "url": "http://host.docker.internal:8085", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        },{
            "url": "http://localhost:8085",
            "description": "Development Server"
        }],
    root_path="/user"
    )


def get_session():
    with Session(engine) as session:
        yield session


def generate_token(subject: str, email:str, expiry_time: timedelta):

    expiry_time = datetime.utcnow() + expiry_time

    print(expiry_time)

    expiry_time_str = int(expiry_time.timestamp())


    to_encode = {"sub": str(subject), "email": str(email), "exp": expiry_time_str}

    encoded_data = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return encoded_data



def generate_token_admin(subject: str, expiry_time: timedelta):

    expiry_time = datetime.utcnow() + expiry_time

    print(expiry_time)

    expiry_time_str = int(expiry_time.timestamp())


    to_encode = {"sub": str(subject), "role": "admin", "exp": expiry_time_str}

    encoded_data = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return encoded_data





def get_token(subject: str):

    access_token_expires = timedelta(minutes=1)


    token = generate_token(subject=subject, expiry_time=access_token_expires)

    return {"access-token": token}




def decode_access_token(token: str):
    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return decoded_token
    
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except JWTError as e:
        raise HTTPException(status_code=401, detail="Invalid token")
    




@app.get("/data")
def get_all_user(token: str, session: Annotated[Session, Depends(get_session)]):

    decoded = decode_access_token(token)

    try:
        user_ = session.exec(select(User).where(User.username == decoded.get('sub'))).one()
    
    except Exception as e:
        user_ = ''



    if user_:
        return user_









@app.post("/register")
def register(data_from_user: User, session: Annotated[Session, Depends(get_session)]):

    if data_from_user:
        new_data = User.model_validate(data_from_user)
        session.add(new_data)
        session.commit()
        session.refresh(new_data)
    
        return new_data


    else:
        raise HTTPException(status_code=400, detail="Enter all the required details")







@app.post("/login")
def login(data_from_user: Annotated[OAuth2PasswordRequestForm, Depends(OAuth2PasswordRequestForm)], session: Annotated[Session, Depends(get_session)]):

    try:
        user_in_db = session.exec(select(User).where(User.username == data_from_user.username)).one()
    
    except Exception as e:
        user_in_db = ''


    if not user_in_db:
        return HTTPException(status_code=400, detail="Incorrect username")

    if not user_in_db.password == data_from_user.password:
        return HTTPException(status_code=400, detail="Incorrect password")


    access_token_expires = timedelta(minutes=3600)


    access_token = generate_token(subject=user_in_db.username, email=user_in_db.email, expiry_time=access_token_expires)


    return {"username": user_in_db.username, "generated-token": access_token}






@app.post("/login_admin")
def login_admin(data_from_user: Annotated[OAuth2PasswordRequestForm, Depends(OAuth2PasswordRequestForm)], session: Annotated[Session, Depends(get_session)]):

    try:
        user_in_db = session.exec(select(Admin).where(Admin.username == data_from_user.username)).one()
    
    except Exception as e:
        user_in_db = ''


    if not user_in_db:
        return HTTPException(status_code=400, detail="Incorrect username")

    if not user_in_db.password == data_from_user.password:
        return HTTPException(status_code=400, detail="Incorrect password")


    access_token_expires = timedelta(minutes=10)


    access_token = generate_token_admin(subject=data_from_user.username, expiry_time=access_token_expires)


    return {"username": data_from_user.username, "generated-token": access_token}


