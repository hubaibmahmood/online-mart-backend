from jose import jwt, JWTError, ExpiredSignatureError
from fastapi import  FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from datetime import datetime, timedelta
from sqlmodel import SQLModel,Field, Session, create_engine, select, Relationship
from typing import Annotated, Optional, AsyncGenerator
from notification_service import settings
from contextlib import asynccontextmanager
import smtplib, ssl, asyncio, json
from email.mime.text import MIMEText
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


ALGORITHM = "HS256"
SECRET_KEY = "A Secure Secret Key"





class Notification(SQLModel, table=True):
    id: Optional[int] = Field(default=None, index=True, primary_key=True)
    order_id: int = Field(index=True)
    username: str = Field(index=True)
    email_address: str = Field(index=True)
    notification_type: str = Field(index=True)
    order_status: str = Field(index=True)
    subject: Optional[str] = Field(default=None, index=True)  # Default None
    body: Optional[str] = Field(default=None, index=True)     # Default None
    status: Optional[str] = Field(default="pending", index=True)  # Default value for status
    timestamp: Optional[str] = Field(default=None, index=True)  # Default None



class SendNotification(SQLModel):
    order_id: int
    username: str
    email_address: str
    notification_type: str






connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)


engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300
)




def create_db_and_tables()->None:
    SQLModel.metadata.create_all(engine)





# Kafka Producer as a dependency
async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()



def get_session():
    with Session(engine) as session:
        yield session





async def consume_messages(topic, bootstrap_servers):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="notification-management",
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        auto_offset_reset='earliest'
    )

    await consumer.start()
    print("Kafka consumer started and consuming messages...")

    try:
        while True:
            messages = await consumer.getmany(timeout_ms=1000)
            for tp, batch in messages.items():
                for message in batch:
                    print(f"Consumer message: {message.value.decode('utf-8')}")
                    json_ = json.loads(message.value.decode('utf-8'))


                    json_['email_address'] = json_.pop("email")


                    session = next(get_session())
                    producer = await anext(get_kafka_producer())


                    send_order_notification(json_, session)
                    
                    # if len(stock_to_add) > 0:
                    #     status = await add_stock(stock_to_add, session, producer)



                    # if len(stock_to_remove) > 0:
                    #     status = await remove_stock(stock_to_remove, session, producer)

                    #     print(status)



            await asyncio.sleep(0.1)

    finally:
        await consumer.stop()
        print("Kafka consumer stopped.")






async def wait_for_kafka_ready(host: str, port: int = 19092, retries: int = 10, delay: int = 5):
    print(f"Waiting for Kafka to be ready at {host}:{port}...")
    for attempt in range(1, retries + 1):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            print(f"Kafka is ready after {attempt} attempt(s).")
            return
        except Exception as e:
            print(f"Kafka connection attempt {attempt} failed: {e}")
            if attempt < retries:
                await asyncio.sleep(delay)
            else:
                raise RuntimeError(f"Kafka not ready after {retries} retries.")






@asynccontextmanager
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("Creating tables..")


    # Check if Kafka is ready before starting the consumer
    try:
        await wait_for_kafka_ready('broker')
    except RuntimeError as e:
        print(f"Error during Kafka startup: {e}")
        raise


    # Start Kafka consumer as a background task
    consumer_task = asyncio.create_task(consume_messages('notification', 'broker:19092'))

    # Create the necessary database tables
    create_db_and_tables()

    # Yield control to FastAPI, starting the web application
    try:
        yield
    
    finally:
        print("Shutting down lifespan...")

        # Cancel the consumer task and handle its graceful shutdown
        if not consumer_task.done():
            consumer_task.cancel()

        try:
            await consumer_task
        except asyncio.CancelledError:
            print("Kafka consumer task cancelled during shutdown.")







app = FastAPI(lifespan=lifespan, title="Notification Management Microservice", 
    version="0.0.1",
    servers=[
        {
            "url": "http://host.docker.internal:8090", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        },{
            "url": "http://localhost:8090",
            "description": "Development Server"
        }],
    root_path="/notification"
    )




def decode_access_token(token: str):
    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return decoded_token
    
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except JWTError as e:
        raise HTTPException(status_code=401, detail="Invalid token")
    






def generate_notification_content(template_id, orderID, username, status):

    if template_id.lower() == "order_status_update":
        
        # orderID = placeholders.get("orderID")
        # username = placeholders.get("username")
        # status = placeholders.get("status")

        subject=f"Your Order #{orderID} Status Update"
        body=f"""
        Hello {username},

        Your order with ID {orderID} has been updated to the following status: {status}.

        Thank you for shopping with us!

        Best regards,
        Hubaib Online Mart
        """


    return subject, body





def send_notification(email, subject, body):
        
    sender_email = 'apkkernal@gmail.com' #USE YOUR TEST EMAIL FOR trail run
    sender_password = 'draaurdybfqkzxnd' #Your PASSWORD 
    # recipient_email = 'hubaibm9@gmail.com'



    message = MIMEText(body)
    message['Subject'] = subject
    message['From'] = sender_email
    message['To'] = email

    context = ssl.create_default_context()


    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)  # Update with your email provider's SMTP details
        server.starttls(context=context)
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, email, message.as_string())
        print("Email notification sent successfully!")
        return True
    except Exception as e:
        print("Error sending email:", str(e))
        return False
    finally:
        server.quit()







@app.post("/send_notification")
def send_order_notification(data_from_user: Notification, session: Annotated[Session, Depends(get_session)]):
    
    # decoded = decode_access_token(token)
    


    if data_from_user:

        new_data = Notification.model_validate(data_from_user)

        print(new_data)
        # return 


        subject, body = generate_notification_content(new_data.notification_type, new_data.order_id, new_data.username, new_data.order_status)


        status = send_notification(new_data.email_address, subject, body)


        if status:
            new_data.status = 'sent'
        else:
            new_data.status = 'failed'


        new_data.subject = subject
        new_data.body = body


        new_data.timestamp = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")



        session.add(new_data)
        session.commit()
        session.refresh(new_data)
    
        return new_data


    else:
        raise HTTPException(status_code=400, detail="Enter all the required details")

