import stripe, asyncio
from jose import jwt, JWTError, ExpiredSignatureError
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.staticfiles import StaticFiles
from sqlmodel import SQLModel,Field, Session, create_engine, select
from typing import Annotated, Optional, AsyncGenerator
from payment_service import settings
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
from urllib.parse import urlencode




stripe.api_key = settings.STRIPE_API_KEY # Your Stripe test secret key

ALGORITHM = "HS256"
SECRET_KEY = "A Secure Secret Key"



class PaymentRecord(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    order_id: int
    client_secret: str
    status: str  # "pending", "completed", "failed", etc.


class PaymentRequest(SQLModel):
    order_id: int
    user_id: str
    amount: float
    currency: Optional[str] = 'usd'


class PaymentStatusUpdate(SQLModel):
    order_id: int
    payment_status: str




connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)


engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300
)



def create_db_and_tables()->None:
    SQLModel.metadata.create_all(engine)






async def consume_messages(topic, bootstrap_servers):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="payment-management",
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


                    session = next(get_session())

                    if json_.get('delete', None) == None:

                        await process_payment_creation(json_, session)
                    
                    elif json_.get('delete', None) != None:

                        del json_['delete']

                        await update_payment_status_from_consumer(json_, session)




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
    consumer_task = asyncio.create_task(consume_messages('payment', 'broker:19092'))

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




app = FastAPI(lifespan=lifespan, title="Payment Management Microservice", 
    version="0.0.1",
    servers=[
        {
            "url": "http://host.docker.internal:8091", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        },{
            "url": "http://localhost:8091",
            "description": "Development Server"
        }],
    root_path="/payment"
    )

app.mount("/static", StaticFiles(directory=".", html=True), name="static")


def get_session():
    with Session(engine) as session:
        yield session




def decode_access_token(token: str):
    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return decoded_token
    
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except JWTError as e:
        raise HTTPException(status_code=401, detail="Invalid token")
    



# Kafka Producer as a dependency
async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()




async def process_payment_creation(payment_data, session: Annotated[Session, Depends(get_session)]):

    # print(payment_data)

    # Step 1: Create a payment intent
    intent = stripe.PaymentIntent.create(
        amount= int(payment_data['amount'] * 100),
        currency=payment_data.get('currency','usd'),
        metadata={"order_id": payment_data["order_id"]}
    )

    # Step 2: Store the payment intent's client secret and order ID in the database
    payment_record = PaymentRecord(
        order_id=payment_data["order_id"],
        client_secret=intent.client_secret,
        status="pending"
    )
    session.add(payment_record)
    session.commit()




# @app.post('/create-payment-intent')
# async def create_payment_intent(request: PaymentRequest):
#     try:
#         intent = stripe.PaymentIntent.create(
#             amount=int(request.amount * 100),  # Stripe amounts are in cents
#             currency=request.currency,
#             metadata={'order_id': request.order_id, 'user_id': request.user_id}
#         )
#         return {'client_secret': intent.client_secret, "intent_id": intent.id}
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))


@app.get("/payment-link")
async def get_payment_link(order_id: int, session: Annotated[Session, Depends(get_session)]):


    # Step 1: Retrieve the payment record
    payment_record = session.exec(select(PaymentRecord).where(PaymentRecord.order_id == int(order_id))).first()
    

    if not payment_record:
        raise HTTPException(status_code=404, detail="Payment record not found")

    params = urlencode({"order_id": payment_record.order_id, "client_secret": payment_record.client_secret})

    if payment_record.status != 'completed':
        # Step 2: Generate a link with the client secret
        payment_link = f"http://localhost:8091/static/payment.html?{params}"
    else:
        payment_link = "Payment already completed"

    return {"payment_link": payment_link}




async def update_payment_status_from_consumer(data_from_user_dict: PaymentStatusUpdate, session: Annotated[Session, Depends(get_session)]):

    # print(data_from_user)

    data_from_user = PaymentStatusUpdate(order_id = data_from_user_dict['order_id'], payment_status = data_from_user_dict['payment_status'])

    payment_record = session.exec(select(PaymentRecord).where(PaymentRecord.order_id == data_from_user.order_id)).first()


    if not payment_record:
        raise HTTPException(status_code=404, detail="Payment record not found")


    # Update the payment status to the provided status
    if data_from_user.payment_status not in ["refunded", "cancelled"]:
        raise HTTPException(status_code=400, detail="Invalid payment status")


    try:
        if "pending" in payment_record.status:
            payment_record.status = "cancelled"
            session.add(payment_record)
            session.commit()

        elif "completed" in payment_record.status:
            payment_record.status = "refunded"
            session.add(payment_record)
            session.commit()

        return {"message": f"Payment status updated to {data_from_user.payment_status} successfully"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))





@app.post("/update-payment-status")
async def update_payment_status(data_from_user: PaymentStatusUpdate, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):

    # Find the payment record based on the order_id
    payment_record = session.exec(select(PaymentRecord).where(PaymentRecord.order_id == data_from_user.order_id)).first()
    
    if not payment_record:
        raise HTTPException(status_code=404, detail="Payment record not found")
    
    # Update the payment status to the provided status
    if data_from_user.payment_status not in ["completed", "failed"]:
        raise HTTPException(status_code=400, detail="Invalid payment status")

    try:
        payment_record.status = data_from_user.payment_status
        session.add(payment_record)
        session.commit()


        data_for_order_updation: dict = {}

        data_for_order_updation['order_id'] = data_from_user.order_id
        data_for_order_updation['payment_status'] = "paid"
        data_for_order_updation['order_status'] = 'processing'

        serialized_data_order_updation = json.dumps(data_for_order_updation).encode('utf-8')

        await producer.send_and_wait("order", serialized_data_order_updation)
    


        return {"message": f"Payment status updated to {data_from_user.payment_status} successfully"}


    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))







# @app.get('/verify-payment/{intent_id}')
# async def verify_payment(token: str, order_id: int, intent_id: str):

#     decoded = decode_access_token(token)


#     try:
#         intent = stripe.PaymentIntent.retrieve(intent_id, expand=['charges'])
#         charges = intent.get('charges', {}).get('data', [])

#         data_for_notification = {}

#         data_for_notification['order_id'] = order_id
#         data_for_notification['username'] = decoded.get('sub')
#         data_for_notification['email'] = decoded.get('email')
#         data_for_notification['notification_type'] = "order_status_update"
#         data_for_notification['order_status'] = "paid"

#         serialized_data_notification = json.dumps(data_for_notification).encode('utf-8')


#         await producer.send_and_wait("inventory", serialized_data_notification)


#         return {
#             'status': intent.status,
#             'charges': charges
#         }
#     except stripe.error.StripeError as e:
#         raise HTTPException(status_code=400, detail=str(e))
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))




@app.get('/list-payment-intents')
async def list_payment_intents(user_id: str, limit: int = 10):
    try:
        all_payment_intents = stripe.PaymentIntent.list(limit=100)  # Fetch a larger batch to filter manually
        user_payment_intents = [pi for pi in all_payment_intents.data if pi.metadata.get('user_id') == user_id]
        return user_payment_intents[:limit]  # Return only the required number of results
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))






@app.post('/stripe-webhook')
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get('stripe-signature')
    endpoint_secret = 'whsec_test_secret'  # Your Stripe test webhook secret
    
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, endpoint_secret
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    if event['type'] == 'payment_intent.succeeded':
        payment_intent = event['data']['object']
        handle_successful_payment(payment_intent)
    elif event['type'] == 'payment_intent.payment_failed':
        payment_intent = event['data']['object']
        handle_failed_payment(payment_intent)
    else:
        print('Unhandled event type {}'.format(event['type']))

    return {'success': True}



def handle_successful_payment(payment_intent):
    print(f"Payment for {payment_intent['amount']} succeeded.")
    # Implement your business logic for successful payment

def handle_failed_payment(payment_intent):
    print(f"Payment for {payment_intent['amount']} failed.")
    # Implement your business logic for failed payment

