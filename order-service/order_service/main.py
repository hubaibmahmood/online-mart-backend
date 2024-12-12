from jose import jwt, JWTError, ExpiredSignatureError
from fastapi import  FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from datetime import datetime, timedelta
from sqlmodel import SQLModel,Field, Session, create_engine, select, Relationship
from typing import Annotated, Optional, AsyncGenerator
from order_service import settings
from contextlib import asynccontextmanager
from sqlalchemy.orm import selectinload
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json, asyncio


ALGORITHM = "HS256"
SECRET_KEY = "A Secure Secret Key"




class Item(SQLModel, table=True):
    id: Optional[int] = Field(default=None, index=True, primary_key=True)
    order_id: Optional[int] = Field(default=None, foreign_key="order.id")
    product_id: int = Field(index=True)
    quantity: int = Field(index=True)
    order: Optional["Order"] = Relationship(back_populates="items")






class Order(SQLModel, table=True):

    id : Optional[int] = Field(default=None, index=True, primary_key=True)
    customer_id: int = Field(index=True)
    items: list["Item"] = Relationship(back_populates="order")
    order_status: str = Field(index=True)
    order_date: str = Field(index=True)
    total_amount: float = Field(index=True)
    payment_status: str = Field(index=True)
    shipping_address: str = Field(index=True)
    billing_address: str = Field(index=True)
    payment_details: str = Field(index=True)





class UpdateItem(SQLModel):
    id: Optional[int]
    product_id: Optional[int]
    quantity: Optional[int]



class UpdateOrder(SQLModel):

    order_id: int
    items: list["UpdateItem"] = []
    order_status : Optional[str] = None
    total_amount: Optional[float] = None
    payment_status: Optional[str] = None
    shipping_address: Optional[str] = None
    billing_address: Optional[str] = None
    payment_details: Optional[str] = None




class OrderStatus:
    PENDING = "Pending"
    CONFIRMED = "Confirmed"
    PROCESSING = "Processing"
    SHIPPED = "Shipped"
    DELIVERED = "Delivered"
    COMPLETED = "Completed"
    CANCELLED = "Cancelled"
    REFUNDED = "Refunded"
    FAILED = "Failed"
    ON_HOLD = "On Hold"
    RETURNED = "Returned"
    AWAITING_PAYMENT = "Awaiting Payment"




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
        group_id="order-management",
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
                    await update_order_from_consumer(json_, session)
                    


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
    consumer_task = asyncio.create_task(consume_messages('order', 'broker:19092'))

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





app = FastAPI(lifespan=lifespan, title="Order Management Microservice", 
    version="0.0.1",
    servers=[
        {
            "url": "http://host.docker.internal:8087", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        },{
            "url": "http://localhost:8087",
            "description": "Development Server"
        }],
    root_path="/order"
    )


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




@app.post("/create_order")
async def create_order(token: str, data_from_user: Order, item_data_from_user: list[Item], session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):



    decoded = decode_access_token(token)

    # if decoded.get("role") == "admin":



    if data_from_user:
        
        # print(data_from_user)
        
        new_data = Order.model_validate(data_from_user)

        # print(new_data)
        
        new_item_dict = []

        
        session.add(new_data)
        session.commit()
        session.refresh(new_data)



        total_items_value: float = round(new_data.total_amount,  3)

        for item in item_data_from_user:
            new_item = Item.model_validate(item)
            new_item.order_id = new_data.id  # Associate the item with the order's ID


            session.add(new_item)


            # Add the item to the order
            new_data.items.append(new_item)

            
            item_dict = new_item.dict()
            del item_dict['order_id']  
            del item_dict['id']        
            item_dict['method'] = "-"  
            new_item_dict.append(item_dict)


        session.commit()
        
        serialized_data = json.dumps(new_item_dict).encode('utf-8')
        await producer.send_and_wait("inventory", serialized_data)


        data_for_notification: dict = {}

        data_for_notification['order_id'] = new_data.id
        data_for_notification['username'] = decoded.get('sub')
        data_for_notification['email'] = decoded.get('email')
        data_for_notification['notification_type'] = "order_status_update"
        data_for_notification['order_status'] = new_data.order_status

        serialized_data_notification = json.dumps(data_for_notification).encode('utf-8')
        
        await producer.send_and_wait("notification", serialized_data_notification)


        data_for_payment: dict = {}

        data_for_payment['order_id'] = new_data.id
        data_for_payment['amount'] = total_items_value

        serialized_data_payment = json.dumps(data_for_payment).encode('utf-8')

        await producer.send_and_wait("payment", serialized_data_payment)


        session.refresh(new_data)


        return new_data


    else:
        raise HTTPException(status_code=400, detail="Enter all the required details")

    # else:
    #     raise HTTPException(status_code=404, detail="You don't have permission to perform this operation")





@app.get("/orders")
def get_all_orders(session: Annotated[Session, Depends(get_session)]):
    # products = session.exec(select(Order)).all()

    # for product in products:
    #     print(product.id)

    #     product_ = Order.model_validate(product)

    #     items_from_db = session.exec(select(Item).where(Item.order_id == product.id)).all()

    #     for item in items_from_db:
    #         # item_class = Item.model_validate(item)
    #         product_.items.append(item)
    #         print(item)

    #     product = product_

    #     print(product_)


    statement = select(Order).options(selectinload(Order.items))
    orders = session.exec(statement).all()

    orders_data = [
            {
                "id": order.id,
                "customer_id": order.customer_id,
                "order_status": order.order_status,
                "order_date": order.order_date,
                "total_amount": order.total_amount,
                "payment_status": order.payment_status,
                "shipping_address": order.shipping_address,
                "billing_address": order.billing_address,
                "payment_details": order.payment_details,
                "items": [
                    {
                        "id": item.id,
                        "product_id": item.product_id,
                        "quantity": item.quantity
                    }
                    for item in order.items
                ]
            }
            for order in orders
        ]


    return orders_data






@app.post("/order")
def get_specific_order(id: int, session: Annotated[Session, Depends(get_session)]):

    order = session.exec(select(Order).options(selectinload(Order.items)).where(Order.id == id)).first()

    if order:
        
        order_data = {
            "id": order.id,
            "customer_id": order.customer_id,
            "order_status": order.order_status,
            "order_date": order.order_date,
            "total_amount": order.total_amount,
            "payment_status": order.payment_status,
            "shipping_address": order.shipping_address,
            "billing_address": order.billing_address,
            "payment_details": order.payment_details,
            "items": [
                {
                    "id": item.id,
                    "product_id": item.product_id,
                    "quantity": item.quantity
                }
                for item in order.items
            ]
        }


        return order_data
        

    else:
        raise HTTPException(status_code=404, detail="Invalid ID. No order found!!!")







@app.patch("/update_order")
def update_order(token: str, id: int, updated_order: UpdateOrder, session: Annotated[Session, Depends(get_session)]):
    

    decoded = decode_access_token(token)

    # if decoded.get("role") == "admin":


    if updated_order:

        order = session.exec(select(Order).options(selectinload(Order.items)).where(Order.id == id)).first()

        if not order:
            raise HTTPException(status_code=404, detail="Order Not found!!!")
        

        new_product_data = updated_order.model_dump(exclude_unset=True)
        order.sqlmodel_update(new_product_data)

        session.add(order)
        session.commit()
        session.refresh(order)


        for new_items in updated_order.items:
            item_ = session.exec(select(Item).where(Item.id == new_items.id)).first()

            if item_:
                updated_items = new_items.model_dump(exclude_unset=True)
                item_.sqlmodel_update(updated_items)
            
                session.add(item_)
                session.commit()
                session.refresh(item_)



        return updated_order


    
    else:
        raise HTTPException(status_code=400, detail="Updated content can't be empty")


    # else:
    #     raise HTTPException(status_code=404, detail="You don't have permission to perform this operation")







async def update_order_from_consumer(details_to_update, session: Annotated[Session, Depends(get_session)]):
    



    if details_to_update:

        updated_order = UpdateOrder(**details_to_update)

        order = session.exec(select(Order).options(selectinload(Order.items)).where(Order.id == updated_order.order_id)).first()

        if not order:
            raise HTTPException(status_code=404, detail="Order Not found!!!")
        

        new_product_data = updated_order.model_dump(exclude_unset=True)
        order.sqlmodel_update(new_product_data)

        session.add(order)
        session.commit()
        session.refresh(order)


        for new_items in updated_order.items:
            item_ = session.exec(select(Item).where(Item.id == new_items.id)).first()

            if item_:
                updated_items = new_items.model_dump(exclude_unset=True)
                item_.sqlmodel_update(updated_items)
            
                session.add(item_)
                session.commit()
                session.refresh(item_)



        return updated_order


    
    else:
        raise HTTPException(status_code=400, detail="Updated content can't be empty")


    # else:
    #     raise HTTPException(status_code=404, detail="You don't have permission to perform this operation")





@app.delete("/order")
async def delete_order(token: str, id: int, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):


    decoded = decode_access_token(token)

    # if decoded.get("role") == "admin":


    order = session.exec(select(Order).options(selectinload(Order.items)).where(Order.id == id)).one_or_none()
    
    if order:

        items_list:list[dict] = []

        for item in order.items:
            item_dict = item.dict()

            session.delete(item)

            del item_dict['order_id']  
            del item_dict['id']        
            item_dict['method'] = "+"  

            items_list.append(item_dict)


        data_for_notification: dict = {}

        data_for_notification['order_id'] = order.id
        data_for_notification['username'] = decoded.get('sub')
        data_for_notification['email'] = decoded.get('email')
        data_for_notification['notification_type'] = "order_status_update"
        data_for_notification['order_status'] = "Canceled"

        serialized_data_notification = json.dumps(data_for_notification).encode('utf-8')

        await producer.send_and_wait("notification", serialized_data_notification)

    
        session.delete(order)
        session.commit()


        serialized_data = json.dumps(items_list).encode('utf-8')
        await producer.send_and_wait("inventory", serialized_data)


        data_for_payment: dict = {}

        data_for_payment['order_id'] = order.id
        data_for_payment['payment_status'] = 'cancelled'
        data_for_payment['delete'] = True


        serialized_data_payment = json.dumps(data_for_payment).encode('utf-8')

        await producer.send_and_wait("payment", serialized_data_payment)



        return order
        # return {"message": f"Order deleted successfullyy!!!"}
    
    else:
        raise HTTPException(status_code=404, detail="Invalid ID. Order Not Found!!!")
        


