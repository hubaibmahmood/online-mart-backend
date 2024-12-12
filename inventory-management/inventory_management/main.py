from jose import jwt, JWTError, ExpiredSignatureError
from fastapi import  FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from datetime import datetime, timedelta
from sqlmodel import SQLModel,Field, Session, create_engine, select, Relationship
from typing import Annotated, Optional, AsyncGenerator
from inventory_management import settings
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio, aiohttp, json

import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


ALGORITHM = "HS256"
SECRET_KEY = "A Secure Secret Key"

consumer: AIOKafkaConsumer = None




class Inventory(SQLModel, table=True):
    id: Optional[int] = Field(default=None, index=True, primary_key=True)
    product_id: int = Field(index=True)
    stock: int = Field(index=True)







class UpdateItem(SQLModel):
    id: Optional[int]
    product_id: Optional[int]
    quantity: Optional[int]



class UpdateOrder(SQLModel):

    items: list["UpdateItem"] = []
    order_status : Optional[str]
    total_amount: Optional[float]
    payment_status: Optional[str] 
    shipping_address: Optional[str]
    billing_address: Optional[str] 
    payment_details: Optional[str]




connection_string = str(settings.DATABASE_URL).replace(
    "postgresql", "postgresql+psycopg"
)


engine = create_engine(
    connection_string, connect_args={}, pool_recycle=300
)




def create_db_and_tables()->None:
    SQLModel.metadata.create_all(engine)





def get_session():
    with Session(engine) as session:
        yield session





# Kafka Producer as a dependency
async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers='broker:19092')
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()






async def consume_messages(topic, bootstrap_servers):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="inventory-management",
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


                    stock_to_remove = []
                    stock_to_add = []

                    for item in json_:
                        update_ = {}
                        update_['product_id'] = item.get('product_id', None)
                        update_['stock'] = item.get('quantity', None)
                        update_['method'] = item.get('method', None)

                        if "-" in update_["method"]:
                            stock_to_remove.append(update_)


                        elif "+" in update_["method"]:
                            stock_to_add.append(update_)



                    session = next(get_session())
                    producer = await anext(get_kafka_producer())


                    if len(stock_to_add) > 0:
                        status = await add_stock(stock_to_add, session, producer)



                    if len(stock_to_remove) > 0:
                        status = await remove_stock(stock_to_remove, session, producer)

                        print(status)



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
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Creating tables..")

    # Check if Kafka is ready before starting the consumer
    try:
        await wait_for_kafka_ready('broker')
    except RuntimeError as e:
        print(f"Error during Kafka startup: {e}")
        raise

    # Start Kafka consumer as a background task
    consumer_task = asyncio.create_task(consume_messages('inventory', 'broker:19092'))

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

        print("Kafka consumer stopped and FastAPI shutdown complete.")





app = FastAPI(lifespan=lifespan, title="Inventory Management Microservice", 
    version="0.0.1",
    servers=[
        {
            "url": "http://host.docker.internal:8089", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        },{
            "url": "http://localhost:8089",
            "description": "Development Server"
        }],
    root_path="/inventory"
    )








def decode_access_token(token: str):
    try:
        decoded_token = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return decoded_token
    
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except JWTError as e:
        raise HTTPException(status_code=401, detail="Invalid token")
    



@app.post("/new_stock")
def add_new_product_stock(token: str, data_from_user: Inventory, session: Annotated[Session, Depends(get_session)]):
    
    decoded = decode_access_token(token)
    

    if data_from_user:

        new_data = Inventory.model_validate(data_from_user)
        session.add(new_data)
        session.commit()
        session.refresh(new_data)
    
        return new_data


    else:
        raise HTTPException(status_code=400, detail="Enter all the required details")







# @app.post("/add_stock")
# async def add_stock(data_from_user: Inventory, session: Annotated[Session, Depends(get_session)]):



#     # decoded = decode_access_token(token)

#     # if decoded.get("role") == "admin":

#     if data_from_user:

#         stock_ = session.exec(select(Inventory).where(Inventory.product_id == data_from_user.product_id)).first()

#         if not stock_:
#             raise HTTPException(status_code=404, detail="Invalid stock ID!!!")

#         new_stock_data = data_from_user.model_dump(exclude_unset=True)

#         stock_.stock = stock_.stock + new_stock_data.get('stock')

        
#         # stock_.sqlmodel_update(new_stock_data)
#         session.add(stock_)
#         session.commit()
#         session.refresh(stock_)
    
#         return stock_

#     else:
#         raise HTTPException(status_code=400, detail="Enter all the required details")

#     # else:
#     #     raise HTTPException(status_code=404, detail="You don't have permission to perform this operation")


@app.post("/add_stock")
async def add_stock(data_from_user_: Inventory, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    
    new_stock_list: list[dict] = []
    product_ids = [data['product_id'] for data in data_from_user_]  # Collect all product_ids at once
    

    # Fetch all inventories in a single query for batch processing
    stock_list = session.exec(select(Inventory).where(Inventory.product_id.in_(product_ids))).all()


    if not stock_list:
        raise HTTPException(status_code=404, detail="No valid stock found for given IDs")

    stock_dict = {stock.product_id: stock for stock in stock_list}


    for data in data_from_user_:
        data_from_user = Inventory.model_validate(data)

        if data_from_user:
            user_product_id = data_from_user.product_id

            stock_ = stock_dict.get(user_product_id)

            if not stock_:
                raise HTTPException(status_code=404, detail=f"Invalid product ID: {user_product_id}")

            # Update the stock
            new_stock_data = data_from_user.model_dump(exclude_unset=True)
            stock_.stock += new_stock_data.get('stock', 0)

            # Append to the new stock list for Kafka message
            new_item_dict = {
                "id": user_product_id,
                "stock": stock_.stock
            }

            new_stock_list.append(new_item_dict)

    # Batch commit after processing all items
    session.add_all(stock_list)
    session.commit()
    
    # Send aggregated stock updates to Kafka
    if new_stock_list:
        serialized_data = json.dumps(new_stock_list).encode('utf-8')
        await producer.send_and_wait("product-stock", serialized_data)


    return new_stock_list







# @app.post("/remove_stock")
# async def remove_stock(data_from_user_: list[Inventory], session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):

#     # decoded = decode_access_token(token)

#     # if decoded.get("role") == "admin":

#     # print(data_from_user)
#     # print(type(data_from_user))   #dict
    
#     # print("remove function called!!!")

#     new_stock_list: list[dict] = []

#     for data in data_from_user_:

#         data_from_user = Inventory.model_validate(data)

#         if data_from_user:

#             # print("inside if remove stock")

#             user_product_id = data_from_user.product_id

#             stock_ = session.exec(select(Inventory).where(Inventory.product_id == user_product_id)).first()

#             if not stock_:
#                 raise HTTPException(status_code=404, detail="Invalid stock ID!!!")
            
#             # print(stock_)

#             new_stock_data = data_from_user.model_dump(exclude_unset=True)

#             stock_.stock = stock_.stock - new_stock_data.get('stock')

#             new_item_dict = {
#                 "id": user_product_id,
#                 "stock": stock_.stock
#             }

#             new_stock_list.append(new_item_dict)
            
#             # stock_.sqlmodel_update(new_stock_data)
#             session.add(stock_)
#             session.commit()
#             session.refresh(stock_)
        
#             return stock_

#         else:
#             raise HTTPException(status_code=400, detail="Enter all the required details")


#     if len(new_stock_list) > 0:

#         serialized_data = json.dumps(new_stock_list).encode('utf-8')

#         await producer.send_and_wait("product-stock", serialized_data)




@app.post("/remove_stock")
async def remove_stock(data_from_user_: list[Inventory], session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):


    new_stock_list: list[dict] = []
    product_ids = [data['product_id'] for data in data_from_user_]  # Collect all product_ids at once
    

    # Fetch all inventories in a single query for batch processing
    stock_list = session.exec(select(Inventory).where(Inventory.product_id.in_(product_ids))).all()


    if not stock_list:
        raise HTTPException(status_code=404, detail="No valid stock found for given IDs")

    stock_dict = {stock.product_id: stock for stock in stock_list}


    for data in data_from_user_:
        data_from_user = Inventory.model_validate(data)

        if data_from_user:
            user_product_id = data_from_user.product_id

            stock_ = stock_dict.get(user_product_id)

            if not stock_:
                raise HTTPException(status_code=404, detail=f"Invalid product ID: {user_product_id}")

            # Update the stock
            new_stock_data = data_from_user.model_dump(exclude_unset=True)
            stock_.stock -= new_stock_data.get('stock', 0)

            # Append to the new stock list for Kafka message
            new_item_dict = {
                "id": user_product_id,
                "stock": stock_.stock
            }

            new_stock_list.append(new_item_dict)

    # Batch commit after processing all items
    session.add_all(stock_list)
    session.commit()
    
    # Send aggregated stock updates to Kafka
    if new_stock_list:
        serialized_data = json.dumps(new_stock_list).encode('utf-8')
        await producer.send_and_wait("product-stock", serialized_data)


    return new_stock_list




@app.patch("/update_stock")
def update_stock(data_from_user: Inventory, session: Annotated[Session, Depends(get_session)]):

    # decoded = decode_access_token(token)

    # if decoded.get("role") == "admin":

    if data_from_user:

        stock_ = session.exec(select(Inventory).where(Inventory.product_id == data_from_user.product_id)).first()

        if not stock_:
            raise HTTPException(status_code=404, detail="Invalid stock ID!!!")


        new_stock_data = data_from_user.model_dump(exclude_unset=True)

        
        stock_.sqlmodel_update(new_stock_data)
        session.add(stock_)
        session.commit()
        session.refresh(stock_)
    
        return stock_

    else:
        raise HTTPException(status_code=400, detail="Enter all the required details")



