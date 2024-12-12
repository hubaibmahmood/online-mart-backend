from jose import jwt, JWTError, ExpiredSignatureError
from fastapi import  FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from datetime import datetime, timedelta
from sqlmodel import SQLModel,Field, Session, create_engine, select
from typing import Annotated, Optional, AsyncGenerator
from product_management import settings
from contextlib import asynccontextmanager
import asyncio, json
from aiokafka import AIOKafkaConsumer




ALGORITHM = "HS256"
SECRET_KEY = "A Secure Secret Key"




class Product(SQLModel, table=True):
    product_id: Optional[int] = Field(default=None, index=True, primary_key=True)
    name: str = Field(index=True)
    description: str = Field(index=True)
    price: float = Field(index=True)
    category: str = Field(index=True)
    stock: int = Field(index=True)


class UpdateProduct(SQLModel):

    id: Optional[int]
    name: Optional[str] 
    description: Optional[str] 
    price: Optional[float]
    category: Optional[str]
    stock: Optional[int]


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
        group_id="product-stock-management",
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

                    items_list: list[dict] = []

                    for item in json_:
                        updated_value = {}
                        updated_value['id'] = item.get('id', None)
                        updated_value['stock'] = item.get('stock', None)

                        items_list.append(updated_value)



                    session = next(get_session())                    
                    status = update_product(items_list, session)

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
async def lifespan(app: FastAPI)-> AsyncGenerator[None, None]:
    print("Creating tables..")

    # Check if Kafka is ready before starting the consumer
    try:
        await wait_for_kafka_ready('broker')
    except RuntimeError as e:
        print(f"Error during Kafka startup: {e}")
        raise


    # Start Kafka consumer as a background task
    consumer_task = asyncio.create_task(consume_messages('product-stock', 'broker:19092'))

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





app = FastAPI(lifespan=lifespan, title="Product Management Microservice", 
    version="0.0.1",
    servers=[
        {
            "url": "http://host.docker.internal:8086", # ADD NGROK URL Here Before Creating GPT Action
            "description": "Development Server"
        },{
            "url": "http://localhost:8086",
            "description": "Development Server"
        }],
    root_path="/product"
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
    


def ensure_all_fields(data: dict):

    defaults = {"name": '', "description": '', "price": '', "category": '', "stock": ''}
    
    # Use dict comprehension to only update missing keys with defaults
    return {key: data.get(key, default) for key, default in defaults.items()}






@app.post("/create")
def create_product(token: str, data_from_user: Product, session: Annotated[Session, Depends(get_session)]):



    decoded = decode_access_token(token)

    if decoded.get("role") == "admin":

        if data_from_user:
            new_data = Product.model_validate(data_from_user)
            session.add(new_data)
            session.commit()
            session.refresh(new_data)
        
            return new_data

        else:
            raise HTTPException(status_code=400, detail="Enter all the required details")

    else:
        raise HTTPException(status_code=404, detail="You don't have permission to perform this operation")




@app.get("/products")
def get_all_products(session: Annotated[Session, Depends(get_session)]):
    products = session.exec(select(Product)).all()

    return products





@app.post("/products")
def get_specific_product(id: int, session: Annotated[Session, Depends(get_session)]):

    product = session.exec(select(Product).where(Product.product_id == id)).one()

    if product:
        return product

    else:
        raise HTTPException(status_code=404, detail="Invalid ID. No product found!!!")





# @app.patch("/products")
# def update_product(updated_product: UpdateProduct, session: Annotated[Session, Depends(get_session)]):
    

#     # decoded = decode_access_token(token)

#     # if decoded.get("role") == "admin":

#     # print("update product called!!!")


#     if updated_product:

#         product = session.exec(select(Product).where(Product.product_id == id)).one()

#         if not product:
#             raise HTTPException(status_code=404, detail="Product Not found!!!")
        
#         # If updated_product_ is a dict, handle it directly
#         if isinstance(updated_product, dict):
#             # Loop through the keys in the updated_product_
#             for key, value in updated_product.items():
#                 if hasattr(product, key):  # Check if the product has the attribute
#                     setattr(product, key, value)  # Update only the fields that are provided

#         # If updated_product_ is an instance of UpdateProduct, use its data
#         elif isinstance(updated_product, UpdateProduct):
#             new_product_data = updated_product.model_dump(exclude_unset=True)
#             for key, value in new_product_data.items():
#                 setattr(product, key, value)



#         # new_product_data = updated_product.model_dump(exclude_unset=True)
#         # product.sqlmodel_update(new_product_data)
#         session.add(product)
#         session.commit()
#         session.refresh(product)

#         return product


    
#     else:
#         raise HTTPException(status_code=400, detail="Updated content can't be empty")


#     # else:
#     #     raise HTTPException(status_code=404, detail="You don't have permission to perform this operation")





@app.patch("/products")
def update_product(updated_product: list[UpdateProduct], session: Annotated[Session, Depends(get_session)]):


    product_ids = [update['id'] for update in updated_product]  

    

    products = session.exec(select(Product).where(Product.product_id.in_(product_ids))).all()

    if not products:
        raise HTTPException(status_code=404, detail="Products not found")

    
    
    product_dict = {product.product_id: product for product in products}

    
    for update in updated_product:
    
        product_id = update.get('id')
    
        if not product_id:
            raise HTTPException(status_code=400, detail="Product ID is missing in the update")

        product = product_dict.get(product_id)

        if not product:
            raise HTTPException(status_code=404, detail=f"Product with ID {product_id} not found")

        # Apply each update field to the product object
        for key, value in update.items():
            if key != 'id' and hasattr(product, key):
                setattr(product, key, value)  # Update the field if it exists on the product model



    # Commit all changes in one batch
    session.add_all(products)
    session.commit()

    # Optionally refresh the products (to get any updated data like timestamps)
    for product in products:
        session.refresh(product)

    return products  # Return the updated products




@app.delete("/products")
def delete_product(token: str, id: int, session: Annotated[Session, Depends(get_session)]):


    decoded = decode_access_token(token)

    if decoded.get("role") == "admin":


        product = session.exec(select(Product).where(Product.product_id == id)).one()
        
        if product:
            session.delete(product)
            session.commit()

            return {"message": f"Product deleted successfullyy!!!"}
        
        else:
            raise HTTPException(status_code=404, detail="Invalid ID. Product Not Found!!!")
        

    else:
        raise HTTPException(status_code=404, detail="You don't have permission to perform this operation")

