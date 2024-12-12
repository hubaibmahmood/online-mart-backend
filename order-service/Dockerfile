FROM python:3.12

LABEL maintainer = "hubaib-mahmood"

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*



WORKDIR /code

COPY . /code/


RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --quiet

EXPOSE 8000


CMD [ "poetry", "run", "uvicorn", "order_service.main:app", "--host", "0.0.0.0", "--port" ,"8087", "--reload"]
