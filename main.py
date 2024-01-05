from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from elasticsearch import Elasticsearch, helpers
import pika
import json
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel
import logging
import os

# Setting up logger
logger = logging.getLogger(__name__)

# Configuration
class Config:
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
    ELASTICSEARCH_HOST = "localhost"
    ELASTICSEARCH_PORT = 9200
    elasticsearch_url = f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"

# Dependency manager class
class DependencyManager:
    def __init__(self):
        self.es = Elasticsearch([{'host': Config.ELASTICSEARCH_HOST, 'port': Config.ELASTICSEARCH_PORT, 'scheme': 'http'}])
        self.connection = None
        self.channel = None

    def get_elasticsearch(self) -> Elasticsearch:
        return self.es

    def get_rabbitmq_connection(self) -> pika.BlockingConnection:
        if self.connection is None or self.connection.is_closed:
            raise HTTPException(status_code=500, detail="RabbitMQ connection is not open.")
        return self.connection

dependency_manager = DependencyManager()

# Initializing RabbitMQ connection
def init_rabbitmq(dependency_manager: DependencyManager):
    try:
        if dependency_manager.connection is None or dependency_manager.connection.is_closed:
            logger.info(f"Connecting to RabbitMQ server at {Config.RABBITMQ_HOST}...")
            dependency_manager.connection = pika.BlockingConnection(pika.ConnectionParameters(Config.RABBITMQ_HOST))
            dependency_manager.channel = dependency_manager.connection.channel()
            dependency_manager.channel.queue_declare(queue='blog_queue')
            logger.info("RabbitMQ connection established successfully.")
        logger.info("RabbitMQ connection is already open.")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error establishing RabbitMQ connection: {e}")
    except Exception as ex:
        logger.error(f"Unexpected error: {ex}", exc_info=True)

# Closing RabbitMQ connection
def close_rabbitmq_connection(dependency_manager: DependencyManager):
    if dependency_manager.connection and dependency_manager.connection.is_open:
        dependency_manager.connection.close()

app = FastAPI()


# OAuth2 password bearer for authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Event handlers for startup and shutdown
@app.on_event("startup")
async def startup_event():
    init_rabbitmq(dependency_manager)

@app.on_event("shutdown")
async def shutdown_event():
    close_rabbitmq_connection(dependency_manager)

# Validation model for Content
class Content(BaseModel):
    title: str
    text: str
    author: str

# Endpoint for inserting content
@app.post("/insert")
async def insert_content(content: Content, current_user: str = Depends(oauth2_scheme)) -> dict:
    data = {
        'title': content.title,
        'text': content.text,
        'author': content.author,
        'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'user': current_user
    }
    dependency_manager.channel.basic_publish(exchange='', routing_key='blog_queue', body=json.dumps(data))
    return {"message": "Content added to the queue for processing"}

# Endpoint for listing content
@app.get("/list", response_model=List[dict])
async def list_content() -> List[dict]:
    res = dependency_manager.es.search(index='blog', body={"_source": ["title", "date"]})
    hits = res['hits']['hits']
    return [{"title": hit['_source']['title'], "date": hit['_source']['date']} for hit in hits]

# Endpoint for searching content
@app.get("/search/{term}", response_model=List[dict])
async def search_content(term: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[dict]:
    query = {
        "query": {
            "bool": {
                "must": [{"match": {"text": term}}],
                "filter": [
                    {"range": {"date": {"gte": start_date, "lte": end_date}}} if start_date and end_date else {}
                ]
            }
        }
    }
    res = dependency_manager.es.search(index='blog', body=query)
    hits = res['hits']['hits']
    return [{"title": hit['_source']['title'], "date": hit['_source']['date']} for hit in hits]

# Bulk insert function
def insert_to_elasticsearch_bulk(data_list: List[dict]) -> None:
    actions = [
        {
            "_op_type": "index",
            "_index": "blog",
            "_source": data
        }
        for data in data_list
    ]
    helpers.bulk(dependency_manager.es, actions)

# Sample usage of bulk insert
@app.post("/insert_bulk")
async def insert_bulk(data_list: List[Content]) -> dict:
    """
    Endpoint to insert content in bulk into the system.

    Parameters:
    - data_list (List[Content]): List of content items to be inserted in bulk.

    Returns:
    - dict: A message indicating that content has been added to the queue for processing.
    """
    bulk_data = [
        {
            'title': content.title,
            'text': content.text,
            'author': content.author,
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        for content in data_list
    ]

    insert_to_elasticsearch_bulk(bulk_data)

    return {"message": "Content added to the queue for processing in bulk"}


