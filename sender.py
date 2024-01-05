import pika
import json

# Establish a connection to the RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue named 'blog_queue'
channel.queue_declare(queue='blog_queue')

# Sample data to be sent to the RabbitMQ queue
data = {
    'title': 'Sample Title',
    'text': 'Sample Text',
    'author': 'John Doe'
}

# Convert the data to JSON format
message_body = json.dumps(data)

# Publish the message to the 'blog_queue' queue
channel.basic_publish(exchange='',
                      routing_key='blog_queue',
                      body=message_body)

print(" [x] Sent data to the queue")

# Close the connection
connection.close()
