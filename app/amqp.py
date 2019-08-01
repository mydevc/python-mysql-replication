#!/usr/bin/env python
import pika
import json

credentials = pika.PlainCredentials("admin", "dirdir")

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='qeue.rs.youfa365.com',
        port=5672,
        credentials=credentials,
        virtual_host='/'
    )
)
channel = connection.channel()
channel.queue_declare(queue='default_queue', durable=True)

message = {
    'class': "\\MZ\Models\\user\\UserModel",
    'method': 'getUserById',
    'data': 1000
}

body = json.dumps(message)
channel.basic_publish(exchange='default_ex', routing_key='default_route', body=body)
print(" [x] Sent body: "+body)
connection.close()
