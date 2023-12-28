import json
from enum import Enum
from typing import Callable

import pika
from pika import exceptions
from retry import retry


class QueueBinding:
    exchange: str
    routing_key: str

    def __init__(self, exchange: str, routing_key: str = '*') -> None:
        self.exchange = exchange
        self.routing_key = routing_key


class Queue:

    name: str
    bindings: list[QueueBinding]
    passive=False
    durable=False
    exclusive=False
    auto_delete=False
    arguments=None

    def __init__(
            self,
            name: str,
            bindings: list[QueueBinding] = None,
            passive=False,
            durable=False,
            exclusive=False,
            auto_delete=False,
            arguments=None
    ) -> None:
        self.name = name
        self.bindings = bindings or []
        self.passive = passive
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments


class AckMethod(Enum):
    OFF = 0
    ON_FETCH = 1
    ON_SUCCESS = 2


class Message:
    data: dict
    delivery_tag: int
    topic: str

    def __init__(self, data: dict, delivery_tag: int, topic: str) -> None:
        self.data = data
        self.delivery_tag = delivery_tag
        self.topic = topic


class Snowshoe:
    connection: pika.BlockingConnection
    channel: pika.adapters.blocking_connection.BlockingChannel
    name: str

    def __init__(self, name, host: str, port: int, username: str, password: str) -> None:
        self.name = name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=pika.PlainCredentials(
                username=username,
                password=password
            )
        ))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(self.name, 'topic')

    @retry(exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
    def start(self):
        self.channel.start_consuming()

    def define_queues(self, queues: list[Queue]):
        for queue in queues:
            queue_name = self.name + ':' + queue.name
            self.channel.queue_declare(
                queue_name,
                passive=queue.passive,
                durable=queue.durable,
                exclusive=queue.exclusive,
                auto_delete=queue.auto_delete,
                arguments=queue.arguments
            )
            for binding in queue.bindings:
                self.channel.queue_bind(queue_name, binding.exchange, binding.routing_key)

    def emit(self, topic: str, data: dict):
        return self.channel.basic_publish(exchange=self.name, routing_key=topic, body=json.dumps(data).encode())

    def on(self, queue: str | Queue, ack_method: AckMethod = AckMethod.ON_SUCCESS):
        if isinstance(queue, Queue):
            queue = self.name + ':' + queue.name
        else:
            queue = self.name + ':' + queue

        def wrapper(handler: Callable[[Message], any]):
            def callback(
                    _channel: pika.adapters.blocking_connection.BlockingChannel,
                    method: pika.spec.Basic.Deliver,
                    _properties: pika.spec.BasicProperties,
                    body: bytes
            ):
                result = handler(Message(data=json.loads(body), topic=method.routing_key, delivery_tag=method.delivery_tag))
                if ack_method == AckMethod.ON_SUCCESS:
                    self.channel.basic_ack(method.delivery_tag)
                return result

            self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=ack_method == AckMethod.ON_FETCH)

        return wrapper
