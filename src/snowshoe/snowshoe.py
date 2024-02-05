import functools
import json
import secrets
import threading
import uuid
from enum import Enum
from json import JSONEncoder, JSONDecoder
from threading import Thread, get_ident
from time import sleep
from typing import Callable, TypedDict

import pika
from pika import exceptions
from pika.exchange_type import ExchangeType
from retry import retry

FAILED_MESSAGES_DLX = '_failed_messages_dlx'


class AckMethod(Enum):
    OFF = 0
    INSTANTLY = 1
    AUTO = 2


class FailureMethod(Enum):
    DROP = 0
    REQUEUE = 1
    DLX = 2


class QueueBinding:
    exchange: str
    routing_key: str

    def __init__(self, exchange: str, routing_key: str = '*') -> None:
        self.exchange = exchange
        self.routing_key = routing_key


class Queue:

    name: str
    bindings: list[QueueBinding]
    passive = False
    durable = False
    exclusive = False
    auto_delete = False
    failure_method = FailureMethod.DROP

    def __init__(
            self,
            name: str = None,
            bindings: list[QueueBinding] = None,
            failure_method=FailureMethod.DROP,
            passive=False,
            durable=False,
            exclusive=False,
            auto_delete=False,
    ) -> None:
        self.name = name
        self.bindings = bindings or []
        self.passive = passive
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.failure_method = failure_method


class Message:
    data: dict
    delivery_tag: int
    topic: str

    def __init__(self, data: dict, delivery_tag: int, topic: str) -> None:
        self.data = data
        self.delivery_tag = delivery_tag
        self.topic = topic


class Consumer(TypedDict):
    queue: Queue
    ack_method: AckMethod
    handler: Callable
    tag: str | None
    thread: Thread | None


class Heartbeat(TypedDict):
    sent: int
    received: int
    consumer: Consumer | None
    topic: str


class Snowshoe:
    connection: pika.BlockingConnection
    consumer_channel: pika.adapters.blocking_connection.BlockingChannel
    producer_channel: pika.adapters.blocking_connection.BlockingChannel
    name: str

    def __init__(
            self,
            name: str,
            host: str = '127.0.0.1',
            port: int = 5672,
            username: str = None,
            password: str = None,
            vhost: str = '/',
            concurrency: int = 1,
            json_encoder_class=JSONEncoder,
            json_decoder_class=JSONDecoder,
    ) -> None:
        self.name = name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=pika.PlainCredentials(
                username=username,
                password=password
            )
        ))
        self.consumer_channel = self.connection.channel()
        self.producer_channel = self.connection.channel()
        self.producer_channel.exchange_declare(self.name, ExchangeType.topic)
        self._queues: dict[str, Queue] = {}
        self._consumers: list[Consumer] = []
        self._heartbeat: Heartbeat = Heartbeat(
            sent=0,
            received=0,
            consumer=None,
            topic='heartbeat:' + secrets.token_urlsafe(15)
        )
        self.is_healthy = True
        self._main_thread_ident = get_ident()
        self.status = 'stopped'
        self.consumer_channel.basic_qos(prefetch_count=concurrency)
        self.json_encoder = json_encoder_class()
        self.json_decoder = json_decoder_class()

    def _echo(self):
        def ear(message: Message):
            if self._heartbeat['received'] < message.data['sequence']:
                self._heartbeat['received'] = message.data['sequence']

        def mouth():
            while self.status == 'running':
                sleep(10)
                self.is_healthy = self._heartbeat['sent'] != self._heartbeat['received']
                self._heartbeat['sent'] += 1
                self.emit('_heartbeat', {'sequence': self._heartbeat['sent']})

        self.emit('_heartbeat', {'sequence': self._heartbeat['sent']})

        Thread(target=mouth).start()

        queue = Queue(
            name='heartbeat[' + str(uuid.uuid4()) + ']',
            bindings=[QueueBinding(self.name, self._heartbeat['topic'])],
            durable=False,
            exclusive=True,
            auto_delete=True
        )
        self.define_queues([queue])
        self._heartbeat['consumer'] = self.on(queue, AckMethod.INSTANTLY)(ear)

    def run(self, wait: bool = True):

        @retry(exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
        def run():
            self._main_thread_ident = get_ident()
            self.status = 'running'
            self._echo()

            try:
                self.consumer_channel.start_consuming()
            except KeyboardInterrupt:
                self.consumer_channel.stop_consuming()

            self.status = 'stopping'
            for consumer in self._consumers:
                if consumer != self._heartbeat['consumer']:
                    consumer['thread'].join()

        thread = Thread(target=run, daemon=True)
        thread.start()
        if wait:
            thread.join()
        return thread

    def pause(self):
        for consumer in self._consumers:
            if consumer != self._heartbeat['consumer'] and consumer['tag'] is not None:
                if self._main_thread_ident == threading.get_ident():
                    self.consumer_channel.basic_cancel(consumer['tag'])
                else:
                    self.connection.add_callback_threadsafe(
                        functools.partial(self.consumer_channel.basic_cancel, consumer['tag'])
                    )
                consumer['tag'] = None

        for consumer in self._consumers:
            if consumer != self._heartbeat['consumer'] and consumer['thread']:
                consumer['thread'].join()

    def resume(self):
        not_running_consumers = [consumer for consumer in self._consumers if consumer['tag'] is None]
        self._consumers = [consumer for consumer in self._consumers if consumer['tag'] is not None]

        for consumer in not_running_consumers:
            self.on(consumer['queue'], consumer['ack_method'])(consumer["handler"])

    def define_queues(self, queues: list[Queue]):
        for queue in queues:
            self._queues[queue.name] = queue
            queue_name = self.name + ':' + queue.name if queue else ''
            if queue.failure_method == FailureMethod.DLX:
                self.consumer_channel.exchange_declare('_failed_messages_dlx', ExchangeType.topic)
                arguments = {'x-dead-letter-exchange': FAILED_MESSAGES_DLX}
            else:
                arguments = None
            self.consumer_channel.queue_declare(
                queue_name,
                passive=queue.passive,
                durable=queue.durable,
                exclusive=queue.exclusive,
                auto_delete=queue.auto_delete,
                arguments=arguments
            )
            for binding in queue.bindings:
                self.consumer_channel.exchange_declare(binding.exchange, ExchangeType.topic)
                self.consumer_channel.queue_bind(queue_name, binding.exchange, binding.routing_key)

    def emit(self, topic: str, data: dict, ttl: int = None):
        if self._main_thread_ident == threading.get_ident():
            return self.producer_channel.basic_publish(
                exchange=self.name,
                routing_key=topic,
                body=self.json_encoder.encode(data).encode(),
                properties=pika.BasicProperties(
                    content_type='application/json',
                    expiration=ttl
                )
            )

        else:
            self.connection.add_callback_threadsafe(
                functools.partial(
                    self.producer_channel.basic_publish,
                    exchange=self.name,
                    routing_key=topic,
                    body=self.json_encoder.encode(data).encode(),
                    properties=pika.BasicProperties(
                        content_type='application/json',
                        expiration=ttl
                    )
                )
            )

    def ack(self, delivery_tag: int = 0, multiple=False):
        if self._main_thread_ident == threading.get_ident():
            self.consumer_channel.basic_ack(delivery_tag, multiple)
        else:
            self.connection.add_callback_threadsafe(
                functools.partial(self.consumer_channel.basic_ack, delivery_tag, multiple)
            )

    def nack(self, delivery_tag: int = 0, multiple=False, requeue=True):
        if self._main_thread_ident == threading.get_ident():
            self.consumer_channel.basic_nack(delivery_tag, multiple, requeue)
        else:
            self.connection.add_callback_threadsafe(
                functools.partial(self.consumer_channel.basic_nack, delivery_tag, multiple, requeue)
            )

    @staticmethod
    def _consume(channel, queue: str, callback: Callable, auto_ack, consumer: Consumer):
        consumer_tag = channel.basic_consume(
            queue=queue,
            on_message_callback=callback,
            auto_ack=auto_ack
        )
        if consumer:
            consumer['tag'] = consumer_tag

    def on(self, queue: str | Queue, ack_method: AckMethod = AckMethod.AUTO):
        if isinstance(queue, str):
            queue: Queue = self._queues.get(queue)

        if not isinstance(queue, Queue):
            raise Exception('Invalid Queue')

        def wrapper(handler: Callable[[Message], any]) -> Consumer:

            def do_works(
                    _channel: pika.adapters.blocking_connection.BlockingChannel,
                    method: pika.spec.Basic.Deliver,
                    _properties: pika.spec.BasicProperties,
                    body: bytes
            ):
                try:
                    message = Message(data=self.json_decoder.decode(body.decode()), topic=method.routing_key, delivery_tag=method.delivery_tag)
                    result = handler(message)
                    if ack_method == AckMethod.AUTO:
                        self.ack(method.delivery_tag)
                    return result
                except Exception as e:
                    if ack_method == AckMethod.AUTO:
                        self.nack(method.delivery_tag, requeue=queue.failure_method == FailureMethod.REQUEUE)
                    raise e

            consumer = Consumer(
                queue=queue,
                ack_method=ack_method,
                handler=handler,
                tag=None,
                thread=None
            )

            def callback(*args, **kwargs):
                thread = Thread(target=do_works, args=args, kwargs=kwargs)
                consumer['thread'] = thread
                thread.start()
                return thread

            if self._main_thread_ident == threading.get_ident():
                self._consume(
                    channel=self.consumer_channel,
                    queue=self.name + ':' + queue.name,
                    callback=callback,
                    auto_ack=ack_method == AckMethod.INSTANTLY,
                    consumer=consumer
                )
            else:
                self.connection.add_callback_threadsafe(
                    functools.partial(
                        self._consume,
                        channel=self.consumer_channel,
                        queue=self.name + ':' + queue.name,
                        callback=callback,
                        auto_ack=ack_method == AckMethod.INSTANTLY,
                        consumer=consumer
                    )
                )

            self._consumers.append(consumer)
            return consumer

        return wrapper
