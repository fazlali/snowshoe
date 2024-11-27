import functools
import secrets
import threading
import uuid
import pika
import pika.exceptions

from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from json import JSONEncoder, JSONDecoder
from threading import Thread, get_ident
from time import sleep
from typing import Callable
from pika.exchange_type import ExchangeType

FAILED_MESSAGES_DLX = '_failed_messages_dlx'


class AckMethod(Enum):
    OFF = 0
    INSTANTLY = 1
    AUTO = 2


class FailureMethod(Enum):
    DROP = 0
    REQUEUE = 1
    DLX = 2


@dataclass
class QueueBinding:
    exchange: str
    routing_key: str = '*'


@dataclass
class Queue:
    name: str
    bindings: list[QueueBinding] = field(default_factory=list)
    passive: bool = False
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    max_priority: int = 0
    consumer_timeout: int = None
    failure_method: FailureMethod = FailureMethod.DROP


@dataclass
class Death:
    reason: str
    queue: str
    time: datetime
    exchange: str
    count: int = 1
    routing_keys: list[str] = field(default_factory=list)


@dataclass
class Message:
    data: dict
    delivery_tag: int
    topic: str
    id: str = None
    exchange: str = None
    redelivered: bool = False
    first_death: Death = None
    deaths: list[Death] = field(default_factory=list)


@dataclass
class Consumer:
    queue: Queue
    handler: Callable
    ack_method: AckMethod
    tag: str = ''
    threads: list[Thread] = field(default_factory=list)
    processed_messages: int = 0
    failed_messages: int = 0

    def join(self):
        threads = self.threads.copy()
        for thread in threads:
            if thread.is_alive():
                thread.join()


@dataclass
class Heartbeat:
    topic: str
    sent: int = 0
    received: int = 0
    consumer: Consumer | None = None


class Snowshoe:
    connection: pika.BlockingConnection | None
    consumer_channel: pika.adapters.blocking_connection.BlockingChannel | None
    producer_channel: pika.adapters.blocking_connection.BlockingChannel | None
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
        self.connection = None
        self.consumer_channel = None
        self.producer_channel = None
        self._connection_thread_ident: int = 0
        self._delivery_tags = set()
        self._thread_idents = set()
        self.concurrency = concurrency
        self.name = name
        self._connection_parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=pika.PlainCredentials(username=username, password=password)
        )

        self._queues: dict[str, Queue] = {}
        self._consumers: list[Consumer] = []
        self._heartbeat: Heartbeat = Heartbeat(topic='_heartbeat:' + secrets.token_urlsafe(15))
        self._is_healthy = True
        self._state = 'stopped'
        self.json_encoder = json_encoder_class()
        self.json_decoder = json_decoder_class()

        self.ensure_connections()
        self.producer_channel.exchange_declare(self.name, ExchangeType.topic)

    @property
    def is_healthy(self):
        return self._is_healthy

    @property
    def status(self):
        return {
            'is_healthy': self.is_healthy,
            'state': self._state,
            'consumers': [
                {
                    'handler': consumer.handler.__name__,
                    'threads': len(consumer.threads),
                    'processed_messages': consumer.processed_messages,
                    'failed_messages': consumer.failed_messages
                }
                for consumer in self._consumers
                if consumer != self._heartbeat.consumer
            ]
        }

    def ensure_connections(self, redefine_queues: bool = True):
        while self._state == 'connecting':
            sleep(1)
        self._state = 'connecting'
        if not self.connection or self.connection.is_closed or self.consumer_channel.is_closed or self.producer_channel.is_closed:
            try:
                self.connection = pika.BlockingConnection(self._connection_parameters)
                self.consumer_channel = self.connection.channel()
                self.producer_channel = self.connection.channel()
                self._delivery_tags.clear()
                self._thread_idents.clear()
                self._connection_thread_ident = get_ident()
                self._state = 'running'
            except Exception as e:
                self._state = 'stopped'
                raise e
            self.consumer_channel.basic_qos(prefetch_count=self.concurrency)
            for consumer in self._consumers:
                consumer.tag = None

            if redefine_queues:
                self.define_queues(list(self._queues.values()))
            self.resume()
        self._state = 'running'

    def _cancel(self, consumer_tag: str):
        return self.consumer_channel.basic_cancel(consumer_tag)

    def _emit(self, topic: str, data: dict, ttl: int = None, priority: int = None):
        return self.producer_channel.basic_publish(
            exchange=self.name,
            routing_key=topic,
            body=self.json_encoder.encode(data).encode(),
            properties=pika.BasicProperties(
                content_type='application/json',
                expiration=ttl,
                priority=priority,
                headers={'x-message-id': str(uuid.uuid4())}
            )
        )

    def _ack(self, delivery_tag: int = 0, multiple=False):
        if delivery_tag in self._delivery_tags:
            self._delivery_tags.remove(delivery_tag)
            return self.consumer_channel.basic_ack(delivery_tag, multiple)

    def _nack(self, delivery_tag: int = 0, multiple=False, requeue=True):
        if delivery_tag in self._delivery_tags:
            self._delivery_tags.remove(delivery_tag)
            return self.consumer_channel.basic_nack(delivery_tag, multiple, requeue)

    def _consume(self, queue: str, callback: Callable, auto_ack, consumer: Consumer):
        consumer_tag = self.consumer_channel.basic_consume(
            queue=queue,
            on_message_callback=callback,
            auto_ack=auto_ack
        )
        if consumer:
            consumer.tag = consumer_tag

    def _echo(self):
        def ear(message: Message):
            if self._heartbeat.received < message.data['sequence']:
                self._heartbeat.received = message.data['sequence']

        def mouth():
            while self._state == 'running':
                sleep(10)
                if self.connection.is_closed:
                    self._is_healthy = False
                else:
                    self._is_healthy = self._heartbeat.sent == self._heartbeat.received
                self._heartbeat.sent += 1
                self.emit(self._heartbeat.topic, {'sequence': self._heartbeat.sent})

        queue = Queue(
            name='heartbeat[' + str(uuid.uuid4()) + ']',
            bindings=[QueueBinding(self.name, self._heartbeat.topic)],
            exclusive=True,
            auto_delete=True,
            durable=False,
        )
        self.define_queues([queue])
        self._heartbeat.consumer = self.on(queue, AckMethod.INSTANTLY)(ear)

        Thread(target=mouth, daemon=True).start()

    def _run(self):
        while True:
            try:
                self.consumer_channel.start_consuming()
            except pika.exceptions.AMQPConnectionError:
                while True:
                    try:
                        self.ensure_connections()
                        break
                    except pika.exceptions.AMQPConnectionError:
                        continue
            except KeyboardInterrupt:
                self.consumer_channel.stop_consuming()
                break

        for consumer in self._consumers:
            if consumer != self._heartbeat.consumer:
                consumer.join()

        self.connection.close()

    def run(self, wait: bool = True):
        self._state = 'running'
        self._echo()

        thread = Thread(target=self._run, daemon=True)
        thread.start()
        if wait:
            thread.join()

        return thread

    def pause(self):
        for consumer in self._consumers:
            if consumer != self._heartbeat.consumer and consumer.tag is not None:
                self.cancel(consumer.tag)
                consumer.tag = None

        for consumer in self._consumers:
            if consumer != self._heartbeat.consumer:
                consumer.join()

    def resume(self):
        not_running_consumers = [consumer for consumer in self._consumers if consumer.tag is None]
        self._consumers = [consumer for consumer in self._consumers if consumer.tag is not None]

        for consumer in not_running_consumers:
            self.on(consumer.queue, consumer.ack_method)(consumer.handler)

    def define_queues(self, queues: list[Queue], force: bool = False):
        for queue in queues:
            self._queues[queue.name] = queue
            queue_name = self.name + ':' + queue.name if queue else ''
            arguments = {}
            if queue.failure_method == FailureMethod.DLX:
                self.consumer_channel.exchange_declare('_failed_messages_dlx', ExchangeType.topic)
                arguments['x-dead-letter-exchange'] = FAILED_MESSAGES_DLX
            if queue.max_priority:
                arguments['x-max-priority'] = queue.max_priority
            if queue.consumer_timeout is not None:
                arguments['x-consumer-timeout'] = queue.consumer_timeout

            try:
                self.consumer_channel.queue_declare(
                    queue_name,
                    passive=queue.passive,
                    durable=queue.durable,
                    exclusive=queue.exclusive,
                    auto_delete=queue.auto_delete,
                    arguments=arguments
                )
            except pika.exceptions.ChannelClosedByBroker:
                self.ensure_connections(redefine_queues=False)
                self.consumer_channel.queue_delete(queue_name, if_unused=not force, if_empty=not force)
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

    def _call_threadsafe(self, callback):
        if self._connection_thread_ident == get_ident():
            self.connection.call_later(0, callback)
        else:
            self.connection.add_callback_threadsafe(callback)

    def emit(self, topic: str, data: dict, ttl: int = None, priority: int = None):
        self.ensure_connections()
        self._call_threadsafe(functools.partial(self._emit, topic=topic, data=data, ttl=ttl, priority=priority))

    def cancel(self, consumer_tag: str):
        self._call_threadsafe(functools.partial(self._cancel, consumer_tag=consumer_tag))

    def ack(self, delivery_tag: int = 0, multiple=False):
        self.ensure_connections()
        self._call_threadsafe(functools.partial(self._ack, delivery_tag=delivery_tag, multiple=multiple))

    def nack(self, delivery_tag: int = 0, multiple=False, requeue=True):
        self.ensure_connections()
        self._call_threadsafe(functools.partial(self._nack, delivery_tag=delivery_tag, multiple=multiple, requeue=requeue))

    def on(self, queue: str | Queue, ack_method: AckMethod = AckMethod.AUTO):
        if isinstance(queue, str):
            queue: Queue = self._queues.get(queue)

        if not isinstance(queue, Queue):
            raise Exception('Invalid Queue')

        def wrapper(handler: Callable[[Message], any]) -> Consumer:

            def do_works(
                    _channel: pika.adapters.blocking_connection.BlockingChannel,
                    method: pika.spec.Basic.Deliver,
                    properties: pika.spec.BasicProperties,
                    body: bytes
            ):
                if properties.headers and properties.headers.get('x-first-death-exchange'):
                    deaths = [
                        Death(
                            count=item['count'],
                            reason=item['reason'],
                            queue=item['queue'],
                            time=item['time'],
                            exchange=item['exchange'],
                            routing_keys=item['routing-keys']
                        )
                        for item in properties.headers.get('x-death', [])
                    ]
                    first_death = next((
                        death
                        for death in deaths
                        if (
                            death.queue == properties.headers['x-first-death-queue']
                            and death.reason == properties.headers['x-first-death-reason']
                            and death.exchange == properties.headers['x-first-death-exchange']
                            )
                    ), None)
                else:
                    deaths = []
                    first_death = None

                if ack_method != AckMethod.INSTANTLY:
                    self._delivery_tags.add(method.delivery_tag)

                message = Message(
                    id=properties.headers.get('x-message-id'),
                    data=self.json_decoder.decode(body.decode()),
                    topic=method.routing_key,
                    delivery_tag=method.delivery_tag,
                    exchange=method.exchange,
                    redelivered=method.redelivered,
                    first_death=first_death,
                    deaths=deaths
                )
                try:
                    result = handler(message)
                    if ack_method == AckMethod.AUTO:
                        self.ack(method.delivery_tag)
                    consumer.processed_messages += 1
                    return result
                except Exception as e:
                    consumer.failed_messages += 1
                    if ack_method == AckMethod.AUTO:
                        self.nack(method.delivery_tag, requeue=queue.failure_method == FailureMethod.REQUEUE)
                    raise e
                finally:
                    consumer.threads.remove(threading.current_thread())

            consumer = Consumer(
                queue=queue,
                ack_method=ack_method,
                handler=handler,
            )

            self._consumers.append(consumer)

            def callback(*args, **kwargs):
                thread = Thread(target=do_works, args=args, kwargs=kwargs)
                consumer.threads.append(thread)
                thread.start()
                return thread

            if self._connection_thread_ident == get_ident():
                self._consume(
                    queue=self.name + ':' + queue.name,
                    callback=callback,
                    auto_ack=ack_method == AckMethod.INSTANTLY,
                    consumer=consumer
                )
            else:
                self.connection.add_callback_threadsafe(
                    functools.partial(
                        self._consume,
                        queue=self.name + ':' + queue.name,
                        callback=callback,
                        auto_ack=ack_method == AckMethod.INSTANTLY,
                        consumer=consumer
                    )
                )

            return consumer

        return wrapper
