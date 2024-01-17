import random

from src.snowshoe import snowshoe

app = snowshoe.Snowshoe(
    name='consumer_3',
    host='46.102.140.9',
    port=5672,
    username='rabbit',
    password='rabbit',
)


app.define_queues([
    snowshoe.Queue(
        name='my_queue',
        bindings=[snowshoe.QueueBinding('emitter_3', 'hello')],
        failure_method=snowshoe.FailureMethod.DLX
    )
])


@app.on('my_queue')
def queue_message_handler(message: snowshoe.Message):
    print(message.topic, message.data, message.delivery_tag)
    if random.randrange(0, 2):
        print('failing process of handling message')
        raise Exception('Fake failure')


app.run()
