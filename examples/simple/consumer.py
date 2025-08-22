from time import sleep

from snowshoe import snowshoe

app = snowshoe.Snowshoe(
    name='consumer_1',
    host='127.0.0.1',
    port=5672,
    username='rabbit',
    password='rabbit',
    concurrency=1
)

app.define_queues([
    snowshoe.Queue('my_queue', [snowshoe.QueueBinding('emitter_1', 'hello')])
])


@app.on('my_queue')
def queue_message_handler(message: snowshoe.Message):
    print(message.topic, message.data, message.delivery_tag)
    sleep(0.5)


app.run()
