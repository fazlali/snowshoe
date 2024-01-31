from time import sleep

from src.snowshoe import snowshoe

app = snowshoe.Snowshoe(
    name='consumer_1',
    host='46.102.140.9',
    port=5672,
    username='rabbit',
    password='rabbit',
    concurrency=50
)

app.define_queues([
    snowshoe.Queue('my_queue', [snowshoe.QueueBinding('emitter_1', 'hello')])
])


@app.on('my_queue')
def queue_message_handler(message: snowshoe.Message):
    print(message.topic, message.data, message.delivery_tag)
    sleep(4)


app.run()
