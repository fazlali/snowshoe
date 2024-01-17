from src.snowshoe import snowshoe

app = snowshoe.Snowshoe(
    name='consumer_1',
    host='46.102.140.9',
    port=5672,
    username='rabbit',
    password='rabbit',
)


app.define_queues([
    snowshoe.Queue('my_queue', [snowshoe.QueueBinding('emitter_1', 'hello')])
])


@app.on('my_queue')
def queue_message_handler(message: snowshoe.Message):
    print(message.topic, message.data, message.delivery_tag)


app.run()
