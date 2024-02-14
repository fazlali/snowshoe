from src.snowshoe import snowshoe

app = snowshoe.Snowshoe(
    name='consumer_3_f',
    host='46.102.140.9',
    port=5672,
    username='rabbit',
    password='rabbit',
)


app.define_queues([
    snowshoe.Queue(
        name='_failed_message_queue',
        bindings=[snowshoe.QueueBinding(snowshoe.FAILED_MESSAGES_DLX, '#')]
    )
])


@app.on('_failed_message_queue', ack_method=snowshoe.AckMethod.INSTANTLY)
def queue_message_handler(message: snowshoe.Message):
    print('received new failed message', message.topic, message.data, message.delivery_tag, message.deaths)


app.run()
