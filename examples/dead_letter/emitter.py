from time import time, sleep

from snowshoe import Snowshoe

app = Snowshoe(
    name='emitter_3',
    host='127.0.0.1',
    port=5672,
    username='rabbit',
    password='rabbit',
)

while True:
    app.emit('hello', {'now': time()})
    sleep(1)
