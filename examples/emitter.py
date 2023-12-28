from time import time, sleep

from snowshoe import Snowshoe

app = Snowshoe(
    name='emitter_1',
    host='127.0.0.1',
    port=5672,
    username='rabbit',
    password='rabbit',
)

while True:
    app.emit('time', {'now': time()})
    sleep(1)
