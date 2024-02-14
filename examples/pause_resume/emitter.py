from time import time, sleep

from src.snowshoe import Snowshoe

app = Snowshoe(
    name='emitter_1',
    host='46.102.140.9',
    port=5672,
    username='rabbit',
    password='rabbit',
)

while True:
    app.emit('hello', {'now': time()})
    sleep(2)
