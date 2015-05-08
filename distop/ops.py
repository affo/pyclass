import pika, logging, json, uuid, math
from threading import Thread, Semaphore, Lock

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

EXCHANGE_NAME = 'distop'
OP_TOPIC = 'ops'
RES_TOPIC = 'result'

class _Result(object):
  def __init__(self, no_chunks):
    super(_Result, self).__init__()
    self._n = no_chunks
    self._res = {}
    self._lock = Lock()
    self._no_collected = 0
    self._completed_sem = Semaphore(0)

  @property
  def completed(self):
    self._lock.acquire()
    comp = self._no_collected == self._n
    self._lock.release()
    return comp

  # no setter provided
  @property
  def partial(self):
    return self._res

  def add_partial(self, key, partial):
    # thread-safe
    self._lock.acquire()
    self._res[key] = partial
    self._no_collected += 1
    self._lock.release()

    if self.completed:
      self._completed_sem.release()

  def get(self):
    self._completed_sem.acquire()
    # release the semaphore for further
    # invokations of get()
    self._completed_sem.release()
    return self._res

class _ConsumerThread(Thread):

  def __init__(self, no_chunks, corr_id, rabbit_host):
    Thread.__init__(self)
    self._corr_id = corr_id
    self._ready_sem = Semaphore(0)
    self._res = _Result(no_chunks)

    # pika stuff
    self._connection = pika.BlockingConnection(
      pika.ConnectionParameters(rabbit_host)
    )
    self._channel = self._connection.channel()
    q = self._channel.queue_declare(exclusive=True)
    self._q_name = q.method.queue
    self._channel.exchange_declare(EXCHANGE_NAME, type='topic')
    self._channel.queue_bind(
      exchange=EXCHANGE_NAME,
      queue=self._q_name,
      routing_key=RES_TOPIC
    )

    self._channel.basic_consume(
      self.on_partial,
      queue=self._q_name
    )

    # consumer is ready, you can run ops now.
    self._ready_sem.release()

  def wait_until_ready(self):
    self._ready_sem.acquire()
    # release the semaphore for further
    # invokations of wait_until_ready()
    self._ready_sem.release()

  def on_partial(self, ch, method, props, body):
    # sending basic ack
    self._channel.basic_ack(delivery_tag=method.delivery_tag)

    if self._corr_id == props.correlation_id:
      body = json.loads(body)
      chunk = body['result']
      i = body['start_index']
      chunk_size = len(chunk)

      self._res.add_partial(i, chunk)

      LOG.info('Partial result: index {}, data {} -> {}'.format(i, chunk, self._res.partial))

  def get_result(self):
    res = self._res.get()
    self.stop() # the result is completed now!
    return res

  def run(self):
    from pika import exceptions
    try:
      self._channel.start_consuming()
    except exceptions.ConnectionClosed:
      pass

  def stop(self):
    try:
      self._connection.close()
    except RuntimeError:
      # https://github.com/pika/pika/pull/500
      # open issue
      pass


class DistOp(object):
  OP_NAME = 'nop'

  def __init__(self, data, group_size=1, rabbit_host='localhost'):
    assert len(data) > 0
    super(DistOp, self).__init__()
    self._rabbit_host = rabbit_host

    self.data = list(data) # we want it to be a list
    self.group_size = group_size
    self.chunk_size = len(data) / group_size
    self.chunk_number = math.ceil(float(len(data)) / self.chunk_size)

    # pika stuff
    self._connection = pika.BlockingConnection(
      pika.ConnectionParameters(rabbit_host)
    )
    self._channel = self._connection.channel()
    q = self._channel.queue_declare(exclusive=True)
    self._q_name = q.method.queue
    self._channel.exchange_declare(EXCHANGE_NAME, type='topic')

  def _chunks(self):
    for i in xrange(0, len(self.data), self.chunk_size):
      yield i, self.data[i:i+self.chunk_size]

  def call(self, **kwargs):
    corr_id = str(uuid.uuid4())
    self._consumer = _ConsumerThread(
      self.chunk_number,
      corr_id,
      self._rabbit_host
    )
    self._consumer.start()

    # wait for the consumer to be ready
    self._consumer.wait_until_ready()

    # scattering
    for i, chunk in self._chunks():
      body = {
        'chunk': chunk,
        'start_index': i,
        'params': kwargs
      }

      self._channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=OP_TOPIC + '.' + self.OP_NAME,
        properties=pika.BasicProperties(
          reply_to=self._q_name,
          correlation_id=corr_id
        ),
        body=json.dumps(body)
      )

  def gather(self, partials):
    '''
      Implements how partials have to be combined.
      :partials: dict
    '''
    return partials.values()

  def get_result(self):
    res = self._consumer.get_result()
    return self.gather(res)


##### Implementations
class PowOp(DistOp):
  OP_NAME = 'pow'

  def gather(self, partials):
    no_el = sum([len(p) for p in partials.values()])
    res = [None for _ in xrange(no_el)]

    for i, chunk in partials.iteritems():
      res[i:i+len(chunk)] = chunk

    return res