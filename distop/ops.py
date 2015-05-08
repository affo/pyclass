import pika, logging, json, uuid, math, socket
from threading import Thread, Semaphore, Lock

_RPC_QUEUE_NAME = 'rpc_queue'
_HOSTNAME = socket.gethostname()

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

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

  def __init__(self, no_chunks, corr_id, rabbit_host, reply_queue):
    Thread.__init__(self)
    self._corr_id = corr_id
    self._res = _Result(no_chunks)

    # pika stuff
    self._connection = pika.BlockingConnection(
      pika.ConnectionParameters(rabbit_host)
    )
    self._channel = self._connection.channel()
    q = self._channel.queue_declare(reply_queue)
    self._q_name = q.method.queue

    self._channel.basic_consume(
      self.on_partial,
      queue=self._q_name
    )

  def on_partial(self, ch, method, props, body):
    # sending basic ack
    self._channel.basic_ack(delivery_tag=method.delivery_tag)

    if self._corr_id == props.correlation_id:
      body = json.loads(body)
      chunk = body['result']
      i = body['start_index']
      chunk_size = len(chunk)

      self._res.add_partial(i, chunk)

      LOG.info('Partial result from {}: index {}, data {} -> {}'.format(body['hostname'], i, chunk, self._res.partial))

  def get_result(self):
    res = self._res.get()
    self.stop() # the result is completed now!
    return res

  def run(self):
    from pika import exceptions
    try:
      self._channel.start_consuming()
    except exceptions.ConnectionClosed, RuntimeError:
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
    q = self._channel.queue_declare('doesntmatter')
    self._q_name = q.method.queue

  def _chunks(self):
    for i in xrange(0, len(self.data), self.chunk_size):
      yield i, self.data[i:i+self.chunk_size]

  def call(self, **kwargs):
    corr_id = str(uuid.uuid4())
    self._consumer = _ConsumerThread(
      self.chunk_number,
      corr_id,
      self._rabbit_host,
      self._q_name
    )
    self._consumer.start()

    # scattering
    for i, chunk in self._chunks():
      body = {
        'chunk': chunk,
        'start_index': i,
        'op_name': self.OP_NAME,
        'params': kwargs
      }

      self._channel.basic_publish(
        exchange='',
        routing_key=_RPC_QUEUE_NAME,
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

class OpExecutor(object):

  def __init__(self, rabbit_host='localhost'):
    super(OpExecutor, self).__init__()

    # pika stuff
    self._connection = pika.BlockingConnection(
      pika.ConnectionParameters(rabbit_host)
    )
    self._channel = self._connection.channel()
    q = self._channel.queue_declare(_RPC_QUEUE_NAME)
    self._q_name = q.method.queue

    self._channel.basic_consume(
      self.on_op,
      queue=self._q_name
    )

  def on_op(self, ch, method, props, body):
    body = json.loads(body)

    LOG.info(str(body))

    chunk = body['chunk']
    i = body['start_index']
    op_name = body['op_name']
    params = body['params']
    corr_id = props.correlation_id

    res = self.execute(op_name, chunk, params)

    self._channel.basic_ack(delivery_tag=method.delivery_tag)
    response = {
      'result': res,
      'start_index': i,
      'hostname': _HOSTNAME,
    }
    self._channel.basic_publish(
      exchange='',
      routing_key=props.reply_to,
      properties=pika.BasicProperties(
        correlation_id=corr_id
      ),
      body=json.dumps(response)
    )

  def execute(self, op_name, chunk, param_dict):
    try:
      op = getattr(self, op_name)
      res = op(chunk, **param_dict)
      return res
    except AttributeError:
      LOG.error('No implementation for op {}'.format(op_name))
      return None # should be handled better...

  def run(self):
    try:
      self._channel.start_consuming()
    except KeyboardInterrupt:
      self._channel.stop_consuming()
    finally:
      self._connection.close()