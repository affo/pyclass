import pika, logging, json, uuid, math
from threading import Thread, Semaphore, Lock
from distop.ops import RPC_QUEUE_NAME

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

class _Result(object):
  '''
    Class for a result composed of partials.  
    Partials are stored in a dict:

    ```  
    {
      start_index: chunk,
      ...
    }
    ```

    To get the result, a call to `get` method must be done.  
    The call to `get` is blocking until the result is `completed`.

    Everytime a partial is added using the method 
    `add_partial`, the count of chunks is incremented and 
    compared with the expected number of chunks (given on `__init__`).  
    If the count is equal, then a semaphore is released.  
    The same semaphore is acquired on `get` call.

    That's why `get` call is blocking.
  '''

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
  '''
    Class for the consumer thread.

    The consumer thread consumes messages received 
    on an exclusive queue. It takes chunks 
    and stores them into a `_Result` object.

    The consumer exposes a `get_result` method which 
    returns the result contained in the `_Result` object.  
    The call to `get_result` is blocking 
    (it is essentially a wrapper fot `_Result`'s `get` method).
  '''

  def __init__(self, no_chunks, job_id, rabbit_host):
    Thread.__init__(self)
    self._job_id = job_id
    self._res = _Result(no_chunks)

    # pika stuff
    self._connection = pika.BlockingConnection(
      pika.ConnectionParameters(rabbit_host)
    )
    self._channel = self._connection.channel()
    q = self._channel.queue_declare(exclusive=True)
    self.q_name = q.method.queue

    self._channel.basic_consume(
      self.on_partial,
      queue=self.q_name
    )

  def on_partial(self, ch, method, props, body):
    if self._job_id == props.correlation_id:
      body = json.loads(body)
      chunk = body['result']
      i = body['start_index']
      chunk_size = len(chunk)

      self._res.add_partial(i, chunk)

      LOG.info(
        (
          'Partial result from {}, '
          'JOB#{}: index {}, '
          'data {} -> {}'
        ).format(body['hostname'], self._job_id[:4], i, chunk, self._res.partial)
      )

      # sending basic ack
      self._channel.basic_ack(delivery_tag=method.delivery_tag)

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
  '''
    Class which represents a distibuted operation.

    Once the client calls the `call` method, a 
    `_ConsumerThread` object is created and a new thread is started 
    and indexed using a unique job ID.

    The data (passed on `__init__`) is splitted into chunks.
    The chunks are sent to `RPC_QUEUE_NAME` queue together with 
    the `OP_NAME` attribute of this class.
    The operation of splitting into chunks and 
    sending to multiple servers is called 'scattering'.

    When the method `get_result` is called it results in 
    a blocking call. In fact, the `get_result` method is invoked 
    choosing the right `_ConsumerThread` object basing on 
    the `job_id` parameter given.
    When the result is available, method `gather` is applied to it 
    and the result obtained returned.

    'Gathering' operation is left to be implemented by the developer. 

    `call` method returns the job_id that has to be passed to 
    `get_result` method (compulsory only if we have concurrent jobs).
  '''

  OP_NAME = 'nop'

  def __init__(self, data, group_size=1, rabbit_host='localhost'):
    assert len(data) > 0
    super(DistOp, self).__init__()
    self._rabbit_host = rabbit_host

    self.data = list(data) # we want it to be a list
    self.group_size = group_size
    self.chunk_size = len(data) / group_size
    self.chunk_number = math.ceil(float(len(data)) / self.chunk_size)

    self._consumers = {}

    # pika stuff
    self._connection = pika.BlockingConnection(
      pika.ConnectionParameters(rabbit_host)
    )
    self._channel = self._connection.channel()
    q = self._channel.queue_declare(RPC_QUEUE_NAME)
    self._q_name = q.method.queue

  def _chunks(self):
    for i in xrange(0, len(self.data), self.chunk_size):
      yield i, self.data[i:i+self.chunk_size]

  def call(self, **kwargs):
    job_id = str(uuid.uuid4())
    self._consumers[job_id] = _ConsumerThread(
      self.chunk_number,
      job_id,
      self._rabbit_host,
    )
    self._consumers[job_id].start()
    result_queue = self._consumers[job_id].q_name

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
        routing_key=RPC_QUEUE_NAME,
        properties=pika.BasicProperties(
          reply_to=result_queue,
          correlation_id=job_id
        ),
        body=json.dumps(body)
      )

    return job_id

  def gather(self, partials):
    '''
      Implements how partials have to be combined.
      :partials: dict
    '''
    return partials.values()

  def get_result(self, job_id=None):
    if job_id is None:
      keys = self._consumers.keys()
      if len(keys) == 1:
        # if we have only one active job
        # we do not need job_id
        job_id = keys[0]
      else:
        # we do not know what to choose
        # or we do not have any active job
        raise Exception('job_id parameter required!')

    res = self._consumers[job_id].get_result()
    del self._consumers[job_id]
    return self.gather(res)
