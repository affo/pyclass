from oslo import messaging
from threading import Thread, Semaphore
import random
from distop import rpc
from distop import log as logging

LOG = logging.getLogger(__name__)

class DistOp(object):
  def __init__(self, data, group_size=1):
    assert len(data) > 0
    super(DistOp, self).__init__()

    self.data = data
    self.group_size = group_size
    self.chunk_size = len(data) / group_size
    self._semaphore = Semaphore(0)
    self.id = int(random.random() * 1000)
    # create empty result list
    self._res = [None for i in xrange(len(self.data))]

    self._listener = rpc.get_notification_listener(topic='results', endpoints=[self])

    # start the server in another process
    def start_listener(l):
      l.start()
      l.wait()

    th = Thread(target=start_listener, args=(self._listener, ))
    self._th = th
    th.start()

  def info(self, ctxt, publisher_id, event_type, payload, metadata):
    res = payload['data']
    i = ctxt['start_index']
    chunk_size = len(res)
    self._res[i:i+chunk_size] = res
    LOG.debug(
      'JOB#{} Partial result: index {}, data {} -> {}'.format(self.id, i, res, self._res)
    )
    # release semaphore if completed
    if None not in self._res:
      self._semaphore.release()
      self._listener.stop()


  def _chunks(self):
    for i in xrange(0, len(self.data), self.chunk_size):
      yield i, self.data[i:i+self.chunk_size]

  def operation(self, ctxt, chunk):
    # this will contain rpc calls
    pass

  def execute(self):
    ctxt = {'job': self.id}
    for i, chunk in self._chunks():
      ctxt['start_index'] = i
      self.operation(ctxt, chunk)

  def get_result(self):
    LOG.debug('Getting result...')
    self._semaphore.acquire()
    # close thread
    self._th.join()
    return self._res

##### Implementations
import rpcapi

class PowOp(DistOp):
  def __init__(self, esp, *args, **kwargs):
    super(PowOp, self).__init__(*args, **kwargs)
    self.esp = esp

  def operation(self, ctxt, chunk):
    rpcapi.pow(ctxt, esp=self.esp, data=chunk)