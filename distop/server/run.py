import socket, sys
from distop import rpc
from distop import log as logging

LOG = logging.getLogger(__name__)

class Endpoints(object):
  _notifier = None
  @property
  def notifier(self):
    if self._notifier is None:
      self._notifier = rpc.get_notifier(socket.gethostname(), 'results')
    return self._notifier

  def pow(self, ctx, esp=1, data=None):
    res = data

    if data is not None:
      res = [el ** esp for el in data]

    LOG.debug(res)
    self.notifier.info(ctx, 'result', {'data': res})

if __name__ == '__main__':
  rpc.init()
  server = rpc.get_server(topic='pyclass', endpoints=[Endpoints(), ])
  server.start()
  server.wait()