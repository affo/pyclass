from distop import rpc, ops
from distop import log as logging

LOG = logging.getLogger(__name__)

if __name__ == '__main__':
  rpc.init()

  powop = ops.PowOp(esp=3, data=list(xrange(10)), group_size=2)
  powop.execute()
  res = powop.get_result()
  LOG.debug('Result obtained: {}'.format(res))