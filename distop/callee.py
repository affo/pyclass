import sys
from distop.ops import OpExecutor

class MyOpExecutor(OpExecutor):

  def nop(self, chunk):
    return chunk

  def pow(self, chunk, esp=1):
    return [el ** esp for el in chunk]

  def plusone(self, chunk):
    return [el + 1 for el in chunk]


if __name__ == '__main__':
  rabbit_host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
  ex = MyOpExecutor(rabbit_host)
  ex.run()
