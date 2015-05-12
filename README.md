# Examples using Pika python library

See [RabbitMQ's Pika tutorials](https://www.rabbitmq.com/tutorials/tutorial-one-python.html).

To install (with virtualenv):

```
$ git clone git@github.com:affear/pyclass.git
$ cd pyclass
$ sudo apt-get install python-virtualenv
$ virtualenv venv
$ source pyclassrc # this will activate venv and add examples to PYTHONPATH
$ pip install -r requirements.txt
```

Every examples needs RabbitMQ (or another messaging service) to run.  
To use RabbitMQ without installing use docker:

```
docker run -d -e RABBITMQ_NODENAME=my-rabbit --name some-rabbit -p 5672:5672 rabbitmq:3
```

### __WARNING__
All commands in next examples (except from `docker run` ones), have to be launched in the root folder of this project.

## `pc` example
A simple producer/consumer example.

We use a named queue (`pc`) to make a producer and a consumer communicate.  
Messages are acked using `channel.basic_ack` function.

Run a consumer:
```
python pc/consumer.py
```

And then run a producer as many times as you want:
```
python pc/producer.py my awesome message
```

See logs to understand what happens.

The consumer can be started even _after_ the producer, given that messages are persisted to the named queue.

## `pikachat` example
A group chat example.

To execute, run a sender and a receiver per host:

  * `python pikachat/receive.py [RABBIT_HOST]`
  * In another shell: `python pikachat/send.py [RABBIT_HOST]`

In this case we use and _exchange_ in _fanout_ mode to communicate. Exchanges cannot store messages, so, if you start the receiver _after_ the sender, the messages will be lost.

## `distop` example
This example shows a scatter/gather (or map/reduce, if you want) operation on a list using RPC calls on a number of servers.  
The input list is divided in equally long chunks basing on the number of servers given. Each chunk is sent to a queue and processed by _one server only_.  
Partial results are sent back to the client that gathers them as it prefers.

The RPC call is designed to be _asynchronous_. This means that after calling, the client can do what it wants and then invoke the method `get_result` (a blocking call) to get the complete result of the operation.

### Implement a new operation
On the client side:

  1. Create a `caller.py` file;
  2. Add a new class extending `distop.ops.DistOp`;
  3. Override the method `gather` to define how the scattered chunks have to be gathered when returned from servers;
  4. Define a name for the operation, overriding the parameter `OP_NAME`.

The result should be something like:

```
from distop.ops import DistOp

class FooOp(DistOp):
  OP_NAME = 'foo'

  def gather(self, partials):
    # define your gather function
    return partials.values()

if __name__ == '__main__':
  op = FooOp(data=list(xrange(30)))
  op.call()
  print op.get_result()
```

For a sample `caller.py` file, see `distop/caller.py`.

On the server side:

  1. Create a `callee.py` file;
  2. Add a new class extending `distop.ops.OpExecutor`;
  3. Add the real implementation of the operation defining a method whose name has to coincide with the name defined in the client overriding the parameter `OP_NAME`.

Example:

```
from distop.ops import OpExecutor

class FooExecutor(OpExecutor):

  # remember that in the client we had
  # OP_NAME = 'foo'?
  def foo(self, chunk):
    # define the logic of the operation
    return chunk

if __name__ == '__main__':
  ex = FooExecutor()
  ex.run()
```

For a sample `callee.py` file, see `distop/callee.py`.

### Executing
Servers can be virtualized using Docker:

  1. Build with `docker build -t distopsrv .`;
  2. run with `docker run -d distopsrv`. If you want to run `N` servers use a command like: `for i in {1..N}; do docker run -d distopsrv; done`;
  3. run the client with `python distop/client.py` (use option `--help` to see possible parameters);
  4. stop servers (if you want) with `docker stop $(docker ps | awk '/distop/ {print $1}')`.

Operations 2 and 3 can be inverted, given that messages are saved to queues.
