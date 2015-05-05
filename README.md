#Sample code for RPC client/server communication with oslo.messaging

To install (with virtualenv):

```
$ git clone git@github.com:affear/pyclass.git
$ cd pyclass
$ sudo apt-get install python-virtualenv
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

To run:

* Start RabbitMQ (or else) service: `docker run -d -e RABBITMQ_NODENAME=my-rabbit --name some-rabbit -p 5672:5672 rabbitmq:3`
* On a shell: `$ python server.py`
* On _another_ shell: `$ python client.py`
* Enjoy