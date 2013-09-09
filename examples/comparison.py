from cell.actors import Actor

__author__ = 'rumi'

from celery import Celery
celery = Celery('hello', broker='amqp://guest@localhost//')

@celery.task
def hello():
    return 'Hello world'

class Greeter(Actor):
    class state:
        def hello(self):
            return 'hello_world'