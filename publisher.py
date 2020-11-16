from kombu import Connection, Exchange, Producer
from kombu.exceptions import KombuError

class Config(object):
    rabbit_mq_host = 'amqp://localhost:5672/'
    rabbit_mq_exchange = 'test-exchange'
    rabbit_mq_routing_key = 'test-key'
    rabbit_mq_queue = 'test'

    def __init__(self, data):
        print(data)
        self._data = data

    def payload(self):
        return {'data': self._data}


class RabbitMQSender(object):

    def __init__(self):
        self._vhost = '/'
        self._exchange = Exchange(Config.rabbit_mq_exchange, type='direct', durable=True)

    def _create_connection(self):
        return Connection(hostname = Config.rabbit_mq_host,
                          virtual_host=self._vhost)

    def check_connection(self):
        try:
            with self._create_connection() as connection:
                connection.connect()
                connected = connection.connected
        except KombuError:
            connected = False

        print(connected)
        return connected

    def send(self, message):
        print("Send called", message)
        with self._create_connection() as connection:
            producer = Producer(connection,
                                exchange=self._exchange,
                                routing_key=Config.rabbit_mq_routing_key)

            payload = message.payload()

            producer.declare()

            producer.publish(payload, exchange=self._exchange, routing_key=Config.rabbit_mq_routing_key)


if __name__ == '__main__':
    Message = Config("Test")
    sender = RabbitMQSender()
    sender.send(Message)












