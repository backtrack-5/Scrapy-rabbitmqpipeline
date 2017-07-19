import pika
import json


class RabbitPipeline(object):
    """
    Created RabbitPipeline to publish crawled data to RabbitMQ using Blocking connection.
    """

    def __init__(self, rabbit_server, rabbit_user,rabbit_password,rabbit_exchange,rabbit_routingkey,rabbit_exchange_type,rabbit_port=5672,rabbit_socket_timeout=10):
        """
        Pipeline __init__ to setup Rabbitmq connections
        :param rabbit_server:
        :param rabbit_user:
        :param rabbit_password:
        :param rabbit_exchange:
        :param rabbit_routingkey:
        :param rabbit_exchange_type:
        :param rabbit_port:
        :param rabbit_socket_timeout:
        :return:
        """
        self.rabbit_server = rabbit_server
        self.rabbit_user = rabbit_user
        self.rabbit_password = rabbit_password
        self.rabbit_exchange = rabbit_exchange
        self.rabbit_routingkey = rabbit_routingkey
        self.rabbit_exchange_type = rabbit_exchange_type
        self.rabbit_port = rabbit_port
        self.rabbit_socket_timeout = rabbit_socket_timeout

    @classmethod
    def from_crawler(cls, crawler):
        """
        reads setting from Crawler settings and returns to Rabbitmq Pipeline.
        :param crawler:
        :return:
        """
        return cls(
            rabbit_server=crawler.settings.get('RABBIT_SERVER'),
            rabbit_user=crawler.settings.get('RABBIT_USER', 'guest'),
            rabbit_password=crawler.settings.get('RABBIT_PASSWORD', 'guest'),
            rabbit_exchange=crawler.settings.get('RABBIT_EXCHANGE', 'exchangeScrapy'),
            rabbit_routingkey=crawler.settings.get('RABBIT_ROUTINGKEY', 'exchangeScrapy'),
            rabbit_exchange_type=crawler.settings.get('RABBIT_EXCHANGE_TYPE', 'direct'),
            rabbit_queue=crawler.settings.get('RABBIT_QUEUE', 'Scrapy'),
            rabbit_port=crawler.settings.get('RABBIT_PORT', 5672),
            rabbit_socket_timeout=crawler.settings.get('RABBIT_SOCKET_TIMEOUT',10)
        )

    def open_spider(self, spider):
        """
        this module creates the Rabbitmq connection and channel when the spider is started.
        :param spider:
        :return:
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_server, port=self.rabbit_port,
                                          credentials= pika.PlainCredentials(self.rabbit_user,self.rabbit_password),
                                          socket_timeout=self.rabbit_socket_timeout))
        self.channel = self.connection.channel()


    def close_spider(self, spider):
        """
        Closes the Rabbitmq connection upon spider shut down.
        :param spider:
        :return:
        """
        self.connection.close()

    def process_item(self, item, spider):
        """
        this module publishes crawled items to Rabbitmq server
        It uses blocking connection.
        :param item:
        :param spider:
        :return:
        """
        self.channel.basic_publish(exchange=self.rabbit_exchange,
                      routing_key=self.rabbit_routingkey,
                      body=json.dumps(item))
        return item