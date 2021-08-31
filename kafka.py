#!/usr/bin/python
import logging
import logging.handlers
from pykafka import KafkaClient

logging.basicConfig(filename="kafka.log", level=logging.INFO)
logger = logging.getLogger()
logger.setLevel('INFO')
BASIC_FORMAT = "%(asctime)s:%(levelname)s:%(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)

# 输出到控制台的handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
# 也可以不设置，不设置就默认用logger的level
console_handler.setLevel('INFO')

# 输出到文件的handler
file_handler = logging.handlers.RotatingFileHandler(
    'kafka.log', maxBytes=500 * 1024 * 1024, backupCount=5)
file_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


class Message:
    def __init__(self, hosts, topic):
        self.hosts = hosts
        self.client = KafkaClient(hosts=hosts)
        self.topic = self.client.topics[bytes(topic, encoding='utf8')]

    def send(self, msg):
        producer = self.topic.get_producer(sync=True)
        producer.start()
        logger.info("准备发送: host: %s topic: %s  msg: %s" % (self.hosts, self.topic, msg))

        with self.topic.get_sync_producer() as producer:
            producer.produce(bytes(msg, encoding='utf8'))
            logging.info("发送成功")


if __name__ == '__main__':
    # kafka地址
    hosts = "192.168.245.31:9092,192.168.245.32:9092,192.168.245.127:9092"
    topic = "extendLine"

    # 生产消息
    msg = """{"line":"182","isUpDown":0,"length":10448,"oId":141,"dId":51681,"passenger":10,"transfer":2,
    "station":134090,"bestExtendStation":135871,"extendLength":0.9390000000000001,"nearestDepot":15,
    "distToDepot":0.97,"points":[{"no":0,"lng":113.55144633684608,"lat":34.80762253678913},{"no":1,
    "lng":113.55579852272906,"lat":34.80763830893417},{"no":2,"lng":113.56059632149147,"lat":34.80763390708342},
    {"no":3,"lng":113.56063754440657,"lat":34.810788127347514}]}"""

    x = Message(hosts=hosts, topic=topic)
    x.send(msg)
