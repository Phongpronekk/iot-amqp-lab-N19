"""Bai 3 - Alert Producer: gui canh bao info/warning/critical.

Gui vao:
  - direct exchange 'iot_alert_exchange' (routing key = info | warning | critical)
  - topic exchange 'iot_alert_topic_exchange' (routing key = iot.alert.<level>)  [mo rong]

Chay:
    python alert_producer_bai3.py
"""
import os

import pika

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
DIRECT_EXCHANGE = "iot_alert_exchange"
TOPIC_EXCHANGE = "iot_alert_topic_exchange"

ALERTS = [
    ("info", "Nhiet do phong may chuot muc info"),
    ("warning", "Nhiet do phong may vuot nguong warning"),
    ("critical", "Cam bien kho lanh mat ket noi critical"),
]


def main():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    channel.exchange_declare(exchange=DIRECT_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=TOPIC_EXCHANGE, exchange_type="topic", durable=True)

    props = pika.BasicProperties(delivery_mode=2)
    for level, message in ALERTS:
        channel.basic_publish(DIRECT_EXCHANGE, routing_key=level, body=message.encode(), properties=props)
        channel.basic_publish(TOPIC_EXCHANGE, routing_key=f"iot.alert.{level}", body=message.encode(), properties=props)
        print(f"[x] Da gui ({level}): {message}")

    connection.close()


if __name__ == "__main__":
    main()
