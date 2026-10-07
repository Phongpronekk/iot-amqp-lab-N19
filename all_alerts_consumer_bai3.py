"""Bai 3 (mo rong) - Consumer nhan TAT CA canh bao qua topic exchange.

Topic exchange 'iot_alert_topic_exchange', bind voi pattern 'iot.alert.#'.
"""
import os
import sys
from datetime import datetime

import pika

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
EXCHANGE = "iot_alert_topic_exchange"
QUEUE_NAME = "all_alerts_queue"
BINDING_KEY = "iot.alert.#"
LOG_FILE = os.path.join("logs", "alerts.log")


def on_message(ch, method, properties, body):
    text = body.decode("utf-8")
    level = method.routing_key.split(".")[-1]
    print(f"[{QUEUE_NAME}] ({level}) Da nhan: {text}")
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} [{QUEUE_NAME}] ({level}) {text}\n")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE, routing_key=BINDING_KEY)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    print(f"[*] '{QUEUE_NAME}' bind '{EXCHANGE}' voi pattern '{BINDING_KEY}'. Ctrl+C de dung.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[!] Da dung.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except pika.exceptions.AMQPConnectionError:
        sys.exit("Khong ket noi duoc RabbitMQ. Kiem tra broker / bien AMQP_URL.")
