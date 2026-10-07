"""Bai 1 - Consumer: lang nghe queue iot_lab_queue, chay lien tuc den khi Ctrl+C."""
import os
import sys
from datetime import datetime

import pika

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE_NAME = "iot_lab_queue"


def on_message(ch, method, properties, body):
    print(f"Da nhan message: {body.decode('utf-8')}")
    print(f"Thoi gian nhan: {datetime.now().strftime('%H:%M:%S')}")
    print()
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    print(f"[*] Dang cho message tu queue '{QUEUE_NAME}'. Nhan Ctrl+C de dung.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[!] Da dung consumer.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except pika.exceptions.AMQPConnectionError:
        sys.exit("Khong ket noi duoc RabbitMQ. Kiem tra broker / bien AMQP_URL.")
