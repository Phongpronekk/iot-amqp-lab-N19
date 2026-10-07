"""Bai 1 - Producer: gui message chao mung vao queue iot_lab_queue.

Chay:
    python producer_bai1.py              # gui 1 message
    python producer_bai1.py -n 5 -i 1    # mo rong: gui 5 message, cach nhau 1 giay
"""
import argparse
import os
import time

import pika

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE_NAME = "iot_lab_queue"
MESSAGE = "Xin chao tu ung dung Python AMQP - B23DCCN644 – Le Cao Phong"


def main():
    parser = argparse.ArgumentParser(description="Bai 1 - Producer")
    parser.add_argument("-n", "--count", type=int, default=1, help="so message can gui")
    parser.add_argument("-i", "--interval", type=float, default=0.5, help="giay giua 2 message")
    args = parser.parse_args()

    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for i in range(1, args.count + 1):
        body = MESSAGE if args.count == 1 else f"{MESSAGE} (message {i}/{args.count})"
        channel.basic_publish(
            exchange="",  # default exchange -> routing_key la ten queue
            routing_key=QUEUE_NAME,
            body=body.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )
        print(f"[x] Da gui: {body}")
        if i < args.count:
            time.sleep(args.interval)

    connection.close()


if __name__ == "__main__":
    main()
