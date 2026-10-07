"""Bai 2 - Monitoring Consumer: nhan telemetry tu sensor_data_queue va canh bao."""
import json
import os
import sys

import pika

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE_NAME = "sensor_data_queue"
TEMP_MAX = 35
HUMIDITY_MIN = 40


def on_message(ch, method, properties, body):
    try:
        data = json.loads(body.decode("utf-8"))
        device = data["device_id"]
        temperature = float(data["temperature"])
        humidity = float(data["humidity"])
    except (ValueError, KeyError, TypeError) as err:
        print(f"[!] Payload khong hop le, bo qua: {err}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f"Device: {device}")
    print(f"Temperature: {temperature}")
    print(f"Humidity: {humidity}")
    if "timestamp" in data:
        print(f"Timestamp: {data['timestamp']}")
    if temperature > TEMP_MAX:
        print("CANH BAO: Nhiet do cao")
    if humidity < HUMIDITY_MIN:
        print("CANH BAO: Do am thap")
    print("-" * 30)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    print(f"[*] Dang giam sat queue '{QUEUE_NAME}'. Ctrl+C de dung.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[!] Da dung monitor.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except pika.exceptions.AMQPConnectionError:
        sys.exit("Khong ket noi duoc RabbitMQ. Kiem tra broker / bien AMQP_URL.")
