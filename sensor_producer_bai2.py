"""Bai 2 - Sensor Producer: mo phong cam bien gui telemetry moi 3 giay (JSON).

Chay:
    python sensor_producer_bai2.py                          # 1 cam bien sensor01
    python sensor_producer_bai2.py -d sensor01 sensor02 sensor03   # mo rong: nhieu cam bien
"""
import argparse
import json
import os
import random
import time
from datetime import datetime

import pika

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE_NAME = "sensor_data_queue"


def main():
    parser = argparse.ArgumentParser(description="Bai 2 - Sensor Producer")
    parser.add_argument("-d", "--devices", nargs="+", default=["sensor01"], help="danh sach device_id")
    parser.add_argument("-i", "--interval", type=float, default=3.0, help="chu ky gui (giay)")
    args = parser.parse_args()

    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"[*] Gui du lieu cho {args.devices} moi {args.interval}s. Ctrl+C de dung.")
    try:
        while True:
            for device_id in args.devices:
                payload = {
                    "device_id": device_id,
                    "temperature": round(random.uniform(25.0, 40.0), 1),
                    "humidity": round(random.uniform(30.0, 80.0), 1),
                    "timestamp": datetime.now().isoformat(timespec="seconds"),  # mo rong
                }
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE_NAME,
                    body=json.dumps(payload),
                    properties=pika.BasicProperties(
                        content_type="application/json", delivery_mode=2
                    ),
                )
                print(f"[x] Da gui: {json.dumps(payload)}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[!] Da dung sensor.")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
