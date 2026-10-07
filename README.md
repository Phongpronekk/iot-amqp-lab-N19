# Thực hành IoT: Lập trình Python với giao thức AMQP

- **Sinh viên:** Lê Cao Phong – B23DCCN644
- **Môn:** Thực hành IoT (IOT_N19_TH2) – PTIT

## 1. Broker sử dụng

- **RabbitMQ 4.3.6** (giao thức AMQP 0-9-1), cài trực tiếp trên Windows, chạy tại `localhost:5672`.
- Thư viện Python: **pika**.
- Giao diện quản trị: http://localhost:15672 (user/pass: `guest`/`guest`).

> **Lưu ý:** RabbitMQ 4.3.x cần **Erlang/OTP 27.x**. Erlang 29 làm node không khởi động được (lỗi `incompatible_feature_flags`), nên máy chạy bài này dùng Erlang 27.

Mặc định các chương trình kết nối `amqp://guest:guest@localhost:5672/%2F`.
Nếu dùng broker khác (ví dụ broker dùng chung của lớp), đặt biến môi trường `AMQP_URL`:

```powershell
$env:AMQP_URL = "amqp://user:pass@<ip-broker>:5672/%2F"      # PowerShell
# export AMQP_URL="amqp://user:pass@<ip-broker>:5672/%2F"    # Linux/macOS
```

Bật giao diện quản trị (chạy một lần, bằng quyền Administrator):

```powershell
rabbitmq-plugins enable rabbitmq_management
```

## 2. Cài đặt

```powershell
python -m venv .venv
.venv\Scripts\activate          # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Cấu trúc thư mục

```
iot-amqp-lab/
├── producer_bai1.py            # Bài 1: gửi message vào iot_lab_queue
├── consumer_bai1.py            # Bài 1: nhận message, in nội dung + thời điểm nhận
├── sensor_producer_bai2.py     # Bài 2: cảm biến gửi JSON mỗi 3 giây
├── monitor_consumer_bai2.py    # Bài 2: nhận dữ liệu, in cảnh báo
├── alert_producer_bai3.py      # Bài 3: gửi cảnh báo info/warning/critical
├── warning_consumer_bai3.py    # Bài 3: warning_queue (key "warning")
├── critical_consumer_bai3.py   # Bài 3: critical_queue (key "critical")
├── all_alerts_consumer_bai3.py # Bài 3 (mở rộng): nhận tất cả qua topic exchange
├── images/                     # Ảnh kết quả chạy
├── requirements.txt
└── README.md
```

## 4. Cách chạy và kết quả

Mỗi chương trình chạy trong một terminal riêng (đã `activate` `.venv`). Consumer chạy liên tục, dừng bằng `Ctrl+C`. **Nên chạy consumer trước, producer sau.**

### Bài 1 – Producer → Queue → Consumer (`iot_lab_queue`)

```powershell
python consumer_bai1.py                 # terminal 1
python producer_bai1.py                 # terminal 2 (gửi 1 message)
python producer_bai1.py -n 5 -i 1       # mở rộng: gửi 5 message, cách nhau 1 giây
```

- Producer kết nối broker, khai báo queue `iot_lab_queue` và gửi message `Xin chao tu ung dung Python AMQP - B23DCCN644 – Le Cao Phong`.
- Consumer in nội dung message và thời điểm nhận.
- **Mở rộng:** producer gửi nhiều message liên tiếp (`-n`, `-i`); consumer chạy liên tục đến khi `Ctrl+C`.

**Producer:**

![Bài 1 - Producer](images/bai1_producer.PNG)

**Consumer:**

![Bài 1 - Consumer](images/bai1_consumer.PNG)

### Bài 2 – Mô phỏng cảm biến IoT (`sensor_data_queue`)

```powershell
python monitor_consumer_bai2.py                                   # terminal 1
python sensor_producer_bai2.py                                    # terminal 2 (1 cảm biến, 3 giây/lần)
python sensor_producer_bai2.py -d sensor01 sensor02 sensor03      # mở rộng: 3 cảm biến
```

- Payload JSON gồm `device_id`, `temperature`, `humidity` (giá trị ngẫu nhiên), gửi mỗi 3 giây.
- Consumer in mã thiết bị, nhiệt độ, độ ẩm; nhiệt độ > 35 → `CANH BAO: Nhiet do cao`, độ ẩm < 40 → `CANH BAO: Do am thap`.
- **Mở rộng:** thêm trường `timestamp`; mô phỏng nhiều cảm biến cùng lúc.

**Sensor Producer:**

![Bài 2 - Sensor Producer](images/bai2_sensor_producer.PNG)

**Monitoring Consumer:**

![Bài 2 - Monitoring Consumer](images/bai2_monitor_consumer.PNG)

### Bài 3 – Exchange và routing key (`iot_alert_exchange`)

```powershell
python warning_consumer_bai3.py         # terminal 1: warning_queue  <- key "warning"
python critical_consumer_bai3.py        # terminal 2: critical_queue <- key "critical"
python all_alerts_consumer_bai3.py      # terminal 3 (mở rộng): topic exchange, pattern "iot.alert.#"
python alert_producer_bai3.py           # terminal 4: gửi info, warning, critical
```

- Producer tạo direct exchange `iot_alert_exchange` và gửi 3 mức cảnh báo với routing key `info`, `warning`, `critical`.
- `warning_queue` chỉ nhận `warning`; `critical_queue` chỉ nhận `critical`; `info` không có queue nào nhận.
- **Mở rộng:**
  - Topic exchange `iot_alert_topic_exchange`: `all_alerts_consumer_bai3.py` bind pattern `iot.alert.#` nên nhận tất cả cảnh báo.
  - Các consumer ghi cảnh báo nhận được ra file `logs/alerts.log`.

**Alert Producer:**

![Bài 3 - Alert Producer](images/bai3_alert_producer.PNG)

**Warning Consumer** (chỉ nhận `warning`):

![Bài 3 - Warning Consumer](images/bai3_warning_consumer.PNG)

**Critical Consumer** (chỉ nhận `critical`):

![Bài 3 - Critical Consumer](images/bai3_critical_consumer.PNG)

**All Alerts Consumer** (topic exchange, nhận tất cả):

![Bài 3 - All Alerts Consumer](images/bai3_all_alerts_consumer.PNG)

**File log cảnh báo (`logs/alerts.log`):**

![Log cảnh báo](images/alerts_log.PNG)

### Giao diện quản trị RabbitMQ

**Danh sách exchange** (`iot_alert_exchange` – direct, `iot_alert_topic_exchange` – topic):

![RabbitMQ - Exchanges](images/RabbitMQ_Exchanges.PNG)

**Bindings** của exchange (queue được bind đúng routing key):

![RabbitMQ - Bindings](images/RabbitMQ_Bindings.PNG)

## 5. Tổng kết

| Bài | Nội dung | Kết quả |
|-----|----------|---------|
| 1 | Gửi/nhận message qua queue, in nội dung + thời điểm nhận | Đạt; mở rộng: gửi nhiều message, consumer chạy liên tục |
| 2 | Telemetry JSON mỗi 3 giây, consumer phát hiện cảnh báo | Đạt; mở rộng: thêm `timestamp`, nhiều cảm biến |
| 3 | Direct exchange + routing key, phân phối đúng queue | Đạt: `info` không ai nhận, `warning` → `warning_queue`, `critical` → `critical_queue`; mở rộng: topic exchange nhận tất cả, ghi log ra file |