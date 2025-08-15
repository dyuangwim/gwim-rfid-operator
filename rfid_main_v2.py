import socket
import pymysql
import threading
import json
import csv
import os
import time
from datetime import datetime

PORT = 8234
CSV_CACHE_FILE = "rfid_offline_log.csv"
READER_ZONE_MAPPING = {}

DB_CONFIG = {
    'host': '192.168.20.17',
    'user': 'itadmin',
    'password': 'itadmin@2018',
    'database': 'staff_gwidb',
    'charset': 'utf8mb4',
    'autocommit': True
}

RFID_START_BYTE = 0x5A
RFID_MIN_PACKET_LENGTH = 12
RFID_UID_START_INDEX = 5
RFID_UID_END_INDEX = 9

try:
    with open("reader_config.json", "r") as f:
        READER_ZONE_MAPPING = json.load(f)
except Exception as e:
    print(f"❌ reader_config.json 载入失败: {e}")
    exit(1)

def extract_uid(data):
    try:
        if data and len(data) >= RFID_MIN_PACKET_LENGTH and data[0] == RFID_START_BYTE:
            uid_bytes = data[RFID_UID_START_INDEX:RFID_UID_END_INDEX]
            return ''.join(f"{b:02X}" for b in uid_bytes)
    except Exception as e:
        print(f"⚠️ UID解析失败: {data.hex()} - {e}")
    return None

def write_to_csv(uid, zone, timestamp):
    try:
        file_exists = os.path.isfile(CSV_CACHE_FILE)
        with open(CSV_CACHE_FILE, mode="a", newline="", encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["UID", "Zone", "Timestamp"])
            writer.writerow([uid, zone, timestamp])
    except Exception as e:
        print(f"❌ 写入 CSV 失败: {e}")

def try_insert_online(query, values):
    try:
        with pymysql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query, values)
        return True
    except Exception as e:
        print(f"🌐 数据库写入失败: {e}")
        return False

def fetch_staffid(uid):
    try:
        with pymysql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_staffid FROM rfid_cards WHERE rfid_id = %s AND status = 'RECEIVED'", (uid,))
                result = cur.fetchone()
                return result[0] if result else None
    except:
        return None

# === 全局缓存 ===
last_scan_time = {}  # {(uid, zone): datetime}

def handle_uid(uid, reader_ip):
    zone = READER_ZONE_MAPPING.get(reader_ip, "UNKNOWN")
    now = datetime.now()
    staffid = fetch_staffid(uid)

    if not staffid:
        print(f"⚠️ 未知或未登记卡片 {uid}，跳过记录")
        return

    # 加入一分钟重复刷卡过滤机制
    key = (uid, zone)
    last_time = last_scan_time.get(key)
    if last_time and (now - last_time).total_seconds() < 60:
        print(f"⏱️ 忽略 {staffid} @ {zone}，刷卡间隔 < 60 秒")
        return
    last_scan_time[key] = now

    print(f"📍 {staffid} 刷卡 @ {zone} @ {now}")
    success = try_insert_online(
        "INSERT INTO rfid_log (rfid_id, staffid, zone, datetime_log) VALUES (%s, %s, %s, %s)",
        (uid, staffid, zone, now)
    )
    if not success:
        write_to_csv(uid, zone, now)


def upload_offline_log():
    while True:
        if not os.path.exists(CSV_CACHE_FILE):
            time.sleep(300)
            continue
        try:
            with open(CSV_CACHE_FILE, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            retained = []
            for row in rows:
                uid, zone, timestamp = row["UID"], row["Zone"], row["Timestamp"]
                staffid = fetch_staffid(uid)
                if not staffid:
                    retained.append(row)
                    continue
                success = try_insert_online(
                    "INSERT INTO rfid_log (rfid_id, staffid, zone, datetime_log) VALUES (%s, %s, %s, %s)",
                    (uid, staffid, zone, timestamp)
                )
                if not success:
                    retained.append(row)

            with open(CSV_CACHE_FILE, mode="w", newline="", encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=["UID", "Zone", "Timestamp"])
                writer.writeheader()
                writer.writerows(retained)
            print(f"📤 离线数据上传完成，剩余 {len(retained)} 条")
        except Exception as e:
            print(f"❌ 离线数据上传异常: {e}")
        time.sleep(300)

def handle_client(client_socket, client_address):
    ip = client_address[0]
    print(f"📡 新连接: {ip}")
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            uid = extract_uid(data)
            if uid:
                handle_uid(uid, ip)
    except Exception as e:
        print(f"⚠️ 客户端异常: {e}")
    finally:
        client_socket.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.bind(("0.0.0.0", PORT))
        server.listen(5)
        print(f"🟢 正在监听 {PORT} ...")
        threading.Thread(target=upload_offline_log, daemon=True).start()
        while True:
            client, addr = server.accept()
            threading.Thread(target=handle_client, args=(client, addr)).start()
    except Exception as e:
        print(f"❌ 服务启动失败: {e}")
    finally:
        server.close()

if __name__ == "__main__":
    start_server()

