import socket
import pymysql
import threading
import json
import csv
import os
import time
from datetime import datetime

# === CONFIG ===
PORT = 8234
COOLDOWN_SECONDS = 30
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

# === RFID 数据包常量 ===
RFID_START_BYTE = 0x5A
RFID_MIN_PACKET_LENGTH = 12
RFID_UID_START_INDEX = 5
RFID_UID_END_INDEX = 9

# === 区域配置 ===
try:
    with open("reader_config.json", "r") as f:
        READER_ZONE_MAPPING = json.load(f)
except Exception as e:
    print(f"❌ reader_config.json 载入失败: {e}")
    exit(1)

# === 内部缓存 ===
active_entries = {}
active_entries_lock = threading.Lock()

# === 解析 UID ===
def extract_uid(data):
    try:
        if data and len(data) >= RFID_MIN_PACKET_LENGTH and data[0] == RFID_START_BYTE:
            uid_bytes = data[RFID_UID_START_INDEX:RFID_UID_END_INDEX]
            return ''.join(f"{b:02X}" for b in uid_bytes)
    except Exception as e:
        print(f"⚠️ UID解析失败: {data.hex()} - {e}")
    return None

# === 写入 CSV（仅最小格式）===
def write_to_csv(uid, zone, when_in, when_out=None, duration=None):
    try:
        file_exists = os.path.isfile(CSV_CACHE_FILE)
        with open(CSV_CACHE_FILE, mode="a", newline="", encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["UID", "Zone", "WhenIn", "WhenOut", "Duration"])
            writer.writerow([uid, zone, when_in, when_out, duration])
    except Exception as e:
        print(f"❌ 写入 CSV 失败: {e}")

# === 数据库上传封装 ===
def try_insert_online(query, values):
    try:
        with pymysql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query, values)
        return True
    except Exception as e:
        print(f"🌐 数据库写入失败: {e}")
        return False

# === 获取 staffid（用于在线或离线上传）===
def fetch_staffid(uid):
    try:
        with pymysql.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_staffid FROM rfid_cards WHERE rfid_id = %s AND status = 'RECEIVED'", (uid,))
                result = cur.fetchone()
                return result[0] if result else None
    except:
        return None

# === 主逻辑处理 ===
def handle_uid(uid, reader_ip):
    zone = READER_ZONE_MAPPING.get(reader_ip, "UNKNOWN")
    now = datetime.now()
    staffid = fetch_staffid(uid)

    if not staffid:
        print(f"⚠️ 未知或未登记卡片 {uid}，跳过记录")
        return

    key = (uid, zone)

    with active_entries_lock:
        if key not in active_entries:
            print(f"🟢 {staffid} 进入 {zone} @ {now}")
            active_entries[key] = now
            success = try_insert_online(
                "INSERT INTO rfid_log (rfid_id, staffid, zone, when_in) VALUES (%s, %s, %s, %s)",
                (uid, staffid, zone, now)
            )
            if not success:
                write_to_csv(uid, zone, now)
        else:
            in_time = active_entries.pop(key)
            duration = now - in_time
            if duration.total_seconds() < COOLDOWN_SECONDS:
                print(f"⚠️ 刷卡过快，忽略 {staffid} @ {zone}")
                active_entries[key] = in_time
                return
            print(f"✅ {staffid} 离开 {zone}，停留 {duration}")
            success = try_insert_online(
                "UPDATE rfid_log SET when_out = %s, duration = %s WHERE rfid_id = %s AND zone = %s AND when_out IS NULL ORDER BY when_in DESC LIMIT 1",
                (now, str(duration), uid, zone)
            )
            if not success:
                write_to_csv(uid, zone, in_time, now, str(duration))

# === 离线补偿线程 ===
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
                uid, zone, when_in, when_out, duration = row["UID"], row["Zone"], row["WhenIn"], row["WhenOut"], row["Duration"]
                staffid = fetch_staffid(uid)
                if not staffid:
                    retained.append(row)
                    continue
                if when_out:
                    success = try_insert_online(
                        "UPDATE rfid_log SET when_out = %s, duration = %s WHERE rfid_id = %s AND zone = %s AND when_out IS NULL ORDER BY when_in DESC LIMIT 1",
                        (when_out, duration, uid, zone)
                    )
                else:
                    success = try_insert_online(
                        "INSERT INTO rfid_log (rfid_id, staffid, zone, when_in) VALUES (%s, %s, %s, %s)",
                        (uid, staffid, zone, when_in)
                    )
                if not success:
                    retained.append(row)

            with open(CSV_CACHE_FILE, mode="w", newline="", encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=["UID", "Zone", "WhenIn", "WhenOut", "Duration"])
                writer.writeheader()
                writer.writerows(retained)
            print(f"📤 离线数据上传完成，剩余 {len(retained)} 条")
        except Exception as e:
            print(f"❌ 离线数据上传异常: {e}")
        time.sleep(300)

# === 网络线程 ===
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

# === 主入口 ===
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
