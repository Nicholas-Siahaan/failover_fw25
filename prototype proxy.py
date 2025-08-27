from pymavlink import mavutil
import socket
import time
import threading
import logging
import csv
from logging.handlers import RotatingFileHandler
from datetime import datetime
import serial.tools.list_ports

# --- Konfigurasi ---
TELEMETRY_PORT = 'COM3'
BAUD = 57600
UDP_PORT_MAV = 14550   # port untuk MAVLink input dari Mission Planner
UDP_PORT_MP  = 14560   # port forward ke Mission Planner
LINK_QUALITY_PORT = 19000
TIMEOUT = 5

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler("hybrid.log", maxBytes=1_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)

# --- Variabel kualitas link ---
last_seq = None
recv_count = 0
lost_count = 0
lock = threading.Lock()

def check_link_quality(msg):
    """Hitung paket hilang berdasar sequence number MAVLink"""
    global last_seq, recv_count, lost_count
    try:
        seq = msg.get_seq()
    except:
        return
    with lock:
        if last_seq is not None:
            expected = (last_seq + 1) % 256
            if seq != expected:
                lost_count += (seq - expected) % 256
        last_seq = seq
        recv_count += 1

def get_quality():
    """Return (persen, recv, lost)"""
    with lock:
        total = recv_count + lost_count
        if total == 0:
            return 0.0, recv_count, lost_count
        return (recv_count / total) * 100.0, recv_count, lost_count

# --- Loop utama MAV->UDP ---
def mav_to_udp_loop(mav, stop_event, udp_out):
    while not stop_event.is_set():
        try:
            msg = mav.recv_match(blocking=False)
            if not msg or msg.get_type() == "BAD_DATA":
                continue

            # hitung kualitas link
            check_link_quality(msg)

            # forward raw MAVLink ke Mission Planner
            udp_out.sendto(msg.get_msgbuf(), ('127.0.0.1', UDP_PORT_MP))
        except Exception as e:
            logging.error(f"mav_to_udp_loop error: {e}")
            stop_event.set()
            break
        time.sleep(0.001)
    logging.info("mav_to_udp_loop selesai.")

# --- Loop utama UDP->MAV ---
def udp_to_mav_loop(mav, stop_event, udp_in):
    udp_in.settimeout(1.0)
    while not stop_event.is_set():
        try:
            data, _ = udp_in.recvfrom(4096)
            mav.write(data)
        except socket.timeout:
            continue
        except Exception as e:
            logging.error(f"udp_to_mav_loop error: {e}")
            stop_event.set()
            break
    logging.info("udp_to_mav_loop selesai.")

# --- Monitor kualitas link ---
def link_quality_monitor(stop_event):
    while not stop_event.is_set():
        q, r, l = get_quality()
        logging.info(f"[LINK] Quality: {q:.2f}% (recv={r}, lost={l})")
        time.sleep(5)

# --- Broadcast kualitas via UDP ---
def udp_quality_broadcast(stop_event):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while not stop_event.is_set():
        q, r, l = get_quality()
        msg = f"{q:.2f},{r},{l}"
        sock.sendto(msg.encode(), ('127.0.0.1', LINK_QUALITY_PORT))
        time.sleep(1)

# --- Logging kualitas ke CSV ---
def log_quality_csv(stop_event, file_path="link_quality.csv"):
    with open(file_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "quality_percent", "recv", "lost"])
        while not stop_event.is_set():
            q, r, l = get_quality()
            writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), round(q, 3), r, l])
            csvfile.flush()
            time.sleep(0.5)

def main():
    ports = list(serial.tools.list_ports.comports())
    print("Port serial yang terdeteksi:")
    for port in ports:
        print(f" - {port}: {port.description}")

    while True:
        try:
            stop_event = threading.Event()
            udp_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_in  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_in.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp_in.bind(('0.0.0.0', UDP_PORT_MAV))

            logging.info(f"Mencoba koneksi ke {TELEMETRY_PORT}...")
            mav = mavutil.mavlink_connection(TELEMETRY_PORT, baud=BAUD)

            if not mav.wait_heartbeat(timeout=TIMEOUT):
                logging.error("Tidak ada heartbeat, keluar")
                return
            logging.info("Heartbeat diterima.")

            threads = [
                threading.Thread(target=mav_to_udp_loop, args=(mav, stop_event, udp_out), daemon=True),
                threading.Thread(target=udp_to_mav_loop, args=(mav, stop_event, udp_in), daemon=True),
                threading.Thread(target=link_quality_monitor, args=(stop_event,), daemon=True),
                threading.Thread(target=log_quality_csv, args=(stop_event,), daemon=True),
                threading.Thread(target=udp_quality_broadcast, args=(stop_event,), daemon=True),
            ]

            for t in threads:
                t.start()

            while not stop_event.is_set():
                time.sleep(1)

        except KeyboardInterrupt:
            stop_event.set()
            try:
                mav.close()
                udp_in.close()
                udp_out.close()
            except:
                pass
            logging.info("Keluar program...")
            break

        except Exception as e:
            logging.error(f"Error utama: {e}")
            time.sleep(3)

if __name__ == "__main__":
    main()
