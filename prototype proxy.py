from pymavlink import mavutil
import socket
import time
import threading
import logging
import csv
from logging.handlers import RotatingFileHandler
from datetime import datetime
import serial.tools.list_ports
import psutil
from collections import deque

# --- Konfigurasi ---
TELEMETRY_PORT = 'COM3'
BAUD = 57600
MP_INPUT_PORT  = 1805
MP_OUTPUT_PORT = 1802    # port forward ke Mission Planner
LINK_QUALITY_PORT = 19000
TIMEOUT = 5
SEQ_LOG_INTERVAL = 50 
last_msg_time = time.time()
timeout_duration = 10
recv_count = 0
lost_count = 0
last_seq = None
lock = threading.Lock()
link_window = deque()
QUALITY_WINDOW = 3
UPDATE_INTERVAL = 1 

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler("hybrid.log", maxBytes=1_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)


def update_link_window(seq):
    global link_window, last_seq
    with lock:
        now = time.time()
        # hapus paket lebih lama dari QUALITY_WINDOW
        while link_window and now - link_window[0][0] > QUALITY_WINDOW:
            link_window.popleft()

        if last_seq is not None:
            expected = (last_seq + 1) % 256
            lost = (seq - expected) % 256
            # masukkan paket hilang
            for _ in range(lost):
                link_window.append((now, 0))
        # masukkan paket diterima
        link_window.append((now, 1))
        last_seq = seq

def get_link_quality():
    with lock:
        if not link_window:
            return 0.0, 0, 0
        recv = sum(1 for _, s in link_window if s == 1)
        lost = sum(1 for _, s in link_window if s == 0)
        total = recv + lost
        quality = (recv / total) * 100 if total > 0 else 0.0
        return quality, recv, lost

def link_quality_monitor(stop_event):
    while not stop_event.is_set():
        q, r, l = get_link_quality()
        logging.info(f"[LINK] Quality: {q:.2f}% (recv={r}, lost={l})")
        time.sleep(UPDATE_INTERVAL)

# --- Broadcast kualitas via UDP ---
def udp_quality_broadcast(stop_event):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while not stop_event.is_set():
        q, r, l = get_link_quality()
        msg = f"{q:.2f},{r},{l}"
        sock.sendto(msg.encode(), ('127.0.0.1', LINK_QUALITY_PORT))
        time.sleep(1)

# --- Logging kualitas ke CSV ---
def log_quality_csv(stop_event, file_path="link_quality.csv"):
    with open(file_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "quality_percent", "recv", "lost"])
        while not stop_event.is_set():
            q, r, l = get_link_quality()
            writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), round(q, 3), r, l])
            csvfile.flush()
            time.sleep(0.5)


def wait_for_mission_planner(port, timeout=10):
    test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    test_data = b'\xfe\x09\x00\x00\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    start = time.time()
    while time.time() - start < timeout:
        try:
            test_sock.sendto(test_data, ("127.0.0.1", port))
            logging.info(f"Mengirim dummy ke Mission Planner di port {port}, asumsi aktif.")
            test_sock.close()
            return
        except Exception as e:
            logging.info(f"Menunggu Mission Planner bind port {port}...")
        time.sleep(1)
    logging.warning(f"Timeout menunggu Mission Planner pada port {port}")
    test_sock.close()
    return False

def mav_to_gcs_loop(mav, stop_event, gcs_socket):
    global last_seq, recv_count, lost_count, last_msg_time
    while not stop_event.is_set():
        try:
            has_msg = False
            while True:
                msg = mav.recv_match(blocking=False)
                if msg is None:
                    break
                if msg.get_type() != "BAD_DATA":
                    try:
                      # --- Hitung kualitas link pakai SEQ (telemetry) ---
                        seq = msg.get_seq()
                        with lock:
                            update_link_window(seq)

                        gcs_socket.sendto(msg.get_msgbuf(), ('127.0.0.1', MP_INPUT_PORT))
                    except Exception as e:
                        logging.error("Serial device dicabut, hentikan mav_to_gcs_loop")
                        stop_event.set()
                        gcs_socket.close()
                        break
                    has_msg = True
            if has_msg:
                last_msg_time = time.time()
            elif time.time() - last_msg_time > timeout_duration:
                logging.error("Tidak ada MAVLink valid selama 10 detik, asumsi disconnect.")
                stop_event.set()
                break
            time.sleep(0.01)

        except Exception as e:
            logging.error(f" mav_to_gcs_loop: {e}")
            stop_event.set()
            break
    logging.info("mav_to_gcs_loop selesai.")
    try:
        mav.close()
    except:
        pass
    


def gcs_to_mav_loop(mav, stop_event, link_type, gcs_socket):
    gcs_socket.settimeout(3.0)
    while not stop_event.is_set():
        try:
            data, _ = gcs_socket.recvfrom(1024)
            mav.write(data)
        except serial.SerialException as e:
            logging.error(f"Serial error: {e}")
            stop_event.set()
            gcs_socket.close()
            break
        except socket.timeout:
            # Tidak ada data masuk dari Mission Planner selama 3 detik
            time.sleep(0.01)
            continue
        except OSError as e:
            if e.errno == 10054:
                logging.warning("Mission Planner menolak koneksi (WinError 10054). Tunggu dan retry...")
                time.sleep(1)
                continue
            else:
                logging.error(f"gcs_to_mav_loop: {e}")
                stop_event.set()
                break

    logging.info("gcs_to_mav_loop selesai.")

def monitor_heartbeat(mav, stop_event, restart_event, current_link):
    while not stop_event.is_set():
        try:
            mav.recv_match(type='HEARTBEAT', blocking=False, timeout=2)
        except Exception:
            logging.warning(f"Lost heartbeat on {current_link}, trigger reconnect...")
            restart_event.set()
            stop_event.set()
            break
        time.sleep(0.5)

def check_for_telemetry_reconnect(current_link, restart_event):
    if current_link != "UDP":
        return  # hanya cek saat sedang pakai UDP
    while not restart_event.is_set():
        ports = [p.device for p in serial.tools.list_ports.comports()]
        if TELEMETRY_PORT in ports:
            logging.info("[RECOVERY] COM Telemetry tersedia lagi, akan switch kembali.")
            restart_event.set()  # trigger restart koneksi
            break
        time.sleep(3)
def is_port_in_use(port):
    try:
        for conn in psutil.net_connections():
            if conn.laddr.port == port:
                return True
    except Exception as e:
        logging.warning(f"Error checking port {port}: {e}")
        return False
def auto_connect(gcs_socket):
    available_ports = [p.device for p in serial.tools.list_ports.comports()]
    if TELEMETRY_PORT in available_ports:
        logging.info(f"[TELEM] {TELEMETRY_PORT} terdeteksi, mencoba koneksi...")

    while True:
        logging.info("[TELEM] Mencoba COM telemetry...")
        try:
            mav = mavutil.mavlink_connection(TELEMETRY_PORT, baud=BAUD)
            logging.info(f"Port COM berhasil dibuka, menunggu heartbeat...")
            if mav.wait_heartbeat(timeout=TIMEOUT):
                logging.info("[OK] Heartbeat via telemetry")
                return mav, "TELEMETRY"
            else:
                mav.close()
        except Exception as e:
            logging.warning(f"[COM3 Error] Tidak bisa membuka {TELEMETRY_PORT}: {e}")
            try:
                mav.close()
            except:
                logging.warning(f"Gagal menutup MAV: {e}")
                pass
            
        except Exception as e:
            try:
                mav.close()
                logging.warning(f"Gagal koneksi UDP (Internet): {e}")
            except:
                pass

        logging.info("Tidak ada koneksi yang berhasil, mencoba lagi dalam 5 detik...")
        time.sleep(5)

if __name__ == "__main__":
    try:
        ports = list(serial.tools.list_ports.comports())
        print("Daftar port serial yang terdeteksi:")
        for port in ports:
            print(f"- {port.device} : {port.description}")

        while is_port_in_use(MP_OUTPUT_PORT):
            logging.warning(f"Port {MP_OUTPUT_PORT} masih digunakan! Menunggu...")
            time.sleep(3)


        while True:
            stop_event = threading.Event()
            restart_event = threading.Event()
            gcs_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            gcs_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                gcs_socket.bind(('127.0.0.1', MP_OUTPUT_PORT))
            except Exception as e:
                logging.error(f"Gagal bind port {MP_OUTPUT_PORT}: {e}")
                stop_event.set()
                continue
            mav, link_type = auto_connect(gcs_socket)
            logging.info(f"Koneksi aktif: {link_type}")
            t1 = threading.Thread(target=mav_to_gcs_loop, args=(mav, stop_event, gcs_socket), name="MavToGCS")
            wait_for_mission_planner(MP_INPUT_PORT)
            t2 = threading.Thread(target=gcs_to_mav_loop, args=(mav, stop_event, link_type, gcs_socket), name="GCSToMav")
            hb = threading.Thread(target=monitor_heartbeat, args=(mav, stop_event, restart_event, link_type), name="HeartbeatMonitor")
            t3 = threading.Thread(target=link_quality_monitor, args=(stop_event,), daemon=True)
            t4 = threading.Thread(target=log_quality_csv, args=(stop_event,), daemon=True)
            t5 = threading.Thread(target=udp_quality_broadcast, args=(stop_event,), daemon=True)
            t1.start()
            t2.start()
            hb.start()
            t3.start()
            t4.start()
            t5.start()
            while not restart_event.is_set():
                if not t1.is_alive() or not t2.is_alive():
                    logging.error("Salah satu thread GCS mati, keluar...")
                    restart_event.set()
                time.sleep(0.5)
            stop_event.set()
            try:
                mav.close()
            except:
                logging.warning("Gagal menutup koneksi MAV")
                logging.info("gcs_socket berhasil ditutup")
            t1.join()
            t2.join()
            hb.join()
            t3.join()
            t4.join()
            t5.join()
            time.sleep(1)
            logging.info("Restarting connection dalam 1 detik...\n")
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt diterima, menghentikan program...")
        stop_event.set()
        gcs_socket.close()
        time.sleep(0.5)