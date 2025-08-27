from pymavlink import mavutil
import socket
import time
import threading
import logging
import platform
import csv
from logging.handlers import RotatingFileHandler
from datetime import datetime
import serial.tools.list_ports
import queue

TELEMETRY_PORT = 'COM3'
BAUD = 57600
UDP_PORT_MAV = 1802
UDP_PORT_MP = 1805
LINK_QUALITY_PORT = 19000
TIMEOUT = 5
QUEUE_MAX = 8192
PARAM_TIMEOUT = 30
PER_PARAM_TIMEOUT = 1.0

param_queue = queue.Queue(maxsize=QUEUE_MAX)
buffer = {}
expected_index = 0
total_param = None

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler("proxy.log", maxBytes=1_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)

last_seq = None
recv_count = 0
lost_count = 0
lock = threading.Lock()

def check_link_quality(msg):
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
    with lock:
        total = recv_count + lost_count
        if total == 0:
            return 0.0, recv_count, lost_count
        return (recv_count / total) * 100.0, recv_count, lost_count

def mav_to_udp_loop(mav, stop_event, udp_out, last_param_time):
    global buffer, expected_index, total_param, last_seen_time, param_sync_done
    param_timeout = 5.0
    while not stop_event.is_set():
        try:
            msg = mav.recv_match(blocking=False)
            if not msg or msg.get_type() == "BAD_DATA":
                if (not param_sync_done) and (time.time() - last_param_time[0] > param_timeout):
                    logging.info("[PARAM] Timeout param, dianggap selesai")
                    param_sync_done = True
                continue

            check_link_quality(msg)

            if msg.get_type() != "PARAM_VALUE" and first_data_time[0] is None:
                first_data_time[0] = time.time()
                logging.info(f"[INFO] Data non-param pertama diterima: {msg.get_type()}")

            if first_data_time[0] and not param_requested[0]:
                if time.time() - first_data_time[0] >= 10:
                    mav.mav.param_request_list_send(mav.target_system, mav.target_component)
                    param_requested[0] = True
                    logging.info("Kirim PARAM_REQUEST_LIST ke MAV...")

            if msg.get_type() == "PARAM_VALUE":
                if param_sync_done:
                    logging.debug(f"[PARAM] Extra param setelah sync: {msg.param_id}")
                    continue
                last_param_time[0] = time.time()
                if total_param is None:
                    total_param = msg.param_count
                    logging.info(f"[PARAM] Total param: {total_param}")

                idx = msg.param_index
                with lock:
                    buffer[idx] = msg
                    param_timeouts[idx] = time.time()
                    while expected_index in buffer:
                        m = buffer.pop(expected_index)
                        param_timeouts.pop(expected_index, None)
                        logging.info(f"[PARAM] {m.param_index}/{total_param-1} {m.param_id} = {m.param_value}")
                        udp_out.sendto(m.get_msgbuf(), ('127.0.0.1', UDP_PORT_MP))
                        expected_index += 1
            else:
                udp_out.sendto(msg.get_msgbuf(), ('127.0.0.1', UDP_PORT_MP))

            with lock:
                to_remove = []
                for idx, t in param_timeouts.items():
                    if time.time() - t > PER_PARAM_TIMEOUT and idx == expected_index:
                        logging.warning(f"[PARAM] Parameter index {idx} tidak diterima, lewati.")
                        to_remove.append(idx)
                        expected_index += 1
                for idx in to_remove:
                    param_timeouts.pop(idx, None)

            time.sleep(0.001)

        except Exception as e:
            logging.error(f"mav_to_udp_loop error: {e}")
            stop_event.set()
            break

    logging.info("mav_to_udp_loop selesai.")

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

def param_monitor(stop_event, last_param_time):
    while not stop_event.is_set():
        if time.time() - last_param_time[0] > PARAM_TIMEOUT:
            logging.info("[PARAM] Timeout param, dianggap selesai")
            param_sync_done = True
            break
        time.sleep(1)

def link_quality_monitor(stop_event):
    while not stop_event.is_set():
        time.sleep(5)
        q, r, l = get_quality()
        logging.info(f"[LINK] Quality: {q:.2f}% (recv={r}, lost={l})")

def udp_quality_broadcast(stop_event):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while not stop_event.is_set():
        q, r, l = get_quality()
        msg = f"{q:.2f},{r},{l}"
        sock.sendto(msg.encode(), ('127.0.0.1', LINK_QUALITY_PORT))
        time.sleep(1)

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

            start_time = time.time()
            heartbeat_received = False
            while time.time() - start_time < TIMEOUT:
                msg = mav.recv_match(type='HEARTBEAT', blocking=False)
                if msg:
                    heartbeat_received = True
                    break
                time.sleep(0.01)

            if not heartbeat_received:
                logging.error("Tidak ada heartbeat, keluar")
                return
            logging.info("Heartbeat diterima.")

            last_param_time = [time.time()]
            first_data_time = [None]
            param_requested = [False]

            threads = [
                threading.Thread(target=mav_to_udp_loop, args=(mav, stop_event, udp_out, last_param_time, first_data_time, param_requested), daemon=True),
                threading.Thread(target=udp_to_mav_loop, args=(mav, stop_event, udp_in), daemon=True),
                threading.Thread(target=param_monitor, args=(stop_event, last_param_time), daemon=True),
                threading.Thread(target=link_quality_monitor, args=(stop_event,), daemon=True),
                threading.Thread(target=log_quality_csv, args=(stop_event,), daemon=True),
                threading.Thread(target=udp_quality_broadcast, args=(stop_event,), daemon=True)
            ]

            for t in threads:
                t.start()

            while not stop_event.is_set():
                if total_param and time.time() - last_param_time[0] > PARAM_TIMEOUT:
                    logging.info("[PARAM] Timeout tercapai, hentikan request param.")
                    break
                time.sleep(1)

        except KeyboardInterrupt:
            stop_event.set()
            try:
                mav.close()
                udp_in.close()
                udp_out.close()
            except:
                pass
            logging.info("Restart dalam 3 detik...")
            time.sleep(3)

        except Exception as e:
            logging.error(f"Error utama: {e}")
            time.sleep(3)

if __name__ == "__main__":
    main()
