from pymavlink import mavutil
import socket
import time
import threading
import logging
import platform
import csv
from logging.handlers import RotatingFileHandler
from datetime import datetime
import serial
import serial.tools.list_ports
import queue

TELEMETRY_PORT = 'COM3'
BAUD = 57600
UDP_PORT_MAV = 1802
UDP_PORT_MP  = 1805
LINK_QUALITY_PORT = 19000 
TIMEOUT = 5

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler("proxy.log", maxBytes=1_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)

def beep():
    try:
        if platform.system() == "Windows":
            import winsound
            winsound.Beep(1000, 300)
        else:
            print('\a')
    except:
        logging.warning("Tidak bisa memutar beep")

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
                gap = (seq - expected) % 256
                lost_count += gap
        last_seq = seq
        recv_count += 1

def get_quality():
    with lock:
        total = recv_count + lost_count
        if total == 0:
            return 0.0, recv_count, lost_count
        return (recv_count / total) * 100.0, recv_count, lost_count

reliable_queue = queue.Queue(maxsize=1024)

def mav_to_udp_loop(mav, stop_event, udp_out):
    while not stop_event.is_set():
        try:
            msg = mav.recv_match(blocking=False)
            if not msg or msg.get_type() == "BAD_DATA":
                continue

            check_link_quality(msg)

            if msg.get_type() in ["PARAM_VALUE", "MISSION_ITEM"]:
                reliable_queue.put(msg.get_msgbuf())
            else:
                udp_out.sendto(msg.get_msgbuf(), ('127.0.0.1', UDP_PORT_MP))
            logging.debug(f"MAVLink message: {msg.get_type()}")

        except Exception as e:
            logging.error(f"mav_to_udp_loop error: {e}")
            stop_event.set()
            break
    logging.info("mav_to_udp_loop selesai.")

def reliable_sender_loop(stop_event, udp_out):
    while not stop_event.is_set():
        try:
            data = reliable_queue.get(timeout=0.5)
            udp_out.sendto(data, ('127.0.0.1', UDP_PORT_MP))
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"reliable_sender_loop error: {e}")
            stop_event.set()
            break

def udp_to_mav_loop(mav, stop_event, udp_in):
    udp_in.settimeout(1.0)
    while not stop_event.is_set():
        try:
            data, _ = udp_in.recvfrom(1024)
            mav.write(data)
        except socket.timeout:
            continue
        except ConnectionResetError:
            continue
        except Exception as e:
            logging.error(f"udp_to_mav_loop error: {e}")
            stop_event.set()
            break
    logging.info("udp_to_mav_loop selesai.")

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

            if heartbeat_received:
                logging.info("[OK] Heartbeat diterima dari telemetry")
                beep()
                try:
                    mav.mav.param_request_list_send(
                        mav.target_system,
                        mav.target_component
                        # b'BARO2_GND_PRESS', 
                        # -1
                    )
                    logging.info("PARAM_REQUEST_LIST dikirim ke autopilot")
                except Exception as e:
                    logging.error(f"Gagal kirim PARAM_REQUEST_LIST: {e}")
            else:
                logging.warning("Tidak ada heartbeat dari telemetry, coba ulang...")
                try:
                    mav.close()
                except:
                    pass
                time.sleep(1)
                continue

            stop_event = threading.Event()
            udp_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_in  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_in.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp_in.bind(('0.0.0.0', UDP_PORT_MAV))

            t1 = threading.Thread(target=mav_to_udp_loop, args=(mav, stop_event, udp_out), daemon=True)
            t2 = threading.Thread(target=reliable_sender_loop, args=(stop_event, udp_out), daemon=True)
            t3 = threading.Thread(target=udp_to_mav_loop, args=(mav, stop_event, udp_in), daemon=True)
            t4 = threading.Thread(target=link_quality_monitor, args=(stop_event,), daemon=True)
            t5 = threading.Thread(target=log_quality_csv, args=(stop_event,), daemon=True)
            t6 = threading.Thread(target=udp_quality_broadcast, args=(stop_event,), daemon=True)

            t1.start()
            t2.start()
            t3.start()
            t4.start()
            t5.start()
            t6.start()

            while not stop_event.is_set():
                if not t1.is_alive() or not t2.is_alive() or not t3.is_alive():
                    logging.error("Thread mati, restart...")
                    stop_event.set()
                    break
                time.sleep(1)

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
