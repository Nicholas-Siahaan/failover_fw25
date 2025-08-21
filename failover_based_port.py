from pymavlink import mavutil
import socket
import time
import threading
import logging
import platform
import psutil
from logging.handlers import RotatingFileHandler
import serial
import serial.tools.list_ports

TELEMETRY_PORT = 'COM3'
BAUD = 57600
BACKUP_IP = '100.81.70.116'   # IP Raspi via Tailscale
BACKUP_PORT = 14553
# LOCAL_FORWARD_PORT = 14600
MP_INPUT_PORT = 14560  
MP_OUTPUT_PORT = 14561 
TIMEOUT = 5


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler("failover.log", maxBytes=1_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)
def beep():
    try:
        if platform.system() == "Windows":
            import winsound
            winsound.Beep(1000, 1000) 
        else:
            print('\a')  
    except:
        logging.warning("Tidak bisa memutar beep")

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


def is_port_in_use(port):
    try:
        for conn in psutil.net_connections():
            if conn.laddr.port == port:
                return True
    except Exception as e:
        logging.warning(f"Error checking port {port}: {e}")
        return False

def mav_to_gcs_loop(mav, stop_event, gcs_socket):
    last_msg_time = time.time()
    timeout_duration = 10
    no_msg_count = 0
    while not stop_event.is_set():
        try:
            has_msg = False
            while True:
                msg = mav.recv_match(blocking=False)
                if msg is None:
                    break
                if msg.get_type() != "BAD_DATA":
                    gcs_socket.sendto(msg.get_msgbuf(), ('127.0.0.1', MP_INPUT_PORT))
                    has_msg = True
                    logging.debug(f"MAVLink message: {msg.get_type()}")
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
    raspi_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
    raspi_addr = (BACKUP_IP, BACKUP_PORT)
    wait_timeout = 5
    waiting = 0
    while not stop_event.is_set():
        try:
            data, _ = gcs_socket.recvfrom(1024)
            if link_type == "UDP":
                raspi_sock.sendto(data, raspi_addr)
            else:
                mav.write(data)

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
    raspi_sock.close()


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
            
        
        logging.info(f"Mencoba koneksi via UDP backup ke {BACKUP_IP}:{BACKUP_PORT}...")
        try:
            # mav = mavutil.mavlink_connection(f"udp:0.0.0.0:{MP_OUTPUT_PORT}@{BACKUP_IP}:{BACKUP_PORT}", source_system=255) #255
            # mav = mavutil.mavlink_connection(
            #     f"udpout:{BACKUP_IP}:{BACKUP_PORT}@0.0.0.0:14556",
            #     source_system=255
            # )
            mav = mavutil.mavlink_connection("udp:0.0.0.0:14556", source_system=255)


            dummy = b'\xfe\x09\x00\x00\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
            # gcs_socket.sendto(dummy, (BACKUP_IP, BACKUP_PORT))
            trigger_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            trigger_sock.sendto(dummy, (BACKUP_IP, BACKUP_PORT))
            trigger_sock.close()
            logging.info("Mengirim paket dummy ke Raspi untuk trigger MAVLink Router")
            ## mav.write(b' ')
            logging.info(f"UDP socket berhasil dibuka, menunggu heartbeat...")
            if mav.wait_heartbeat(timeout=TIMEOUT):
                logging.warning(f"[UDP] Fallback berhasil ke {BACKUP_IP}:{BACKUP_PORT}")
                return mav, "UDP"
            else:
                mav.close()
        except Exception as e:
            try:
                mav.close()
                logging.warning(f"Gagal koneksi UDP (Internet): {e}")
            except:
                pass

        logging.info("Tidak ada koneksi yang berhasil, mencoba lagi dalam 5 detik...")
        time.sleep(5)

if __name__ == "__main__":
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
        beep() 
        t1 = threading.Thread(target=mav_to_gcs_loop, args=(mav, stop_event, gcs_socket), name="MavToGCS")
        wait_for_mission_planner(MP_INPUT_PORT)
        t2 = threading.Thread(target=gcs_to_mav_loop, args=(mav, stop_event, link_type, gcs_socket), name="GCSToMav")
        hb = threading.Thread(target=monitor_heartbeat, args=(mav, stop_event, restart_event, link_type), name="HeartbeatMonitor")    
        t1.start()
        t2.start()
        hb.start()
        if link_type == "UDP":
            telemetry_check = threading.Thread(
                target=check_for_telemetry_reconnect,
                args=(link_type, restart_event),
                name="TelemetryRecovery"
            )
            telemetry_check.start()
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
        # try:
        #     gcs_socket.close()
        # except:
        #     logging.warning("Gagal menutup gcs_socket")
        #     time.sleep(1)
        #     pass
        t1.join()
        t2.join()
        hb.join()
        if link_type == "UDP":
            telemetry_check.join()
        time.sleep(1)
        logging.info("Restarting connection dalam 1 detik...\n")
        time.sleep(1)
