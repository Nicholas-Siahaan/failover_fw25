from pymavlink import mavutil
import socket
import threading
import time
import logging
from logging.handlers import RotatingFileHandler
import platform
from plyer import notification
import winsound
import simpleaudio as sa

PROXY_UDP_IP = '127.0.0.1'   # Telemetry utama
PROXY_UDP_PORT = 1810         # Internal failover port (tidak sama dengan MP port)
QUALITY_UDP_IP = '127.0.0.1'
QUALITY_UDP_PORT = 19000
BACKUP_IP = '100.81.70.116'
BACKUP_PORT = 14553

MP_INPUT_PORT = 1805          # Port default MP, tidak di-bind oleh failover
MP_OUTPUT_PORT = 1802         # Port MP mengirim perintah

# Mapping sumber link
LINK_SOURCES = {
    "TELEMETRY": {
        "data": (PROXY_UDP_IP, PROXY_UDP_PORT),   # data masuk dari proxy
        "cmd":  (PROXY_UDP_IP, MP_OUTPUT_PORT)    # perintah dikirim ke proxy (diteruskan ke COM)
    },
    "BACKUP": {
        "data": (BACKUP_IP, BACKUP_PORT),
        "cmd":  (BACKUP_IP, BACKUP_PORT)
    }
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler("failover_default1805.log", maxBytes=1_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)

def alert(message, sound_file="alert.wav"):
    try:
        notification.notify(title="Failover Switch", message=message, timeout=3)
    except Exception as e:
        print(f"Popup error: {e}")
    try:
        if platform.system() == "Windows":
            winsound.PlaySound(sound_file, winsound.SND_FILENAME | winsound.SND_ASYNC)
        else:
            wave_obj = sa.WaveObject.from_wave_file(sound_file)
            wave_obj.play()
    except Exception as e:
        print(f"Sound error: {e}")

def source_to_mp_loop(source_ip, source_port, stop_event, label):
    sock_in = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_in.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock_in.bind(('0.0.0.0', source_port))
    sock_in.settimeout(0.5)

    while not stop_event.is_set():
        try:
            data, _ = sock_in.recvfrom(4096)
            sock_out.sendto(data, ('127.0.0.1', MP_INPUT_PORT))
        except socket.timeout:
            continue
        except Exception as e:
            logging.error(f"source_to_mp_loop[{label}]: {e}")
            break
    sock_in.close()
    sock_out.close()


def mp_to_source_loop(stop_event, current_target, target_lock):
    sock_in = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_in.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock_in.bind(('0.0.0.0', MP_OUTPUT_PORT))
    sock_in.settimeout(0.5)

    while not stop_event.is_set():
        try:
            data, _ = sock_in.recvfrom(4096)
        except socket.timeout:
            continue
        except Exception as e:
            logging.error(f"mp_to_source_loop: {e}")
            break

        with target_lock:
            dest = current_target["cmd"]
        try:
            sock_out.sendto(data, dest)
        except Exception as e:
            logging.warning(f"Send MP->source gagal ke {dest}: {e}")

    sock_in.close()
    sock_out.close()

def quality_listener(stop_event, quality_container):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((QUALITY_UDP_IP, QUALITY_UDP_PORT))
    sock.settimeout(1.0)
    last_receive_time = time.time()
    while not stop_event.is_set():
        try:
            data, _ = sock.recvfrom(64)
            text = data.decode().strip()
            quality = float(text.split(",")[0])
            quality_container[0] = quality
            last_receive_time = time.time()
            logging.info(f"[Quality]: {quality:.2f}%")
        except socket.timeout:
            if time.time() - last_receive_time > 2:
                quality_container[0] = 0.0
        except Exception as e:
            logging.warning(f"Quality listener error: {e}")
    sock.close()

def failover_manager(global_stop_event, quality_container, current_target, target_lock):
    current_source_key = None
    data_thread = None
    data_stop_event = threading.Event()

    low_quality_start = None
    high_quality_start = None
    while not global_stop_event.is_set():
        quality = quality_container[0]
        target_key = None
        now = time.time()

        if quality >= 60:
            if high_quality_start is None:
                high_quality_start = now
            if now - high_quality_start >= 3:
                target_key = "TELEMETRY"
            low_quality_start = None
        elif quality < 30:
            if low_quality_start is None:
                low_quality_start = now
            if now - low_quality_start >= 3:
                target_key = "BACKUP"
            high_quality_start = None
        else:
            high_quality_start = None
            low_quality_start = None
            time.sleep(0.2)
            continue

        if target_key and current_source_key != target_key:
            target = LINK_SOURCES[target_key]
            logging.info(f"[SWITCH] Switching source to {target_key} {target['data']}")

            data_stop_event.set()
            if data_thread:
                data_thread.join(timeout=1.0)
            data_stop_event = threading.Event()
            data_thread = threading.Thread(
                target=source_to_mp_loop,
                args=(target["data"][0], target["data"][1], data_stop_event, target_key),
                daemon=True
            )
            data_thread.start()

            with target_lock:
                current_target["cmd"] = target["cmd"]

            if target_key == "TELEMETRY":
                alert("Switched to Telemetry Link", "telemetry.wav")
            else:
                alert("Switched to Backup Link", "backup.wav")

            current_source_key = target_key

        time.sleep(0.2)

    data_stop_event.set()
    if data_thread:
        data_thread.join(timeout=1.0)

if __name__ == "__main__":
    global_stop_event = threading.Event()
    quality_container = [100.0]
    current_target = {"cmd": LINK_SOURCES["TELEMETRY"]["cmd"]}
    target_lock = threading.Lock()

    t_quality = threading.Thread(target=quality_listener, args=(global_stop_event, quality_container), daemon=True)
    t_quality.start()

    t_failover = threading.Thread(
        target=failover_manager,
        args=(global_stop_event, quality_container, current_target, target_lock)
    )
    t_failover.start()

    t_mp_to_src = threading.Thread(
        target=mp_to_source_loop,
        args=(global_stop_event, current_target, target_lock),
        daemon=True
    )
    t_mp_to_src.start()

    logging.info("Failover program running with MP port 1805...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping failover...")
        global_stop_event.set()
        t_failover.join()
        t_quality.join()
        t_mp_to_src.join()
