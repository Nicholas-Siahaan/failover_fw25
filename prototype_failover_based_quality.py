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

# --- Konfigurasi ---
PROXY_UDP_IP = '127.0.0.1'   # Telemetry utama
PROXY_UDP_PORT = 1810         # Internal failover port (tidak sama dengan MP port)
QUALITY_UDP_IP = '127.0.0.1'
QUALITY_UDP_PORT = 19000
BACKUP_IP = '100.81.70.116'
BACKUP_PORT = 14553

MP_INPUT_PORT = 1805          # Port default MP, tidak di-bind oleh failover
# MP_OUTPUT_PORT tidak digunakan di Solusi B

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler("failover_default1805.log", maxBytes=1_000_000, backupCount=3),
        logging.StreamHandler()
    ]
)

# --- Alert function ---
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

# --- Forward telemetry / backup ke MP (satu arah, source -> MP) ---
def source_to_mp_loop(source_ip, source_port, stop_event):
    sock_in = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_in.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock_in.bind(('0.0.0.0', source_port))  # bind hanya ke source port
    sock_in.settimeout(0.5)

    while not stop_event.is_set():
        try:
            data, _ = sock_in.recvfrom(4096)
            # kirim ke MP 1805, tidak perlu bind
            sock_out.sendto(data, ('127.0.0.1', MP_INPUT_PORT))
        except socket.timeout:
            continue
        except Exception as e:
            logging.error(f"source_to_mp_loop: {e}")
            break
    sock_in.close()
    sock_out.close()

# --- Listener kualitas link ---
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

# --- Failover manager ---
def failover_manager(global_stop_event, quality_container):
    current_source = None
    thread_obj = None
    local_stop_event = threading.Event()

    low_quality_start = None
    high_quality_start = None
    while not global_stop_event.is_set():
        quality = quality_container[0]
        target_source = None
        now = time.time()

        if quality >= 60:
            if high_quality_start is None:
                high_quality_start = now
            if now - high_quality_start >= 3:
                target_source = (PROXY_UDP_IP, PROXY_UDP_PORT)
            low_quality_start = None
        elif quality < 30:
            if low_quality_start is None:
                low_quality_start = now
            if now - low_quality_start >= 3:
                target_source = (BACKUP_IP, BACKUP_PORT)
            high_quality_start = None
        else:
            high_quality_start = None
            low_quality_start = None
            time.sleep(0.2)
            continue

        if target_source and current_source != target_source:
            logging.info(f"[SWITCH] Switching source to {target_source}")

            # Stop old threads
            local_stop_event.set()
            if thread_obj:
                for t in thread_obj:
                    t.join(timeout=1.0)
            local_stop_event.clear()
            local_stop_event = threading.Event()

            # Start new source -> MP thread
            t1 = threading.Thread(
                target=source_to_mp_loop,
                args=(target_source[0], target_source[1], local_stop_event),
                daemon=True
            )
            t1.start()
            thread_obj = [t1]

            if target_source[0] == PROXY_UDP_IP:
                alert("Switched to Telemetry Link", "telemetry.wav")
            else:
                alert("Switched to Backup Link", "backup.wav")

            current_source = target_source

        time.sleep(0.2)

# --- Main ---
if __name__ == "__main__":
    global_stop_event = threading.Event()
    quality_container = [100.0]

    t_quality = threading.Thread(target=quality_listener, args=(global_stop_event, quality_container), daemon=True)
    t_quality.start()

    t_failover = threading.Thread(target=failover_manager, args=(global_stop_event, quality_container))
    t_failover.start()

    logging.info("Failover program running with MP port 1805...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping failover...")
        global_stop_event.set()
        t_failover.join()
        t_quality.join()
