from email.mime import message
import json
import socket
import os
import time
import shutil
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
import ctypes

# SETUP LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AdvancedFileWatcherHandler(FileSystemEventHandler):
    def __init__(self, watch_folder, processed_folder, kegiatan_map_path, bahanpustaka_map_path):
        self.watch_folder = watch_folder
        self.processed_folder = processed_folder
        self.kegiatan_map = self.load_mapping(kegiatan_map_path)
        self.bahanpustaka_map = self.load_mapping(bahanpustaka_map_path)
        logger.info(f"Watcher initialized. Watch folder: {watch_folder}")

    def load_mapping(self, path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                mapping = json.load(f)
                logger.info(f"Mapping loaded from {path}: {len(mapping)} entries")
                return mapping
        except Exception as e:
            logger.error(f"ERROR loading mapping from {path}: {e}")
            return {}

    def on_created(self, event):
        logger.info(f"File created: {event.src_path}")
        if not event.is_directory:
            self.process_file(event.src_path)

    def get_local_ip(self):
        try:
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            return ip
        except Exception as e:
            logger.error(f"ERROR getting local IP: {e}")
            return "Unknown"

    def show_message_box(self, title, message):
        ctypes.windll.user32.MessageBoxW(0, message, title, 0x10) 
    
    def process_file(self, file_path):
        try:
            logger.info(f"Processing file: {file_path}")
            source_ip = self.get_local_ip()
            logger.info(f"Source IP: {source_ip}")

            self.wait_for_file_ready(file_path)

            file_name = os.path.basename(file_path)
            destination_folder, new_file_name = self.get_destination_folder_and_filename(file_name)

            if destination_folder is None:
                msg = f"File '{file_name}' tidak sesuai format dan akan dihapus.\n\nFormat yang benar: BAHANPUSTAKA_KEGIATAN_JUDUL.ext"
                logger.error(msg)
                self.show_message_box("File Watcher Error", msg)
                
                time.sleep(1)
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"File dihapus: {file_path}")
                return
            
            now = datetime.now()
            year_folder = str(now.year)
            month_folder = now.strftime("%B")
            day_folder = now.strftime("%d")

            final_destination = os.path.join(destination_folder, year_folder, month_folder, day_folder)
            os.makedirs(final_destination, exist_ok=True)
            final_destination_path = os.path.join(final_destination, new_file_name)

            logger.info(f"Moving to: {final_destination_path}")
            shutil.move(file_path, final_destination_path)
            logger.info(f"SUCCESS: {file_name} -> {final_destination_path}")

        except Exception as ex:
            logger.error(f"ERROR processing file: {str(ex)}")
            error_msg = f"Error memproses file: {str(ex)}"
            self.show_message_box("File Watcher Error", error_msg)

    def get_destination_folder_and_filename(self, file_name):
        parts = file_name.split('_')
        if len(parts) < 3:
            logger.error(f"ERROR: Nama file '{file_name}' tidak sesuai format 'BAHANPUSTAKA_KEGIATAN_JUDUL.ext'")
            return None, None
        
        # Cek ekstensi file
        if not '.' in parts[-1]:
            logger.error(f"ERROR: File '{file_name}' tidak memiliki ekstensi")
            return None, None
        
        bahanpustaka_code = parts[0].upper()
        kegiatan_code = parts[1].upper()
        
        judul_parts = parts[2:]
        original_extension = os.path.splitext(file_name)[1]
        
        # Gabungkan parts judul dan pertahankan ekstensi asli
        new_file_name = '_'.join(judul_parts)
        
        bahanpustaka_folder = self.bahanpustaka_map.get(bahanpustaka_code, bahanpustaka_code)
        kegiatan_folder = self.kegiatan_map.get(kegiatan_code, kegiatan_code)
        full_path = os.path.join(self.processed_folder, bahanpustaka_folder, kegiatan_folder)
        
        logger.info(f"Destination folder: {full_path}")
        logger.info(f"New filename: {new_file_name}")
        
        return full_path, new_file_name

    def wait_for_file_ready(self, file_path, max_attempts=99, delay_ms=500):
        logger.info(f"Waiting for file ready: {file_path}")
        for i in range(max_attempts):
            try:
                file_size = os.path.getsize(file_path)
                if file_size > 0:  
                    with open(file_path, 'rb') as f:
                        f.read(1)  
                    logger.info("File is ready")
                    return
            except (IOError, OSError) as e:
                logger.debug(f"File not ready yet (attempt {i+1}/{max_attempts}): {e}")
                time.sleep(delay_ms / 1000.0)
        
        logger.warning(f"File may not be fully ready after {max_attempts} attempts")

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    watch_folder = r"C:\TestWatch"
    # processed_folder = os.path.join(watch_folder, "Processed")
    processed_folder = watch_folder
    kegiatan_map_path = os.path.join(script_dir, "kegiatan_map.json")
    bahanpustaka_map_path = os.path.join(script_dir, "bahanpustaka_map.json")

    logger.info("=== ADVANCED FILE WATCHER STARTING ===")
    logger.info(f"Script directory: {script_dir}")
    logger.info(f"Watch folder: {watch_folder}")
    logger.info(f"Processed folder: {processed_folder}")
    logger.info(f"Kegiatan mapping path: {kegiatan_map_path}")
    logger.info(f"Bahan Pustaka mapping path: {bahanpustaka_map_path}")

    if not os.path.exists(watch_folder):
        logger.error(f"Watch folder does not exist: {watch_folder}")
        os.makedirs(watch_folder, exist_ok=True)
        logger.info(f"Created watch folder: {watch_folder}")

    # Create mapping files if not exist dengan contoh data
    if not os.path.exists(kegiatan_map_path):
        logger.info("Creating new kegiatan mapping file")
        sample_kegiatan = {
            "KEG01": "Seminar",
            "KEG02": "Workshop", 
            "KEG03": "Pelatihan"
        }
        with open(kegiatan_map_path, "w", encoding="utf-8") as f:
            json.dump(sample_kegiatan, f, indent=2, ensure_ascii=False)

    if not os.path.exists(bahanpustaka_map_path):
        logger.info("Creating new bahan pustaka mapping file")
        sample_bahanpustaka = {
            "BHP01": "Buku",
            "BHP02": "Jurnal",
            "BHP03": "Laporan",
            "BHP04": "Presentasi"
        }
        with open(bahanpustaka_map_path, "w", encoding="utf-8") as f:
            json.dump(sample_bahanpustaka, f, indent=2, ensure_ascii=False)

    os.makedirs(processed_folder, exist_ok=True)
    logger.info("Directory check completed")

    event_handler = AdvancedFileWatcherHandler(watch_folder, processed_folder, kegiatan_map_path, bahanpustaka_map_path)
    observer = Observer()
    observer.schedule(event_handler, watch_folder, recursive=False)
    observer.start()
    logger.info("Observer started successfully")

    try:
        while True:
            time.sleep(10)
            logger.debug("Watcher is still running...")
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
        observer.stop()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_msg = f"CRITICAL ERROR: {e}"
        logger.error(error_msg)
        ctypes.windll.user32.MessageBoxW(0, error_msg, "File Watcher Critical Error", 0x10)
        with open('file_watcher_error.log', 'a') as f:
            f.write(f"{datetime.now()} - {error_msg}\n")