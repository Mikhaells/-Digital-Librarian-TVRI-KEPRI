import json
import socket
import os
import time
import shutil
import logging
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
import ctypes

# SETUP LOGGING dengan encoding yang benar
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_watcher.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MagicSoftFileWatcher(FileSystemEventHandler):
    def __init__(self, watch_folder, processed_folder, kegiatan_map_path, bahanpustaka_map_path):
        self.watch_folder = watch_folder
        self.processed_folder = processed_folder
        self.kegiatan_map = self.load_mapping(kegiatan_map_path)
        self.bahanpustaka_map = self.load_mapping(bahanpustaka_map_path)
        self.processed_files = set()
        
        # PARAMETERS - TANPA INITIAL DELAY
        self.wait_delay = 10  # Delay 10 detik antar pengecekan
         
        logger.info(f"Watch folder: {watch_folder}") 

    def load_mapping(self, path):
        """Load mapping dari file JSON"""
        try:
            with open(path, "r", encoding="utf-8") as f:
                mapping = json.load(f)
                logger.info(f"Mapping loaded from {path}: {len(mapping)} entries")
                return mapping
        except Exception as e:
            logger.error(f"ERROR loading mapping from {path}: {e}")
            return {}

    def on_created(self, event):
        """Handle ketika file baru dibuat - LANGSUNG PROSES"""
        if not event.is_directory:
            file_path = event.src_path
            file_name = os.path.basename(file_path)
            
            # Abaikan file temporary
            if file_name.lower().endswith('.tmp'):
                #logger.info(f"Ignoring temporary file: {file_name}")
                return
                
            # Abaikan file tanpa ekstensi
            if '.' not in file_name:
                #logger.info(f"Ignoring file without extension: {file_name}")
                return
            
            # Cek jika file sudah pernah diproses
            if file_path in self.processed_files:
                logger.info(f"File already processed: {file_name}")
                return
                
            logger.info(f"New file detected: {file_name}")
            
            # Tandai sebagai sedang diproses
            self.processed_files.add(file_path)
            
            # LANGSUNG PROSES - TANPA TUNGGU INITIAL DELAY
            self.process_file_immediately(file_path)

    def process_file_immediately(self, file_path):
        """PROSES FILE LANGSUNG - TANPA INITIAL DELAY"""
        file_name = os.path.basename(file_path)
        
        logger.info(f"IMMEDIATE PROCESSING: {file_name}")
        
        # Cek jika file masih exists
        if not os.path.exists(file_path):
            logger.warning(f"File disappeared: {file_name}")
            if file_path in self.processed_files:
                self.processed_files.remove(file_path)
            return
            
        # Cek file size minimal
        try:
            file_size = os.path.getsize(file_path)
            if file_size < 5 * 1024 * 1024:  # Minimal 5MB
                logger.info(f"File too small ({file_size} bytes), waiting...")
                self.retry_later(file_path, delay=30)
                return
        except Exception as e:
            logger.error(f"Error checking file size: {e}")
            self.retry_later(file_path, delay=30)
            return
            
        # LANGSUNG CEK APAKAH FILE SUDAH BEBAS DARI SEMUA LOCK
        self.wait_for_file_completely_unlocked_then_process(file_path)

    def wait_for_file_completely_unlocked_then_process(self, file_path):
        """TUNGGU SAMPAI FILE BENAR-BENAR TIDAK ADA LOCK SAMA SEKALI"""
        file_name = os.path.basename(file_path)
        file_size_mb = self.get_file_size_mb(file_path)
        
        logger.info(f"WAITING FOR FILE COMPLETELY UNLOCKED: {file_name} ({file_size_mb})")
        logger.info(f"Will wait until file is COMPLETELY FREE from all locks...")
        
        attempt = 0
        start_time = time.time()
        
        while True:
            attempt += 1
            try:
                # CEK APAKAH FILE SUDAH BENAR-BENAR BEBAS DARI SEMUA LOCK
                if self.is_file_completely_unlocked(file_path):
                    total_wait_time = int(time.time() - start_time)
                    logger.info(f"SUCCESS: File completely unlocked (attempt {attempt}, waited {total_wait_time}s): {file_name}")
                    
                    # FILE SUDAH BENAR-BENAR BEBAS, COPY DAN HAPUS SEKALI
                    success = self.process_file_completely(file_path)
                    if success:
                        logger.info(f"COMPLETE SUCCESS: {file_name}")
                        if file_path in self.processed_files:
                            self.processed_files.remove(file_path)
                        return
                    else:
                        logger.error(f"PROCESS FAILED: {file_name}")
                        self.handle_failure(file_path, "Gagal memproses file")
                        return
                
                else:
                    # File masih ada lock, tunggu dan coba lagi - TANPA BATAS
                    total_wait_seconds = int(time.time() - start_time)
                    total_wait_minutes = total_wait_seconds // 60
                    
                    if attempt == 1:
                        logger.info(f"File still locked, starting wait process...") 
                    
                    time.sleep(self.wait_delay)
                        
            except Exception as e:
                logger.error(f"ERROR during wait: {str(e)}")
                self.handle_failure(file_path, f"Error: {str(e)}")
                return

    def is_file_completely_unlocked(self, file_path):
        """CEK FILE SUDAH BENAR-BENAR BEBAS DARI SEMUA LOCK (READ & DELETE)"""
        try:
            # 1. Cek file exists
            if not os.path.exists(file_path):
                return False
                
            # 2. Cek size minimal (5MB)
            file_size = os.path.getsize(file_path)
            if file_size < 5 * 1024 * 1024:
                return False
                
            # 3. CEK BISA DIBACA (READ LOCK)
            if not self.is_file_readable(file_path):
                return False
                
            # 4. CEK BISA DIHAPUS (DELETE LOCK) - INI YANG PENTING!
            if not self.is_file_deletable(file_path):
                return False
                
            # 5. Cek stability - file tidak berubah size
            if not self.is_file_stable(file_path):
                return False
                
            return True
            
        except Exception as e:
            return False

    def is_file_readable(self, file_path):
        """Cek apakah file tidak locked untuk dibaca"""
        try:
            with open(file_path, 'rb') as f:
                f.read(1024)  # Baca 1KB data
            return True
        except (IOError, PermissionError):
            return False

    def is_file_deletable(self, file_path):
        """CEK UTAMA: Apakah file bisa dihapus (tidak ada process yang memegang lock)"""
        try:
            # Coba rename file sementara - jika berhasil berarti tidak ada lock
            temp_name = file_path + ".delete_test"
            os.rename(file_path, temp_name)
            # Kembalikan nama asli
            os.rename(temp_name, file_path)
            return True
        except (IOError, PermissionError, OSError):
            return False
        except Exception:
            return False

    def is_file_stable(self, file_path, check_interval=3):
        """Cek apakah ukuran file sudah stabil dengan toleransi 1%"""
        try:
            size1 = os.path.getsize(file_path)
            time.sleep(check_interval)
            size2 = os.path.getsize(file_path)
            
            # Toleransi 1% untuk perubahan kecil
            change_percent = abs(size2 - size1) / max(size1, 1) * 100
            is_stable = (change_percent < 1.0)
            
            if not is_stable:
                logger.info(f"File size change: {size1} -> {size2} ({change_percent:.2f}%)")
                    
            return is_stable
            
        except Exception as e:
            logger.error(f"Error checking stability: {e}")
            return False

    def process_file_completely(self, file_path):
        """PROSES FILE SETELAH BENAR-BENAR BEBAS DARI SEMUA LOCK"""
        try:
            file_name = os.path.basename(file_path)
            final_size = self.get_file_size_mb(file_path)
            logger.info(f"PROCESSING COMPLETELY UNLOCKED FILE: {file_name} ({final_size})")

            # Validasi format filename
            destination_folder, new_file_name = self.get_destination_folder_and_filename(file_name)
            if destination_folder is None:
                self.handle_invalid_file(file_path, file_name)
                return False
            
            # Buat folder tujuan
            now = datetime.now()
            year_folder = str(now.year)
            month_folder = now.strftime("%B")
            day_folder = now.strftime("%d")

            final_destination = os.path.join(destination_folder, year_folder, month_folder, day_folder)
            os.makedirs(final_destination, exist_ok=True)
            final_destination_path = os.path.join(final_destination, new_file_name)

            logger.info(f"Moving to: {final_destination_path}")
            
            # COPY FILE - karena sudah dipastikan benar-benar bebas
            copy_success = self.safe_copy_file(file_path, final_destination_path, file_name)
            
            if copy_success:
                # HAPUS ORIGINAL FILE - karena sudah dipastikan bisa dihapus
                delete_success = self.safe_delete_file(file_path, file_name)
                if delete_success:
                    logger.info(f"COMPLETE SUCCESS: Copied and deleted original: {file_name}")
                    return True
                else:
                    logger.error(f"COPY SUCCESS BUT DELETE FAILED: {file_name}")
                    return False
            else:
                logger.error(f"COPY FAILED: {file_name}")
                return False

        except Exception as ex:
            logger.error(f"Error in process_file_completely: {ex}")
            return False

    def safe_copy_file(self, src_path, dst_path, file_name):
        """Copy file dengan verifikasi"""
        try:
            logger.info("Copying file...")
            
            shutil.copy2(src_path, dst_path)
            
            # Verify copy success
            if os.path.exists(dst_path):
                src_size = os.path.getsize(src_path)
                dst_size = os.path.getsize(dst_path)
                
                if src_size == dst_size:
                    logger.info(f"COPY VERIFIED: {src_size} bytes")
                    return True
                else:
                    logger.error(f"COPY SIZE MISMATCH: {src_size} vs {dst_size}")
                    if os.path.exists(dst_path):
                        os.remove(dst_path)
                    return False
            else:
                logger.error("COPY FAILED: Destination file not created")
                return False
                
        except Exception as e:
            logger.error(f"Error in safe_copy_file: {e}")
            return False

    def safe_delete_file(self, file_path, file_name):
        """Hapus file dengan confidence tinggi"""
        try:
            if not os.path.exists(file_path):
                logger.info(f"ORIGINAL ALREADY DELETED: {file_name}")
                return True
                
            # Double check: pastikan file masih bisa dihapus
            if self.is_file_deletable(file_path):
                os.remove(file_path)
                logger.info(f"ORIGINAL DELETED: {file_name}")
                return True
            else:
                logger.error(f"DELETE FAILED: File became locked again: {file_name}")
                return False
                
        except Exception as e:
            logger.error(f"DELETE ERROR: {e} for file: {file_name}")
            return False

    def retry_later(self, file_path, delay=30):
        """Coba lagi nanti untuk file kecil"""
        logger.info(f"Retrying small file in {delay}s: {os.path.basename(file_path)}")
        threading.Timer(delay, self.process_file_immediately, [file_path]).start()

    def get_file_size_mb(self, file_path):
        """Get file size in MB"""
        try:
            size_bytes = os.path.getsize(file_path)
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        except:
            return "Unknown"

    def get_local_ip(self):
        """Get local IP address"""
        try:
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            return ip
        except Exception as e:
            logger.error(f"ERROR getting local IP: {e}")
            return "Unknown"

    def show_message_box(self, title, message):
        """Show Windows message box"""
        ctypes.windll.user32.MessageBoxW(0, message, title, 0x10) 
    
    def handle_invalid_file(self, file_path, file_name):
        """Handle file dengan format tidak valid"""
        msg = f"File '{file_name}' tidak sesuai format.\n\nFormat: BAHANPUSTAKA_KEGIATAN_JUDUL.ext"
        logger.error(msg)
        self.show_message_box("Format Error", msg)
        
        time.sleep(2)
        if os.path.exists(file_path) and self.is_file_deletable(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Deleted invalid file: {file_path}")
            except:
                logger.warning(f"Failed to delete invalid file: {file_path}")

    def handle_failure(self, file_path, message):
        """Handle ketika file gagal diproses"""
        file_name = os.path.basename(file_path)
        logger.error(f"Failed to process: {file_name} - {message}")
        
        if file_path in self.processed_files:
            self.processed_files.remove(file_path)
            
        self.show_message_box("File Watcher Error", 
                             f"Gagal memproses: {file_name}\n\n{message}")

    def get_destination_folder_and_filename(self, file_name):
        """Parse filename dan tentukan folder tujuan"""
        parts = file_name.split('_')
        if len(parts) < 3:
            logger.error(f"Invalid filename format: {file_name}")
            return None, None
        
        if not '.' in parts[-1]:
            logger.error(f"File without extension: {file_name}")
            return None, None
        
        bahanpustaka_code = parts[0].upper()
        kegiatan_code = parts[1].upper()
        judul_parts = parts[2:]
        new_file_name = '_'.join(judul_parts)
        
        bahanpustaka_folder = self.bahanpustaka_map.get(bahanpustaka_code, bahanpustaka_code)
        kegiatan_folder = self.kegiatan_map.get(kegiatan_code, kegiatan_code)
        full_path = os.path.join(self.processed_folder, bahanpustaka_folder, kegiatan_folder)
        
        logger.info(f"Destination folder: {full_path}")
        logger.info(f"New filename: {new_file_name}")
        
        return full_path, new_file_name

def create_sample_mapping_files(kegiatan_map_path, bahanpustaka_map_path):
    """Buat sample mapping files jika tidak exist"""
    # Sample kegiatan mapping
    if not os.path.exists(kegiatan_map_path):
        sample_kegiatan = {
	   			"KHI": "KEPRI HARI INI",
    				"KM": "KEPRI MENYAPA",
    				"NB": "NGAJI BARENG",
    				"MA": "MIMBAR AGAMA",
    				"KS": "KEPRI SEPEKAN",
    				"RM": "RUMAH MUSIK",
    				"HPK": "HALO PEMIRSA KEPRI"
			}

        with open(kegiatan_map_path, "w", encoding="utf-8") as f:
            json.dump(sample_kegiatan, f, indent=2, ensure_ascii=False) 

    # Sample bahan pustaka mapping
    if not os.path.exists(bahanpustaka_map_path):
        sample_bahanpustaka = {
            "KL": "KONTEN LOKAL",
            "KN": "KONTEN NASIONAL"
        }
        with open(bahanpustaka_map_path, "w", encoding="utf-8") as f:
            json.dump(sample_bahanpustaka, f, indent=2, ensure_ascii=False) 

def main():
    """Main function"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    watch_folder = r"/volume1/Produksi TVRI"
    processed_folder =  watch_folder
    kegiatan_map_path = os.path.join(script_dir, "kegiatan_map.json")
    bahanpustaka_map_path = os.path.join(script_dir, "bahanpustaka_map.json")

    logger.info("=== MAGICSOFT FILE WATCHER STARTING ===")  

    # Cek dan buat watch folder jika tidak exist
    if not os.path.exists(watch_folder):
        logger.info(f"Watch folder does not exist, creating: {watch_folder}")
        os.makedirs(watch_folder, exist_ok=True)

    # Buat sample mapping files
    create_sample_mapping_files(kegiatan_map_path, bahanpustaka_map_path)

    # Buat processed folder
    os.makedirs(processed_folder, exist_ok=True) 

    # Inisialisasi dan start file watcher
    event_handler = MagicSoftFileWatcher(watch_folder, processed_folder, kegiatan_map_path, bahanpustaka_map_path)
    observer = Observer()
    observer.schedule(event_handler, watch_folder, recursive=False)
    observer.start()
    logger.info("File watcher started successfully")

    try:
        # Main loop
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
        observer.stop()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        observer.stop()
    
    observer.join()
    logger.info("File watcher stopped")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_msg = f"CRITICAL ERROR: {e}"
        logger.error(error_msg)
        
        # Show error message box
        try:
            ctypes.windll.user32.MessageBoxW(0, error_msg, "File Watcher Critical Error", 0x10)
        except:
            pass
            
        # Write to error log
        with open('file_watcher_critical.log', 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now()} - {error_msg}\n")