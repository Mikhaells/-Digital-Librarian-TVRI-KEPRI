# Digital Librarian — TVRI KEPRI

Ringkasan singkat: Python utility yang memantau folder (watch folder) pada Windows, memeriksa file baru sampai benar‑benar bebas (tidak dikunci), memverifikasi ukuran dan stabilitas, lalu menyalin file ke struktur folder tujuan berdasarkan kode pada nama file.

## Fitur utama
- Memantau folder (watch folder) untuk file baru (Windows).
- Validasi ukuran minimum (default 5 MB) dan stabilitas ukuran sebelum memproses.
- Cek apakah file dapat dibaca dan dihapus (tidak dikunci oleh proses lain).
- Menyalin file ke folder tujuan berdasarkan mapping kode BAHANPUSTAKA dan KEGIATAN.
- Struktur tujuan: <processed_folder>/<BAHANPUSTAKA>/<KEGIATAN>/YYYY/Month/DD/<filename>
- Logging ke `file_watcher.log` dan log kritikal ke `file_watcher_critical.log`.
- Popup message box Windows untuk notifikasi error/format.

## Teknologi
- Python 3.8+ (disarankan)
- watchdog (watchdog.observers)
- standard library: os, shutil, threading, logging, ctypes, socket, datetime

## Prasyarat
- Windows OS (menggunakan MessageBox via ctypes dan perilaku rename/delete Windows)
- Python 3.8 atau lebih tinggi terinstall
- Module pip: watchdog

Contoh instalasi dependencies:
```powershell
python -m pip install watchdog
```

## Instalasi & konfigurasi
1. Clone atau salin repo ke mesin Windows.
2. Pastikan file `PCRecord.py` berada di folder proyek.
3. Pastikan mapping file `kegiatan_map.json` dan `bahanpustaka_map.json` ada di folder yang sama. Jika tidak ada, script akan membuat sample mapping otomatis.
4. Ubah variabel `watch_folder` dalam `PCRecord.py` jika ingin folder selain default (`C:\TestWatch`).

## Struktur project (ringkasan)
- PCRecord.py — main script pemantau dan pemroses file
- kegiatan_map.json — mapping kode kegiatan -> nama folder (dibuat otomatis bila tidak ada)
- bahanpustaka_map.json — mapping kode bahan pustaka -> nama folder (dibuat otomatis bila tidak ada)
- file_watcher.log — log operasi (UTF-8)
- file_watcher_critical.log — log error kritikal

Contoh struktur:
```
C:\TestWatch\                           # watch folder (default)
project_root/
├─ PCRecord.py
├─ kegiatan_map.json
├─ bahanpustaka_map.json
├─ file_watcher.log
└─ file_watcher_critical.log
```

## Contoh penggunaan
1. Jalankan script:
```powershell
python PCRecord.py
```
2. Letakkan file dengan format nama: `BAHANPUSTAKA_KEGIATAN_JUDUL.ext`
   - Contoh: `KL_KHI_PeluncuranProgram.mp4`
3. Script akan:
   - Mengabaikan file temporary (ekstensi `.tmp`) dan file tanpa ekstensi.
   - Menunggu hingga file tidak terkunci, stabil, dan ukuran >= 5 MB.
   - Menyalin ke folder tujuan berdasarkan mapping dan