@echo off                           
echo Menjalankan Python script di ASUSTOR NAS...

:: Cek apakah proses PCRecord.py sudah berjalan
ssh admin@10.10.0.113 "pgrep -f 'PCRecord.py'"

:: Simpan exit code dari perintah pgrep
if %errorlevel% == 0 (
    echo PCRecord.py sudah berjalan.
    echo Tidak menjalankan instance baru.
) else (
    echo PCRecord.py tidak berjalan.
    echo Menjalankan PCRecord.py...
    ssh admin@10.10.0.113 "cd '/volume1/Produksi TVRI/File Watcher py' && python3 PCRecord.py"
)

echo Script selesai dijalankan
pause