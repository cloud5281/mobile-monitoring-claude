import csv
import os
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class BackupManager:
    # 欄位順序必須與網頁「下載 CSV」完全一致，這樣備份檔可直接用網頁的「上傳」還原
    # (note / images 由網頁註記產生，備份時留空)
    FIELDNAMES = [
        'timestamp', 'lat', 'lon', 'conc', 'conc_unit', 'status',
        'wind_speed', 'wind_dir', 'temperature', 'humidity',
        'note', 'images', 'station_name', 'wind_time'
    ]
    FSYNC_INTERVAL = 5.0   # 秒；每隔這段時間強制寫入磁碟，斷電最多只會遺失這段時間內的資料

    def __init__(self):
        self.save_dir = "backups"
        self.file = None
        self.writer = None
        self.fieldnames = list(self.FIELDNAMES)
        self._last_fsync = 0.0

    def _sync(self, force=False):
        """flush 只是交給作業系統；fsync 才會真正寫入磁碟"""
        if not self.file:
            return
        now = time.monotonic()
        if force or now - self._last_fsync >= self.FSYNC_INTERVAL:
            try:
                os.fsync(self.file.fileno())
            except OSError as e:
                logger.warning(f"⚠️ 備份檔 fsync 失敗: {e}")
            self._last_fsync = now

    def start(self):
        """成功回傳 True，失敗回傳 False"""
        try:
            os.makedirs(self.save_dir, exist_ok=True)
            now_str = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(self.save_dir, f"{now_str}.csv")
            self.file = open(filename, mode='w', newline='', encoding='utf-8-sig')
            self.writer = csv.DictWriter(self.file, fieldnames=self.fieldnames,
                                         extrasaction='ignore', restval='')
            self.writer.writeheader()
            self.file.flush()
            self._sync(force=True)
            logger.info(f"💾 本地備份已啟動: {filename}")
            return True
        except Exception as e:
            logger.error(f"❌ 無法建立備份檔案: {e}")
            self.stop()
            return False

    def write(self, data):
        if self.file and self.writer and data:
            try:
                self.writer.writerow(data)
                self.file.flush()
                self._sync()
            except Exception as e:
                logger.error(f"⚠️ 寫入備份失敗: {e}")

    def stop(self):
        if self.file:
            try:
                self.file.flush()
                self._sync(force=True)
                self.file.close()
                logger.info("💾 備份檔案已存檔關閉。")
            except Exception as e:
                logger.error(f"❌ 關閉備份檔案錯誤: {e}")
            finally:
                self.file = None
                self.writer = None
