import socket
import logging
import pynmea2
import threading
import time

from datetime import datetime, timezone

logger = logging.getLogger(__name__)

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _now_str():
    """系統時間（本機時區），僅在取不到 GPS 時間時使用"""
    return datetime.now().astimezone().strftime(TIME_FORMAT)


def _fix_time_str(msg):
    """取 NMEA RMC 句子內的 UTC 定位時間，轉成本機時區字串（格式與原本一致）。
    取不到時（欄位空白等）退回系統時間。"""
    try:
        dt = msg.datetime
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime(TIME_FORMAT)
    except (AttributeError, TypeError, ValueError):
        return _now_str()


class GPSReader:
    def __init__(self):
        # 給定
        self.wifi_ip = None
        self.port = None
        self.gps_queue = None
        # 內部
        self.timeout_limit = 30 

        self.socket = None   
        self.file_obj = None 
        self.latest_data = {
            "timestamp": "", 
            "lat": 0.0, 
            "lon": 0.0, 
            "alt": None, 
            "status": "V"
        }     # 緩存最新資料
        self.last_yield_time = None
        self.running = threading.Event()
        self.thread = None
        self.lock = threading.Lock()  
        self._last_rx = time.time()   # 最後一次收到任何 NMEA 資料的時間（跨重連累計）

    def _cleanup(self):
        """明確釋放所有連線資源"""
        with self.lock:
            if self.socket is None and self.file_obj is None:
                logger.info("🔌 GPS 資源已釋放。")
                return
            
            try:
                self.file_obj.close()
            except Exception: 
                pass
            finally:
                self.file_obj = None
                
            try:
                self.socket.close()
            except Exception: 
                pass
            finally:
                self.socket = None
            logger.info("🔌 GPS 連線中斷，資源已釋放。")
    
    def stop(self):
        self.running.clear()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)

    def _rx_timed_out(self):
        """已連續 timeout_limit 秒沒有收到任何資料（包含連線失敗、連上卻沒資料）"""
        return time.time() - self._last_rx >= self.timeout_limit

    def _producer(self):
        """
        背景執行緒：
        - 每 5 秒嘗試重連一次
        - 連續 timeout_limit 秒收不到任何資料（無論是連不上，或連上了卻沒資料）則自動終止
        持續讀取資料塞入 Queue
        """
        timed_out = False
        self._last_rx = time.time()

        while self.running.is_set():
            try:
                logger.info(f"📡 嘗試連線至 GPS: {self.wifi_ip}::{self.port}")
                with self.lock:
                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.socket.settimeout(5)
                    self.socket.connect((self.wifi_ip, self.port))
                    self.file_obj = self.socket.makefile('r', encoding='utf-8', errors='ignore')

                # --- 連線成功（注意：不重置計時，只有「收到資料」才會重置） ---
                logger.info(f"✅ GPS 連線成功！ {self.wifi_ip}::{self.port}")

                for line in self.file_obj:
                    if not self.running.is_set(): break
                    self._last_rx = time.time()
                    self._parse_and_push(line.strip())
                self._cleanup()
                if self.running.is_set():
                    if self._rx_timed_out():
                        logger.error(f"❌ GPS 已超過 {self.timeout_limit} 秒沒有收到資料，停止嘗試。")
                        timed_out = True
                        self.running.clear()
                        break
                    logger.warning("⚠️ GPS 連線被關閉，1 秒後重新連線")
                    time.sleep(1)
            
            # --- 連線失敗 / 讀取逾時 ---
            except (socket.timeout, socket.error, ConnectionRefusedError):
                if not self.running.is_set(): break
                if self._rx_timed_out():
                    logger.error(f"❌ GPS 已超過 {self.timeout_limit} 秒沒有收到資料，停止嘗試。")
                    timed_out = True
                    self.running.clear() # 關閉主迴圈標記
                    break
                else:
                    logger.warning(f"⚠️ GPS 連線失敗或無資料，5 秒後重試...")
                    self._cleanup()
                    time.sleep(5)       # 等待 5 秒後再嘗試

        self._cleanup()
        if timed_out:
            logger.error("🏁 逾時連線，GPS 讀取執行緒已停止。")
        self.gps_queue.put(None)

    def _parse_and_push(self, line):
        """解析 NMEA，每個「定位時間（秒）」只放一筆進 Queue。
        時間戳取自 GPS 本身的定位時間，不受 Wi-Fi 延遲或突發傳輸影響。"""
        if not line.startswith('$'):
            return
        
        try:
            msg = pynmea2.parse(line)

            with self.lock:
                if isinstance(msg, pynmea2.types.talker.RMC):
                    fix_time = _fix_time_str(msg)
                    if msg.status == 'A' and msg.latitude and msg.longitude:
                        self.latest_data.update({
                            "timestamp": fix_time,
                            "lat": msg.latitude, 
                            "lon": msg.longitude, 
                            "status": "A"
                        })
                    else:
                        # 定位中或無效數據 -> 只更新時間，狀態設為 V
                        self.latest_data.update({
                            "timestamp": fix_time,
                            "status": "V"
                        })
                elif isinstance(msg, pynmea2.types.talker.GGA):     # 高度資訊
                    if msg.altitude:
                        self.latest_data["alt"] = msg.altitude

            # 檢查秒數是否改變，決定是否塞入 Queue
            curr_t = self.latest_data["timestamp"]
            if self.latest_data["status"] == "A" and \
               curr_t != self.last_yield_time and \
               self.latest_data["lat"] != 0:
                self.last_yield_time = curr_t
                self.gps_queue.put(self.latest_data.copy())

        except pynmea2.ParseError as e:
            logger.warning(f"GPS NMEA 解析失敗: {e} | 原始資料: {line}")
            
        except Exception as e:
            logger.error(f"GPS 處理未預期錯誤: {e}")

    def run(self):
        self.running.set()
        # 啟動背景執行緒 (daemon=True 確保主程式關閉時執行緒也結束)
        logger.info(f"🚀 開始處理 GPS 數據...")
        self.thread = threading.Thread(target=self._producer, daemon=True)
        self.thread.start()

### ---test--- ###
if __name__ == "__main__":
    import queue

    gps_queue = queue.Queue()
    gps_reader = GPSReader()
    gps_reader.wifi_ip = "192.168.199.103"
    gps_reader.port = 11123
    gps_reader.gps_queue = gps_queue
    gps_reader.run()

    try:
        while True:
            data = gps_queue.get()
            if data is None:
                print("GPS 讀取執行緒已停止")
                break
            print(f"收到 GPS 數據: {data}")
    except KeyboardInterrupt:
        gps_reader.stop()
