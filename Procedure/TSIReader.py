import logging
import threading
import time
import re
import socket

from datetime import datetime

logger = logging.getLogger(__name__)

# DusttrakII 8530
class TSIReader:
    def __init__(self):
        self.ip = None
        self.port = None 
        self.unit = None 
        self.conc_queue = None

        self.timeout_limit = 30

        self.running = threading.Event()
        self.socket = None
        self.thread = None
        self.lock = threading.Lock()
        
    def _cleanup(self):
        """釋放資源"""
        with self.lock:
            if self.socket is None:
                logger.info("🔌 TSI 資源已釋放。")
                return
            try:
                self.socket.close()
            except Exception:
                pass
            self.socket = None
            logger.info("🔌 TSI 連線中斷，資源已釋放。")

    def stop(self):
        self.running.clear()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)
        logger.info("🛑 TSIReader 執行緒已停止")

    RESPONSE_TERMINATORS = (b'\r', b'\n')
    MAX_RESPONSE_BYTES = 4096
    _warned_no_terminator = False

    def _drain_input(self):
        """清掉上一輪殘留在接收緩衝區的資料，避免把舊回應當成這一輪的回應（回應錯位）"""
        self.socket.setblocking(False)
        try:
            while True:
                try:
                    stale = self.socket.recv(1024)
                except (BlockingIOError, socket.timeout):
                    break
                if not stale:
                    raise ConnectionError("TSI 儀器斷開連線")
                logger.debug(f"TSI 清除殘留資料 {len(stale)} bytes")
        finally:
            self.socket.settimeout(2.0)

    def _read_response(self):
        """讀到結尾符 (\r 或 \n) 才算一筆完整回應；TCP 是串流，不能假設一次 recv 就是一整筆。
        回傳 b'' 代表對方已關閉連線。
        若逾時但已收到部分資料，視為儀器沒送結尾符，直接用現有資料解析（只警告一次）。"""
        buf = b''
        while True:
            try:
                chunk = self.socket.recv(1024)
            except socket.timeout:
                if buf:
                    if not self._warned_no_terminator:
                        logger.warning("⚠️ TSI 回應未偵測到結尾符，改以逾時前收到的資料解析")
                        self._warned_no_terminator = True
                    return buf
                raise
            if not chunk:
                return b''
            buf += chunk
            if any(term in buf for term in self.RESPONSE_TERMINATORS) or len(buf) >= self.MAX_RESPONSE_BYTES:
                return buf
    def _producer(self):
        """背景執行緒：持續讀取 TSI 數據並放入 Queue"""
        first_failure_time = None
        empty_reads = 0
        timed_out = False
        while self.running.is_set():
            try:
                # 1. 嘗試連線 (如果尚未連線)
                with self.lock:
                    is_open = self.socket is not None

                if not is_open:
                    logger.info(f"📡 嘗試連線至 TSI: {self.ip}::{self.port}")
                    connect_start_time = time.time()
                    while True:
                        if not self.running.is_set(): return
                        
                        try:
                            # 建立 TCP Socket 連線
                            with self.lock:
                                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                self.socket.settimeout(2.0) # 設定讀取與連線的逾時時間
                                self.socket.connect((self.ip, self.port))
                            break       # 如果成功執行到這行，代表連線成功，跳出迴圈
                        except (socket.timeout, socket.error, ConnectionRefusedError):
                            # 如果連線失敗，檢查是否已經嘗試超過 5 秒
                            if time.time() - connect_start_time > 5.0:
                                raise # 超過時間，將錯誤往外拋
                            time.sleep(1)       # 還沒超過時間，休息 1 秒後重試
                            
                    logger.info(f"✅ TSI 連線成功！ {self.ip}::{self.port}")
                    empty_reads = 0   # 失敗計時器改在「收到有效數據」時才重置

                # 2~3. 清掉殘留資料 -> 發送讀取即時濃度指令 (RMMEAS) -> 讀到結尾符為止
                # 注意：TSI 必須以 \r 作為結尾
                try:
                    with self.lock:
                        if self.socket:
                            self._drain_input()
                            self.socket.sendall(b'RMMEAS\r')
                            raw_data = self._read_response()
                        else:
                            raw_data = None
                except socket.timeout:
                    # 逾時代表儀器沒回應；連續 5 次(約 10 秒)視為斷線，進入重連/逾時流程
                    empty_reads += 1
                    if empty_reads >= 5:
                        raise ConnectionError("TSI 儀器連續 5 次無回應")
                    time.sleep(0.1)
                    continue
                if not raw_data:
                    # 收到空資料通常代表儀器端主動斷開了連線
                    raise ConnectionError("TSI 儀器斷開連線")
                empty_reads = 0

                # 4. 解析數據
                try:
                    decoded = raw_data.decode('ascii', errors='ignore')
                    lines = [ln.strip() for ln in re.split(r'[\r\n]+', decoded) if ln.strip()]
                    decoded_str = lines[0] if lines else ''
                    
                    # 這裡以逗號分割，並取第二筆資料
                    parts = decoded_str.split(',')
                    
                    if len(parts) > 1 and parts[1]:
                        # 擷取字串中的浮點數
                        match = re.search(r"[-+]?\d*\.\d+|\d+", parts[1])
                        
                        if match:
                            val = float(match.group())
                            conc_packet = {
                                "timestamp": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"),
                                "conc": val,
                                "conc_unit": self.unit
                            }
                            if self.conc_queue:
                                self.conc_queue.put(conc_packet)
                            first_failure_time = None
                        else:
                            logger.warning(f"⚠️ TSI 數值格式異常: {parts[1]}")
                    else:
                        logger.warning(f"⚠️ TSI 回傳無法解析: {decoded_str}")

                except ValueError:
                    logger.warning(f"⚠️ TSI 數值轉換錯誤: {decoded_str}")

                # 儀器讀取間隔 (TSI 建議至少 1 秒，可依照你的需求調整)
                time.sleep(1.0)

            except (socket.error, ConnectionError) as e:
                logger.error(f"❌ TSI 連線失敗或中斷: {e}")
                self._cleanup() # 這裡需要實作一個關閉 socket 並設為 None 的方法

                if not self.running.is_set(): break

                current_time = time.time()
                # 如果是連續失敗的第一筆，紀錄開始時間
                if first_failure_time is None:
                    first_failure_time = current_time

                elapsed = current_time - first_failure_time       
                
                if elapsed >= self.timeout_limit:
                    logger.error(f"❌ TSI 已超過 {self.timeout_limit} 秒無法連線，停止嘗試。")
                    timed_out = True
                    self.running.clear()
                    break
                else:
                    time.sleep(5)       # 等待 5 秒後再嘗試連線

            except Exception as e:
                logger.error(f"❌ TSI 未預期錯誤: {e}")
                time.sleep(1)
        
        self._cleanup()
        if timed_out:
            logger.error("🏁 逾時連線，TSI 讀取執行緒已停止。")
        if self.conc_queue:
            self.conc_queue.put(None)

    def run(self):
        self.running.set()
        logger.info(f"🚀 開始處理 TSI 數據...")
        self.thread = threading.Thread(target=self._producer, daemon=True)
        self.thread.start()

### ---test--- ###
if __name__ == "__main__":
    import queue

    conc_queue = queue.Queue()
    conc_reader = TSIReader()
    conc_reader.ip = '192.168.10.100' 
    conc_reader.port = 3602             # 3603 3604 也可
    conc_reader.unit = 'mg/m³'
    conc_reader.conc_queue = conc_queue
    conc_reader.run()

    try:
        while True:
            try:
                data = conc_queue.get(timeout=2)
                if data:
                    print(f"Received Concentration Data: {data}")
            except queue.Empty:
                print("No data received in the last 5 seconds.")
    except KeyboardInterrupt:
        print("Stopping Concentration Reader...")
        conc_reader.stop()
        time.sleep(2)

    # def _fake_producer(self):
    #     while self.running:
    #         try:
    #             # 模擬讀取濃度 
    #             fake_val = round(random.uniform(200, 1500), 2)
                
    #             # 封裝資料
    #             conc_packet = {
    #                 "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    #                 "conc": fake_val,
    #                 "conc_unit": self.unit
    #             }
                
    #             # 塞入 Queue
    #             self.conc_queue.put(conc_packet)
                
    #             # 控制頻率 (盡量接近 1 秒 1 次，與 GPS 同步)
    #             time.sleep(1) 
                
    #         except Exception as e:
    #             logger.error(f"濃度讀取錯誤: {e}")
    #             time.sleep(1)