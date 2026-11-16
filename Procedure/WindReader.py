import os
import truststore
truststore.inject_into_ssl()

import requests
import queue
import math
import time
import threading
import logging

from datetime import datetime
from urllib.parse import quote

logger = logging.getLogger(__name__)

class WindReader:
    def __init__(self):
        """
        初始化 WindReader
        :param update_interval: API 更新間隔時間（預設 300 秒 = 5 分鐘）
        """
        self.api_key = None
        
        self.update_interval = 300  # 預設每 5 分鐘更新一次
        self.retry_interval = 30    # 抓取失敗時，30 秒後重試
        self.rematch_interval = 30  # 每 30 秒依最新位置重新挑選最近測站
        self.max_obs_age_sec = 1800     # 測站觀測時間超過 30 分鐘視為過期（資料可能已停止更新），不採用
        self._warned_obs_format = False
        self.latest_loc = [None, None]  # [lat, lon]
        self.data_id = "O-A0003-001"    # 氣象觀測站-10分鐘綜觀氣象資料
        self.station_data = []

        # 存放計算好的最新風場資料，供主程式隨時讀取
        self.current_wind = {"測站名稱": "", "觀測時間": "", "風速": None, "風向": None, "溫度": None, "濕度": None}
        self.wind_queue = None
        
        # 執行緒控制標籤與鎖
        self.running = threading.Event()
        self.thread = None
        self.lock = threading.Lock()

    @staticmethod
    def _valid_number(value, min_value=0.0):
        """轉成數字；轉不成、或低於 min_value（氣象署缺值通常為 -99）一律視為缺值，回傳 None"""
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        return num if num >= min_value else None

    def _redact(self, err):
        """例外訊息常會帶完整網址（含 API key），寫入日誌前先遮蔽"""
        text = str(err)
        if self.api_key:
            for secret in (self.api_key, quote(self.api_key, safe="")):
                text = text.replace(secret, "***")
        return text

    def _is_obs_fresh(self, obs_time):
        """觀測時間是否在 max_obs_age_sec 內；格式無法解析時不做過期判斷（只警告一次）"""
        if not obs_time:
            return False
        try:
            obs_dt = datetime.fromisoformat(obs_time)
        except ValueError:
            if not self._warned_obs_format:
                logger.warning(f"⚠️ 無法解析氣象站觀測時間格式 ({obs_time})，略過過期判斷")
                self._warned_obs_format = True
            return True
        age = (datetime.now().astimezone() - obs_dt.astimezone()).total_seconds()
        return age <= self.max_obs_age_sec

    def _fetch_cwa_data(self):
        """負責戳 API 並解析全台測站資料"""
        if not self.api_key:
            logger.warning("⚠️ 未設定 API Key")
            return False
        
        url = f"https://opendata.cwa.gov.tw/api/v1/rest/datastore/{self.data_id}?Authorization={self.api_key}&format=JSON"
        try:
            # 加上 timeout 避免網路異常卡死
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            stations = data.get('records', {}).get('Station', [])
            parsed_data = []
            stale_count = 0
            
            for st in stations:
                try:
                    # 1. 解析 WGS84 經緯度
                    lat, lon = None, None
                    for coord in st.get('GeoInfo', {}).get('Coordinates', []):
                        if coord.get('CoordinateName') == 'WGS84':
                            lat = float(coord.get('StationLatitude'))
                            lon = float(coord.get('StationLongitude'))
                            break
                    
                    if lat is None or lon is None:
                        continue
                    
                    # 2. 解析觀測時間
                    station_name = st.get('StationName', '')
                    obs_time = st.get('ObsTime', {}).get('DateTime', '')
                    
                    # 3. 解析風速、風向、溫度、濕度（缺值為 None，不再因為單一欄位缺值就整站捨棄）
                    we = st.get('WeatherElement', {})
                    wind_speed = self._valid_number(we.get('WindSpeed'))
                    wind_dir = self._valid_number(we.get('WindDirection'))
                    temperature = self._valid_number(we.get('AirTemperature'), min_value=-90.0)
                    humidity = self._valid_number(we.get('RelativeHumidity'))
                    
                    # 四項全缺 -> 沒有可用資料
                    if all(v is None for v in (wind_speed, wind_dir, temperature, humidity)):
                        continue
                    
                    # 觀測時間過期 -> 測站可能已停止回報
                    if not self._is_obs_fresh(obs_time):
                        stale_count += 1
                        continue
                        
                    parsed_data.append({
                        '測站名稱': station_name,
                        'lat': lat,
                        'lon': lon,
                        '觀測時間': obs_time,
                        '風速': wind_speed,
                        '風向': wind_dir,
                        '溫度': temperature,  
                        '濕度': humidity
                    })
                except (ValueError, TypeError):
                    continue
                    
            # 只有在成功解析出有效資料時才更新
            if parsed_data:
                self.station_data = parsed_data
                extra = f"（略過 {stale_count} 個觀測時間過期的測站）" if stale_count else ""
                logger.info(f"⛅ 已更新氣象署風場資料，共載入 {len(parsed_data)} 個有效測站{extra}")
                return True

            logger.warning(f"⚠️ 氣象署資料中沒有可用的測站（過期 {stale_count} 個）")
            return False

        except Exception as e:
            logger.warning(f"⚠️ 取得氣象資料失敗: {self._redact(e)}")
            return False
    def _haversine(self, lat1, lon1, lat2, lon2):
        """使用半正矢公式計算兩點 GPS 之間的直線距離（單位：公尺）"""
        R = 6371000 
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0) ** 2 + \
            math.cos(phi1) * math.cos(phi2) * \
            math.sin(delta_lambda / 2.0) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def get_wind_data(self):
        """
        輸入當前車輛的經緯度，自動配對距離最近的測站，輸出字典格式
        """
        # 如果 GPS 無效，直接回傳空資料
        if self.latest_loc[0] is None or self.latest_loc[1] is None or not self.station_data:
            return 
        
        min_distance = float('inf')
        nearest_station = None
        
        with self.lock:
            for st in self.station_data:
                dist = self._haversine(self.latest_loc[0], self.latest_loc[1], st['lat'], st['lon'])
                if dist < min_distance:
                    min_distance = dist
                    nearest_station = st
                    
        if nearest_station:
            prev_wind = self.current_wind
            self.current_wind = {
                "測站名稱": nearest_station['測站名稱'],
                "觀測時間": nearest_station['觀測時間'],
                "風速": nearest_station['風速'],
                "風向": nearest_station['風向'],
                "溫度": nearest_station['溫度'],
                "濕度": nearest_station['濕度']
            }
            if self.current_wind != prev_wind:
                logger.info(f"📍 已更新最近測站風場： {self.current_wind}")

            if self.wind_queue:
                self.wind_queue.put(self.current_wind.copy())

    def _producer(self):
        """背景執行緒：定期向氣象署 API 更新資料"""
        logger.info("🌬️ WindReader 執行緒已啟動，開始抓取風場資料...")
        
        # 強制觸發第一次抓取與計算
        last_fetch_time = 0
        last_match_time = 0

        while self.running.is_set():
            current_time = time.time()
            if current_time - last_fetch_time >= self.update_interval:
                ok = self._fetch_cwa_data()
                self.get_wind_data()
                # 失敗時不用等滿整個更新週期，retry_interval 秒後再試
                last_fetch_time = current_time if ok else current_time - self.update_interval + self.retry_interval
                last_match_time = current_time
            elif current_time - last_match_time >= self.rematch_interval:
                self.get_wind_data()     # 不打 API，只依最新座標重新挑最近測站
                last_match_time = current_time

            # 短暫休眠，讓收到 stop 訊號時能迅速結束
            time.sleep(1)

    def stop(self):
        """停止風場讀取模組"""
        self.running.clear()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)
        logger.info("🛑 WindReader 執行緒已停止")

    def run(self):
        """啟動風場讀取模組的背景執行緒"""
        self.running.set()
        self.thread = threading.Thread(target=self._producer, daemon=True)
        self.thread.start()

# ==========================================
# 獨立測試區塊 (直接執行此檔案時才會運作)
# ==========================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

    wind_reader = WindReader()
    wind_reader.update_interval = 60
    
    # 測試前請替換為你的 API Key
    wind_reader.api_key = os.environ.get("CWA_API_KEY") 
    wind_reader.latest_loc = [25.062, 121.593]
    wind_reader.wind_queue = queue.Queue()
    
    # 2. 啟動背景執行緒 (run)
    wind_reader.run()
    
    try:
        while True:
            try:
                data = wind_reader.wind_queue.get(timeout=2)
                print(f"✅ Queue 收到風場快取： {data}")
            except queue.Empty:
                pass
    except KeyboardInterrupt:
        wind_reader.stop()