import json
import queue

from pathlib import Path


def _get(section, key, label, default=None, cast=None):
    """讀取設定值。缺少必要欄位或格式錯誤時，丟出說明清楚的 ValueError。
    default=None 代表此欄位為必填。"""
    value = section.get(key)
    if value is None or value == "":
        if default is None:
            raise ValueError(f"config.json 缺少必要欄位: {label}")
        value = default
    if cast is None:
        return value
    try:
        return cast(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"config.json 欄位 {label} 格式錯誤，預期為 {cast.__name__}，目前為: {value!r}"
        ) from None


class Config:
    def __init__(self, config_name):
        # 1. 取得目前程式碼所在的資料夾路徑 (取代 os.path.dirname)
        self.BASE_DIR = Path(__file__).parent.resolve()
        # 2. 組合設定檔的完整路徑 (取代 os.path.join)
        config_path = self.BASE_DIR / config_name
        # 3. 檢查檔案是否存在 (取代 os.path.exists)
        if not config_path.exists():
            raise FileNotFoundError(f"找不到設定檔: {config_path}")
        # 4. 讀取 JSON
        try:
            with config_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"設定檔不是有效的 JSON（第 {e.lineno} 行，第 {e.colno} 欄）: {e.msg}"
            ) from e

        # --- Firebase 分類 ---
        fb = data.get("firebase", {})
        self.DB_ID = fb.get("db_id")
        self.API_KEY = fb.get("api_key")
        self.FIREBASE_KEY = _get(fb, "key_path", "firebase.key_path")
        self.REGION = fb.get("region")
        self.DB_URL = fb.get("db_url")
        if not self.DB_URL:
            if not self.DB_ID:
                raise ValueError("config.json 的 firebase 需要設定 db_url，或設定 db_id（及 region）")
            if self.REGION == "us-central1":
                self.DB_URL = f"https://{self.DB_ID}.firebaseio.com"
            else:
                self.DB_URL = f"https://{self.DB_ID}-default-rtdb.{self.REGION}.firebasedatabase.app"

        # --- GPS 分類 ---
        gps = data.get("gps", {})
        self.GPS_IP = gps.get("wifi_ip")
        self.GPS_PORT = _get(gps, "port", "gps.port", cast=int)

        # --- CONC 分類 ---
        conc = data.get("conc", {})
        self.CONC_INSTRUMENT = conc.get("instrument")
        self.CONC_SERIAL_PORT = conc.get("serial_port") or ""
        self.CONC_BAUDRATE = _get(conc, "baudrate", "conc.baudrate", default=9600, cast=int)
        self.CONC_IP = conc.get("wifi_ip") or ""
        self.CONC_PORT = _get(conc, "port", "conc.port", default=3602, cast=int)
        unit_map = {
            "PID": "ppb",
            "TSI": "mg/m³"
        }
        self.CONC_UNIT = unit_map.get(self.CONC_INSTRUMENT)

        # ---Wind 分類 ---
        wind = data.get("wind", {})
        self.WIND_API_KEY = wind.get("api_key")

        # --- Camera 分類 ---
        camera = data.get("camera", {})
        self.CAMERA_ENABLED = bool(camera.get("enabled", False))
        self.CAMERA_INDEX = _get(camera, "index", "camera.index", default=1, cast=int)
        self.CAMERA_THRESHOLD = _get(camera, "threshold", "camera.threshold", default=800.0, cast=float)

        # --- Settings 分類 ---
        stg = data.get("settings", {})
        self.PROJECT_NAME = _get(stg, "project_name", "settings.project_name")
        self.TIME_DELAY = _get(stg, "time_delay", "settings.time_delay", cast=float)
        if self.TIME_DELAY < 0:
            raise ValueError(f"config.json 欄位 settings.time_delay 不可為負數，目前為: {self.TIME_DELAY}")
        self.MAP_URL = stg.get("map_url")

        # --- 固定參數 ---
        self.GPS_QUEUE = queue.Queue()      # 接收 GPS 數據
        self.CONC_QUEUE = queue.Queue()     # 接收 CONC 數據
        self.WIND_QUEUE = queue.Queue()     # 接收 Wind 數據
        self.SHARED_QUEUE = queue.Queue()   # 合併 GPS 和 CONC 數據，以上傳至 firebase
        self.CAMERA_QUEUE = queue.Queue()   # 傳遞即時濃度給相機模組
