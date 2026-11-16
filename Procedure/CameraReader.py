import os
# 必須在 import cv2 之前設定環境變數，以隱藏 DSHOW 輪詢時產生的底層警告
os.environ["OPENCV_LOG_LEVEL"] = "FATAL"

import cv2
import time
import threading
import numpy as np
import ctypes
import argparse
import logging

from ctypes import wintypes
from pygrabber.dshow_graph import FilterGraph
from queue import Empty

logger = logging.getLogger(__name__)

# DirectShow(pygrabber) 屬於 COM 元件：每個「背景執行緒」使用前都必須先 CoInitialize
def _com_init():
    try:
        import comtypes
        comtypes.CoInitialize()
    except Exception:
        pass

class CameraReader:
    def __init__(self, camera_index=1, cooldown=5.0):
        self.camera_index = camera_index
        self.cooldown = cooldown
        self.threshold = None
        self.running = False
        self.is_connected = False
        self.cap = None
        self.save_dir = "events_photos"
        self.last_photo_time = 0.0
        self.camera_queue = None
        self.thread = None
        
        # 視窗參數設定
        self.window_name = "Camera"
        self.width = 320
        self.height = 240

    def stop(self):
        # 由擷取執行緒自己釋放 cap，避免跨執行緒 release 造成 OpenCV 當機
        self.running = False
        t = self.thread
        if t is not None and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=3.0)

    def _capture_loop(self):
        cv2.namedWindow(self.window_name, cv2.WINDOW_NORMAL)
        cv2.setWindowProperty(self.window_name, cv2.WND_PROP_TOPMOST, 1)
        cv2.resizeWindow(self.window_name, self.width, self.height)
        
        # --- 新增：透過 Windows API 停用最大化與關閉按鈕 ---
        cv2.waitKey(1)  # 確保 OpenCV 視窗已於系統中建立，否則抓不到 Handle
        try:
            user32 = ctypes.windll.user32
            hwnd = user32.FindWindowW(None, self.window_name)
            if hwnd:
                # 1. 停用最大化按鈕
                GWL_STYLE = -16
                WS_MAXIMIZEBOX = 0x00010000
                style = user32.GetWindowLongW(hwnd, GWL_STYLE)
                user32.SetWindowLongW(hwnd, GWL_STYLE, style & ~WS_MAXIMIZEBOX)
                
                # 2. 停用關閉按鈕
                hmenu = user32.GetSystemMenu(hwnd, False)
                if hmenu:
                    SC_CLOSE = 0xF060
                    MF_GRAYED = 0x00000001
                    user32.EnableMenuItem(hmenu, SC_CLOSE, MF_GRAYED)
        except Exception as e:
            logger.warning(f"[Camera] 視窗樣式修改失敗：{e}")
        # ----------------------------------------------------
        
        # 動態取得螢幕工作區高度（排除 Windows 工作列），並推算 Y 座標
        try:
            rect = wintypes.RECT()
            # 48 對應 SPI_GETWORKAREA，取得不含工作列的桌面範圍
            ctypes.windll.user32.SystemParametersInfoW(48, 0, ctypes.byref(rect), 0)
            work_area_height = rect.bottom
            # 扣除視窗本身高度與上方標題列的緩衝（約 40 px）
            y_pos = work_area_height - self.height - 40
        except Exception:
            y_pos = 700  # API 呼叫失敗時的備用 Y 座標

        cv2.moveWindow(self.window_name, 0, y_pos)

        current_conc = 0.0

        _com_init()   # 此執行緒要用 FilterGraph，必須先初始化 COM
        graph = FilterGraph()
        initial_devices = graph.get_input_devices()
        target_device_name = None
        if self.camera_index < len(initial_devices):
            target_device_name = initial_devices[self.camera_index]

        while self.running:
            # 1. 斷線重連邏輯（輪詢偵測）
            if not self.is_connected:
                graph = FilterGraph()
                current_devices = graph.get_input_devices()
                target_idx = -1
                if target_device_name and (target_device_name in current_devices):
                    target_idx = current_devices.index(target_device_name)
                elif not target_device_name and (self.camera_index < len(current_devices)):
                    target_idx = self.camera_index
                    target_device_name = current_devices[target_idx]

                if target_idx != -1:
                    if self.cap is not None:
                        self.cap.release()

                    self.cap = cv2.VideoCapture(target_idx, cv2.CAP_DSHOW)   # 用剛才比對出的索引開啟，鏡頭重插後順序變了也不會開錯
                    if self.cap.isOpened():
                        ret, test_frame = self.cap.read()
                        if ret:
                            self.is_connected = True
                            self.camera_index = target_idx
                        else:
                            self.cap.release()
                            self.cap = None
                else:
                    pass

            # 2. 畫面讀取邏輯
            if self.is_connected and self.cap is not None:
                ret, frame = self.cap.read()
                if not ret:
                    logger.warning("[Camera] 鏡頭連線中斷")
                    self.is_connected = False
                    self.cap.release()
                    self.cap = None
                    continue
            else:
                # 裝置未連線時，產生全黑畫面並繪製置中提示文字
                frame = np.zeros((self.height, self.width, 3), dtype=np.uint8)
                text = "Device Disconnected"
                font = cv2.FONT_HERSHEY_SIMPLEX
                font_scale = 0.6
                thickness = 2
                
                # 動態計算文字的長寬，以達成完美置中
                text_size = cv2.getTextSize(text, font, font_scale, thickness)[0]
                text_x = (self.width - text_size[0]) // 2
                text_y = (self.height + text_size[1]) // 2
                
                cv2.putText(frame, text, (text_x, text_y), 
                            font, font_scale, (0, 0, 255), thickness)
                time.sleep(1)  # 斷線時每秒重試一次

            # 3. 處理佇列與濃度觸發邏輯
            if self.camera_queue is not None:
                try:
                    while not self.camera_queue.empty():
                        current_conc = self.camera_queue.get_nowait()
                except Empty:
                    pass

                if self.threshold is not None and current_conc >= self.threshold and self.is_connected:
                    now = time.time()
                    if (now - self.last_photo_time) > self.cooldown:
                        timestamp_str = time.strftime("%Y%m%d_%H%M%S")
                        filename = os.path.join(self.save_dir, f"alert_{timestamp_str}.jpg")
                        success = cv2.imwrite(filename, frame)
                        if success:
                            self.last_photo_time = now
                            logger.info(f"[Camera] 濃度超標，已儲存：{filename}")
                        else:
                            logger.error(f"[Camera] 濃度超標，但儲存失敗：{filename}")

            cv2.imshow(self.window_name, frame)
            cv2.waitKey(30)

        if self.cap is not None:
            self.cap.release()
        cv2.destroyAllWindows()

    def run(self):
        self.running = True
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir, exist_ok=True)
        self.thread = threading.Thread(target=self._capture_loop, daemon=True)
        self.thread.start()

# 模組獨立執行區塊
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="獨立測試相機模組")
    parser.add_argument("--index", type=int, default=1,
                        help="鏡頭硬體索引值，0 通常為筆電內建，1 為外接 USB 鏡頭")
    args = parser.parse_args()

    logger.info(f"啟動相機模組獨立測試，持續偵測鏡頭索引：{args.index}")
    logger.info("測試方式：請嘗試拔除並重新插上 USB 鏡頭。")
    
    cam = CameraReader(camera_index=args.index)
    cam.run()