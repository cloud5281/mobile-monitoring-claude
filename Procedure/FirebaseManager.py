import firebase_admin
import logging
import queue
import time
from collections import deque
from firebase_admin import credentials, db

logger = logging.getLogger(__name__)

class FirebaseManager:
    BATCH_SIZE = 50        # 單次請求最多寫入幾筆（網路恢復後補傳用）
    MAX_PENDING = 5000     # 斷網時最多暫存幾筆，超過會丟棄最舊的（本機 CSV 備份仍完整）

    def __init__(self, key_path, db_url):
        self.key_path = key_path
        self.db_url = db_url
        self.project_name = None
        self.data_queue = None
        self.running = False
        self._seq = 0
        self._initialize_firebase()

    def _initialize_firebase(self):
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(self.key_path)
                firebase_admin.initialize_app(cred, {'databaseURL': self.db_url})
                logger.info("🔥 Firebase 初始化: 同步管理已就緒")
        except Exception as e:
            logger.error(f"❌ Firebase 連線失敗: {e}")
            raise   # 初始化失敗就不要繼續

    def _update_status(self, ref_status, state, message=""):
        try:
            ref_status.update({'state': state, 'message': message})
        except Exception as e:
            logger.error(f"狀態更新失敗: {e}")

    def stop(self):
        self.running = False
        if self.data_queue: self.data_queue.put(None)

    @staticmethod
    def _state_from(d_status):
        if d_status == 'Connecting':
            return 'connecting', '等待連線中...'
        if d_status == 'Conc Lost':
            return 'conc_lost', 'Conc 連線失敗'
        if d_status in ('GPS Lost', 'V'):
            return 'gps_lost', 'GPS 連線失敗'
        if d_status == 'All Lost':
            return 'all_lost', 'GPS 和 Conc 皆連線失敗'
        return 'active', '連線成功'

    def _next_key(self):
        """依時間遞增的 key：補傳時維持順序，且重送同一筆不會產生重複資料"""
        self._seq = (self._seq + 1) % 1000000
        return f"p{int(time.time() * 1000):013d}_{self._seq:06d}"

    def _try_flush(self, root, pending):
        """把暫存資料以「單一請求」寫入 (history + latest + status)。失敗則保留，下一輪重試。"""
        last_warn = 0.0
        while pending:
            n = min(len(pending), self.BATCH_SIZE)
            batch = [pending[i] for i in range(n)]
            updates = {}
            latest = None
            for key, d in batch:
                updates[f'history/{key}'] = d
                if d.get('status') == 'A':
                    latest = d
            if latest is not None:
                updates['latest'] = latest
            state, msg = self._state_from(batch[-1][1].get('status'))
            updates['status/state'] = state
            updates['status/message'] = msg

            try:
                root.update(updates)
            except Exception as fb_err:
                if time.time() - last_warn > 10:
                    logger.warning(f"⚠️ Firebase 連線中斷（本機備份仍在進行，待傳 {len(pending)} 筆）: {fb_err}")
                    last_warn = time.time()
                time.sleep(0.5)
                return False

            for _ in range(n):
                pending.popleft()
            for _, d in batch:
                lat, lon = d.get('lat'), d.get('lon')
                coord_str = f"({lat:.6f}, {lon:.6f})" if (lat is not None and lon is not None) else "(No GPS)"
                logger.info(f"座標: {coord_str} || 濃度: {d.get('conc', 'N/A')} {d.get('conc_unit', '')} （狀態: {msg}）")
        return True

    def run(self):
        self.running = True
        root = db.reference(f'{self.project_name}')
        ref_status = db.reference(f'{self.project_name}/status')
        logger.info(f"🚀 開始同步 Firebase ...")

        pending = deque(maxlen=self.MAX_PENDING)
        dropped_total = 0
        last_drop_warn = 0.0
        last_data_receive_time = time.time()
        grace_period = 2.0
        exit_state = 'offline'
        exit_msg = '程式已停止運作'

        try:
            while self.running:
                stop_signal = False
                try:
                    items = [self.data_queue.get(timeout=1)]
                    # 把佇列內已累積的資料一併取出，減少請求次數
                    while len(items) < self.BATCH_SIZE:
                        try:
                            items.append(self.data_queue.get_nowait())
                        except queue.Empty:
                            break
                except queue.Empty:
                    items = []
                    if not pending and time.time() - last_data_receive_time >= grace_period:
                        try:
                            ref_status.update({'state': 'connecting', 'message': '等待訊號...'})
                        except Exception:
                            pass

                for d in items:
                    if d is None:
                        stop_signal = True
                        break
                    last_data_receive_time = time.time()
                    if len(pending) == pending.maxlen:
                        dropped_total += 1   # 暫存已滿，加入新資料會擠掉最舊的一筆
                    pending.append((self._next_key(), d))

                if dropped_total and time.time() - last_drop_warn > 10:
                    logger.warning(f"⚠️ 暫存已滿 ({self.MAX_PENDING} 筆)，累計丟棄最舊資料 {dropped_total} 筆"
                                   f"（本機 CSV 備份仍完整）")
                    last_drop_warn = time.time()
                if pending:
                    self._try_flush(root, pending)

                if stop_signal:
                    if self.running:
                        exit_state = 'timeout'
                        exit_msg = '程式逾時停止，請重新開始'
                    else:
                        exit_state = 'manual_stop'
                        exit_msg = '程式已手動停止'
                    break
        except Exception as e:
            exit_state = 'error'
            exit_msg = f'程式錯誤: {str(e)}'
            logger.error(f"❌ 錯誤: {e}")
        finally:
            if exit_state != 'manual_stop':
                try:
                    self._update_status(ref_status, exit_state, exit_msg)
                except Exception:
                    pass
            logger.info(f"🏁 服務停止，原因: {exit_state}")
