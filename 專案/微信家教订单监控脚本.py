# -*- coding: utf-8 -*-
"""
微信家教订单监控脚本
====================
功能：
  - 轮询本地微信接口，获取指定群聊的新消息。
  - 解析消息中的科目、年级、报价、地址等字段。
  - 通过高德/腾讯双地图进行地理编码与驾车时间计算。
  - 符合条件（科目、年级、报价、通勤时间）的订单推送到飞书。
  - 支持自动停止、状态保存、异常通知。

环境变量（推荐）：
  FEISHU_WEBHOOK  飞书机器人 Webhook 地址
  AMAP_KEY        高德地图 API Key
  TENCENT_KEY     腾讯地图 API Key
  WETRACE_API_BASE WetraceTool 本地接口地址，例如 http://127.0.0.1:5200
  HOME_ADDRESS    家庭住址（用于计算通勤）
  TALKER_IDS      监控的群聊 ID，逗号分隔，例如 wxid_xxx,123456@chatroom

若不想用环境变量，可直接修改下方“配置区”的占位符。
"""

import requests
import time
import os
import re
import sys
import traceback
import json
import threading
import queue
import tempfile
from datetime import datetime, timedelta
from pyproj import Geod
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# 日志模块：同时输出到控制台和文件
# ============================================================
class Logger:
    def __init__(self, filename="monitor.log"):
        self.file = open(filename, "a", encoding="utf-8")
        self.original_stdout = sys.__stdout__  # 保存原始 stdout，避免递归

    def write(self, msg):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{timestamp}] {msg}"
        self.original_stdout.write(line + "\n")
        self.original_stdout.flush()
        self.file.write(line + "\n")
        self.file.flush()

    def flush(self):
        self.file.flush()

sys.stdout = Logger()
sys.stderr = sys.stdout

# ============================================================
# 配置区（请根据实际情况修改，或通过环境变量注入）
# ============================================================
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "YOUR_FEISHU_WEBHOOK")
AMAP_KEY = os.getenv("AMAP_KEY", "YOUR_AMAP_KEY")
TENCENT_KEY = os.getenv("TENCENT_KEY", "YOUR_TENCENT_KEY")
API_BASE = os.getenv("WETRACE_API_BASE", "http://127.0.0.1:5200")

# 监控的群聊 ID 列表，从环境变量读取（逗号分隔），或直接在此填写
_talker_env = os.getenv("TALKER_IDS", "")
TALKER_IDS = [x.strip() for x in _talker_env.split(",") if x.strip()] if _talker_env else []

HOME_ADDRESS = os.getenv("HOME_ADDRESS", "YOUR_HOME_ADDRESS")

CHECK_INTERVAL = 1.0          # 主循环轮询间隔（秒）
SYNC_INTERVAL = 10            # 空闲时同步间隔（秒）
ACTIVE_SYNC_INTERVAL = 1      # 有新消息时的同步间隔（秒）
SYNC_RETRIES = 2              # 同步失败重试次数
SYNC_RETRY_DELAY = 0.8        # 重试延迟（秒）
ERROR_NOTIFY_INTERVAL = 60    # 同类错误通知最小间隔（秒）

STRAIGHT_DIST_THRESHOLD = 20  # 直线距离阈值（公里），超过则直接拒绝
ULTRA_NEAR_DIST = 5           # 超近单阈值（公里），低于此值直接通过
MAX_DRIVING_TIME = 25         # 最大驾车时间（分钟）

# 地理位置无法确认时是否推送（False 表示不推送，避免误报）
ALLOW_UNKNOWN_LOCATION = False

# 地址黑名单关键词（命中即拒绝）
BLACKLIST_KEYWORDS = ["直接遇到即不测距的过远关键词，以免浪费API"]

# WetraceTool 接口路径
DECRYPT_URL = f"{API_BASE}/system/decrypt?format=json"   # 解密/同步
COMPLIANCE_URL = f"{API_BASE}/system/compliance"         # 合规刷新
MESSAGES_URL = f"{API_BASE}/messages"                    # 获取聊天记录

# ============================================================
# 缓存文件
# ============================================================
SEQ_FILE = "last_seq_dict.json"       # 每个群的 last_seq
CACHE_FILE = "dist_cache.json"        # 地理编码缓存
DRIVING_CACHE_FILE = "driving_cache.json"  # 驾车时间缓存

AMAP_BATCH_SIZE = 10   # 高德批量地理编码并发数
AMAP_INTERVAL = 0.3    # 高德请求最小间隔（秒）
TENCENT_INTERVAL = 0.2 # 腾讯请求最小间隔（秒）

geod = Geod(ellps='WGS84')  # 用于计算直线距离
DIST_CACHE = {}             # 内存中的地理缓存
DRIVING_CACHE = {}          # 内存中的驾车缓存
_cache_lock = threading.Lock()
_driving_lock = threading.Lock()
_file_lock = threading.Lock()
_last_error_time = {}
_thread_local = threading.local()

# 允许的科目（可自行增删）
SUBJECT_TOKENS = ("xx", "xx")   # 请替换为实际科目，例如 ("数学", "英语", "物理")
# 禁止的科目，格式：(关键词, 显示名称)
FORBIDDEN_SUBJECT_TOKENS = (
    ("xx", "xx"),
    ("xx", "xx"),
)

# ============================================================
# 限流器：保证对同一地图服务的请求间隔
# ============================================================
class ApiRateLimiter:
    """简单的令牌桶限流，确保请求之间至少间隔 interval 秒。"""
    def __init__(self, interval=1.05):
        self.interval = interval
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            wait_for = max(0.0, self._next_at - now)
            self._next_at = max(now, self._next_at) + self.interval
        if wait_for:
            time.sleep(wait_for)

# 高德、腾讯各自限流
AMAP_LIMITER = ApiRateLimiter(1.05)
TENCENT_LIMITER = ApiRateLimiter(1.05)

# ============================================================
# 工具函数
# ============================================================
def get_session():
    """获取线程独立的 requests.Session，复用连接。"""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.timeout = (3.05, 10)
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=2
        )
        _thread_local.session.mount('http://', adapter)
        _thread_local.session.mount('https://', adapter)
    return _thread_local.session

def log(msg):
    """简易日志，自动加时间戳。"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def load_cache_file(file):
    """读取 JSON 缓存文件，不存在则返回空字典。"""
    if os.path.exists(file):
        with open(file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache_file(file, data):
    """原子写入 JSON 缓存，避免写入中断导致文件损坏。"""
    directory = os.path.dirname(os.path.abspath(file)) or "."
    fd, temp_file = tempfile.mkstemp(prefix=".cache-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        with _file_lock:
            os.replace(temp_file, file)
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)

def load_last_seq_dict():
    return load_cache_file(SEQ_FILE)

def save_last_seq_dict(data):
    save_cache_file(SEQ_FILE, data)

def load_dist_cache():
    return load_cache_file(CACHE_FILE)

def save_dist_cache(data):
    save_cache_file(CACHE_FILE, data)

def load_driving_cache():
    return load_cache_file(DRIVING_CACHE_FILE)

def save_driving_cache(data):
    save_cache_file(DRIVING_CACHE_FILE, data)

def save_runtime_state(last_seq_dict):
    """程序退出前保存所有可恢复状态。"""
    try:
        save_last_seq_dict(last_seq_dict)
        save_dist_cache(DIST_CACHE)
        save_driving_cache(DRIVING_CACHE)
    except Exception as e:
        log(f" 保存状态失败: {e}")

def parse_message_datetime(value):
    """解析多种格式的时间戳：Unix 秒/毫秒、ISO 字符串、常见日期格式。"""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 100_000_000_000:  # 毫秒
            timestamp /= 1000
        try:
            return datetime.fromtimestamp(timestamp)
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(text)
        return parse_message_datetime(numeric)
    except ValueError:
        pass

    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is not None:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed
    except ValueError:
        for fmt in (
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M",
            "%Y年%m月%d日 %H:%M:%S", "%Y年%m月%d日 %H:%M"
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
    return None

def get_today_window():
    """返回今天 00:00、明天 00:00 和当前时间。"""
    now = datetime.now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return today, today + timedelta(days=1), now

def get_message_datetime(msg):
    """从消息字典中提取时间，按常见字段顺序尝试。"""
    for key in ("time", "timestamp", "create_time", "createTime"):
        parsed = parse_message_datetime(msg.get(key))
        if parsed is not None:
            return parsed
    # 后备：接口的 seq 可能是毫秒时间戳
    return parse_message_datetime(msg.get("seq"))

def parse_message_seq(value):
    """将 seq 转为整数，失败返回 None。"""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def prepare_last_seq_dict(raw_state):
    """
    清理旧游标：只保留今天的 seq。
    返回 (清理后的字典, 是否有变更)
    """
    raw_state = raw_state if isinstance(raw_state, dict) else {}
    today_start, tomorrow_start, now = get_today_window()
    prepared = {}
    changed = False

    for talker_id in TALKER_IDS:
        original = raw_state.get(talker_id, 0)
        seq = parse_message_seq(original) or 0
        seq_time = parse_message_datetime(seq) if seq else None
        if seq and (
            seq_time is None
            or seq_time < today_start
            or seq_time >= tomorrow_start
            or seq_time > now
        ):
            log(f" {talker_id} 的旧游标不属于今天，已从 {seq} 调整为 0")
            seq = 0
        prepared[talker_id] = seq
        if original != seq:
            changed = True

    if set(raw_state) != set(prepared):
        changed = True
    return prepared, changed

def notify_error(error_msg, is_fatal=False, category="系统", impact="", action=""):
    """
    发送错误通知到飞书，并写入日志。
    同一类错误在 ERROR_NOTIFY_INTERVAL 秒内不会重复推送。
    """
    global _last_error_time
    now = time.time()
    error_msg = str(error_msg)
    title = f"【监控异常｜{category}】"
    detail_lines = [
        title,
        f"时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"错误：{error_msg}",
    ]
    if impact:
        detail_lines.append(f"影响：{impact}")
    if action:
        detail_lines.append(f"处理：{action}")
    detail_text = "\n".join(detail_lines)

    if not is_fatal:
        key = f"{category}:{error_msg[:160]}"
        if key in _last_error_time and (now - _last_error_time[key]) < ERROR_NOTIFY_INTERVAL:
            return
        _last_error_time[key] = now

    log(f" {detail_text.replace(chr(10), ' | ')}")
    payload = {
        "msg_type": "text",
        "content": {
            "text": ("【严重】\n" if is_fatal else "") + detail_text
        }
    }
    try:
        session = get_session()
        session.post(FEISHU_WEBHOOK, json=payload, timeout=5)
    except:
        pass

def push_to_feishu(text):
    """推送文本到飞书，返回是否成功。"""
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        session = get_session()
        resp = session.post(FEISHU_WEBHOOK, json=payload, timeout=5)
        if resp.status_code == 200 and resp.json().get("code") == 0:
            log(" 飞书推送成功")
            return True
        else:
            log(f" 飞书推送失败: {resp.text}")
            notify_error(
                f"飞书接口HTTP {resp.status_code}：{resp.text[:300]}",
                category="飞书推送",
                impact="当前匹配单没有确认送达，消息游标会保留并重试。",
                action="下一轮自动重试；请检查 webhook 是否有效。",
            )
            return False
    except Exception as e:
        log(f" 飞书推送异常: {e}")
        notify_error(
            f"{type(e).__name__}: {e}",
            category="飞书推送",
            impact="当前匹配单没有确认送达，消息游标会保留并重试。",
            action="下一轮自动重试；请检查网络和 webhook。",
        )
        return False

def sync_data():
    """
    调用 WetraceTool 的解密和合规接口刷新本地数据。
    返回 True 表示成功，False 表示失败。
    """
    last_error = None
    for attempt in range(1, SYNC_RETRIES + 1):
        try:
            session = get_session()
            decrypt_resp = session.post(DECRYPT_URL, timeout=8)
            if decrypt_resp.status_code != 200:
                last_error = f"解密接口HTTP {decrypt_resp.status_code}：{decrypt_resp.text[:200]}"
            else:
                try:
                    decrypt_data = decrypt_resp.json()
                except ValueError:
                    decrypt_data = {}
                if isinstance(decrypt_data, dict) and decrypt_data.get("success") is False:
                    last_error = f"解密接口返回失败：{decrypt_data}"
                else:
                    params = {
                        "limit": 200,
                        "offset": 0,
                        "_t": int(time.time() * 1000),
                        "format": "json",
                    }
                    compliance_resp = session.get(COMPLIANCE_URL, params=params, timeout=8)
                    if compliance_resp.status_code == 200:
                        return True
                    last_error = (
                        f"合规同步接口HTTP {compliance_resp.status_code}："
                        f"{compliance_resp.text[:200]}"
                    )
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"

        if attempt < SYNC_RETRIES:
            time.sleep(SYNC_RETRY_DELAY)

    notify_error(
        f"同步失败（重试{SYNC_RETRIES}次）：{last_error or '未知错误'}",
        category="数据同步",
        impact="本轮可能拿不到最新微信消息；不会因同步失败而推进消息游标。",
        action="继续重试；请确认微信和 WetraceTool 的本地接口仍在运行。",
    )
    return False

def fetch_all_messages(limit=1000):
    """
    拉取所有监控群今天的消息。
    返回消息列表；若任一 group 拉取失败，返回 None（表示本轮数据不完整）。
    """
    all_msgs = []
    session = get_session()
    today_start, tomorrow_start, now = get_today_window()
    today_text = today_start.strftime("%Y-%m-%d")
    tomorrow_text = tomorrow_start.strftime("%Y-%m-%d")
    failed_talkers = []

    for talker_id in TALKER_IDS:
        try:
            offset = 0
            # 最多分页 20 次，防止无限循环
            for _ in range(20):
                params = {
                    "limit": limit,
                    "offset": offset,
                    "talker_id": talker_id,
                    "time": f"{today_text}~{tomorrow_text}",
                    "bottom": 0,
                    "format": "json",
                    "_t": int(time.time()*1000)
                }
                resp = session.get(MESSAGES_URL, params=params, timeout=10)
                if resp.status_code != 200:
                    failed_talkers.append(talker_id)
                    notify_error(
                        f"消息接口HTTP {resp.status_code}：{resp.text[:200]}",
                        category="消息拉取",
                        impact=f"监控对象 {talker_id} 本轮消息未确认，不能推进该对象的 last_seq。",
                        action="保留当前游标，下一轮自动重试。",
                    )
                    break

                data = resp.json()
                msg_list = data.get("data", []) if isinstance(data, dict) else []
                if not isinstance(msg_list, list):
                    failed_talkers.append(talker_id)
                    notify_error(
                        f"消息接口返回格式异常：data 不是列表（实际为 {type(msg_list).__name__}）",
                        category="消息拉取",
                        impact=f"监控对象 {talker_id} 本轮消息未确认。",
                        action="保留当前游标，下一轮自动重试。",
                    )
                    break

                for msg in msg_list:
                    if isinstance(msg, dict):
                        message_time = get_message_datetime(msg)
                        # 只保留今天且不晚于当前时间的消息
                        if message_time is None or not (today_start <= message_time < tomorrow_start and message_time <= now):
                            continue
                        msg["talker"] = talker_id
                        all_msgs.append(msg)

                if len(msg_list) < limit:
                    break
                offset += len(msg_list)

        except Exception as e:
            failed_talkers.append(talker_id)
            notify_error(
                f"拉取 {talker_id} 异常：{type(e).__name__}: {e}",
                category="消息拉取",
                impact=f"监控对象 {talker_id} 本轮消息未确认，不能推进 last_seq。",
                action="保留当前游标，下一轮自动重试；请检查本地接口。",
            )

    if failed_talkers:
        return None
    return all_msgs

def clean_address(raw):
    """清理地址字符串：去掉括号内容、常见修饰词，并统一加上“天津市”前缀。"""
    if not raw:
        return ""
    addr = raw.strip().replace('\n', '').replace('\r', '').replace('\t', '')
    addr = re.sub(r'[（(][^）)]*[）)]', '', addr)
    for word in ["地铁旁", "附近", "旁", "小区", "内", "里面", "门口", "对面"]:
        addr = addr.replace(word, "")
    addr = re.sub(r'[，,、;；]', ' ', addr)
    addr = " ".join(addr.split())
    if not addr.startswith("天津市"):
        addr = "天津市" + addr
    return addr

def parse_field(text, field_name):
    """从消息文本中解析形如【字段名】的值。"""
    pattern = rf'【{field_name}】[:：]?\s*(.*?)(?=\n【|$)'
    match = re.search(pattern, text, re.S)
    return match.group(1).strip() if match else None

def canonicalize_subject(subject):
    """
    规范化科目字符串：
    - 去掉数量词（如“数学一位”）
    - 只保留允许的科目 token
    - 返回统一格式，如“数学、英语”
    若包含不允许的科目，返回 None。
    """
    if not subject:
        return None

    text = str(subject).strip()
    count_number = r"(?:\d+|[零〇一二三四五六七八九十百两]+)"
    count_unit = r"(?:位|名|人|科)"
    text = re.sub(
        rf"[（(]\s*(?:各|每|共)?\s*{count_number}\s*{count_unit}\s*[）)]",
        "", text,
    )
    text = re.sub(rf"(?:各|每|共)?\s*{count_number}\s*{count_unit}", "", text)
    text = re.sub(r"(?:一对一|一对多|1对1)", "", text)
    text = re.sub(r"[\s、,，；;：:/／+&和及与跟以及|]+", "", text)
    if not text:
        return None

    tokens = []
    while text:
        token = next((item for item in SUBJECT_TOKENS if text.startswith(item)), None)
        if token is None:
            return None
        tokens.append(token)
        text = text[len(token):]

    # 简写展开
    expanded = {"数": "数学", "英": "英语", "生": "生物"}
    normalized = [expanded.get(token, token) for token in tokens]
    return "、".join(normalized)

def parse_subject(text):
    """解析【补习科目】字段，返回规范化后的科目或原始值。"""
    raw_subject = parse_field(text, "补习科目")
    canonical = canonicalize_subject(raw_subject)
    if canonical is not None:
        return canonical
    return raw_subject

def explain_subject_rejection(subject):
    """如果科目被拒绝，返回具体命中的禁用科目，方便日志排查。"""
    if not subject:
        return ""
    text = str(subject).strip()
    count_number = r"(?:\d+|[零〇一二三四五六七八九十百两]+)"
    count_unit = r"(?:位|名|人|科)"
    text = re.sub(
        rf"[（(]\s*(?:各|每|共)?\s*{count_number}\s*{count_unit}\s*[）)]",
        "", text,
    )
    text = re.sub(rf"(?:各|每|共)?\s*{count_number}\s*{count_unit}", "", text)
    text = re.sub(r"(?:一对一|一对多|1对1)", "", text)
    text = re.sub(r"[\s、,，；;：:/／+&和及与跟以及|]+", "", text)

    forbidden = []
    while text:
        allowed = next((item for item in SUBJECT_TOKENS if text.startswith(item)), None)
        if allowed:
            text = text[len(allowed):]
            continue
        forbidden_item = next(
            (item for item, label in FORBIDDEN_SUBJECT_TOKENS if text.startswith(item)),
            None,
        )
        if forbidden_item:
            label = next(label for item, label in FORBIDDEN_SUBJECT_TOKENS if item == forbidden_item)
            if label not in forbidden:
                forbidden.append(label)
            text = text[len(forbidden_item):]
            continue
        text = text[1:]

    return f"；包含不允许科目：{'、'.join(forbidden)}" if forbidden else ""

def parse_price(text):
    """解析【报价】字段，返回最大整数价格。"""
    match = re.search(r'【报价】[:：]?\s*(.*?)(?=\n【|$)', text, re.S)
    if match:
        price_line = match.group(1).strip()
        nums = re.findall(r'(\d+)', price_line)
        return max(int(n) for n in nums) if nums else None
    return None

def parse_address(text):
    return parse_field(text, "地址")

def parse_teacher_requirement(text):
    return parse_field(text, "对老师要求")

def parse_grade(text):
    return parse_field(text, "学生年级")

def is_valid_subject(subject):
    return canonicalize_subject(subject) is not None

def is_valid_grade(grade):
    """判断年级是否符合要求：高中可以，初中/小学不行。"""
    if not grade:
        return True
    if "高" in grade and "初" not in grade and "小" not in grade:
        return True
    if "初" in grade or "小" in grade:
        return False
    nums = re.findall(r'[一二三四五六七八九]', grade)
    for n in nums:
        if n in "一二三四五六":
            return False
        if n in "七八九":
            return False
    return True

# ============================================================
# 地理编码与驾车相关
# ============================================================

def search_poi(keywords, city="天津市"):
    """使用高德关键字搜索获取 POI 坐标，返回 (lon, lat, name, address, confidence)。"""
    url = "https://restapi.amap.com/v3/place/text"
    params = {
        "key": AMAP_KEY,
        "keywords": keywords,
        "city": city,
        "offset": 1,
        "extensions": "base"
    }
    try:
        AMAP_LIMITER.wait()
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1" and data.get("pois"):
                poi = data["pois"][0]
                location = poi["location"]
                lon, lat = map(float, location.split(','))
                name = poi.get("name", "")
                address = poi.get("address", "")
                confidence = "高"
                log(f"  POI搜索成功: {keywords} -> {lon},{lat} (名称: {name})")
                return lon, lat, confidence
    except Exception as e:
        log(f"POI搜索异常: {e}")
    return None, None, None

def get_confidence_from_amap(level):
    if level in ["门牌号", "兴趣点"]:
        return "高"
    elif level in ["街道", "区县"]:
        return "中"
    else:
        return "低"

def get_confidence_from_tencent(reliability):
    if reliability >= 9:
        return "高"
    elif reliability >= 7:
        return "中"
    else:
        return "低"

def get_driving_distance(origin_lon, origin_lat, dest_lon, dest_lat):
    """高德驾车距离（公里），失败返回 None。"""
    AMAP_LIMITER.wait()
    url = "https://restapi.amap.com/v3/distance"
    params = {
        "origins": f"{origin_lon},{origin_lat}",
        "destination": f"{dest_lon},{dest_lat}",
        "type": 1,
        "key": AMAP_KEY,
        "output": "json"
    }
    try:
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1" and data.get("results"):
                distance_m = int(data["results"][0]["distance"])
                return distance_m / 1000.0
    except Exception as e:
        log(f"驾车距离API异常: {e}")
    return None

def amap_batch_geocode(batch, home_lon, home_lat):
    """高德批量地理编码，结果写入 DIST_CACHE。返回成功地址列表。"""
    cleaned_batch = [clean_address(addr) for addr in batch]
    addr_str = "|".join(cleaned_batch)
    url = "https://restapi.amap.com/v3/geocode/geo"
    params = {
        "address": addr_str,
        "city": "天津市",
        "key": AMAP_KEY,
        "output": "json"
    }
    success = []
    try:
        AMAP_LIMITER.wait()
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1":
                geocodes = data.get("geocodes", [])
                for idx, geocode in enumerate(geocodes):
                    if idx < len(batch):
                        addr = batch[idx]
                        loc = geocode.get("location")
                        if loc:
                            lon, lat = map(float, loc.split(','))
                            level = geocode.get("level", "")
                            confidence = get_confidence_from_amap(level)
                            if confidence in ("中", "低"):
                                poi_lon, poi_lat, poi_conf = search_poi(addr, "天津市")
                                if poi_lon is not None:
                                    lon, lat = poi_lon, poi_lat
                                    confidence = poi_conf
                                    log(f"  POI补充坐标: {addr} -> {lon},{lat}")
                            dist = straight_distance(home_lon, home_lat, lon, lat)
                            with _cache_lock:
                                DIST_CACHE[addr] = (dist, lon, lat, confidence)
                            success.append(addr)
                            log(f"  高德成功: {addr} -> {lon},{lat} (精度: {level}，可信度: {confidence})")
                        else:
                            log(f"  高德无坐标: {addr}")
            else:
                log(f"高德批量返回非成功: {data}")
        else:
            notify_error(f"高德批量HTTP错误: {resp.status_code}")
    except Exception as e:
        notify_error(f"高德批量异常: {e}")
    return success

def check_condition(content, home_lon, home_lat):
    """
    核心判断函数：解析消息并判断是否符合推送条件。
    返回 (passed, reason, subject, straight_dist, driving_time, confidence)
    """
    subject = parse_subject(content)
    grade = parse_grade(content)
    teacher_req = parse_teacher_requirement(content)
    price = parse_price(content)
    addr = parse_address(content)

    # 硬性条件
    if not subject or not is_valid_subject(subject):
        detail = explain_subject_rejection(subject)
        return False, f"科目不符（{subject or '无'}）{detail}", subject, None, None, None
    if grade and not is_valid_grade(grade):
        return False, f"年级不符（{grade}）", subject, None, None, None
    if teacher_req and "男" in teacher_req and "女" not in teacher_req and "不限" not in teacher_req and "男女" not in teacher_req:
        return False, f"老师仅限男（{teacher_req}）", subject, None, None, None
    if price is None or price < 140:
        return False, f"报价{price or '未找到'}<140", subject, None, None, None
    if not addr:
        return False, "未找到地址", subject, None, None, None
    if any(kw in addr for kw in BLACKLIST_KEYWORDS):
        return False, "地址含黑名单词", subject, None, None, None
    if any(kw in addr for kw in ["线上", "远程", "网课"]):
        return True, "线上/远程", subject, None, None, None

    # 地理编码
    cache_entry = DIST_CACHE.get(addr)
    if cache_entry is None:
        if ALLOW_UNKNOWN_LOCATION:
            return True, "位置未知（地理编码失败）", subject, None, None, None
        else:
            return False, "地理编码失败", subject, None, None, None

    dist, lon, lat, confidence = cache_entry
    if dist is None or lon is None or lat is None:
        if ALLOW_UNKNOWN_LOCATION:
            return True, "位置未知（坐标缺失）", subject, None, None, None
        else:
            return False, "坐标缺失", subject, None, None, None

    # 直线距离初筛
    if dist <= ULTRA_NEAR_DIST:
        return True, f"超近单({dist:.1f}km)", subject, dist, None, confidence

    if dist > STRAIGHT_DIST_THRESHOLD:
        return False, f"直线距离{dist:.1f}km>{STRAIGHT_DIST_THRESHOLD}", subject, dist, None, confidence

    # 中等距离，获取驾车信息
    driving_dist, driving_time = get_driving_info(addr, home_lon, home_lat, lon, lat)

    if driving_dist is None or driving_time is None:
        log(f" 驾车信息获取失败，回退到直线距离 {dist:.1f}km")
        driving_dist = dist
        driving_time = None
        confidence = "低（驾车信息获取失败）"

    # 异常检测：距离大于 5km 但驾车时间小于 2 分钟，数据明显不合理
    if driving_time is not None and driving_dist > 5 and driving_time < 2:
        log(f" 驾车数据异常: {driving_dist:.1f}km, {driving_time:.0f}分钟，拒绝")
        return False, "驾车数据异常", subject, driving_dist, driving_time, confidence

    if driving_time is not None and driving_time <= MAX_DRIVING_TIME:
        return True, f"直线{dist:.1f}km，驾车{driving_dist:.1f}km，约{driving_time:.0f}分钟", subject, driving_dist, driving_time, confidence
    else:
        return False, f"驾车时间{driving_time:.0f}分钟超时" if driving_time else "驾车时间获取失败", subject, driving_dist, driving_time, confidence

def search_poi_tencent(keywords, city="天津市"):
    """腾讯地点搜索，返回 (lon, lat, confidence)。"""
    url = "https://apis.map.qq.com/ws/place/v1/search"
    params = {
        "key": TENCENT_KEY,
        "keyword": keywords,
        "boundary": f"region({city},0)",
        "page_size": 1,
    }
    try:
        TENCENT_LIMITER.wait()
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == 0 and data.get("data"):
                poi = data["data"][0]
                location = poi.get("location")
                if location:
                    lon, lat = location["lng"], location["lat"]
                    return lon, lat, "高"
    except Exception as e:
        log(f"腾讯POI搜索异常: {e}")
    return None, None, None

def tencent_single_geocode(addr, home_lon, home_lat):
    """腾讯单地址地理编码，成功写入 DIST_CACHE，返回布尔值。"""
    if not TENCENT_KEY:
        return False

    cleaned = clean_address(addr)
    url = "https://apis.map.qq.com/ws/geocoder/v1/"
    params = {
        "address": cleaned,
        "key": TENCENT_KEY,
        "output": "json"
    }
    lon, lat, confidence = None, None, None

    try:
        TENCENT_LIMITER.wait()
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == 0:
                reliability = data["result"].get("reliability", 0)
                confidence = get_confidence_from_tencent(reliability)
                loc = data["result"]["location"]
                lon, lat = loc["lng"], loc["lat"]
                log(f"  腾讯地理编码成功: {addr} -> {lon},{lat} (可信度: {confidence})")
            elif data.get("status") == 121:
                pass  # 每日调用量超限
            else:
                log(f"  腾讯地理编码错误: {data}")
    except Exception as e:
        log(f"腾讯地理编码异常: {e}")

    if confidence in ("中", "低") or lon is None:
        log(f"  腾讯地理编码精度不足，尝试POI搜索: {addr}")
        poi_lon, poi_lat, poi_conf = search_poi_tencent(addr, "天津市")
        if poi_lon is not None:
            lon, lat = poi_lon, poi_lat
            confidence = poi_conf
            log(f"  腾讯POI补充坐标成功: {addr} -> {lon},{lat}")
        else:
            log(f"  腾讯POI搜索失败: {addr}")
            return False

    if lon is None:
        return False

    dist = straight_distance(home_lon, home_lat, lon, lat)
    with _cache_lock:
        DIST_CACHE[addr] = (dist, lon, lat, confidence)
    return True

def geocode_scheduler(addresses, home_lon, home_lat):
    """
    双服务并发地理编码调度器：
    高德批量 + 腾讯单条，失败互相转交。
    """
    if not addresses:
        return
    unique_addrs = list(set(addresses))
    with _cache_lock:
        need_geocode = [addr for addr in unique_addrs if addr not in DIST_CACHE]
    if not need_geocode:
        return

    log(f" 地址池双服务并发解析 {len(need_geocode)} 个地址...")

    pending = queue.Queue()
    for addr in need_geocode:
        pending.put((addr, None))

    state_lock = threading.Lock()
    attempts = {addr: {"amap": 0, "tencent": 0} for addr in need_geocode}
    finished = set()
    total_tasks = len(need_geocode)

    def claim_tasks(provider, batch_size):
        claimed = []
        deferred = []
        while len(claimed) < batch_size:
            try:
                addr, required_provider = pending.get_nowait()
            except queue.Empty:
                break
            if required_provider is None or required_provider == provider:
                claimed.append(addr)
            else:
                deferred.append((addr, required_provider))
        for item in deferred:
            pending.put(item)
        return claimed

    def complete(addr):
        with state_lock:
            finished.add(addr)

    def retry_or_finish(addr, provider):
        other = "tencent" if provider == "amap" else "amap"
        with state_lock:
            attempts[addr][provider] += 1
            if attempts[addr][other] == 0:
                pending.put((addr, other))
                return
            finished.add(addr)

    def all_finished():
        with state_lock:
            return len(finished) >= total_tasks

    def amap_worker():
        while not all_finished():
            batch = claim_tasks("amap", AMAP_BATCH_SIZE)
            if not batch:
                time.sleep(0.05)
                continue
            success_addrs = set(amap_batch_geocode(batch, home_lon, home_lat))
            for addr in batch:
                if addr in success_addrs:
                    complete(addr)
                else:
                    retry_or_finish(addr, "amap")

    def tencent_worker():
        while not all_finished():
            batch = claim_tasks("tencent", 1)
            if not batch:
                time.sleep(0.05)
                continue
            addr = batch[0]
            if tencent_single_geocode(addr, home_lon, home_lat):
                complete(addr)
            else:
                retry_or_finish(addr, "tencent")

    t1 = threading.Thread(target=amap_worker, name="amap-geocoder", daemon=True)
    t2 = threading.Thread(target=tencent_worker, name="tencent-geocoder", daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    log(f" 地址池解析完成：{len(finished)}/{total_tasks} 个地址已结束")

def straight_distance(lon1, lat1, lon2, lat2):
    """计算两点直线距离（公里）。"""
    az12, az21, dist_m = geod.inv(lon1, lat1, lon2, lat2)
    return dist_m / 1000.0

def get_driving_duration_amap(origin_lon, origin_lat, dest_lon, dest_lat):
    AMAP_LIMITER.wait()
    url = "https://restapi.amap.com/v3/direction/driving"
    params = {
        "origin": f"{origin_lon},{origin_lat}",
        "destination": f"{dest_lon},{dest_lat}",
        "key": AMAP_KEY,
        "output": "json",
        "extensions": "base"
    }
    try:
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1" and data.get("route", {}).get("paths"):
                seconds = int(data["route"]["paths"][0]["duration"])
                return seconds / 60.0
        else:
            notify_error(f"高德驾车API HTTP错误: {resp.status_code}")
            return None
    except Exception as e:
        notify_error(f"高德驾车异常: {e}")
        return None

def get_driving_duration_tencent(origin_lon, origin_lat, dest_lon, dest_lat):
    TENCENT_LIMITER.wait()
    url = "https://apis.map.qq.com/ws/direction/v1/driving/"
    params = {
        "from": f"{origin_lat},{origin_lon}",
        "to": f"{dest_lat},{dest_lon}",
        "key": TENCENT_KEY,
        "output": "json"
    }
    try:
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == 0:
                routes = data.get("result", {}).get("routes", [])
                if routes:
                    seconds = routes[0]["duration"]
                    return seconds / 60.0
        else:
            notify_error(f"腾讯驾车API HTTP错误: {resp.status_code}")
            return None
    except Exception as e:
        notify_error(f"腾讯驾车异常: {e}")
        return None

def amap_drive(origin_lon, origin_lat, dest_lon, dest_lat):
    """高德驾车路线规划，返回 (距离km, 时间min)。"""
    AMAP_LIMITER.wait()
    url = "https://restapi.amap.com/v3/direction/driving"
    params = {
        "origin": f"{origin_lon},{origin_lat}",
        "destination": f"{dest_lon},{dest_lat}",
        "key": AMAP_KEY,
        "output": "json",
        "extensions": "base"
    }
    try:
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1" and data.get("route", {}).get("paths"):
                path = data["route"]["paths"][0]
                distance_km = int(path["distance"]) / 1000.0
                time_min = int(path["duration"]) / 60.0
                return distance_km, time_min
    except Exception as e:
        log(f"高德驾车异常: {e}")
    return None, None

def tencent_drive(origin_lon, origin_lat, dest_lon, dest_lat):
    """腾讯驾车路线规划，返回 (距离km, 时间min)。"""
    TENCENT_LIMITER.wait()
    url = "https://apis.map.qq.com/ws/direction/v1/driving/"
    params = {
        "from": f"{origin_lat},{origin_lon}",
        "to": f"{dest_lat},{dest_lon}",
        "key": TENCENT_KEY,
        "output": "json"
    }
    try:
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == 0 and data.get("result", {}).get("routes"):
                route = data["result"]["routes"][0]
                distance_km = int(route["distance"]) / 1000.0
                time_min = int(route["duration"]) / 60.0
                return distance_km, time_min
    except Exception as e:
        log(f"腾讯驾车异常: {e}")
    return None, None

def get_driving_info(addr, home_lon, home_lat, lon, lat):
    """获取驾车信息，优先高德，失败转腾讯；结果缓存到 DRIVING_CACHE。"""
    with _driving_lock:
        cached = DRIVING_CACHE.get(addr)
        if isinstance(cached, dict):
            origin = cached.get("origin")
            destination = cached.get("destination")
            if origin == [home_lon, home_lat] and destination == [lon, lat]:
                return cached.get("distance_km"), cached.get("time_min")
        elif isinstance(cached, (list, tuple)) and len(cached) == 2:
            # 旧格式缓存，无法验证起终点，忽略并重新查询
            pass

    dist_km, time_min = amap_drive(home_lon, home_lat, lon, lat)
    if dist_km is not None:
        result = (dist_km, time_min)
        with _driving_lock:
            DRIVING_CACHE[addr] = {
                "origin": [home_lon, home_lat],
                "destination": [lon, lat],
                "distance_km": result[0],
                "time_min": result[1]
            }
            save_driving_cache(DRIVING_CACHE)
        log(f"  高德驾车: {dist_km:.1f}km, {time_min:.0f}分钟")
        return result

    log(f"  高德驾车失败，尝试腾讯...")
    dist_km, time_min = tencent_drive(home_lon, home_lat, lon, lat)
    if dist_km is not None:
        result = (dist_km, time_min)
        with _driving_lock:
            DRIVING_CACHE[addr] = {
                "origin": [home_lon, home_lat],
                "destination": [lon, lat],
                "distance_km": result[0],
                "time_min": result[1]
            }
            save_driving_cache(DRIVING_CACHE)
        log(f"  腾讯驾车: {dist_km:.1f}km, {time_min:.0f}分钟")
        return result

    log(f"  驾车信息获取失败: {addr}")
    return None, None

def driving_scheduler(addresses, home_lon, home_lat):
    """驾车地址池调度器：高德/腾讯并行，失败转交。"""
    if not addresses:
        return

    unique_addrs = list(dict.fromkeys(addresses))
    need_driving = []
    with _driving_lock:
        for addr in unique_addrs:
            cached = DRIVING_CACHE.get(addr)
            entry = DIST_CACHE.get(addr)
            if not entry or len(entry) < 4:
                continue
            _, lon, lat, _ = entry
            cache_valid = (
                isinstance(cached, dict)
                and cached.get("origin") == [home_lon, home_lat]
                and cached.get("destination") == [lon, lat]
                and cached.get("distance_km") is not None
                and cached.get("time_min") is not None
            )
            if not cache_valid:
                need_driving.append((addr, lon, lat))

    if not need_driving:
        return

    log(f" 驾车地址池并发解析 {len(need_driving)} 个地址...")
    pending = queue.Queue()
    for item in need_driving:
        pending.put((item, None))

    state_lock = threading.Lock()
    attempts = {addr: {"amap": 0, "tencent": 0} for addr, _, _ in need_driving}
    finished = set()
    total_tasks = len(need_driving)

    def claim(provider):
        deferred = []
        selected = None
        while selected is None:
            try:
                item, required_provider = pending.get_nowait()
            except queue.Empty:
                break
            if required_provider is None or required_provider == provider:
                selected = item
            else:
                deferred.append((item, required_provider))
        for item in deferred:
            pending.put(item)
        return selected

    def finish(addr):
        with state_lock:
            finished.add(addr)

    def retry_or_finish(item, provider):
        addr, _, _ = item
        other = "tencent" if provider == "amap" else "amap"
        with state_lock:
            attempts[addr][provider] += 1
            if attempts[addr][other] == 0:
                pending.put((item, other))
            else:
                finished.add(addr)

    def done():
        with state_lock:
            return len(finished) >= total_tasks

    def save_result(item, result):
        addr, lon, lat = item
        distance_km, time_min = result
        with _driving_lock:
            DRIVING_CACHE[addr] = {
                "origin": [home_lon, home_lat],
                "destination": [lon, lat],
                "distance_km": distance_km,
                "time_min": time_min
            }
            save_driving_cache(DRIVING_CACHE)
        finish(addr)

    def amap_worker():
        while not done():
            item = claim("amap")
            if item is None:
                time.sleep(0.05)
                continue
            addr, lon, lat = item
            result = amap_drive(home_lon, home_lat, lon, lat)
            if result[0] is not None and result[1] is not None:
                save_result(item, result)
            else:
                retry_or_finish(item, "amap")

    def tencent_worker():
        while not done():
            item = claim("tencent")
            if item is None:
                time.sleep(0.05)
                continue
            addr, lon, lat = item
            result = tencent_drive(home_lon, home_lat, lon, lat)
            if result[0] is not None and result[1] is not None:
                save_result(item, result)
            else:
                retry_or_finish(item, "tencent")

    t1 = threading.Thread(target=amap_worker, name="amap-driving", daemon=True)
    t2 = threading.Thread(target=tencent_worker, name="tencent-driving", daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    log(f" 驾车地址池解析完成：{len(finished)}/{total_tasks} 个地址已结束")

def geocode_address_dual(address):
    """双地图地理编码（高德优先，腾讯后备），返回 (lon, lat)。"""
    url = "https://restapi.amap.com/v3/geocode/geo"
    params = {"address": clean_address(address), "city": "天津市", "key": AMAP_KEY, "output": "json"}
    try:
        AMAP_LIMITER.wait()
        session = get_session()
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1" and data.get("geocodes"):
                loc = data["geocodes"][0]["location"]
                lon, lat = map(float, loc.split(','))
                return lon, lat
    except:
        pass
    if TENCENT_KEY:
        url = "https://apis.map.qq.com/ws/geocoder/v1/"
        params = {"address": clean_address(address), "key": TENCENT_KEY}
        try:
            TENCENT_LIMITER.wait()
            resp = session.get(url, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == 0:
                    loc = data["result"]["location"]
                    return loc["lng"], loc["lat"]
        except:
            pass
    return None, None

# ============================================================
# 主程序
# ============================================================
def main():
    global DIST_CACHE, DRIVING_CACHE
    print("[初始化]")
    DIST_CACHE = load_dist_cache()
    DRIVING_CACHE = load_driving_cache()
    print(f"缓存: {len(DIST_CACHE)} 条地理, {len(DRIVING_CACHE)} 条驾车")

    # 初始解密
    try:
        session = get_session()
        resp = session.post(DECRYPT_URL, timeout=10)
        if resp.status_code != 200:
            print("初始解密失败")
            notify_error(
                f"初始解密HTTP {resp.status_code}：{resp.text[:200]}",
                is_fatal=True,
                category="启动",
                impact="监控无法启动，不会读取或处理消息。",
                action="请先确认微信和 WetraceTool 已登录并提供 5200 接口。",
            )
            sys.exit(1)
        data = resp.json()
        if not data.get("success"):
            print("初始解密失败")
            notify_error(
                f"初始解密返回失败：{data}",
                is_fatal=True,
                category="启动",
                impact="监控无法启动，不会读取或处理消息。",
                action="请检查微信登录状态和本地接口。",
            )
            sys.exit(1)
        print(f"初始解密成功")
    except Exception as e:
        print(f"初始解密异常: {e}")
        notify_error(
            f"{type(e).__name__}: {e}",
            is_fatal=True,
            category="启动",
            impact="监控无法启动，不会读取或处理消息。",
            action="请检查微信和 WetraceTool 是否正常运行。",
        )
        sys.exit(1)

    # 家庭坐标
    home_lon, home_lat = geocode_address_dual(HOME_ADDRESS)
    if home_lon is None:
        error = "无法解析家庭地址"
        print(error)
        notify_error(
            error,
            is_fatal=True,
            category="启动",
            impact="监控无法启动，不会读取或处理消息。",
            action="请检查地图 API 和家庭地址配置。",
        )
        sys.exit(1)
    print(f"家庭坐标: {home_lon}, {home_lat}")

    last_seq_dict, state_changed = prepare_last_seq_dict(load_last_seq_dict())
    if state_changed:
        save_last_seq_dict(last_seq_dict)
    print(f"监控 {len(TALKER_IDS)} 个群")
    print(f"开始监控 (轮询 {CHECK_INTERVAL}s, 同步间隔动态调整)")

    # 动态同步参数
    sync_interval = SYNC_INTERVAL
    idle_counter = 0
    MAX_IDLE_ROUNDS = 10
    STOP_HOUR = 18  # 晚上 6 点自动停止

    if datetime.now().hour >= STOP_HOUR:
        print(f" 当前时间已过 {STOP_HOUR} 点，自动退出")
        sys.exit(0)

    last_sync_at = time.monotonic()
    loop_counter = 0
    while True:
        if datetime.now().hour >= STOP_HOUR:
            log(f" 已到 {STOP_HOUR} 点，自动退出")
            save_runtime_state(last_seq_dict)
            sys.exit(0)

        loop_counter += 1
        if loop_counter % 20 == 0:
            log(f" 监控中... last_seq={last_seq_dict}, 同步间隔={sync_interval}")

        try:
            now = time.monotonic()
            if now - last_sync_at >= sync_interval:
                sync_ok = sync_data()
                last_sync_at = now
                if not sync_ok:
                    sync_interval = 1

            all_msgs = fetch_all_messages(limit=1000)
            if all_msgs is None:
                idle_counter = 0
                sync_interval = 1
                time.sleep(CHECK_INTERVAL)
                continue

            had_new_messages = False
            if all_msgs:
                for talker_id in TALKER_IDS:
                    group_msgs = [m for m in all_msgs if m.get("talker") == talker_id]
                    if not group_msgs:
                        continue
                    last_seq = parse_message_seq(last_seq_dict.get(talker_id, 0)) or 0
                    new_msgs = []
                    for message in group_msgs:
                        message_seq = parse_message_seq(message.get("seq"))
                        if message_seq is not None and message_seq > last_seq:
                            new_msgs.append(message)
                    if not new_msgs:
                        continue

                    # 去重并按 seq 排序
                    seen = set()
                    unique = []
                    for m in new_msgs:
                        seq = parse_message_seq(m.get("seq"))
                        if seq is not None and seq not in seen:
                            seen.add(seq)
                            unique.append(m)
                    new_msgs = sorted(unique, key=lambda m: parse_message_seq(m.get("seq")) or 0)
                    log(f" {talker_id} 有新消息 ({len(new_msgs)} 条)")
                    had_new_messages = True

                    # 收集需要地理编码的地址
                    addr_list = []
                    for msg in new_msgs:
                        content = msg.get("content", "")
                        if not isinstance(content, str) or not content.strip():
                            continue
                        content = content.strip()
                        subject = parse_subject(content)
                        grade = parse_grade(content)
                        price = parse_price(content)
                        addr = parse_address(content)
                        if addr and subject and is_valid_subject(subject) and (not grade or is_valid_grade(grade)):
                            if price is not None and price >= 140:
                                if not any(kw in addr for kw in BLACKLIST_KEYWORDS):
                                    if addr not in DIST_CACHE:
                                        addr_list.append(addr)
                    if addr_list:
                        geocode_scheduler(addr_list, home_lon, home_lat)
                        save_dist_cache(DIST_CACHE)

                    # 预先计算驾车信息
                    driving_addr_list = []
                    for msg in new_msgs:
                        content = msg.get("content", "")
                        if not isinstance(content, str) or not content.strip():
                            continue
                        content = content.strip()
                        subject = parse_subject(content)
                        grade = parse_grade(content)
                        teacher_req = parse_teacher_requirement(content)
                        price = parse_price(content)
                        addr = parse_address(content)
                        teacher_only_male = (
                            teacher_req and "男" in teacher_req
                            and "女" not in teacher_req
                            and "不限" not in teacher_req
                            and "男女" not in teacher_req
                        )
                        if (addr and subject and is_valid_subject(subject)
                                and (not grade or is_valid_grade(grade))
                                and not teacher_only_male
                                and price is not None and price >= 140
                                and not any(kw in addr for kw in BLACKLIST_KEYWORDS)
                                and not any(kw in addr for kw in ["线上", "远程", "网课"])):
                            with _cache_lock:
                                entry = DIST_CACHE.get(addr)
                            if entry and len(entry) >= 4:
                                straight_dist = entry[0]
                                if ULTRA_NEAR_DIST < straight_dist <= STRAIGHT_DIST_THRESHOLD:
                                    driving_addr_list.append(addr)
                    if driving_addr_list:
                        driving_scheduler(driving_addr_list, home_lon, home_lat)

                    # 逐条处理消息
                    for msg in new_msgs:
                        seq = parse_message_seq(msg.get("seq"))
                        if seq is None:
                            log(f" {talker_id} 收到无有效seq的消息，本轮跳过，绝不更新last_seq")
                            continue

                        content = msg.get("content", "")
                        if not isinstance(content, str):
                            content = ""
                        content = content.strip()
                        if not content:
                            log(f" 跳过无文本消息：talker={talker_id}，seq={seq}，type={msg.get('type')}；"
                                "该消息没有可供筛选的订单文本，提交游标避免阻塞后续消息")
                            last_seq_dict[talker_id] = seq
                            save_last_seq_dict(last_seq_dict)
                            continue

                        if msg.get("type") != 1:
                            log(f" 消息type={msg.get('type')}但含有文本，seq={seq}，按订单文本继续判断，不直接丢弃")

                        passed, reason, subject, straight_dist, driving_time, confidence = check_condition(content, home_lon, home_lat)
                        if passed:
                            log(f" 通过 → talker={talker_id} seq={seq} 时间={msg.get('time')} 科目={subject} 原因={reason}")
                            if straight_dist is not None and straight_dist <= ULTRA_NEAR_DIST:
                                loc_info = f"超近单({straight_dist:.1f}km)"
                            elif straight_dist is None:
                                loc_info = "位置未知"
                            else:
                                loc_info = f"直线{straight_dist:.1f}km"
                                if driving_time is not None:
                                    loc_info += f"，驾车约{driving_time:.0f}分钟"
                            confidence_text = f"可信度: {confidence}" if confidence else "可信度: 未知"
                            push_text = (f"【家教匹配】\n发送者: {msg.get('senderName')}\n时间: {msg.get('time')}\n"
                                         f"科目: {subject}\n通勤: {loc_info}\n{confidence_text}\n理由: {reason}\n\n完整原文：\n{content}")
                            if not push_to_feishu(push_text):
                                log(f" 推送失败，保留 seq={seq}，下一轮重试")
                                break
                        else:
                            log(f" 拒绝 → talker={talker_id} seq={seq} 时间={msg.get('time')} 科目={subject} 原因={reason}")
                            if reason in ("地理编码失败", "坐标缺失", "驾车时间获取失败"):
                                log(f" 地址暂时无法确认，保留 seq={seq}，下一轮重试")
                                break

                        last_seq_dict[talker_id] = seq
                        save_last_seq_dict(last_seq_dict)

                if not had_new_messages:
                    idle_counter += 1
                else:
                    idle_counter = 0
                sync_interval = ACTIVE_SYNC_INTERVAL
                if idle_counter >= MAX_IDLE_ROUNDS:
                    sync_interval = SYNC_INTERVAL
                    idle_counter = 0
            else:
                idle_counter += 1
                if idle_counter >= MAX_IDLE_ROUNDS:
                    sync_interval = SYNC_INTERVAL
                    idle_counter = 0

        except Exception as e:
            error_msg = traceback.format_exc()
            log(f"主循环异常: {e}")
            notify_error(
                error_msg,
                is_fatal=True,
                category="主循环",
                impact="监控已停止，未完成消息会在游标未推进的情况下等待重试。",
                action="守护程序会尝试重启；请查看本地日志中的完整堆栈。",
            )
            save_runtime_state(last_seq_dict)
            sys.exit(1)

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"未捕获异常: {e}")
        notify_error(
            error_msg,
            is_fatal=True,
            category="主程序",
            impact="监控已停止，未完成消息不会推进游标。",
            action="请查看完整堆栈并重新启动守护程序。",
        )
        sys.exit(1)
