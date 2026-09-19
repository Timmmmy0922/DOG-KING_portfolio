from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import atexit
import os
import random
import signal
import sys
import time
import traceback
from datetime import datetime

import requests


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ERROR_LOG_PATH = os.path.join(BASE_DIR, f"{os.path.splitext(os.path.basename(__file__))[0]}_errors.log")
SAVE_FOLDER = os.getenv("IMAGE_OUTPUT_DIR", os.path.join(BASE_DIR, "images"))
BAIDU_COOKIE = os.getenv("BAIDU_COOKIE", "").strip()
START_IMAGE_INDEX = int(os.getenv("START_IMAGE_INDEX", "21"))
SEARCH_PAGE_RANGE = range(2, 29)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 "
    "Safari/537.36 Edg/146.0.0.0"
)
REQUEST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "cache-control": "max-age=0",
    "connection": "keep-alive",
    "dnt": "1",
    "origin": "https://tieba.baidu.com",
    "referer": "https://tieba.baidu.com/",
    "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": USER_AGENT,
}

driver = None


def report_error(context, exc):
    """把异常显示到控制台，并追加写入当前脚本的错误日志。"""
    error_title = f"{context}：{type(exc).__name__}: {exc}"
    error_detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    print(error_title)
    try:
        with open(ERROR_LOG_PATH, "a", encoding="utf-8") as log_file:
            log_file.write(
                f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {error_title}\n"
                f"{error_detail}"
            )
    except OSError as log_exc:
        print(f"错误日志写入失败：{log_exc}")


def handle_uncaught_exception(exc_type, exc, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        print("\n程序已由用户终止。")
        return
    report_error("未捕获异常，程序已停止", exc)
    sys.__excepthook__(exc_type, exc, exc_tb)


def cleanup():
    global driver
    active_driver = driver
    driver = None
    if active_driver is not None:
        try:
            active_driver.quit()
        except Exception:
            pass


def handle_terminal_stop(_signum, _frame):
    print("\n程序已由用户终止。")
    cleanup()
    raise SystemExit(0)


def install_shutdown_handlers():
    sys.excepthook = handle_uncaught_exception
    atexit.register(cleanup)
    signal.signal(signal.SIGINT, handle_terminal_stop)
    signal.signal(signal.SIGTERM, handle_terminal_stop)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handle_terminal_stop)


def build_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option(
        "excludeSwitches", ["enable-automation", "ignore-certificate-errors"]
    )
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"user-agent={USER_AGENT}")
    browser = webdriver.Chrome(options=options)
    browser.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    browser.implicitly_wait(10)
    browser.set_page_load_timeout(30)
    return browser


def inject_cookie(browser):
    """把环境变量中的 Cookie 注入浏览器；未配置时使用匿名会话。"""
    if not BAIDU_COOKIE:
        print("未配置 BAIDU_COOKIE，使用匿名会话。")
        return
    for item in BAIDU_COOKIE.split(";"):
        item = item.strip()
        if "=" not in item:
            continue
        name, value = item.split("=", 1)
        try:
            browser.add_cookie({"name": name.strip(), "value": value})
        except Exception as exc:
            report_error(f"Cookie 字段 {name.strip()} 注入失败，已跳过", exc)
    browser.refresh()
    print("Cookie 已注入。")


def collect_post_links(browser):
    """遍历搜索结果页，返回按发现顺序去重的帖子链接。"""
    links = []
    seen = set()
    for page in SEARCH_PAGE_RANGE:
        try:
            url = (
                "https://tieba.baidu.com/f/search/res?isnew=1"
                f"&kw=%B0%C2%B1%C8%BD%BB%D2%D7&qw=%B3%F6%CE%EF&rn=10"
                f"&un=&only_thread=0&sm=1&sd=&ed=&pn={page}"
            )
            browser.get(url)
            time.sleep(random.uniform(5, 8))
            for link_tag in browser.find_elements(By.CSS_SELECTOR, "a.bluelink"):
                link = link_tag.get_attribute("href")
                if link and "/p/" in link and link not in seen:
                    seen.add(link)
                    links.append(link)
        except Exception as exc:
            report_error(f"采集搜索结果第 {page} 页失败，已跳过", exc)
    return links


def build_download_session(browser):
    """把 Selenium 会话 Cookie 同步到 requests，以复用登录状态下载图片。"""
    session = requests.Session()
    for cookie in browser.get_cookies():
        session.cookies.set(cookie["name"], cookie["value"])
    return session


def download_post_images(browser, session, post_links):
    image_index = START_IMAGE_INDEX
    downloaded_count = 0
    for post_url in post_links:
        try:
            print(f"打开帖子：{post_url}")
            browser.get(post_url)
            time.sleep(random.uniform(5, 8))
            browser.execute_script(f"window.scrollTo(0, {random.randint(300, 600)});")
            time.sleep(random.uniform(3, 5))

            image_tags = browser.find_elements(By.CSS_SELECTOR, "img[data-v-8830a27a]")
            print(f"找到图片：{len(image_tags)}")
            for image in image_tags:
                image_url = image.get_attribute("data-src") or image.get_attribute("src")
                if not image_url or "/pic/item/" not in image_url:
                    continue
                if "w%3D120%3Bh%3D120" in image_url:
                    continue
                try:
                    headers = REQUEST_HEADERS.copy()
                    headers["referer"] = post_url
                    headers["sec-fetch-dest"] = "image"
                    response = session.get(
                        image_url, headers=headers, timeout=15, allow_redirects=True
                    )
                    response.raise_for_status()
                    content_type = response.headers.get("content-type", "").lower()
                    if content_type and not content_type.startswith("image/"):
                        raise ValueError(f"响应不是图片：{content_type}")

                    image_path = os.path.join(SAVE_FOLDER, f"{image_index}.jpg")
                    with open(image_path, "wb") as image_file:
                        image_file.write(response.content)
                    print(f"下载成功：{image_path}")
                    image_index += 1
                    downloaded_count += 1
                    time.sleep(random.uniform(3, 5))
                except Exception as exc:
                    report_error(f"图片下载失败，已跳过（{image_url}）", exc)
        except Exception as exc:
            report_error(f"帖子处理异常，已跳过（{post_url}）", exc)
            time.sleep(3)
    return downloaded_count


def main():
    global driver
    os.makedirs(SAVE_FOLDER, exist_ok=True)
    install_shutdown_handlers()
    driver = build_driver()
    try:
        print("正在打开贴吧...")
        driver.get("https://tieba.baidu.com/")
        time.sleep(random.uniform(5, 7))
        inject_cookie(driver)
        time.sleep(random.uniform(5, 7))

        post_links = collect_post_links(driver)
        print(f"采集到帖子：{len(post_links)} 个")
        session = build_download_session(driver)
        downloaded_count = download_post_images(driver, session, post_links)
        print(f"\n任务完成，共下载 {downloaded_count} 张图片。")
        print(f"保存路径：{SAVE_FOLDER}")
    finally:
        cleanup()


if __name__ == "__main__":
    main()
