#name=html_scraper_images.py
"""
HTML页面抓取工具 - 主程序入口（虚拟机优化版，长页面截图等待优化）

优化：
- 降低 CPU 占用（通过浏览器重建冷却、代理失败去重、减少页面 JS 滚动开销）
- 增加内存管理
- 限制并发资源（脚本保持顺序抓取，但在每条请求之间放缓速率）
- 集成代理池（通过 lib/proxy_pool.py）
- 截图前等待图片加载（有超时回退）
- 新增：单条URL总处理超时(3分钟)强制跳过，更新 crawl_state=3
- 新增：当图片等待超时 warning 出现时，当前URL直接置 crawl_state=3，释放资源后继续下一条
- 调整：失败时不创建对应目录（仅成功后创建）
- 新增：每条URL耗时日志
- 新增：轻量内存优化（不改变抓取逻辑）
"""

import os
import re
import random
import asyncio
import time
import gc
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from typing import Optional, List, Tuple, Dict, Set
import pymysql
import json

from lib.playwright_scraper import PlaywrightScraper
from lib import config


class ImageWaitTimeoutError(Exception):
    """图片等待超时，标记为 crawl_state=3 并跳过当前条"""
    pass


class URLParser:
    """URL解析器，负责将URL转换为本地文件路径"""

    def __init__(self, date_str: Optional[str] = None, id_to_domain_key: Optional[Dict[int, str]] = None):
        self.date_str = date_str or datetime.now().strftime("%Y%m%d")
        self.id_to_domain_key = id_to_domain_key or {}

    def parse(self, url: str, domain_key: Optional[str] = None) -> Tuple[str, str]:
        parsed = urlparse(url)
        domain = parsed.hostname or ""
        domain = re.sub(r"^www\.", "", domain)
        domain = domain.replace("hihonor", "honor")

        folder_domain = (domain_key or domain).strip()
        if not folder_domain:
            folder_domain = domain
        first_dir = "{}_{}".format(folder_domain, self.date_str)

        path = parsed.path.rstrip("/")

        query_suffix = ""
        if parsed.query:
            query_suffix = "_" + re.sub(r"[^a-zA-Z0-9_\-]", "_", parsed.query)

        if not path:
            return os.path.join(first_dir, "index.html"), os.path.join(first_dir, "index.png")

        parts = path.strip("/").split("/")
        filename = parts[-1]
        sub_dirs = parts[:-1]

        if query_suffix:
            filename = filename + query_suffix

        html_filename = filename if filename.lower().endswith(".html") else "{}.html".format(filename)
        img_filename = re.sub(r"\.html$", "", filename, flags=re.IGNORECASE) + ".png"

        return os.path.join(first_dir, *sub_dirs, html_filename), os.path.join(first_dir, *sub_dirs, img_filename)


class DBURLReader:
    """URL 数据库读取器"""

    def __init__(self, domain_urls: dict = {'ankersolix.com': 1}):
        self.domain_urls = domain_urls
        self.db_config = config.DB_TM
        self.connection = None
        self.url_domain_key_map: Dict[str, str] = {}
        self.id_to_domain_key: Dict[int, str] = {v: k for k, v in self.domain_urls.items()}

    def _get_db_connection(self):
        return pymysql.connect(
            host=self.db_config['host'],
            user=self.db_config['user'],
            password=self.db_config['passwd'],
            database=self.db_config['db'],
            port=self.db_config['port'],
            charset='utf8mb4'
        )

    def read_urls(self) -> List[str]:
        urls: List[str] = []
        self.url_domain_key_map = {}
        try:
            self.connection = self._get_db_connection()
            cursor = self.connection.cursor()

            for domain_key, url_id in self.domain_urls.items():
                sql = f"""select block_urls from {config.TABLE_DOMAINS} where state = 1 and id = {url_id} """
                cursor.execute(sql)
                block_urls_str = cursor.fetchall()

                block_urls = []
                if block_urls_str and block_urls_str[0] and block_urls_str[0][0]:
                    try:
                        block_urls = json.loads(block_urls_str[0][0])
                    except Exception:
                        block_urls = []

                conditions = ''
                if block_urls:
                    conditions += "AND u.url NOT REGEXP '(" + "|".join(block_urls) + ")'"

                query = f"""SELECT u.url
FROM {config.TABLE_URLS} u 
LEFT JOIN {config.TABLE_SITEMAPS} s on u.sitemap_id = s.id AND s.state = 1
WHERE u.operation_status <> 3 
  AND u.domain_id ={url_id} 
  AND u.state = 1 
  AND u.crawl_state = 0 
  {conditions} """
                print(query)
                cursor.execute(query)

                for row in cursor.fetchall():
                    url = row[0].strip()
                    if url:
                        urls.append(url)
                        self.url_domain_key_map[url] = domain_key

            cursor.close()
            print(f"  [INFO] 从数据库成功读取 {len(urls)} 条URL")
        except pymysql.MySQLError as e:
            print(f"  [ERROR] 读取数据库URL失败: {e}")
            urls = []
        finally:
            if self.connection:
                self.connection.close()
        return urls


class SuccessURLWriter:
    URL_EXTRACT_RE = re.compile(r'<a\s+href="([^"]+)"', re.IGNORECASE)

    def __init__(self, root_dir=".", day_dir=None, day_compact=None, domain_urls=None):
        self.root_dir = root_dir
        self.day_dir = day_dir or datetime.now().strftime("%Y-%m-%d")
        self.day_compact = day_compact or datetime.now().strftime("%Y%m%d")
        self.domain_urls = domain_urls or {'ankersolix.com': 1}
        self.domain_html_paths = {
            k: os.path.join(self.root_dir, self.day_dir, k, "index.html")
            for k in self.domain_urls.keys()
        }
        self._existing_urls: Set[str] = set()
        self._load_existing_urls()

    def _ensure_parent(self, filepath: str):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

    def _init_html_if_needed(self, filepath: str, title: str):
        if os.path.exists(filepath):
            return
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8" /><meta name="viewport" content="width=device-width, initial-scale=1.0" /><title>{title}</title></head>
<body>
<!-- urls -->
</body></html>""")

    def _load_existing_urls(self):
        for path in self.domain_html_paths.values():
            try:
                if not os.path.exists(path):
                    continue
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                for m in self.URL_EXTRACT_RE.finditer(content):
                    self._existing_urls.add(m.group(1))
            except Exception as e:
                print("  [WARN] 读取已存在URL失败 {}: {}".format(path, e))

    def _rewrite_for_backup(self, url: str, domain_key: str) -> str:
        parsed = urlparse(url)
        path = parsed.path or "/"
        prefix = "http://bak.wdsystem.cn/{}/{}".format(domain_key, self.day_compact)
        p2 = urlparse(prefix)
        new_path = (p2.path.rstrip("/") + "/" + path.lstrip("/")).replace("//", "/")
        if new_path and new_path != "/" and not new_path.lower().endswith(".html"):
            new_path = new_path.rstrip("/") + ".html"
        return urlunparse((p2.scheme, p2.netloc, new_path, "", parsed.query, parsed.fragment))

    def append_success_url(self, raw_url: str, domain_key: Optional[str] = None):
        if not domain_key or domain_key not in self.domain_html_paths:
            host = (urlparse(raw_url).hostname or "").lower().replace("www.", "")
            if host in self.domain_html_paths:
                domain_key = host
            else:
                print("  [SKIP] 未匹配到domain_key，跳过写入成功URL: {}".format(raw_url))
                return

        target = self.domain_html_paths[domain_key]
        rewritten = self._rewrite_for_backup(raw_url, domain_key)
        if rewritten in self._existing_urls:
            print("  [SKIP] 成功URL已存在，跳过写入: {}".format(rewritten))
            return

        line = (
            '<a href="{0}" target="_blank">{0}</a>'
            ' >>> '
            '<a href="{1}" target="_blank">【原始页面】</a><br>\n'
        ).format(rewritten, raw_url)

        try:
            self._ensure_parent(target)
            self._init_html_if_needed(target, "{} URLs - {}".format(domain_key, self.day_dir))
            with open(target, "r", encoding="utf-8") as f:
                content = f.read()
            content = content.replace("</body>", line + "</body>", 1) if "</body>" in content else content + line
            with open(target, "w", encoding="utf-8") as f:
                f.write(content)
            self._existing_urls.add(rewritten)
            print("  [OK] 成功URL已写入: {}".format(rewritten))
        except Exception as e:
            print("  [WARN] 写入成功URL到 {} 失败: {}".format(target, e))


class HTMLProcessor:
    @staticmethod
    def generate_image_only_html(img_path: str, page_size: Dict):
        try:
            base_name = os.path.splitext(os.path.basename(img_path))[0]
            img_only_html_path = os.path.join(os.path.dirname(img_path), "{}.html".format(base_name))
            img_filename = os.path.basename(img_path)
            page_width = page_size.get("width", 1920)
            with open(img_only_html_path, "w", encoding="utf-8") as f:
                f.write(f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>页面截图 - {base_name}</title>
<style>*{{margin:0;padding:0;box-sizing:border-box}} body{{background:#f5f5f5;display:flex;justify-content:center;min-height:100vh}}
.screenshot-container{{max-width:100%;width:{page_width}px;background:#fff}} .screenshot-image{{width:100%;height:auto;display:block}}</style></head>
<body><div class="screenshot-container"><img src="{img_filename}" alt="页面截图" class="screenshot-image"></div></body></html>""")
            print("  [OK] 纯图片HTML已保存: {}".format(img_only_html_path))
        except Exception as e:
            print("  [ERROR] 生成纯图片HTML失败: {}".format(e))


class HTMLScraper:
    def __init__(
        self,
        output_dir="output",
        sleep_min=0.8,
        sleep_max=2.0,
        timeout=45000,
        headless=True,
        max_retries=3,
        gc_interval=5,
        use_proxy=False,
        proxy_pool_size=5,
        proxy_max_switch=3,
        per_url_total_timeout=180,
        domain_urls=None,
        url_domain_key_map=None,
    ):
        self.output_dir = output_dir
        self.images_dir = os.path.join(output_dir, "images")
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self.timeout = timeout
        self.headless = headless
        self.max_retries = max_retries
        self.gc_interval = gc_interval
        self.use_proxy = use_proxy
        self.proxy_pool_size = proxy_pool_size
        self.proxy_max_switch = proxy_max_switch
        self.per_url_total_timeout = per_url_total_timeout

        self.domain_urls = domain_urls or {'ankersolix.com': 1}
        self.url_domain_key_map = url_domain_key_map or {}
        self.url_parser = URLParser()
        self.success_writer = SuccessURLWriter(root_dir=".", domain_urls=self.domain_urls)
        self.html_processor = HTMLProcessor()
        self.error_url_path = "errorUrl.txt"

        self.pw_scraper: PlaywrightScraper = None
        self._error_urls_set: Set[str] = set()
        self._load_existing_error_urls()

        # 内存优化：更积极地回收分代对象（不改变业务逻辑）
        gc.set_threshold(500, 8, 8)

    def _load_existing_error_urls(self):
        try:
            if os.path.exists(self.error_url_path):
                with open(self.error_url_path, "r", encoding="utf-8") as f:
                    self._error_urls_set = {line.strip() for line in f if line.strip()}
                if self._error_urls_set:
                    print("  [INFO] 已加载 {} 条历史错误URL".format(len(self._error_urls_set)))
        except Exception as e:
            print("  [WARN] 加载历史错误URL失败: {}".format(e))

    @staticmethod
    def _ensure_dir(filepath: str):
        d = os.path.dirname(filepath)
        if d:
            os.makedirs(d, exist_ok=True)

    def _append_error_url(self, url: str):
        if not url or url in self._error_urls_set:
            return
        try:
            with open(self.error_url_path, "a", encoding="utf-8") as f:
                f.write(url + "\n")
            self._error_urls_set.add(url)
            print("  [ERROR-LOG] 已记录失败URL: {}".format(url))
        except Exception as e:
            print("  [WARN] 写入失败URL失败: {}".format(e))

    def _update_crawl_state(self, url: str, crawl_state: int):
        conn = None
        try:
            db = config.DB_TM
            conn = pymysql.connect(
                host=db['host'], user=db['user'], password=db['passwd'],
                database=db['db'], port=db['port'], charset='utf8mb4'
            )
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE {} SET crawl_state = %s WHERE url = %s AND state = 1".format(config.TABLE_URLS),
                (crawl_state, url)
            )
            conn.commit()
            cursor.close()
        except pymysql.MySQLError as e:
            print("  [DB-ERROR] 更新 crawl_state 失败: {}".format(e))
        finally:
            if conn:
                conn.close()

    def _get_domain_key_for_url(self, url: str) -> str:
        dk = self.url_domain_key_map.get(url)
        if dk:
            return dk
        host = (urlparse(url).hostname or "").lower().replace("www.", "")
        if host in self.domain_urls:
            return host
        for k in self.domain_urls.keys():
            if k in host:
                return k
        return host or "unknown"

    async def _scrape_one(self, url: str, attempt: int) -> bool:
        domain_key = self._get_domain_key_for_url(url)
        _, img_relative_path = self.url_parser.parse(url, domain_key=domain_key)
        img_save_path = os.path.join(self.images_dir, img_relative_path)

        html_content, fetch_success = await self.pw_scraper.fetch_page(url, attempt)
        # 内存优化：fetch 后尽早释放 html_content 引用（原逻辑不使用该内容）
        del html_content

        if not fetch_success:
            return False

        self._ensure_dir(img_save_path)
        screenshot_success, page_size = await self.pw_scraper.capture_screenshot(img_save_path)

        if not screenshot_success and getattr(self.pw_scraper, "last_image_wait_timed_out", False):
            print("  [TIMEOUT] 图片加载等待超时，当前URL按超时处理: {}".format(url))
            self._update_crawl_state(url, 3)
            raise ImageWaitTimeoutError(url)

        if not screenshot_success:
            self._append_error_url(url)
            self._update_crawl_state(url, 2)
            return False

        self.html_processor.generate_image_only_html(img_save_path, page_size)
        self.success_writer.append_success_url(url, domain_key=domain_key)
        self._update_crawl_state(url, 1)
        return True

    async def scrape_urls(self, urls: List[str]):
        self.pw_scraper = PlaywrightScraper(
            timeout=self.timeout,
            headless=self.headless,
            use_proxy=self.use_proxy,
            proxy_pool_size=self.proxy_pool_size,
            proxy_max_switch=self.proxy_max_switch,
            min_launch_interval=4.0,
            image_wait_timeout=12000,
        )

        batch_start = time.perf_counter()

        # 统计计数（仅日志统计，不影响逻辑）
        total_count = len(urls)
        success_count = 0
        fail_count = 0
        timeout_count = 0

        await self.pw_scraper.start()
        try:
            for idx, url in enumerate(urls, 1):
                url_start = time.perf_counter()
                start_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                print("[{}/{}] 正在抓取: {}".format(idx, len(urls), url))
                print("  [TIMER] 开始时间: {}".format(start_at))

                self.pw_scraper.reset_proxy_switch_count()
                ok, is_timeout = False, False

                try:
                    async def run_with_retries() -> bool:
                        for attempt in range(1, self.max_retries + 1):
                            #if attempt > 1:
                            #    await asyncio.sleep(1.5 + (attempt - 1) * 0.5)
                            if await self._scrape_one(url, attempt):
                                return True
                        return False

                    ok = await asyncio.wait_for(run_with_retries(), timeout=self.per_url_total_timeout)

                except ImageWaitTimeoutError:
                    is_timeout = True
                    self._append_error_url(url)
                    try:
                        await self.pw_scraper.stop()
                    except Exception:
                        pass
                    await self.pw_scraper.start()

                except asyncio.TimeoutError:
                    is_timeout = True
                    self._append_error_url(url)
                    self._update_crawl_state(url, 3)
                    try:
                        await self.pw_scraper.stop()
                    except Exception:
                        pass
                    await self.pw_scraper.start()

                if not ok and not is_timeout:
                    self._append_error_url(url)
                    self._update_crawl_state(url, 2)

                # 每条URL耗时日志 + 数量统计
                elapsed = time.perf_counter() - url_start
                end_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if ok:
                    status = "SUCCESS"
                    success_count += 1
                elif is_timeout:
                    status = "TIMEOUT"
                    timeout_count += 1
                else:
                    status = "FAIL"
                    fail_count += 1

                print(
                    "  [TIMER] {} 结束时间: {} | 状态: {} | 单条耗时: {:.3f}s".format(
                        url, end_at, status, elapsed
                    )
                )

                if idx % self.gc_interval == 0:
                    gc.collect()

                # 轻量内存优化：每轮结束显式清理临时引用
                del elapsed, end_at, start_at, status, ok, is_timeout, url_start

                #if idx < len(urls):
                #    await asyncio.sleep(random.uniform(self.sleep_min, self.sleep_max))
        finally:
            await self.pw_scraper.stop()
            gc.collect()

            batch_elapsed = time.perf_counter() - batch_start
            print("\n========== 采集统计 ==========")
            print("总数: {}".format(total_count))
            print("成功: {}".format(success_count))
            print("失败: {}".format(fail_count))
            print("超时: {}".format(timeout_count))
            print("批次总耗时: {:.3f}s".format(batch_elapsed))
            print("=============================\n")


class ScraperApp:
    def __init__(self, output_dir="output", sleep_min=0.8, sleep_max=2.0, headless=True, timeout=45000,
                 max_retries=3, use_proxy=False, proxy_pool_size=5, proxy_max_switch=3, per_url_total_timeout=180):
        self.output_dir = output_dir
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self.headless = headless
        self.timeout = timeout
        self.max_retries = max_retries
        self.use_proxy = use_proxy
        self.proxy_pool_size = proxy_pool_size
        self.proxy_max_switch = proxy_max_switch
        self.per_url_total_timeout = per_url_total_timeout

    def run(self):
        domain_urls = {'soundcore.com': 5, 'ankersolix.com': 1,'eufy.com':3, 'anker.com': 4}
        reader = DBURLReader(domain_urls=domain_urls)
        urls = reader.read_urls()
        if not urls:
            print("[WARN] 未读取到任何URL")
            return

        scraper = HTMLScraper(
            output_dir=self.output_dir,
            sleep_min=self.sleep_min,
            sleep_max=self.sleep_max,
            headless=self.headless,
            timeout=self.timeout,
            max_retries=self.max_retries,
            use_proxy=self.use_proxy,
            proxy_pool_size=self.proxy_pool_size,
            proxy_max_switch=self.proxy_max_switch,
            per_url_total_timeout=self.per_url_total_timeout,
            domain_urls=domain_urls,
            url_domain_key_map=reader.url_domain_key_map,
        )
        asyncio.run(scraper.scrape_urls(urls))


if __name__ == "__main__":
    ScraperApp(
        output_dir="output",
        sleep_min=0.8,
        sleep_max=2.0,
        headless=True,
        timeout=45000,
        max_retries=3,
        use_proxy=True,
        proxy_pool_size=5,
        proxy_max_switch=3,
        per_url_total_timeout=180,
    ).run()