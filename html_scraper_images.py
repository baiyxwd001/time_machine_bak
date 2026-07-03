#name=html_scraper_images.py
"""
HTML页面抓取工具 - 主程序入口（虚拟机优化版）

优化：
- 降低 CPU 占用（通过浏览器重建冷却、代理失败去重、减少页面 JS 滚动开销）
- 增加内存管理
- 限制并发资源（脚本保持顺序抓取，但在每条请求之间放缓速率）
- 集成代理池（通过 lib/proxy_pool.py）
- 截图前等待图片加载（有超时回退）
- 新增：单条URL总处理超时(3分钟)强制跳过，更新 crawl_state=3
- 新增：当图片等待超时 warning 出现时，当前URL直接置 crawl_state=3，释放资源后继续下一条
- 调整：失败时不创建对应目录（仅成功后创建）
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


# ----------------------------
# URL 解析：输出本地 HTML / 图片保存路径
# ----------------------------
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
            html_path = os.path.join(first_dir, "index.html")
            img_path  = os.path.join(first_dir, "index.png")
            return html_path, img_path

        parts    = path.strip("/").split("/")
        filename = parts[-1]
        sub_dirs = parts[:-1]

        if query_suffix:
            filename = filename + query_suffix

        html_filename = filename if filename.lower().endswith(".html") else "{}.html".format(filename)
        img_filename  = re.sub(r"\.html$", "", filename, flags=re.IGNORECASE) + ".png"

        html_path = os.path.join(first_dir, *sub_dirs, html_filename)
        img_path  = os.path.join(first_dir, *sub_dirs, img_filename)

        return html_path, img_path


class URLReader:
    """URL 文件读取器"""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def read_urls(self) -> List[str]:
        urls: List[str] = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)
        return urls


class DBURLReader:
    """URL 数据库读取器"""

    def __init__(self, domain_urls: dict = {'ankersolix.com': 1}):
        self.domain_urls = domain_urls
        self.db_config = config.DB_TM
        self.connection = None

        self.url_domain_key_map: Dict[str, str] = {}
        self.id_to_domain_key: Dict[int, str] = {v: k for k, v in self.domain_urls.items()}

    def _get_db_connection(self):
        try:
            conn = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['passwd'],
                database=self.db_config['db'],
                port=self.db_config['port'],
                charset='utf8mb4'
            )
            return conn
        except pymysql.MySQLError as e:
            print(f"  [ERROR] 数据库连接失败: {e}")
            raise

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
                    conditions += "AND u.url NOT REGEXP '("
                    for block_url in block_urls:
                        condition_str = f"{block_url}|"
                        conditions += condition_str
                    conditions = conditions[:-1] + ")'"

                query = f"""SELECT u.url
FROM {config.TABLE_URLS} u 
LEFT JOIN {config.TABLE_SITEMAPS} s on u.sitemap_id = s.id AND s.state = 1
WHERE u.operation_status <> 3 
  AND u.domain_id ={url_id} 
  AND u.state = 1 
  AND u.crawl_state = 0 and u.url like "%/collections/%" 
  {conditions} limit 10"""
                print(query)
                cursor.execute(query)

                results = cursor.fetchall()
                for row in results:
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
    """将成功的 URL 逐条写入分类HTML"""

    URL_EXTRACT_RE = re.compile(r'<a\s+href="([^"]+)"', re.IGNORECASE)

    def __init__(
        self,
        root_dir: str = ".",
        day_dir: Optional[str] = None,
        day_compact: Optional[str] = None,
        domain_urls: Optional[Dict[str, int]] = None
    ):
        self.root_dir    = root_dir
        self.day_dir     = day_dir     or datetime.now().strftime("%Y-%m-%d")
        self.day_compact = day_compact or datetime.now().strftime("%Y%m%d")
        self.domain_urls = domain_urls or {'ankersolix.com': 1}

        # domain_key -> index.html path
        self.domain_html_paths: Dict[str, str] = {}
        for domain_key in self.domain_urls.keys():
            p = os.path.join(self.root_dir, self.day_dir, domain_key, "index.html")
            self.domain_html_paths[domain_key] = p

        self._existing_urls: Set[str] = set()
        self._load_existing_urls()

    def _ensure_parent(self, filepath: str):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

    def _init_html_if_needed(self, filepath: str, title: str):
        if os.path.exists(filepath):
            return
        base = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{}</title>
</head>
<body>
<!-- urls -->
</body>
</html>
""".format(title)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(base)

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
        parsed   = urlparse(url)
        path     = parsed.path or "/"
        query    = parsed.query
        fragment = parsed.fragment

        prefix = "http://bak.wdsystem.cn/{}/{}".format(domain_key, self.day_compact)
        p2 = urlparse(prefix)
        new_path = (p2.path.rstrip("/") + "/" + path.lstrip("/")).replace("//", "/")

        if new_path and new_path != "/" and not new_path.lower().endswith(".html"):
            new_path = new_path.rstrip("/") + ".html"

        return urlunparse((p2.scheme, p2.netloc, new_path, "", query, fragment))

    def append_success_url(self, raw_url: str, domain_key: Optional[str] = None):
        if not domain_key or domain_key not in self.domain_html_paths:
            host = (urlparse(raw_url).hostname or "").lower().replace("www.", "")
            if host in self.domain_html_paths:
                domain_key = host
            else:
                print("  [SKIP] 未匹配到domain_key，跳过写入成功URL: {}".format(raw_url))
                return

        target   = self.domain_html_paths[domain_key]
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
            # ========================= 改动A 开始 =========================
            # 成功时才创建目录并初始化 index.html（避免失败时创建目录）
            self._ensure_parent(target)
            self._init_html_if_needed(target, title="{} URLs - {}".format(domain_key, self.day_dir))
            # ========================= 改动A 结束 =========================

            with open(target, "r", encoding="utf-8") as f:
                content = f.read()

            if "</body>" in content:
                content = content.replace("</body>", line + "</body>", 1)
            else:
                content += line

            with open(target, "w", encoding="utf-8") as f:
                f.write(content)

            self._existing_urls.add(rewritten)
            print("  [OK] 成功URL已写入: {}".format(rewritten))
        except Exception as e:
            print("  [WARN] 写入成功URL到 {} 失败: {}".format(target, e))


class HTMLProcessor:
    """HTML 内容处理器"""

    @staticmethod
    def inject_image_to_html(html_content: str, html_path: str, img_path: str) -> str:
        try:
            html_dir = os.path.dirname(html_path)
            img_relative_path = os.path.relpath(img_path, html_dir).replace("\\", "/")
            img_tag = """
<!-- 页面截图 -->
<div style="width:100%; max-width:1200px; margin:0 auto; padding:20px 0;">
    <img src="{0}" alt="页面截图" style="width:100%; height:auto; border:1px solid #eee;" />
</div>
""".format(img_relative_path)

            if "<body" in html_content.lower():
                html_content = re.sub(
                    r"(?i)<body([^>]*)>",
                    r"<body\1>" + img_tag,
                    html_content, count=1
                )
            else:
                html_content = "<html><body>{}{}</body></html>".format(img_tag, html_content)
            return html_content
        except Exception as e:
            print("  [WARN] 注入图片标签失败: {}".format(e))
            return html_content

    @staticmethod
    def generate_image_only_html(img_path: str, page_size: Dict):
        try:
            base_name        = os.path.splitext(os.path.basename(img_path))[0]
            img_only_html_path = os.path.join(os.path.dirname(img_path), "{}.html".format(base_name))
            img_filename     = os.path.basename(img_path)
            page_width       = page_size.get("width", 1920)

            content = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>页面截图 - {0}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ background-color: #f5f5f5; display: flex; justify-content: center; min-height: 100vh; }}
        .screenshot-container {{ max-width: 100%; width: {1}px; background-color: white; }}
        .screenshot-image {{ width: 100%; height: auto; display: block; }}
    </style>
</head>
<body>
    <div class="screenshot-container">
        <img src="{2}" alt="页面截图" class="screenshot-image">
    </div>
</body>
</html>""".format(base_name, page_width, img_filename)

            with open(img_only_html_path, "w", encoding="utf-8") as f:
                f.write(content)
            print("  [OK] 纯图片HTML已保存: {}".format(img_only_html_path))
        except Exception as e:
            print("  [ERROR] 生成纯图片HTML失败: {}".format(e))


class HTMLScraper:
    """HTML 页面抓取器（虚拟机优化版）"""

    def __init__(
        self,
        output_dir: str = "output",
        sleep_min: float = 0.8,
        sleep_max: float = 2.0,
        timeout: int = 45000,
        headless: bool = True,
        max_retries: int = 3,
        gc_interval: int = 5,
        use_proxy: bool = False,
        proxy_pool_size: int = 5,
        proxy_max_switch: int = 3,
        per_url_total_timeout: int = 180,
        domain_urls: Optional[Dict[str, int]] = None,
        url_domain_key_map: Optional[Dict[str, str]] = None,
    ):
        self.output_dir      = output_dir
        self.images_dir      = os.path.join(output_dir, "images")
        self.sleep_min       = sleep_min
        self.sleep_max       = sleep_max
        self.timeout         = timeout
        self.headless        = headless
        self.max_retries     = max_retries
        self.gc_interval     = gc_interval
        self.use_proxy       = use_proxy
        self.proxy_pool_size = proxy_pool_size
        self.proxy_max_switch = proxy_max_switch
        self.per_url_total_timeout = per_url_total_timeout

        self.domain_urls = domain_urls or {'ankersolix.com': 1}
        self.url_domain_key_map = url_domain_key_map or {}
        self.id_to_domain_key = {v: k for k, v in self.domain_urls.items()}

        self.url_parser = URLParser(id_to_domain_key=self.id_to_domain_key)
        self.success_writer = SuccessURLWriter(root_dir=".", domain_urls=self.domain_urls)
        self.html_processor = HTMLProcessor()
        self.error_url_path = "errorUrl.txt"

        self.pw_scraper: PlaywrightScraper = None

        self._error_urls_set: Set[str] = set()
        self._load_existing_error_urls()

    def _load_existing_error_urls(self):
        try:
            if os.path.exists(self.error_url_path):
                with open(self.error_url_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            self._error_urls_set.add(line)
                if self._error_urls_set:
                    print("  [INFO] 已加载 {} 条历史错误URL".format(len(self._error_urls_set)))
        except Exception as e:
            print("  [WARN] 加载历史错误URL失败: {}".format(e))

    def _random_sleep(self) -> float:
        return random.uniform(self.sleep_min, self.sleep_max)

    @staticmethod
    def _ensure_dir(filepath: str):
        dir_path = os.path.dirname(filepath)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

    def _append_error_url(self, url: str):
        url = url.strip()
        if not url:
            return

        if url in self._error_urls_set:
            print("  [SKIP] 错误URL已存在，跳过写入: {}".format(url))
            return

        try:
            with open(self.error_url_path, "a", encoding="utf-8") as f:
                f.write(url + "\n")
            self._error_urls_set.add(url)
            print("  [ERROR-LOG] 已记录失败URL: {}".format(url))
        except Exception as e:
            print("  [WARN] 写入失败URL到 {} 失败: {}".format(self.error_url_path, e))

    def _update_crawl_state(self, url: str, crawl_state: int):
        conn = None
        try:
            db_config = config.DB_TM
            conn = pymysql.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['passwd'],
                database=db_config['db'],
                port=db_config['port'],
                charset='utf8mb4'
            )
            cursor = conn.cursor()
            update_sql = "UPDATE {} SET crawl_state = %s WHERE url = %s AND state = 1".format(
                config.TABLE_URLS
            )
            cursor.execute(update_sql, (crawl_state, url))
            conn.commit()
            affected = cursor.rowcount
            cursor.close()

            state_map = {1: "成功", 2: "失败", 3: "超时"}
            state_label = state_map.get(crawl_state, "未知")
            if affected > 0:
                print("  [DB] crawl_state 已更新为 {} ({}): {}".format(
                    crawl_state, state_label, url))
            else:
                print("  [DB-WARN] 未匹配到URL记录: {}".format(url))
        except pymysql.MySQLError as e:
            print("  [DB-ERROR] 更新 crawl_state 失败: {}".format(e))
        finally:
            if conn:
                conn.close()

    def _run_gc(self):
        gc.collect()

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
        html_relative_path, img_relative_path = self.url_parser.parse(url, domain_key=domain_key)

        html_save_path = os.path.join(self.output_dir, html_relative_path)
        img_save_path  = os.path.join(self.images_dir,  img_relative_path)

        # ========================= 改动B 开始 =========================
        # 失败不创建目录：这里不再预先创建目录
        # self._ensure_dir(html_save_path)
        # self._ensure_dir(img_save_path)
        # ========================= 改动B 结束 =========================

        html_content, fetch_success = await self.pw_scraper.fetch_page(url, attempt)
        if not fetch_success:
            return False

        # ========================= 改动C 开始 =========================
        # 只有 fetch 成功后才为截图目录建目录
        self._ensure_dir(img_save_path)
        # ========================= 改动C 结束 =========================

        screenshot_success, page_size = await self.pw_scraper.capture_screenshot(img_save_path)

        if not screenshot_success and getattr(self.pw_scraper, "last_image_wait_timed_out", False):
            print("  [TIMEOUT] 图片加载等待超时，当前URL按超时处理: {}".format(url))
            self._update_crawl_state(url, 3)
            raise ImageWaitTimeoutError(url)

        if not screenshot_success:
            print("  [ERROR] 截图失败，记录到错误列表: {}".format(url))
            self._append_error_url(url)
            self._update_crawl_state(url, 2)
            return False

        self.html_processor.generate_image_only_html(img_save_path, page_size)
        _ = self.html_processor.inject_image_to_html(html_content, html_save_path, img_save_path)

        print("  [SKIP] 已注释HTML文件保存逻辑，跳过存储: {}".format(html_save_path))

        self.success_writer.append_success_url(url, domain_key=domain_key)
        self._update_crawl_state(url, 1)

        return True

    async def scrape_urls(self, urls: List[str]):
        total         = len(urls)
        success_count = 0
        fail_count    = 0
        timeout_count = 0
        program_start = time.perf_counter()

        self.pw_scraper = PlaywrightScraper(
            timeout=self.timeout,
            headless=self.headless,
            use_proxy=self.use_proxy,
            proxy_pool_size=self.proxy_pool_size,
            proxy_max_switch=self.proxy_max_switch,
            min_launch_interval=4.0,
            image_wait_timeout=12000,
        )

        print("[INFO] 共 {} 条URL待抓取".format(total))
        print("[INFO] HTML输出目录: {}".format(self.output_dir))
        print("[INFO] 图片输出目录: {}".format(self.images_dir))
        print("[INFO] 失败URL输出: {}".format(self.error_url_path))
        for d, p in self.success_writer.domain_html_paths.items():
            print("[INFO] 成功URL汇总({}): {}".format(d, p))
        print("[INFO] 单次超时: {}ms, 最大重试: {}次".format(self.timeout, self.max_retries))
        print("[INFO] 单条URL总超时: {}s".format(self.per_url_total_timeout))
        print("[INFO] 等待策略: {}".format(" -> ".join(self.pw_scraper.get_wait_strategies())))
        print("[INFO] 运行模式: 虚拟机低资源优化")
        print("[INFO] 代理模式: {}".format(
            "启用（pool_size={}, max_switch={})".format(
                self.proxy_pool_size, self.proxy_max_switch
            ) if self.use_proxy else "关闭（直连）"
        ))
        print("-" * 60)

        await self.pw_scraper.start()

        try:
            for idx, url in enumerate(urls, 1):
                url_start = time.perf_counter()
                print("[{}/{}] 正在抓取: {}".format(idx, total, url))

                self.pw_scraper.reset_proxy_switch_count()

                ok = False
                is_timeout = False

                try:
                    async def run_with_retries() -> bool:
                        local_ok = False
                        for attempt in range(1, self.max_retries + 1):
                            if attempt > 1:
                                wait_time = 1.5 + (attempt - 1) * 0.5
                                await asyncio.sleep(wait_time)
                                print("  [RETRY] 第 {}/{} 次重试...".format(attempt, self.max_retries))

                            local_ok = await self._scrape_one(url, attempt)
                            if local_ok:
                                break
                        return local_ok

                    ok = await asyncio.wait_for(
                        run_with_retries(),
                        timeout=self.per_url_total_timeout
                    )

                except ImageWaitTimeoutError:
                    is_timeout = True
                    ok = False
                    timeout_count += 1

                    self._append_error_url(url)

                    try:
                        await self.pw_scraper.stop()
                    except Exception:
                        pass
                    try:
                        await self.pw_scraper.start()
                    except Exception as e:
                        print("  [WARN] 图片等待超时后重启浏览器失败: {}".format(str(e)[:120]))

                except asyncio.TimeoutError:
                    is_timeout = True
                    ok = False
                    timeout_count += 1
                    print("  [TIMEOUT] 单条URL处理超过 {} 秒，强制跳过: {}".format(
                        self.per_url_total_timeout, url
                    ))

                    try:
                        await self.pw_scraper.stop()
                    except Exception:
                        pass
                    try:
                        await self.pw_scraper.start()
                    except Exception as e:
                        print("  [WARN] 超时后重启浏览器失败: {}".format(str(e)[:120]))

                    self._append_error_url(url)
                    self._update_crawl_state(url, 3)

                url_cost = time.perf_counter() - url_start

                if ok:
                    success_count += 1
                    print("  [TIME] 本条成功，耗时: {:.2f}s".format(url_cost))
                else:
                    fail_count += 1
                    if not is_timeout:
                        print("  [FAIL] 多次重试仍失败: {}".format(url))
                        self._append_error_url(url)
                        self._update_crawl_state(url, 2)
                    print("  [TIME] 本条失败，耗时: {:.2f}s".format(url_cost))

                if idx % self.gc_interval == 0:
                    self._run_gc()
                    print("  [GC] 已执行垃圾回收")

                if idx < total:
                    sleep_time = self._random_sleep()
                    print("  [WAIT] 等待 {:.2f}s ...".format(sleep_time))
                    await asyncio.sleep(sleep_time)

        finally:
            await self.pw_scraper.stop()
            self._run_gc()

        program_cost = time.perf_counter() - program_start
        print("-" * 60)
        print("[DONE] 抓取完成! 成功: {}, 失败: {}, 超时: {}, 总计: {}".format(
            success_count, fail_count, timeout_count, total
        ))
        print("[TIME] 程序总耗时: {:.2f}s ({:.2f}分钟)".format(
            program_cost, program_cost / 60
        ))


class ScraperApp:
    """应用入口类"""

    def __init__(
        self,
        db_table_name: str = "url_table",
        db_url_column: str = "url",
        output_dir: str = "output",
        sleep_min: float = 0.8,
        sleep_max: float = 2.0,
        headless: bool = True,
        timeout: int = 45000,
        max_retries: int = 3,
        use_proxy: bool = False,
        proxy_pool_size: int = 5,
        proxy_max_switch: int = 3,
        per_url_total_timeout: int = 180,
    ):
        self.db_table_name = db_table_name
        self.db_url_column = db_url_column
        self.output_dir      = output_dir
        self.sleep_min       = sleep_min
        self.sleep_max       = sleep_max
        self.headless        = headless
        self.timeout         = timeout
        self.max_retries     = max_retries
        self.use_proxy       = use_proxy
        self.proxy_pool_size = proxy_pool_size
        self.proxy_max_switch = proxy_max_switch
        self.per_url_total_timeout = per_url_total_timeout

    def run(self):
        # 采集url：domain_urls = {'ankersolix.com': 1,'eufy':3, 'anker.com': 4, 'soundcore.com': 5}
        domain_urls = {'eufy': 3}
        reader = DBURLReader(domain_urls=domain_urls)
        urls   = reader.read_urls()
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
    app = ScraperApp(
        output_dir="output",
        sleep_min=0.8,
        sleep_max=2.0,
        headless=True,
        timeout=60000,
        max_retries=3,
        use_proxy=True,
        proxy_pool_size=5,
        proxy_max_switch=3,
        per_url_total_timeout=180,
    )
    app.run()
