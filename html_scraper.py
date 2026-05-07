"""
HTML页面抓取工具 - 使用Playwright获取动态加载完成后的完整HTML源码
"""

import os
import re
import time
import random
import asyncio
from datetime import datetime
from urllib.parse import urlparse
from playwright.async_api import async_playwright


class URLParser:
    """URL解析器，负责将URL转换为本地文件路径"""

    def __init__(self, date_str: str = None):
        """
        Args:
            date_str: 时间标记，格式如 '20260413'，默认为当天日期
        """
        self.date_str = date_str or datetime.now().strftime("%Y%m%d")

    def parse(self, url: str) -> str:
        """
        将URL解析为本地保存路径

        规则：
        - 第一层文件夹：域名(去掉www.，hihonor替换为honor) + 时间标记
        - 按URL路径创建子文件夹
        - 路径最后一段作为文件名(加.html，若已有.html则不加)
        - 若最后一段为空(以/结尾)，使用index.html

        Args:
            url: 要解析的URL

        Returns:
            本地文件保存路径
        """
        parsed = urlparse(url)

        # 处理域名：去掉www.，hihonor替换为honor
        domain = parsed.hostname or ""
        domain = re.sub(r"^www\.", "", domain)
        domain = domain.replace("hihonor", "honor")

        # 第一层文件夹：域名 + 时间标记
        first_dir = f"{domain}_{self.date_str}"

        # 处理路径
        path = parsed.path.rstrip("/")

        # 处理query参数中的variant等，附加到文件名
        query_suffix = ""
        if parsed.query:
            # 将query参数转为安全的文件名部分
            query_suffix = "_" + re.sub(r"[^a-zA-Z0-9_\-]", "_", parsed.query)

        if not path:
            # 路径为空，使用index.html
            return os.path.join(first_dir, "index.html")

        # 分割路径
        parts = path.strip("/").split("/")

        if len(parts) == 0:
            return os.path.join(first_dir, "index.html")

        # 最后一段作为文件名
        filename = parts[-1]
        sub_dirs = parts[:-1]

        # 添加query后缀到文件名
        if query_suffix:
            filename = filename + query_suffix

        # 如果文件名已经有.html后缀则不加，否则加上
        if not filename.lower().endswith(".html"):
            filename = filename + ".html"

        # 组合完整路径
        full_path = os.path.join(first_dir, *sub_dirs, filename)
        return full_path


class URLReader:
    """URL读取器，从txt文件中逐条读取URL"""

    def __init__(self, filepath: str):
        """
        Args:
            filepath: txt文件路径
        """
        self.filepath = filepath

    def read_urls(self) -> list:
        """
        从txt文件中读取URL列表

        Returns:
            URL列表
        """
        urls = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    urls.append(line)
        return urls


class HTMLScraper:
    """HTML抓取器，使用Playwright获取动态加载完成后的完整HTML"""

    def __init__(self, output_dir: str = "output", sleep_min: float = 1.2, sleep_max: float = 3.0,
                 timeout: int = 60000, headless: bool = True):
        """
        Args:
            output_dir: 输出根目录
            sleep_min: 最小等待时间(秒)
            sleep_max: 最大等待时间(秒)
            timeout: 页面加载超时时间(毫秒)
            headless: 是否无头模式运行浏览器
        """
        self.output_dir = output_dir
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self.timeout = timeout
        self.headless = headless
        self.url_parser = URLParser()

    def _random_sleep(self) -> float:
        """生成随机等待时间"""
        return random.uniform(self.sleep_min, self.sleep_max)

    def _ensure_dir(self, filepath: str):
        """确保文件所在目录存在"""
        dir_path = os.path.dirname(filepath)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

    async def _fetch_page(self, page, url: str) -> str:
        """
        使用Playwright获取页面完整HTML

        Args:
            page: Playwright page对象
            url: 页面URL

        Returns:
            页面完整HTML源码
        """
        try:
            # 导航到页面，等待网络空闲（动态数据加载完成）
            await page.goto(url, wait_until="networkidle", timeout=self.timeout)

            # 额外等待确保JS渲染完成
            await page.wait_for_timeout(2000)

            # 尝试滚动页面以触发懒加载内容
            await page.evaluate("""
                async () => {
                    const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
                    const height = document.body.scrollHeight;
                    const step = window.innerHeight;
                    for (let i = 0; i < height; i += step) {
                        window.scrollTo(0, i);
                        await delay(300);
                    }
                    window.scrollTo(0, 0);
                }
            """)

            # 等待滚动后的动态内容加载
            await page.wait_for_timeout(2000)

            # 获取完整HTML
            html_content = await page.content()
            return html_content

        except Exception as e:
            print(f"  [ERROR] 获��页面失败: {url}, 错误: {e}")
            return None

    async def scrape_urls(self, urls: list):
        """
        批量抓取URL列表

        Args:
            urls: URL列表
        """
        total = len(urls)
        success_count = 0
        fail_count = 0

        print(f"[INFO] 共 {total} 条URL待抓取")
        print(f"[INFO] 输出目录: {self.output_dir}")
        print(f"[INFO] 随机等待时间: {self.sleep_min}s - {self.sleep_max}s")
        print("-" * 60)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            page = await context.new_page()

            for idx, url in enumerate(urls, 1):
                print(f"[{idx}/{total}] 正在抓取: {url}")

                # 解析保存路径
                relative_path = self.url_parser.parse(url)
                save_path = os.path.join(self.output_dir, relative_path)

                # 确保目录存在
                self._ensure_dir(save_path)

                # 抓取页面
                html_content = await self._fetch_page(page, url)

                if html_content:
                    # 保存HTML文件
                    with open(save_path, "w", encoding="utf-8") as f:
                        f.write(html_content)
                    print(f"  [OK] 已保存: {save_path}")
                    success_count += 1
                else:
                    fail_count += 1

                # 随机等待，避免请求过快
                if idx < total:
                    sleep_time = self._random_sleep()
                    print(f"  [WAIT] 等待 {sleep_time:.2f}s ...")
                    await asyncio.sleep(sleep_time)

            await browser.close()

        print("-" * 60)
        print(f"[DONE] 抓取完成! 成功: {success_count}, 失败: {fail_count}, 总计: {total}")


class ScraperApp:
    """抓取应用主类"""

    def __init__(self, url_file: str = "urls.txt", output_dir: str = "output",
                 sleep_min: float = 1.2, sleep_max: float = 3.0,
                 headless: bool = True):
        """
        Args:
            url_file: URL列表文件路径
            output_dir: 输出根目录
            sleep_min: 最小等待时间(秒)
            sleep_max: 最大等待时间(秒)
            headless: 是否无头模式
        """
        self.url_file = url_file
        self.output_dir = output_dir
        self.sleep_min = sleep_min
        self.sleep_max = sleep_max
        self.headless = headless

    def run(self):
        """运行抓取任务"""
        # 读取URL
        reader = URLReader(self.url_file)
        urls = reader.read_urls()

        if not urls:
            print("[WARN] 未读取到任何URL，请检查文件内容")
            return

        # 创建抓取器并执行
        scraper = HTMLScraper(
            output_dir=self.output_dir,
            sleep_min=self.sleep_min,
            sleep_max=self.sleep_max,
            headless=self.headless
        )

        asyncio.run(scraper.scrape_urls(urls))


if __name__ == "__main__":
    app = ScraperApp(
        url_file="urls.txt",
        output_dir="output",
        sleep_min=1.2,
        sleep_max=3.0,
        headless=True
    )
    app.run()
