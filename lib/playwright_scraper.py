#name=lib/playwright_scraper.py
"""
Playwright 页面抓取器（虚拟机优化版）
- 集成代理池切换
- 多等待策略降级
- null 安全的 JS evaluate
- 浏览器崩溃自动恢复
- 降低 CPU 的重启冷却与代理失败去重策略（优化）
- 等待图片加载并有超时回退策略
"""

import asyncio
import time
from typing import Optional, List, Tuple, Dict
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Error as PlaywrightError

from lib.proxy_pool import ProxyPool


class PlaywrightScraper:
    """基于 Playwright 的页面抓取器"""

    WAIT_STRATEGIES = ["domcontentloaded", "load", "commit"]

    def __init__(
        self,
        timeout: int = 45000,
        headless: bool = True,
        use_proxy: bool = False,
        proxy_pool_size: int = 5,
        proxy_max_switch: int = 3,
        # 新增可配置项
        min_launch_interval: float = 4.0,
        image_wait_timeout: int = 12000,  # 等待图片加载（ms）
    ):
        self.timeout = timeout
        self.headless = headless
        self.use_proxy = use_proxy
        self.proxy_pool_size = proxy_pool_size
        self.proxy_max_switch = proxy_max_switch

        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

        self.proxy_pool: Optional[ProxyPool] = None
        self.current_proxy: Optional[str] = None
        self._proxy_switch_count = 0
        self._just_rebuilt = False

        # 失败代理集合，避免短时间重复使用坏代理
        self._failed_proxies: set = set()

        # 最小重启间隔（秒），防止短时间内频繁重启 chrome 引发 CPU 峰值
        self._last_launch_time: float = 0.0
        self.min_launch_interval: float = min_launch_interval

        # 等待图片加载超时时间（ms）
        self.image_wait_timeout = image_wait_timeout

        # 新增：记录最近一次“图片等待是否超时”
        self.last_image_wait_timed_out: bool = False

    def get_wait_strategies(self) -> List[str]:
        return self.WAIT_STRATEGIES

    async def start(self):
        """启动浏览器"""
        if self.use_proxy:
            self.proxy_pool = ProxyPool(pool_size=self.proxy_pool_size)
            try:
                self.proxy_pool.refresh()
            except Exception:
                pass

        self.playwright = await async_playwright().start()
        await self._launch_browser()

    async def stop(self):
        """关闭浏览器"""
        try:
            if self.page and not self.page.is_closed():
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            print("  [WARN] 关闭浏览器异常: {}".format(e))

    def _get_fresh_proxy(self):
        if not self.proxy_pool:
            return None

        for _ in range(8):
            try:
                self.proxy_pool.refresh()
            except Exception:
                pass

            proxy_config = None
            try:
                proxy_config = self.proxy_pool.get_pw_proxy()
            except Exception:
                try:
                    proxy_config = self.proxy_pool.get_proxy()
                except Exception:
                    proxy_config = None

            if not proxy_config:
                continue

            if isinstance(proxy_config, dict) and "server" in proxy_config:
                proxy_addr = proxy_config["server"]
            elif isinstance(proxy_config, str):
                proxy_addr = proxy_config
            else:
                proxy_addr = str(proxy_config)

            if proxy_addr not in self._failed_proxies:
                return proxy_config

            print("  [PROXY] 跳过已失败代理: {}".format(proxy_addr))

        self._failed_proxies.clear()
        try:
            self.proxy_pool.refresh()
        except Exception:
            pass
        try:
            return self.proxy_pool.get_pw_proxy()
        except Exception:
            try:
                return self.proxy_pool.get_proxy()
            except Exception:
                return None

    async def _launch_browser(self):
        now = time.time()
        since = now - self._last_launch_time
        if since < self.min_launch_interval:
            wait_for = self.min_launch_interval - since
            print(f"  [BROWSER] 上次重启 {since:.2f}s 前，冷却 {wait_for:.2f}s 后再重建")
            await asyncio.sleep(wait_for)

        try:
            if self.page and not self.page.is_closed():
                await self.page.close()
        except Exception:
            pass
        try:
            if self.context:
                await self.context.close()
        except Exception:
            pass
        try:
            if self.browser:
                await self.browser.close()
        except Exception:
            pass

        self.page = None
        self.context = None
        self.browser = None

        launch_args = {
            "headless": self.headless,
            "args": [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-background-timer-throttling",
                "--disable-client-side-phishing-detection",
            ],
        }

        proxy_config = None
        if self.use_proxy and self.proxy_pool:
            proxy_config = self._get_fresh_proxy()
            if proxy_config:
                if isinstance(proxy_config, dict) and "server" in proxy_config:
                    self.current_proxy = proxy_config["server"]
                elif isinstance(proxy_config, str):
                    self.current_proxy = proxy_config
                    proxy_config = {"server": "http://{}".format(proxy_config)}
                else:
                    self.current_proxy = str(proxy_config)
                    proxy_config = {"server": "http://{}".format(proxy_config)}
                print("  [PROXY] 当前代理: {}".format(self.current_proxy))
            else:
                print("  [PROXY-WARN] 未获取到可用代理，使用直连")

        if proxy_config:
            launch_args["proxy"] = proxy_config

        self.browser = await self.playwright.chromium.launch(**launch_args)
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        self.page = await self.context.new_page()
        self._just_rebuilt = True
        self._last_launch_time = time.time()
        print("  [BROWSER] 浏览器已启动/重建")

    async def _ensure_browser_alive(self):
        need_rebuild = False
        if not self.browser or not self.browser.is_connected():
            need_rebuild = True
        elif not self.page or self.page.is_closed():
            need_rebuild = True
        if need_rebuild:
            print("  [BROWSER] 检测到浏览器/页面不可用，重建中...")
            await self._launch_browser()

    async def _switch_proxy(self) -> bool:
        if not self.use_proxy or not self.proxy_pool:
            await self._ensure_browser_alive()
            return False

        self._proxy_switch_count += 1
        if self._proxy_switch_count > self.proxy_max_switch:
            print("  [PROXY] 已达最大切换次数 {}/{}，放弃切换".format(
                self._proxy_switch_count, self.proxy_max_switch))
            await self._ensure_browser_alive()
            return False

        print("  [PROXY] 代理失败，切换第 {}/{} 次...".format(
            self._proxy_switch_count, self.proxy_max_switch))

        if self.current_proxy:
            print("  [PROXY] 标记失效: {}".format(self.current_proxy))
            self._failed_proxies.add(self.current_proxy)
            if hasattr(self.proxy_pool, 'mark_failed'):
                try:
                    self.proxy_pool.mark_failed(self.current_proxy)
                except Exception:
                    pass
            elif hasattr(self.proxy_pool, 'mark_bad'):
                try:
                    self.proxy_pool.mark_bad(self.current_proxy)
                except Exception:
                    pass

        try:
            self.proxy_pool.refresh()
        except Exception:
            pass

        print("  [PROXY] 重启浏览器，切换代理...")
        await self._launch_browser()
        if self.current_proxy:
            print("  [PROXY] 新代理: {}".format(self.current_proxy))
        return True

    def reset_proxy_switch_count(self):
        self._proxy_switch_count = 0

    async def _dismiss_popups(self):
        try:
            if not self.page or self.page.is_closed():
                return
            if not self.browser or not self.browser.is_connected():
                return

            await self.page.evaluate("""() => {
                try {
                    if (typeof document === 'undefined' || !document) return;
                    if (!document.body) return;
                    const selectors = [
                        '[class*="modal"] [class*="close"]',
                        '[class*="popup"] [class*="close"]',
                        '[class*="overlay"] [class*="close"]',
                        '[aria-label="Close"]',
                        '[data-dismiss="modal"]',
                        'button[class*="close"]',
                        '.cookie-banner button',
                        '[class*="consent"] button'
                    ];
                    for (const sel of selectors) {
                        try {
                            const el = document.querySelector(sel);
                            if (el && typeof el.click === 'function') {
                                el.click();
                            }
                        } catch(e) {}
                    }
                    try {
                        const overlays = document.querySelectorAll('[class*="overlay"], [class*="modal-backdrop"], [class*="cookie"]');
                        if (overlays && overlays.length > 0) {
                            overlays.forEach(el => {
                                if (el && el.style) {
                                    el.style.display = 'none';
                                }
                            });
                        }
                    } catch(e) {}
                } catch(e) {}
            }""")
        except Exception as e:
            msg = str(e).split('\n')[0][:100]
            print("  [WARN] 关闭弹窗失败: {}".format(msg))

    async def _scroll_to_load(self):
        try:
            if not self.page or self.page.is_closed():
                return
            if not self.browser or not self.browser.is_connected():
                return

            await self.page.evaluate("""async () => {
                try {
                    if (typeof document === 'undefined' || !document) return;
                    if (!document.body) return;
                    const body = document.body;
                    const scrollHeight = body.scrollHeight || 0;
                    if (scrollHeight <= 0) return;
                    const viewportHeight = window.innerHeight || 800;
                    let currentPosition = 0;
                    const step = Math.floor(viewportHeight * 0.8);
                    const maxScrolls = 6;
                    let scrollCount = 0;
                    while (currentPosition < scrollHeight && scrollCount < maxScrolls) {
                        currentPosition += step;
                        window.scrollTo(0, currentPosition);
                        await new Promise(r => setTimeout(r, 300));
                        scrollCount++;
                    }
                    window.scrollTo(0, 0);
                } catch(e) {}
            }""")
        except Exception as e:
            msg = str(e).split('\n')[0][:100]
            print("  [WARN] 滚动触发懒加载失败: {}".format(msg))

    async def _get_page_size(self) -> Dict:
        default = {"width": 1920, "height": 1080}
        try:
            if not self.page or self.page.is_closed():
                return default
            if not self.browser or not self.browser.is_connected():
                return default
            size = await self.page.evaluate("""() => {
                try {
                    if (typeof document === 'undefined' || !document) return { width: 1920, height: 1080 };
                    var body = document.body;
                    var html = document.documentElement;
                    if (!body && !html) return { width: 1920, height: 1080 };
                    var w = Math.max(
                        (body ? (body.scrollWidth || 0) : 0),
                        (body ? (body.offsetWidth || 0) : 0),
                        (html ? (html.clientWidth || 0) : 0),
                        (html ? (html.scrollWidth || 0) : 0),
                        (html ? (html.offsetWidth || 0) : 0)
                    );
                    var h = Math.max(
                        (body ? (body.scrollHeight || 0) : 0),
                        (body ? (body.offsetHeight || 0) : 0),
                        (html ? (html.clientHeight || 0) : 0),
                        (html ? (html.scrollHeight || 0) : 0),
                        (html ? (html.offsetHeight || 0) : 0)
                    );
                    return { width: w || 1920, height: h || 1080 };
                } catch(e) { return { width: 1920, height: 1080 }; }
            }""")
            return size if isinstance(size, dict) else default
        except Exception as e:
            print("  [ERROR] 获取页面尺寸失败: {}".format(str(e).split('\n')[0][:100]))
            return default

    async def is_page_valid(self) -> bool:
        try:
            if not self.page or self.page.is_closed():
                return False
            if not self.browser or not self.browser.is_connected():
                return False
            result = await self.page.evaluate("""() => {
                try { return !!document && !!document.body; }
                catch(e) { return false; }
            }""")
            return bool(result)
        except Exception:
            return False

    async def fetch_page(self, url: str, attempt: int = 1) -> Tuple[str, bool]:
        strategy_idx = min(attempt - 1, len(self.WAIT_STRATEGIES) - 1)
        wait_until = self.WAIT_STRATEGIES[strategy_idx]
        print("  [INFO] 使用等待策略: {} (第{}次尝试)".format(wait_until, attempt))

        if self.current_proxy:
            print("  [PROXY] 当前代理: {}".format(self.current_proxy))

        try:
            await self._ensure_browser_alive()

            if attempt == 1 or self._just_rebuilt:
                self._just_rebuilt = False
                await self.page.goto(url, wait_until=wait_until, timeout=self.timeout)
            else:
                print("  [INFO] 尝试刷新页面...")
                try:
                    await self.page.reload(wait_until=wait_until, timeout=self.timeout)
                except Exception:
                    await self.page.goto(url, wait_until=wait_until, timeout=self.timeout)

            await asyncio.sleep(1.0)

            if await self.is_page_valid():
                await self._dismiss_popups()
                await self._scroll_to_load()
                await asyncio.sleep(0.5)
                await self._dismiss_popups()

            html_content = await self._get_page_content()

            if not html_content or len(html_content.strip()) < 100:
                print("  [WARN] 页面内容过少 ({}字节)".format(len(html_content) if html_content else 0))
                await asyncio.sleep(2.0)
                html_content = await self._get_page_content()
                if not html_content or len(html_content.strip()) < 100:
                    return ("", False)

            return (html_content, True)

        except PlaywrightError as e:
            error_msg = str(e)
            if "net::ERR_PROXY_AUTH_UNSUPPORTED" in error_msg or "ERR_PROXY_AUTH_UNSUPPORTED" in error_msg:
                print("  [ERROR] 代理不支持认证或不兼容: {}".format(error_msg.split('\n')[0][:100]))
                if self.use_proxy:
                    if self.current_proxy:
                        self._failed_proxies.add(self.current_proxy)
                        if hasattr(self.proxy_pool, 'mark_failed'):
                            try:
                                self.proxy_pool.mark_failed(self.current_proxy)
                            except Exception:
                                pass
                        elif hasattr(self.proxy_pool, 'mark_bad'):
                            try:
                                self.proxy_pool.mark_bad(self.current_proxy)
                            except Exception:
                                pass
                    await self._switch_proxy()
                    return ("", False)
            if "net::ERR_TIMED_OUT" in error_msg or "net::ERR_CONNECTION" in error_msg:
                print("  [ERROR] 网络错误: {}".format(error_msg.split('\n')[0][:80]))
                if self.use_proxy:
                    switched = await self._switch_proxy()
                    if not switched:
                        return ("", False)
            elif "Timeout" in error_msg or "timeout" in error_msg:
                print("  [ERROR] 超时 (策略: {})".format(wait_until))
                if self.use_proxy:
                    await self._switch_proxy()
            else:
                print("  [ERROR] Playwright错误: {}".format(error_msg.split('\n')[0][:120]))
            return ("", False)

        except Exception as e:
            print("  [ERROR] 未知错误: {}".format(str(e)[:120]))
            try:
                await self._ensure_browser_alive()
            except Exception:
                pass
            return ("", False)

    async def _get_page_content(self) -> str:
        try:
            if not self.page or self.page.is_closed():
                return ""

            has_body = await self.page.evaluate("""() => {
                try { return !!document && !!document.body; }
                catch(e) { return false; }
            }""")
            if not has_body:
                return ""

            content = await self.page.content()
            return content or ""
        except Exception as e:
            print("  [WARN] 获取页面内容失败: {}".format(str(e)[:120]))
            return ""

    async def capture_screenshot(self, save_path: str) -> Tuple[bool, Dict]:
        """
        全页截图
        Returns: (success, page_size_dict)
        """
        default_size = {"width": 1920, "height": 1080}
        # 每次截图前重置标记
        self.last_image_wait_timed_out = False

        try:
            await self._ensure_browser_alive()

            if not self.page or self.page.is_closed():
                print("  [WARN] 页面无效，无法截图")
                return (False, default_size)

            if not await self.is_page_valid():
                print("  [WARN] 页面body为空，无法截图")
                return (False, default_size)

            await self._dismiss_popups()

            # 等待图片加载：若超时，直接标记并返回失败（交给上层按超时处理）
            try:
                img_wait_timeout = max(4000, min(self.image_wait_timeout, 20000))
                await self.page.wait_for_function(
                    """() => {
                        try {
                            if (!document || !document.images) return true;
                            const imgs = Array.from(document.images);
                            if (imgs.length === 0) return true;
                            return imgs.every(i => i.complete === true && (typeof i.naturalWidth !== 'undefined' ? i.naturalWidth > 0 : true));
                        } catch(e) { return true; }
                    }""",
                    timeout=img_wait_timeout
                )
            except Exception as e:
                msg = str(e).split('\n')[0][:120]
                print("  [WARN] 等待图片加载超时/失败: {}".format(msg))
                # 核心改动：标记为“图片等待超时”，并立即返回失败
                self.last_image_wait_timed_out = True
                return (False, default_size)

            page_size = await self._get_page_size()

            width = min(page_size.get("width", 1920), 1920)
            height = min(page_size.get("height", 1080), 30000)

            try:
                await self.page.set_viewport_size({"width": width, "height": height})
            except Exception:
                try:
                    await self.page.set_viewport_size({"width": 1920, "height": 1080})
                except Exception:
                    pass

            await asyncio.sleep(0.25)

            try:
                await self.page.screenshot(path=save_path, full_page=True, timeout=30000)
                print("  [OK] 截图已保存: {}".format(save_path))
            except Exception as e:
                msg = str(e).split('\n')[0][:120]
                print("  [ERROR] 截图失败: {}".format(msg))
                if "Target page, context or browser has been closed" in msg:
                    await self._ensure_browser_alive()
                return (False, page_size)

            try:
                await self.page.set_viewport_size({"width": 1920, "height": 1080})
            except Exception:
                pass

            return (True, page_size)

        except Exception as e:
            msg = str(e).split('\n')[0][:120]
            print("  [ERROR] 截图失败: {}".format(msg))
            try:
                await self._ensure_browser_alive()
            except Exception:
                pass
            return (False, default_size)