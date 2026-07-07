#name=lib/playwright_scraper.py
"""
Playwright 抓取与截图（资源受控 + 长页面优化增强版）

说明：
- 不改变上层业务逻辑接口：
  - fetch_page(url, attempt) -> (html_content, success)
  - capture_screenshot(save_path) -> (success, page_size)
  - last_image_wait_timed_out 语义保持
- 优化长页面等待图片策略：
  - 分阶段等待（DOM就绪 -> 渐进滚动触发懒加载 -> 可见区优先判定 -> 全量兜底）
  - 动态预算（基于页面高度与图片规模，带上限）
  - 超时时输出更可调试信息
"""

import asyncio
import random
import time
from typing import Dict, Optional, List, Tuple

from playwright.async_api import async_playwright, Browser, BrowserContext, Page


class PlaywrightScraper:
    def __init__(
        self,
        timeout: int = 45000,
        headless: bool = True,
        use_proxy: bool = False,
        proxy_pool_size: int = 5,
        proxy_max_switch: int = 3,
        min_launch_interval: float = 4.0,
        image_wait_timeout: int = 24000,
    ):
        self.timeout = timeout
        self.headless = headless
        self.use_proxy = use_proxy
        self.proxy_pool_size = proxy_pool_size
        self.proxy_max_switch = proxy_max_switch
        self.min_launch_interval = min_launch_interval
        self.image_wait_timeout = image_wait_timeout

        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

        self._last_launch_ts = 0.0
        self._proxy_switch_count = 0

        # 上层依赖这个字段判断是否按超时处理
        self.last_image_wait_timed_out = False

    def get_wait_strategies(self) -> List[str]:
        return [
            "domcontentloaded",
            "networkidle(soft)",
            "progressive-scroll-lazyload",
            "visible-images-priority-check",
            "global-images-fallback-check",
        ]

    def reset_proxy_switch_count(self):
        self._proxy_switch_count = 0

    async def start(self):
        now = time.time()
        elapsed = now - self._last_launch_ts
        if elapsed < self.min_launch_interval:
            await asyncio.sleep(self.min_launch_interval - elapsed)

        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-features=site-per-process,IsolateOrigins",
            ],
        )

        context_kwargs = {
            "viewport": {"width": 1366, "height": 900},
            "ignore_https_errors": True,
        }

        self.context = await self.browser.new_context(**context_kwargs)
        self.context.set_default_timeout(self.timeout)
        self.context.set_default_navigation_timeout(self.timeout)

        self.page = await self.context.new_page()

        # 资源控制：拦截明显非必要资源
        async def route_handler(route):
            req = route.request
            rtype = req.resource_type
            url = req.url.lower()

            if rtype in ("font", "media"):
                await route.abort()
                return

            if any(x in url for x in [".mp4", ".avi", ".mov", ".webm", ".woff", ".woff2", ".ttf", ".otf"]):
                await route.abort()
                return

            await route.continue_()

        await self.page.route("**/*", route_handler)

        self._last_launch_ts = time.time()

    async def stop(self):
        try:
            if self.page:
                await self.page.close()
        except Exception:
            pass
        self.page = None

        try:
            if self.context:
                await self.context.close()
        except Exception:
            pass
        self.context = None

        try:
            if self.browser:
                await self.browser.close()
        except Exception:
            pass
        self.browser = None

        try:
            if self.playwright:
                await self.playwright.stop()
        except Exception:
            pass
        self.playwright = None

    async def fetch_page(self, url: str, attempt: int) -> Tuple[str, bool]:
        if not self.page:
            return "", False

        try:
            self.last_image_wait_timed_out = False

            await self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout)

            try:
                await self.page.wait_for_load_state("networkidle", timeout=min(8000, self.timeout))
            except Exception:
                pass

            await asyncio.sleep(0.3 + random.random() * 0.4)

            html = await self.page.content()
            return html, True
        except Exception as e:
            print("  [WARN] 页面抓取失败(attempt={}): {}".format(attempt, str(e)[:220]))
            return "", False

    async def _get_page_metrics(self) -> Dict:
        if not self.page:
            return {"width": 1366, "height": 900, "scroll_height": 0, "img_total": 0, "img_complete": 0}

        js = """
() => {
  const de = document.documentElement || {};
  const body = document.body || {};
  const width = Math.max(de.clientWidth || 0, body.clientWidth || 0, window.innerWidth || 1366);
  const height = Math.max(de.clientHeight || 0, body.clientHeight || 0, window.innerHeight || 900);
  const scrollHeight = Math.max(
    de.scrollHeight || 0,
    body.scrollHeight || 0,
    de.offsetHeight || 0,
    body.offsetHeight || 0
  );

  const imgs = Array.from(document.images || []);
  const validImgs = imgs.filter(img => {
    const src = (img.currentSrc || img.src || "").toLowerCase();
    if (!src) return false;
    if (src.startsWith("data:image/")) return false;  // 内联图不作为网络加载判定
    if (src.startsWith("blob:")) return false;        // blob 图不稳定
    if (src.endsWith(".svg")) return false;           // svg 通常很快，不作为关键判定
    // 排除极小 tracking 像素图
    const w = img.naturalWidth || img.width || 0;
    const h = img.naturalHeight || img.height || 0;
    if (w > 0 && h > 0 && w <= 2 && h <= 2) return false;
    return true;
  });

  let complete = 0;
  for (const img of validImgs) {
    if (img.complete && img.naturalWidth > 0) complete++;
  }

  return {
    width, height, scroll_height: scrollHeight,
    img_total: validImgs.length, img_complete: complete
  };
}
"""
        try:
            return await self.page.evaluate(js)
        except Exception:
            return {"width": 1366, "height": 900, "scroll_height": 0, "img_total": 0, "img_complete": 0}

    async def _progressive_scroll_trigger(self, max_steps: int = 12):
        """渐进滚动触发懒加载（低频、受控）"""
        if not self.page:
            return

        try:
            metrics = await self._get_page_metrics()
            total_h = max(1400, int(metrics.get("scroll_height", 0) or 0))
            vp_h = max(700, int(metrics.get("height", 900) or 900))

            # 步长偏大，减少次数控CPU
            step_px = max(600, int(vp_h * 1.05))
            steps = min(max_steps, max(3, total_h // step_px + 1))

            for i in range(steps):
                y = min(total_h, i * step_px)
                await self.page.evaluate("(y) => window.scrollTo(0, y)", y)
                await asyncio.sleep(0.22)

            # 到底部短暂停顿，给懒加载一点机会
            await asyncio.sleep(0.35)

            # 回到顶部，保证 full_page 截图起点稳定
            await self.page.evaluate("() => window.scrollTo(0, 0)")
            await asyncio.sleep(0.18)

        except Exception:
            pass

    async def _image_state(self) -> Dict:
        """返回可见区与全量图片加载状态"""
        if not self.page:
            return {
                "ok_visible": False, "ok_global": False,
                "visible_total": 0, "visible_complete": 0, "visible_ratio": 0.0,
                "global_total": 0, "global_complete": 0, "global_ratio": 0.0,
                "visible_target": 0.85, "global_target": 0.70
            }

        js = """
() => {
  const vh = window.innerHeight || 900;
  const imgs = Array.from(document.images || []);

  const valid = imgs.filter(img => {
    const src = (img.currentSrc || img.src || "").toLowerCase();
    if (!src) return false;
    if (src.startsWith("data:image/")) return false;
    if (src.startsWith("blob:")) return false;
    if (src.endsWith(".svg")) return false;
    const w = img.naturalWidth || img.width || 0;
    const h = img.naturalHeight || img.height || 0;
    if (w > 0 && h > 0 && w <= 2 && h <= 2) return false;
    return true;
  });

  const isComplete = (img) => !!(img.complete && img.naturalWidth > 0);

  const visible = valid.filter(img => {
    const r = img.getBoundingClientRect();
    // 可见区 + 1.2屏缓冲（首屏优先）
    return r.bottom >= -50 && r.top <= (vh * 2.2);
  });

  let vc = 0;
  for (const i of visible) if (isComplete(i)) vc++;

  let gc = 0;
  for (const i of valid) if (isComplete(i)) gc++;

  const vt = visible.length;
  const gt = valid.length;

  const vr = vt === 0 ? 1 : (vc / vt);
  const gr = gt === 0 ? 1 : (gc / gt);

  // 阈值策略：可见区更严格，全量适中
  const visible_target = vt <= 8 ? 0.9 : 0.85;
  const global_target  = gt <= 12 ? 0.75 : 0.65;

  return {
    ok_visible: vr >= visible_target,
    ok_global: gr >= global_target,

    visible_total: vt,
    visible_complete: vc,
    visible_ratio: vr,

    global_total: gt,
    global_complete: gc,
    global_ratio: gr,

    visible_target,
    global_target
  };
}
"""
        try:
            return await self.page.evaluate(js)
        except Exception:
            return {
                "ok_visible": False, "ok_global": False,
                "visible_total": 0, "visible_complete": 0, "visible_ratio": 0.0,
                "global_total": 0, "global_complete": 0, "global_ratio": 0.0,
                "visible_target": 0.85, "global_target": 0.70
            }

    async def _wait_images_progressive(self, base_timeout_ms: int) -> bool:
        """
        分阶段等待图片：
        - Stage1: 快速轮询（优先看可见区）
        - Stage2: 渐进滚动触发懒加载后继续轮询
        返回 True 表示达标，False 表示超时或未达标
        """
        if not self.page:
            return False

        metrics = await self._get_page_metrics()
        scroll_h = int(metrics.get("scroll_height", 0) or 0)
        img_total = int(metrics.get("img_total", 0) or 0)

        # 动态预算（受控上限 24s）
        # 长页面 + 大图量页面加少量预算，减少误超时
        extra_h = min(7000, max(0, (scroll_h // 1800) * 500))
        extra_i = min(5000, max(0, (img_total // 25) * 700))
        budget_ms = min(24000, max(base_timeout_ms, 7000) + extra_h + extra_i)

        start = time.time()

        # Stage1: 先短等一段
        stage1_s = min(3.8, budget_ms / 1000.0 * 0.30)
        stage1_deadline = start + stage1_s
        last_state = None

        while time.time() < stage1_deadline:
            st = await self._image_state()
            last_state = st
            if st["ok_visible"] and st["ok_global"]:
                return True
            await asyncio.sleep(0.20)

        # Stage2: 触发懒加载
        await self._progressive_scroll_trigger(max_steps=12)

        final_deadline = start + (budget_ms / 1000.0)
        while time.time() < final_deadline:
            st = await self._image_state()
            last_state = st
            if st["ok_visible"] and st["ok_global"]:
                return True
            await asyncio.sleep(0.26)

        if last_state:
            print(
                "  [WARN] 等待图片加载超时/失败: "
                "visible={}/{}, ratio={:.2f}(<{:.2f}), "
                "global={}/{}, ratio={:.2f}(<{:.2f}), budget={}ms".format(
                    last_state.get("visible_complete", -1),
                    last_state.get("visible_total", -1),
                    float(last_state.get("visible_ratio", 0.0)),
                    float(last_state.get("visible_target", 0.85)),
                    last_state.get("global_complete", -1),
                    last_state.get("global_total", -1),
                    float(last_state.get("global_ratio", 0.0)),
                    float(last_state.get("global_target", 0.70)),
                    budget_ms,
                )
            )
        else:
            print("  [WARN] 等待图片加载超时/失败: 无法获取图片状态, budget={}ms".format(budget_ms))

        return False

    async def capture_screenshot(self, save_path: str) -> Tuple[bool, Dict]:
        """
        返回: (success, page_size)
        语义保持：
        - 图片等待超时 => last_image_wait_timed_out=True 且 success=False
        """
        if not self.page:
            return False, {"width": 1366, "height": 900}

        try:
            self.last_image_wait_timed_out = False

            ok = await self._wait_images_progressive(self.image_wait_timeout)
            if not ok:
                self.last_image_wait_timed_out = True
                return False, {"width": 1366, "height": 900}

            metrics = await self._get_page_metrics()
            page_size = {
                "width": int(metrics.get("width", 1366) or 1366),
                "height": int(metrics.get("height", 900) or 900),
                "scroll_height": int(metrics.get("scroll_height", 0) or 0),
            }

            await self.page.screenshot(
                path=save_path,
                full_page=True,
                type="png",
                timeout=min(45000, self.timeout),
            )
            return True, page_size

        except Exception as e:
            print("  [WARN] 截图失败: {}".format(str(e)[:240]))
            return False, {"width": 1366, "height": 900}
