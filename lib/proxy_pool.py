"""
代理池模块
从 server_page_v3.py 提取，供 PlaywrightScraper 等模块调用

提供：
  - fetch_proxies(num)     : 从代理 API 获取一批代理
  - build_pw_proxy(info)   : 构造 Playwright 用的 proxy dict
  - build_requests_proxy(info): 构造 requests 用的 proxies dict
  - ProxyPool              : 代理池管理类（自动轮换、补充）
"""

import time
import logging
import requests
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

# ── 代理 API 配置（与 server_page_v3.py 保持一致）──────────────────────────
PROXY_API_URL   = "http://8.218.184.135:3334/getips"
PROXY_API_TOKEN = "kdNJ5dzn6FKTwwyZf4rkjRm4beR1DBFBfE4pm497XCxH6fMskeKEYWxXkY9tAJ"
PROXY_TYPE      = 4   # 4 -> 青果代理（美国IP）


# ── 纯函数 ──────────────────────────────────────────────────────────────────

def fetch_proxies(num: int = 5) -> List[Dict]:
    """
    从代理 API 获取一批代理

    Args:
        num: 期望获取的代理数量

    Returns:
        代理信息列表，每项格式：
        {"server": "host:port", "username": "...", "password": "..."}
        失败时返回空列表
    """
    params = {
        "token": PROXY_API_TOKEN,
        "num":   num,
        "type":  PROXY_TYPE,
    }
    for attempt in range(3):
        try:
            resp = requests.get(PROXY_API_URL, params=params, timeout=15)
            data = resp.json()
            if data.get("code") is not False and data.get("proxy"):
                proxies = data["proxy"]
                logger.info("代理池：获取到 %d 个代理", len(proxies))
                return proxies
            else:
                logger.warning("代理池：API 返回异常: %s", data)
        except Exception as e:
            logger.warning("代理池：第%d次获取失败: %s", attempt + 1, e)
        time.sleep(2)
    logger.error("代理池：连续3次获取失败，返回空列表")
    return []


def build_pw_proxy(proxy_info: Dict) -> Dict:
    """
    构造 Playwright 用的 proxy 字典

    Args:
        proxy_info: {"server": "host:port", "username": "u", "password": "p"}

    Returns:
        {"server": "http://host:port", "username": "u", "password": "p"}
    """
    server = proxy_info.get("server", "")
    u = proxy_info.get("username", "")
    p = proxy_info.get("password", "")
    pw = {"server": "http://{}".format(server)}
    if u and p:
        pw["username"] = u
        pw["password"] = p
    return pw


def build_requests_proxy(proxy_info: Dict) -> Dict:
    """
    构造 requests 用的 proxies 字典

    Args:
        proxy_info: {"server": "host:port", "username": "u", "password": "p"}

    Returns:
        {"http": "http://u:p@host:port", "https": "..."}
    """
    server = proxy_info.get("server", "")
    u = proxy_info.get("username", "")
    p = proxy_info.get("password", "")
    if u and p:
        url = "http://{}:{}@{}".format(u, p, server)
    else:
        url = "http://{}".format(server)
    return {"http": url, "https": url}


# ── 代理池管理类 ─────────────────────────────────────────────────────────────

class ProxyPool:
    """
    代理池管理器

    - 懒加载：首次调用 get() 时才从 API 拉取
    - 自动轮换：池耗尽时自动补充
    - 失败剔除：标记失效代理后自动换下一个

    使用示例::

        pool = ProxyPool(pool_size=5)
        proxy_info = pool.get()          # 取一个代理
        pw_proxy   = pool.get_pw_proxy() # 取一个并转为 Playwright 格式
        pool.report_bad(proxy_info)      # 标记当前代理失效，自动换下一个
    """

    def __init__(
        self,
        pool_size: int = 5,
        enabled: bool = True,
    ):
        """
        Args:
            pool_size: 每次从 API 拉取的代理数量
            enabled  : False 时所有方法返回 None（直连模式）
        """
        self.pool_size = pool_size
        self.enabled   = enabled
        self._pool: List[Dict] = []
        self._bad:  List[str]  = []   # 失效 server 列表

    # ── 公开接口 ─────────────────────────────────────────────────────────

    def get(self) -> Optional[Dict]:
        """
        取一个可用代理

        Returns:
            代理信息字典，禁用或无可用代理时返回 None
        """
        if not self.enabled:
            return None
        self._ensure_pool()
        return self._pool[0] if self._pool else None

    def get_pw_proxy(self) -> Optional[Dict]:
        """取一个代理并转为 Playwright proxy 格式"""
        info = self.get()
        return build_pw_proxy(info) if info else None

    def report_bad(self, proxy_info: Optional[Dict]):
        """
        标记一个代理为失效，从池中移除

        Args:
            proxy_info: 之前通过 get() 获取的代理信息
        """
        if not proxy_info:
            return
        server = proxy_info.get("server", "")
        if server:
            self._bad.append(server)
            logger.info("代理池：标记失效 %s", server)
        # 从池中移除该代理
        self._pool = [p for p in self._pool if p.get("server") != server]
        logger.info("代理池：剩余 %d 个可用代理", len(self._pool))

    def refresh(self) -> int:
        """
        强制刷新代理池

        Returns:
            新池中的代理数量
        """
        self._pool = []
        self._ensure_pool()
        return len(self._pool)

    def is_empty(self) -> bool:
        """代理池是否为空"""
        return len(self._pool) == 0

    def __len__(self) -> int:
        return len(self._pool)

    # ── 内部方法 ─────────────────────────────────────────────────────────

    def _ensure_pool(self):
        """确保池非空，不足时自动补充"""
        if self._pool:
            return
        logger.info("代理池：补充代理（请求 %d 个）...", self.pool_size)
        new_proxies = fetch_proxies(num=self.pool_size)
        # 过滤掉已知失效的代理
        self._pool = [
            p for p in new_proxies
            if p.get("server", "") not in self._bad
        ]
        if not self._pool:
            logger.warning("代理池：补充失败或全部失效，当前为空")
        else:
            logger.info("代理池：补充完成，当前 %d 个", len(self._pool))