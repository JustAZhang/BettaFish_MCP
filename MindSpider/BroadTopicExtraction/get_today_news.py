#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BroadTopicExtraction模块 - 新闻获取和收集
整合新闻API调用和数据库存储功能
"""

import sys
import asyncio
import httpx
import json
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional
import os
from loguru import logger

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from BroadTopicExtraction.database_manager import DatabaseManager
except ImportError as e:
    raise ImportError(f"导入模块失败: {e}")

# 新闻API基础URL
BASE_URL = "https://newsnow.busiyi.world"

# 深度抓取平台配置（严格白名单）
PLATFORM_REGISTRY = {
    "wallstreetcn-hot": {
        "name": "华尔街见闻 最热",
        "entry_url": "https://wallstreetcn.com/news/hot",
        "feed_url": f"{BASE_URL}/api/s?id=wallstreetcn-hot&latest",
        "requires_login": False,
    },
    "wallstreetcn-quick": {
        "name": "华尔街见闻 快讯",
        "entry_url": "https://wallstreetcn.com/live",
        "feed_url": f"{BASE_URL}/api/s?id=wallstreetcn-quick&latest",
        "requires_login": False,
    },
    "wallstreetcn-news": {
        "name": "华尔街见闻 最新",
        "entry_url": "https://wallstreetcn.com/news",
        "feed_url": f"{BASE_URL}/api/s?id=wallstreetcn-news&latest",
        "requires_login": False,
    },
    "cls-hot": {
        "name": "财联社热门",
        "entry_url": "https://www.cls.cn/telegraph",
        "feed_url": f"{BASE_URL}/api/s?id=cls-hot&latest",
        "requires_login": False,
    },
    "gelonghui": {
        "name": "格隆汇",
        "entry_url": "https://www.gelonghui.com/live",
        "feed_url": f"{BASE_URL}/api/s?id=gelonghui&latest",
        "requires_login": False,
    },
    "xueqiu": {
        "name": "雪球",
        "entry_url": "https://xueqiu.com/",
        "feed_url": f"{BASE_URL}/api/s?id=xueqiu&latest",
        "requires_login": True,
        "cookie_env": "XUEQIU_COOKIE",
    },
    "jin10": {
        "name": "金十数据",
        "entry_url": "https://www.jin10.com/",
        "feed_url": f"{BASE_URL}/api/s?id=jin10&latest",
        "requires_login": False,
    },
    "fastbull": {
        "name": "快讯通",
        "entry_url": "https://www.fastbull.com/",
        "feed_url": f"{BASE_URL}/api/s?id=fastbull&latest",
        "requires_login": False,
    },
    "weibo": {
        "name": "微博",
        "entry_url": "https://weibo.com/",
        "feed_url": f"{BASE_URL}/api/s?id=weibo&latest",
        "requires_login": True,
        "cookie_env": "WEIBO_COOKIE",
    },
    "zhihu": {
        "name": "知乎",
        "entry_url": "https://www.zhihu.com/",
        "feed_url": f"{BASE_URL}/api/s?id=zhihu&latest",
        "requires_login": True,
        "cookie_env": "ZHIHU_COOKIE",
    },
}

PLATFORM_WHITELIST = tuple(PLATFORM_REGISTRY.keys())

# 新闻源中文名称映射
SOURCE_NAMES = {platform_id: info["name"] for platform_id, info in PLATFORM_REGISTRY.items()}

class NewsCollector:
    """新闻收集器 - 整合API调用和数据库存储"""
    
    def __init__(self):
        """初始化新闻收集器"""
        self.db_manager = DatabaseManager()
        self.supported_sources = list(PLATFORM_WHITELIST)
    
    def close(self):
        """关闭资源"""
        if self.db_manager:
            self.db_manager.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # ==================== 新闻API调用 ====================
    
    def _filter_sources(self, sources: List[str]) -> List[str]:
        """过滤非白名单平台"""
        valid_sources = []
        for source in sources:
            if source in PLATFORM_WHITELIST:
                valid_sources.append(source)
            else:
                logger.warning(f"⚠️ 跳过非白名单平台: {source}")
        return valid_sources

    def _require_login(self, platform_id: str) -> bool:
        """检查平台是否需要登录且提供了Cookie"""
        platform_config = PLATFORM_REGISTRY.get(platform_id)
        if not platform_config or not platform_config.get("requires_login"):
            return True
        cookie_env = platform_config.get("cookie_env")
        cookie_value = os.getenv(cookie_env or "")
        if cookie_value:
            return True
        logger.warning(
            f"⚠️ 平台 {platform_id} 需要登录/需要 cookie，未配置账号无法抓取。"
        )
        return False

    async def fetch_news(self, source: str) -> dict:
        """从指定源获取最新新闻"""
        platform_config = PLATFORM_REGISTRY.get(source)
        if not platform_config:
            return {
                "source": source,
                "status": "skipped",
                "error": f"非白名单平台已跳过: {source}",
                "timestamp": datetime.now().isoformat()
            }

        if not self._require_login(source):
            return {
                "source": source,
                "status": "login_required",
                "error": f"平台 {source} 需要登录/需要 cookie，未配置账号无法抓取",
                "timestamp": datetime.now().isoformat()
            }

        url = platform_config.get("feed_url") or platform_config["entry_url"]
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Referer": platform_config["entry_url"],
            "Connection": "keep-alive",
        }
        
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                
                # 解析JSON响应
                data = response.json()
                return {
                    "source": source,
                    "status": "success",
                    "data": data,
                    "timestamp": datetime.now().isoformat()
                }
        except httpx.TimeoutException:
            return {
                "source": source,
                "status": "timeout",
                "error": f"请求超时: {source}({url})",
                "timestamp": datetime.now().isoformat()
            }
        except httpx.HTTPStatusError as e:
            return {
                "source": source,
                "status": "http_error",
                "error": f"HTTP错误: {source}({url}) - {e.response.status_code}",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "source": source,
                "status": "error",
                "error": f"未知错误: {source}({url}) - {str(e)}",
                "timestamp": datetime.now().isoformat()
            }
    
    async def get_popular_news(self, sources: List[str] = None) -> List[dict]:
        """获取热门新闻"""
        if sources is None:
            sources = list(PLATFORM_WHITELIST)
        sources = self._filter_sources(sources)
        
        logger.info(f"正在获取 {len(sources)} 个新闻源的最新内容...")
        logger.info("=" * 80)
        
        results = []
        for source in sources:
            if source not in PLATFORM_WHITELIST:
                logger.warning(f"⚠️ 平台不在白名单中，跳过: {source}")
                continue
            source_name = SOURCE_NAMES.get(source, source)
            logger.info(f"正在获取 {source_name} 的新闻...")
            result = await self.fetch_news(source)
            results.append(result)
            
            if result["status"] == "success":
                data = result["data"]
                if 'items' in data and isinstance(data['items'], list):
                    count = len(data['items'])
                    logger.info(f"✓ {source_name}: 获取成功，共 {count} 条新闻")
                else:
                    logger.info(f"✓ {source_name}: 获取成功")
            else:
                logger.error(f"✗ {source_name}: {result.get('error', '获取失败')}")
            
            # 避免请求过快
            await asyncio.sleep(0.5)
        
        return results
    
    # ==================== 数据处理和存储 ====================
    
    async def collect_and_save_news(self, sources: Optional[List[str]] = None) -> Dict:
        """
        收集并保存每日热点新闻
        
        Args:
            sources: 指定的新闻源列表，None表示使用所有支持的源
            
        Returns:
            包含收集结果的字典
        """
        collection_summary_message = ""
        collection_summary_message += "\n开始收集每日热点新闻...\n"
        collection_summary_message += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        # 选择新闻源
        if sources is None:
            # 使用所有支持的新闻源
            sources = list(PLATFORM_WHITELIST)
        sources = self._filter_sources(sources)
        if not sources:
            logger.error("未找到可用的白名单平台，终止抓取")
            return {
                'success': False,
                'error': '未找到可用的白名单平台',
                'news_list': [],
                'total_news': 0,
                'successful_sources': 0,
                'total_sources': 0,
                'collection_time': datetime.now().isoformat()
            }
        
        collection_summary_message += f"将从 {len(sources)} 个新闻源收集数据:\n"
        for source in sources:
            source_name = SOURCE_NAMES.get(source, source)
            collection_summary_message += f"  - {source_name}\n"
        
        logger.info(collection_summary_message)
        
        try:
            # 获取新闻数据
            results = await self.get_popular_news(sources)
            
            # 处理结果
            processed_data = self._process_news_results(results)
            
            # 保存到数据库（覆盖模式）
            if processed_data['news_list']:
                saved_count = self.db_manager.save_daily_news(
                    processed_data['news_list'], 
                    date.today()
                )
                processed_data['saved_count'] = saved_count
            
            # 打印统计信息
            self._print_collection_summary(processed_data)
            
            return processed_data
            
        except Exception as e:
            logger.exception(f"收集新闻失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'news_list': [],
                'total_news': 0
            }
    
    def _process_news_results(self, results: List[Dict]) -> Dict:
        """处理新闻获取结果"""
        news_list = []
        successful_sources = 0
        total_news = 0
        
        for result in results:
            source = result['source']
            status = result['status']
            
            if status == 'success':
                successful_sources += 1
                data = result['data']
                
                if 'items' in data and isinstance(data['items'], list):
                    source_news_count = len(data['items'])
                    total_news += source_news_count
                    
                    # 处理该源的新闻
                    for i, item in enumerate(data['items'], 1):
                        processed_news = self._process_news_item(item, source, i)
                        if processed_news:
                            news_list.append(processed_news)
        
        return {
            'success': True,
            'news_list': news_list,
            'successful_sources': successful_sources,
            'total_sources': len(results),
            'total_news': total_news,
            'collection_time': datetime.now().isoformat()
        }
    
    def _process_news_item(self, item: Dict, source: str, rank: int) -> Optional[Dict]:
        """处理单条新闻"""
        try:
            if isinstance(item, dict):
                title = item.get('title', '无标题').strip()
                url = item.get('url', '')
                
                # 生成新闻ID
                news_id = f"{source}_{item.get('id', f'rank_{rank}')}"
                
                return {
                    'id': news_id,
                    'title': title,
                    'url': url,
                    'source': source,
                    'rank': rank
                }
            else:
                # 处理字符串类型的新闻
                title = str(item)[:100] if len(str(item)) > 100 else str(item)
                return {
                    'id': f"{source}_rank_{rank}",
                    'title': title,
                    'url': '',
                    'source': source,
                    'rank': rank
                }
                
        except Exception as e:
            logger.exception(f"处理新闻项失败: {e}")
            return None
    
    def _print_collection_summary(self, data: Dict):
        """打印收集摘要"""
        collection_summary_message = ""
        collection_summary_message += f"\n总新闻源: {data['total_sources']}\n"
        collection_summary_message += f"成功源数: {data['successful_sources']}\n"
        collection_summary_message += f"总新闻数: {data['total_news']}\n"
        if 'saved_count' in data:
            collection_summary_message += f"已保存数: {data['saved_count']}\n"
        logger.info(collection_summary_message)
    
    def get_today_news(self) -> List[Dict]:
        """获取今天的新闻"""
        try:
            return self.db_manager.get_daily_news(date.today())
        except Exception as e:
            logger.exception(f"获取今日新闻失败: {e}")
            return []

async def main():
    """测试新闻收集器"""
    logger.info("测试新闻收集器...")
    
    async with NewsCollector() as collector:
        # 收集新闻
        result = await collector.collect_and_save_news(
            sources=["weibo", "zhihu"]  # 测试用，只使用两个源
        )
        
        if result['success']:
            logger.info(f"收集成功！共获取 {result['total_news']} 条新闻")
        else:
            logger.error(f"收集失败: {result.get('error', '未知错误')}")

if __name__ == "__main__":
    asyncio.run(main())
