import aiohttp
from typing import List, Dict

async def fetch_gdelt_data(query: str) -> List[Dict]:
    """异步请求 GDELT/GKG 数据库 API (假设使用 RESTful 接口)"""
    # 实际应替换为 GDELT 的 API URL 和认证逻辑
    api_url = f"https://gdelt-api.example.com/query?q={query}"
    
    # aiohttp.ClientSession 确保非阻塞 I/O
    async with aiohttp.ClientSession() as session: 
        async with session.get(api_url) as response:
            # 在等待网络响应时，控制权被释放，允许其他任务运行 [19]
            response.raise_for_status() 
            data = await response.json()
            return data