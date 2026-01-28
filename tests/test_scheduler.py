"""
测试定时任务

手动触发 daily_maintenance 函数，验证清理和刷新逻辑
将刷新所有 10 个国家的昨天数据
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-7s %(message)s')

from podcast_generator.api.scheduler import daily_maintenance

if __name__ == "__main__":
    print("=" * 60)
    print("测试定时任务 - 每日维护（10个国家）")
    print("=" * 60)
    
    daily_maintenance()
    
    print("=" * 60)
    print("测试完成！")
    print("=" * 60)
