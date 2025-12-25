"""
GDELT 数据加载模块
从本地 CSV 文件加载已保存的 GDELT 数据

公开方法：
    - load_gdelt_data: 加载数据的唯一入口
"""

import os
import pandas as pd
from typing import List, Tuple

from .model import GKGModel, EventModel
from .gdelt_gkg import _row_to_gkg_model
from .gdelt_event import _row_to_event_model


# ========== 私有常量 ==========
_GDELT_CSV_DIR = os.path.join(os.path.dirname(__file__), "gdelt_csv")


def load_gdelt_data(country_code: str = None) -> Tuple[List[GKGModel], List[EventModel]]:
    """
    从本地 CSV 加载 GDELT 数据，返回 GKGModel 和 EventModel 列表
        
    这是加载本地数据的唯一公开入口。
    
    Args:
        country_code: 国家代码（如 "CH"），用于确定文件名
        
    Returns:
        (GKGModel 列表, EventModel 列表)
        
    Examples:
        # 加载中国相关数据
        gkg_models, event_models = load_gdelt_data(country_code="CH")
    """
    prefix = country_code.upper() if country_code else "default"
    gkg_path = os.path.join(_GDELT_CSV_DIR, f"{prefix}_gkg.csv")
    event_path = os.path.join(_GDELT_CSV_DIR, f"{prefix}_event.csv")
    
    gkg_models = []
    event_models = []
    
    # 加载 GKG 数据
    if os.path.exists(gkg_path):
        gkg_df = pd.read_csv(gkg_path, encoding='utf-8-sig')
        gkg_models = [_row_to_gkg_model(row) for _, row in gkg_df.iterrows()]
        print(f"✓ GKG 数据已加载: {prefix}_gkg.csv ({len(gkg_models)} 条)")
    else:
        print(f"⚠️ GKG 文件不存在: {prefix}_gkg.csv")
    
    # 加载 Event 数据（复用 gdelt_event 的转换函数）
    if os.path.exists(event_path):
        event_df = pd.read_csv(event_path, encoding='utf-8-sig')
        event_models = [_row_to_event_model(row) for _, row in event_df.iterrows()]
        print(f"✓ Event 数据已加载: {prefix}_event.csv ({len(event_models)} 条)")
    else:
        print(f"⚠️ Event 文件不存在: {prefix}_event.csv")
    
    # 打印文章信息
    if gkg_models:
        _print_article_info(gkg_models)
    
    return gkg_models, event_models


# ========== 私有方法 ==========

def _print_article_info(gkg_models: List[GKGModel]):
    """打印文章详细信息"""
    if not gkg_models:
        return
    
    print(f"\n📰 文章列表 ({len(gkg_models)} 篇)：")
    print("=" * 80)
    
    for i, gkg in enumerate(gkg_models, 1):
        print(f"\n   📄 [{i}] {gkg.article_title}")
        print(f"      EventID: {gkg.event_id} | 来源: {gkg.source_common_name}")
        print(f"      基调: {gkg.tone.avg_tone:.2f} | 主题: {', '.join(gkg.v2_themes[:3])}")
        
        if gkg.persons:
            print(f"      人物: {', '.join([p.name for p in gkg.persons[:3]])}")
        if gkg.organizations:
            print(f"      组织: {', '.join(gkg.organizations[:3])}")
        if gkg.quotations:
            print(f"      引语: {len(gkg.quotations)} 条")
    
    print("\n" + "=" * 80)
