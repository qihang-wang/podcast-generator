"""
GDELT 数据加载模块
从本地 CSV 文件加载已保存的 GDELT 数据

公开方法：
    - load_gdelt_data: 加载数据的唯一入口
"""

import os
import pandas as pd
from typing import List, Optional


from .model import GKGModel
from .gdelt_gkg import _row_to_gkg_model


# ========== 私有常量 ==========
_GKG_CSV_DIR = os.path.join(os.path.dirname(__file__), "gkg_csv")


def load_gdelt_data(filename: str = None, country_code: str = None) -> List[GKGModel]:
    """
    从本地 CSV 加载 GDELT 数据并转换为 GKGModel
        
    这是加载本地数据的唯一公开入口。加载后自动打印文章信息。
    
    Args:
        filename: 文件名或完整路径。不指定则自动加载最新文件
        country_code: 国家代码过滤（如 "CH"），用于筛选文件
        
    Returns:
        GKGModel 列表
        
    Examples:
        # 加载最新数据
        models = load_gdelt_data()
        
        # 加载中国相关数据
        models = load_gdelt_data(country_code="CH")
        
        # 加载指定文件
        models = load_gdelt_data("gkg_CH_20251225_150000_gkg.csv")
    """
    # 确定文件路径
    file_path = _resolve_file_path(filename, country_code)
    if not file_path:
        return []
    
    # 加载 CSV
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"✓ 数据已加载: {os.path.basename(file_path)} ({len(df)} 条)")
    
    # 转换为 Model
    gkg_models = [_row_to_gkg_model(row) for _, row in df.iterrows()]
    
    # 打印文章信息
    _print_article_info(gkg_models)
    
    return gkg_models


# ========== 私有方法 ==========

def _resolve_file_path(filename: str = None, country_code: str = None) -> Optional[str]:
    """解析文件路径"""
    if not os.path.exists(_GKG_CSV_DIR):
        print(f"⚠️ 数据目录不存在: {_GKG_CSV_DIR}")
        return None
    
    # 如果指定了文件名，直接使用
    if filename:
        file_path = filename if os.path.isabs(filename) else os.path.join(_GKG_CSV_DIR, filename)
        if not os.path.exists(file_path):
            print(f"⚠️ 文件不存在: {file_path}")
            return None
        return file_path
    
    # 按 country_code 查找文件，如 CH.csv
    if country_code:
        file_path = os.path.join(_GKG_CSV_DIR, f"{country_code.upper()}.csv")
        if os.path.exists(file_path):
            print(f"📂 加载文件: {country_code.upper()}.csv")
            return file_path
        else:
            print(f"⚠️ 文件不存在: {country_code.upper()}.csv")
            return None
    
    # 默认文件
    default_path = os.path.join(_GKG_CSV_DIR, "default.csv")
    if os.path.exists(default_path):
        print(f"📂 加载文件: default.csv")
        return default_path
    
    print("⚠️ 未找到数据文件")
    return None



def _print_article_info(gkg_models: List[GKGModel]):
    """打印文章详细信息"""
    if not gkg_models:
        print("⚠️ 无数据")
        return
    
    print(f"\n📰 文章列表 ({len(gkg_models)} 篇)：")
    print("=" * 80)
    
    for i, gkg in enumerate(gkg_models, 1):
        print(f"\n   📄 [{i}] {gkg.article_title}")
        print(f"      来源: {gkg.source_common_name} | 作者: {gkg.authors or '未知'}")
        print(f"      基调: {gkg.tone.avg_tone:.2f} | 主题: {', '.join(gkg.v2_themes[:3])}")
        
        if gkg.persons:
            print(f"      人物: {', '.join([p.name for p in gkg.persons[:3]])}")
        if gkg.organizations:
            print(f"      组织: {', '.join(gkg.organizations[:3])}")
        if gkg.quotations:
            print(f"      引语: {len(gkg.quotations)} 条")
    
    print("\n" + "=" * 80)
