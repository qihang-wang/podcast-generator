"""
GDELT 数据获取模块
从 BigQuery 获取 Event -> Mentions -> GKG 完整数据流

公开方法：
    - fetch_gdelt_data: 获取 GDELT 数据的唯一入口
"""

import os
import pandas as pd
from datetime import datetime
from collections import defaultdict


from .gdelt_service import GDELTQueryService
from .gdelt_mentions import select_best_mentions_per_event


# ========== 私有常量 ==========
_GKG_CSV_DIR = os.path.join(os.path.dirname(__file__), "gkg_csv")


def fetch_gdelt_data(
    location_name: str = None,
    country_code: str = None,
    hours_back: int = 24,
    event_limit: int = 100,
    min_confidence: int = 80,
    max_sentence_id: int = 1
):
    """
    获取 GDELT 完整数据并保存到 CSV 文件
    
    这是获取 GDELT 数据的唯一公开入口。获取完成后自动保存到本地 CSV。
    
    Args:
        location_name: 地点名称（模糊匹配）
        country_code: 国家代码（如 "CH" 表示中国）
        hours_back: 查询最近N小时的数据，默认24小时
        event_limit: 事件数量限制，默认100
        min_confidence: Mentions 最小置信度，默认80%
        max_sentence_id: 句子ID限制（1=仅导语），默认1
        
    Examples:
        # 获取中国相关事件
        fetch_gdelt_data(country_code="CH")
        
        # 获取北京相关事件
        fetch_gdelt_data(location_name="Beijing")
    """
    print("\n" + "=" * 80)
    print("🚀 开始 GDELT 数据获取")
    print("=" * 80)
    
    service = GDELTQueryService()
    
    # Step 1: 获取事件
    print(f"\n📍 步骤 1/3: 查询 Event 表")
    print(f"   参数: location={location_name or '不限'}, country={country_code or '不限'}, hours={hours_back}h")
    
    events = service.query_events_by_location(
        location_name=location_name,
        country_code=country_code,
        hours_back=hours_back,
        limit=event_limit,
        print_progress=True
    )
    
    if not events:
        print("⚠️ 未找到符合条件的事件")
        return
    
    print(f"✓ 找到 {len(events)} 个事件")
    
    # Step 2: 获取 Mentions
    print(f"\n📰 步骤 2/3: 查询 Mentions 表")
    print(f"   参数: Confidence>={min_confidence}%, SentenceID<={max_sentence_id}")
    
    event_ids = [e.global_event_id for e in events]
    all_mentions = service.query_mentions_by_event_ids(
        event_ids=event_ids,
        min_confidence=min_confidence,
        sentence_id=max_sentence_id,
        print_progress=True
    )
    
    if not all_mentions:
        print("⚠️ 未找到相关报道")
        return
    
    # 打印事件汇总
    _print_event_summary(events, all_mentions)
    
    # 筛选最佳报道
    all_mentions = select_best_mentions_per_event(all_mentions)
    
    # Step 3: 获取 GKG 数据
    print(f"\n🔍 步骤 3/3: 查询 GKG 表")
    
    mention_urls = [m.mention_identifier for m in all_mentions if m.mention_identifier]
    if not mention_urls:
        print("⚠️ 无有效 URL")
        return
    
    gkg_df = service.query_gkg_raw(mention_urls, print_progress=True)
    
    if gkg_df.empty:
        print("⚠️ 未获取到 GKG 数据")
        return
    
    print(f"✓ 获取到 {len(gkg_df)} 条 GKG 数据")
    
    # 保存到 CSV
    _save_to_csv(gkg_df, country_code)
    
    # 完成
    print("\n" + "=" * 80)
    print(f"✅ 完成！{len(events)} 个事件，{len(gkg_df)} 篇文章")
    print("=" * 80 + "\n")



# ========== 私有方法 ==========

def _save_to_csv(gkg_df: pd.DataFrame, country_code: str = None) -> str:
    """保存 DataFrame 到 CSV 文件，按 country_code 命名"""
    os.makedirs(_GKG_CSV_DIR, exist_ok=True)
    
    # 文件名按 country_code 命名，如 CH.csv, US.csv
    if country_code:
        filename = f"{country_code.upper()}.csv"
    else:
        filename = "default.csv"
    
    file_path = os.path.join(_GKG_CSV_DIR, filename)
    gkg_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"✓ 数据已保存: {filename} ({len(gkg_df)} 条)")
    
    return file_path



def _print_event_summary(events, mentions):
    """打印事件汇总信息"""
    events_dict = {e.global_event_id: e for e in events}
    mentions_by_event = defaultdict(list)
    
    for mention in mentions:
        mentions_by_event[mention.global_event_id].append(mention)
    
    print(f"\n📊 {len(mentions)} 条报道按事件分组：")
    for event_id, event_mentions in mentions_by_event.items():
        event = events_dict.get(event_id)
        if event:
            print(f"   EventID {event_id} | "
                  f"QuadClass={event.quad_class} | "
                  f"EventCode={event.event_code} | "
                  f"{event.action_geo.full_name} | "
                  f"{event.actor1.name or event.actor1.code} → "
                  f"{event.actor2.name or event.actor2.code} | "
                  f"{len(event_mentions)} 条")
