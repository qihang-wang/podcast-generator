"""
GDELT 数据获取模块
从 BigQuery 获取 Event -> Mentions -> GKG 完整数据流

公开方法：
    - fetch_gdelt_data: 获取 GDELT 数据的唯一入口
    - fetch_gkg_data: 直接获取 GKG 数据
"""

import os
import logging
import pandas as pd
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Any, Optional

from .gdelt_service import GDELTQueryService
from .gdelt_mentions import select_best_mentions_per_event


# ========== 私有常量 ==========
_GDELT_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", ".gdelt_data")


def fetch_gdelt_data(location_name: str = None, country_code: str = None,
                     hours_back: int = None, date: str = None,
                     event_limit: int = 100, min_confidence: int = 80, max_sentence_id: int = 1):
    """获取 GDELT 完整数据（Event -> Mentions -> GKG）并保存到 CSV"""
    logging.info("\n" + "=" * 80)
    logging.info("🚀 开始 GDELT 数据获取")
    logging.info("=" * 80)
    
    service = GDELTQueryService()
    
    # Step 1: 获取事件
    logging.info(f"\n📍 步骤 1/3: 查询 Event 表")
    if date:
        logging.info(f"   参数: location={location_name or '不限'}, country={country_code or '不限'}, 日期={date}")
    else:
        logging.info(f"   参数: location={location_name or '不限'}, country={country_code or '不限'}, hours={hours_back or 24}h")
    
    events = service.query_events_by_location(
        location_name=location_name, country_code=country_code,
        hours_back=hours_back, date=date, limit=event_limit, print_progress=True
    )
    
    if not events:
        logging.warning("⚠️ 未找到符合条件的事件")
        return
    
    logging.info(f"✓ 找到 {len(events)} 个事件")
    
    # Step 2: 获取 Mentions
    logging.info(f"\n📰 步骤 2/3: 查询 Mentions 表")
    logging.info(f"   参数: Confidence>={min_confidence}%, SentenceID<={max_sentence_id}")
    
    event_ids = [e.global_event_id for e in events]
    all_mentions = service.query_mentions_by_event_ids(
        event_ids=event_ids,
        min_confidence=min_confidence,
        sentence_id=max_sentence_id,
        print_progress=True
    )
    
    if not all_mentions:
        logging.warning("⚠️ 未找到相关报道")
        return
    
    # 打印事件汇总
    _print_event_summary(events, all_mentions)
    
    # 筛选最佳报道
    all_mentions = select_best_mentions_per_event(all_mentions)
    
    # 筛选出与 mentions 相关的事件
    related_event_ids = set(m.global_event_id for m in all_mentions)
    related_events = [e for e in events if e.global_event_id in related_event_ids]
    logging.info(f"✓ 筛选出 {len(related_events)} 个相关事件")
    
    # Step 3: 获取 GKG 数据
    logging.info(f"\n🔍 步骤 3/3: 查询 GKG 表")
    
    mention_urls = [m.mention_identifier for m in all_mentions if m.mention_identifier]
    if not mention_urls:
        logging.warning("⚠️ 无有效 URL")
        return
    
    gkg_df = service.query_gkg_raw(mention_urls, print_progress=True)
    
    if gkg_df.empty:
        logging.warning("⚠️ 未获取到 GKG 数据")
        return
    
    # 建立 URL -> EventID 映射，添加到 GKG DataFrame
    url_to_event = {m.mention_identifier: m.global_event_id for m in all_mentions}
    gkg_df['event_id'] = gkg_df['DocumentIdentifier'].map(url_to_event)
    
    logging.info(f"✓ 获取到 {len(gkg_df)} 条 GKG 数据，已关联 event_id")
    
    # 保存到 CSV
    _save_gkg_to_csv(gkg_df, country_code)
    _save_events_to_csv(related_events, country_code)
    
    # 同步到数据库（如果启用）
    _sync_to_supabase(gkg_df, country_code)

    
    # 完成
    logging.info("\n" + "=" * 80)
    logging.info(f"✅ 完成！{len(related_events)} 个事件，{len(gkg_df)} 篇文章")
    logging.info("=" * 80 + "\n")


def fetch_gkg_data(country_code: str, hours_back: int = None, date: str = None,
                   themes: list = None, allowed_languages: list = None,
                   min_word_count: int = 200, limit: int = 20):
    """直接获取 GKG 数据并保存到 CSV"""
    logging.info("\n" + "=" * 80)
    logging.info("🚀 开始 GKG 数据直接获取")
    logging.info("=" * 80)
    
    if date:
        logging.info(f"\n📍 参数: country={country_code}, 日期={date}, limit={limit}")
    else:
        logging.info(f"\n📍 参数: country={country_code}, hours={hours_back or 24}h, limit={limit}")
    
    if themes:
        logging.info(f"   主题过滤: {themes}")
    if allowed_languages:
        logging.info(f"   语言过滤: {allowed_languages}")
    
    service = GDELTQueryService()
    
    logging.info(f"\n🔍 查询 GKG 表...")
    gkg_df = service.query_gkg_by_country(
        country_code=country_code, hours_back=hours_back, date=date,
        themes=themes, allowed_languages=allowed_languages,
        min_word_count=min_word_count, limit=limit, print_progress=True
    )
    
    if gkg_df.empty:
        logging.warning("⚠️ 未获取到 GKG 数据")
        return
    
    logging.info(f"✓ 获取到 {len(gkg_df)} 条 GKG 数据")
    
    # 先去重（确保保存和同步使用相同的去重后数据）
    gkg_df = _deduplicate_by_url(gkg_df)
    
    # 保存到 CSV
    _save_gkg_to_csv(gkg_df, country_code, skip_dedup=True)
    
    # 同步到数据库（如果启用）
    _sync_to_supabase(gkg_df, country_code)
    
    # 完成
    logging.info("\n" + "=" * 80)
    logging.info(f"✅ 完成！{len(gkg_df)} 篇文章")
    logging.info("=" * 80 + "\n")




# ========== 数据库同步 ==========

def _sync_to_supabase(gkg_df: pd.DataFrame, country_code: str):
    """
    将 GKG 数据同步到 Supabase（按时间排序存储）
    
    仅在 ENABLE_DATABASE_SYNC=true 时执行
    """
    try:
        from database import ArticleRepository
        from gdelt.gdelt_parse import parse_gdelt_article
        from .gdelt_gkg import _row_to_gkg_model
        
        repo = ArticleRepository()
        
        if not repo.is_sync_enabled():
            logging.debug("数据库同步未启用，跳过")
            return
        
        logging.info("\n📤 同步数据到 Supabase...")
        
        records = []
        for _, row in gkg_df.iterrows():
            gkg = _row_to_gkg_model(row)
            params = parse_gdelt_article(gkg, event=None, fetch_content=False)
            
            record = {
                "country_code": country_code.upper() if country_code else "UNKNOWN",
                "gkg_record_id": gkg.gkg_record_id,  # 包含时间戳，用于排序
                "date_added": gkg.date,  # GDELT 批次时间戳（用于查询过滤，与 BigQuery 一致）
                "source": params.get("source"),
                "url": params.get("url"),
                "persons": params.get("persons", []),
                "organizations": params.get("organizations", []),
                "themes": params.get("themes", []),
                "locations": params.get("locations", []),
                "quotations": params.get("quotations", []),
                "amounts": params.get("amounts", []),
                "tone": params.get("tone"),
                "emotion": params.get("emotion"),
                "emotion_instruction": params.get("emotion_instruction"),
                "event": params.get("event"),
                "images": gkg.image_embeds,  # 来自 SocialImageEmbeds
                "videos": gkg.video_embeds,  # 来自 SocialVideoEmbeds
            }
            records.append(record)
        
        # 批量插入
        count = repo.bulk_upsert(records)
        logging.info(f"✅ 已同步 {count} 条数据到 Supabase")
        
    except ImportError as e:
        logging.debug(f"数据库模块未安装: {e}")
    except Exception as e:
        logging.error(f"❌ Supabase 同步失败: {e}")


# ========== 私有方法 ==========

def _deduplicate_by_url(gkg_df: pd.DataFrame) -> pd.DataFrame:
    """
    基于 URL 去重，移除重复文章
    
    同一 URL 不应出现多次。
    """
    if 'DocumentIdentifier' not in gkg_df.columns:
        return gkg_df
    
    # 记录原始数量
    original_count = len(gkg_df)
    
    # 找出重复的记录（保留第一条，标记其余为重复）
    duplicates = gkg_df[gkg_df.duplicated(subset=['DocumentIdentifier'], keep='first')]
    
    # 打印被移除的文章信息
    if not duplicates.empty:
        logging.info(f"\n📋 去重: 移除 {len(duplicates)} 条重复文章")
        for _, row in duplicates.iterrows():
            source = row.get('SourceCommonName', 'N/A')
            url = row.get('DocumentIdentifier', 'N/A')[:60]  # 截断URL
            logging.info(f"   - [{source}] {url}...")
    
    # 精确匹配去重 - 保留第一条
    gkg_df = gkg_df.drop_duplicates(subset=['DocumentIdentifier'], keep='first')
    
    return gkg_df.reset_index(drop=True)


def _save_gkg_to_csv(gkg_df: pd.DataFrame, country_code: str = None, skip_dedup: bool = False) -> str:
    """保存 GKG DataFrame 到 CSV 文件
    
    Args:
        gkg_df: GKG DataFrame
        country_code: 国家代码
        skip_dedup: 是否跳过去重（如果外部已去重则设为 True）
    """
    os.makedirs(_GDELT_DATA_DIR, exist_ok=True)
    
    # 去重：基于标题去除相似文章（如果未跳过）
    if not skip_dedup:
        gkg_df = _deduplicate_by_url(gkg_df)
    
    if country_code:
        filename = f"{country_code.upper()}_gkg.csv"
    else:
        filename = "default_gkg.csv"
    
    file_path = os.path.join(_GDELT_DATA_DIR, filename)
    gkg_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    logging.info(f"✓ GKG 数据已保存: {filename} ({len(gkg_df)} 条)")
    
    return file_path



def _save_events_to_csv(events, country_code: str = None) -> str:
    """保存 EventModel 列表到 CSV 文件（使用 BigQuery 列名以便复用加载函数）"""
    os.makedirs(_GDELT_DATA_DIR, exist_ok=True)
    
    if country_code:
        filename = f"{country_code.upper()}_event.csv"
    else:
        filename = "default_event.csv"
    
    # 将 EventModel 转换为 DataFrame（使用 BigQuery 原始列名）
    rows = []
    for e in events:
        rows.append({
            'GLOBALEVENTID': e.global_event_id,
            'SQLDATE': e.sql_date,
            'Actor1Code': e.actor1.code,
            'Actor1Name': e.actor1.name,
            'Actor1CountryCode': e.actor1.country_code,
            'Actor1Type1Code': e.actor1.type1_code,
            'Actor2Code': e.actor2.code,
            'Actor2Name': e.actor2.name,
            'Actor2CountryCode': e.actor2.country_code,
            'Actor2Type1Code': e.actor2.type1_code,
            'EventCode': e.event_code,
            'EventBaseCode': e.event_base_code,
            'EventRootCode': e.event_root_code,
            'QuadClass': e.quad_class,
            'GoldsteinScale': e.goldstein_scale,
            'NumMentions': e.num_mentions,
            'NumSources': e.num_sources,
            'NumArticles': e.num_articles,
            'AvgTone': e.avg_tone,
            'ActionGeo_Type': e.action_geo.geo_type,
            'ActionGeo_FullName': e.action_geo.full_name,
            'ActionGeo_CountryCode': e.action_geo.country_code,
            'ActionGeo_ADM1Code': e.action_geo.adm1_code,
            'ActionGeo_Lat': e.action_geo.lat,
            'ActionGeo_Long': e.action_geo.long,
            'ActionGeo_FeatureID': e.action_geo.feature_id,
            'SOURCEURL': e.source_url,
            'DATEADDED': e.date_added,
        })
    
    df = pd.DataFrame(rows)
    file_path = os.path.join(_GDELT_DATA_DIR, filename)
    df.to_csv(file_path, index=False, encoding='utf-8-sig')
    logging.info(f"✓ Event 数据已保存: {filename} ({len(rows)} 条)")
    
    return file_path



def _print_event_summary(events, mentions):
    """打印事件汇总信息"""
    events_dict = {e.global_event_id: e for e in events}
    mentions_by_event = defaultdict(list)
    
    for mention in mentions:
        mentions_by_event[mention.global_event_id].append(mention)
    
    logging.info(f"\n📊 {len(mentions)} 条报道按事件分组：")
    for event_id, event_mentions in mentions_by_event.items():
        event = events_dict.get(event_id)
        if event:
            logging.info(f"   EventID {event_id} | "
                  f"QuadClass={event.quad_class} | "
                  f"EventCode={event.event_code} | "
                  f"{event.action_geo.full_name} | "
                  f"{event.actor1.name or event.actor1.code} → "
                  f"{event.actor2.name or event.actor2.code} | "
                  f"{len(event_mentions)} 条")
