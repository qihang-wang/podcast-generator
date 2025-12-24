"""
GDELT 数据获取模块
实现完整的 Event -> Mentions -> GKG 数据获取流程
"""

from typing import List, Dict, Any
from datetime import datetime

from gdelt.query_service import GDELTQueryService
from gdelt.model import EventModel, MentionsModel, GKGModel


def fetch_complete_gdelt_data(
    location_name: str = None,
    country_code: str = None,
    hours_back: int = 24,
    event_limit: int = 100,
    min_confidence: int = 90,  # 严格过滤：只保留高置信度记录
    max_sentence_id: int = 1   # 句子ID限制：1=仅导语首句, 3=导语段落
) -> List[Dict[str, Any]]:
    """
    完整的 GDELT 数据获取流程
    
    流程：
    1. 确定目标事件：在 Event 表中锁定 GlobalEventID
    2. 寻找报道链条：在 Mentions 表中通过 GLOBALEVENTID 找到所有 MentionIdentifier (URL)
    3. 提取详尽元数据：将 URL 与 GKG 表的 DocumentIdentifier 联结，提取深度分析数据
    
    Args:
        location_name: 地点名称
        country_code: 国家代码
        hours_back: 查询时间范围（小时）
        event_limit: 事件数量限制
        min_confidence: Mentions 最小置信度
        
    Returns:
        包含完整数据的字典列表，每个字典包含：
        - event: EventModel（事件数据）
        - mentions: List[MentionsModel]（报道数据）
        - gkg_data: List[GKGModel]（深度分析数据）
    """
    print("\n" + "=" * 100)
    print("🚀 开始 GDELT 完整数据获取流程")
    print("=" * 100)
    
    service = GDELTQueryService()
    
    # ========== 步骤 1: 确定目标事件 ==========
    print(f"\n📍 步骤 1/3: 从 Event 表锁定目标事件")
    print(f"   参数: location={location_name or '不限'}, country={country_code or '不限'}, hours={hours_back}h")
    
    events = service.query_events_by_location(
        location_name=location_name,
        country_code=country_code,
        hours_back=hours_back,
        limit=event_limit,
        print_progress=True
    )
    
    if not events:
        print("⚠️  未找到符合条件的事件")
        return []
    
    print(f"\n✓ 找到 {len(events)} 个事件")
    
    # ========== 步骤 2: 寻找报道链条 ==========
    print(f"\n📰 步骤 2/3: 从 Mentions 表查找所有相关报道")
    print(f"   参数: Confidence>={min_confidence}% 且 SentenceID<={max_sentence_id}（{'仅导语首句' if max_sentence_id == 1 else '导语段落'}）")
    
    event_ids = [e.global_event_id for e in events]
    all_mentions = service.query_mentions_by_event_ids(
        event_ids=event_ids,
        min_confidence=min_confidence,
        sentence_id=max_sentence_id,
        print_progress=True
    )
    
    if not all_mentions:
        print("⚠️  未找到相关报道")
        return []
    
    print(f"\n✓ 找到 {len(all_mentions)} 条报道")
    
    # ========== 筛选：每个事件只保留最佳报道 ==========
    from gdelt.gdelt_mentions import select_best_mentions_per_event
    
    # 建立事件映射并筛选
    events_dict = {e.global_event_id: e for e in events}
    all_mentions = select_best_mentions_per_event(all_mentions, events_dict=events_dict, print_stats=True)
    
    # ========== 步骤 3: 提取详尽元数据 ==========
    print(f"\n🔍 步骤 3/3: 从 GKG 表提取深度分析数据")
    
    mention_urls = [m.mention_identifier for m in all_mentions if m.mention_identifier]
    
    if mention_urls:
        gkg_data = service.query_gkg_by_mention_urls(mention_urls, print_progress=True)
        print(f"\n✓ 获取到 {len(gkg_data)} 条 GKG 深度分析数据")
        gkg_by_url = {gkg.document_identifier: gkg for gkg in gkg_data}
    else:
        gkg_by_url = {}
    
    # ========== 组装完整数据 ==========
    print(f"\n📦 组装完整数据...")
    
    # 建立 event_id -> mention 映射
    mentions_by_event = {m.global_event_id: m for m in all_mentions}
    
    # 组装结果（同时记录URL关联的事件）
    results = []
    url_to_events = {}  # 记录每个URL关联的事件ID
    
    for event in events:
        mention = mentions_by_event.get(event.global_event_id)
        if not mention:
            continue
        
        # 获取对应的GKG数据
        gkg = gkg_by_url.get(mention.mention_identifier)
        
        # 记录URL关联的事件
        if mention.mention_identifier:
            if mention.mention_identifier not in url_to_events:
                url_to_events[mention.mention_identifier] = []
            url_to_events[mention.mention_identifier].append(event.global_event_id)
        
        results.append({
            'event': event,
            'mentions': [mention],  # 只有1条最佳报道
            'gkg_data': [gkg] if gkg else []
        })
    
    # 打印文章信息
    for i, (url, gkg) in enumerate(gkg_by_url.items(), 1):
        related_events = url_to_events.get(url, [])
        
        print(f"\n   📄 文章 {i}: {gkg.article_title}")
        print(f"      URL: {gkg.document_identifier}")
        print(f"      来源: {gkg.source_common_name}")
        print(f"      作者: {gkg.authors or '未知'}")
        print(f"      关联事件: {', '.join(map(str, related_events))}")
        print(f"      情感基调: {gkg.tone.avg_tone:.2f}")
        print(f"      主题: {', '.join(gkg.v2_themes[:5])}")
        print(f"      人物: {', '.join([p.name for p in gkg.persons[:5]])}")
        print(f"      组织: {', '.join(gkg.organizations[:5])}")
        if gkg.quotations:
            print(f"      引语数: {len(gkg.quotations)}")
    
    print("\n" + "=" * 100)
    print(f"✅ 完成！共获取 {len(results)} 个事件，{len(gkg_by_url)} 篇唯一文章")
    print("=" * 100 + "\n")
    
    return results


def print_detailed_summary(results: List[Dict[str, Any]]):
    """
    打印详细数据摘要
    
    Args:
        results: fetch_complete_gdelt_data 的返回结果
    """
    print("\n" + "=" * 100)
    print("📊 详细数据摘要")
    print("=" * 100)
    
    for i, result in enumerate(results, 1):
        event: EventModel = result['event']
        mentions: List[MentionsModel] = result['mentions']
        gkg_data: List[GKGModel] = result['gkg_data']
        
        print(f"\n【事件 {i}】EventID: {event.global_event_id}")
        print(f"  📍 地点: {event.action_geo.full_name}")
        print(f"  🎭 参与方: {event.actor1.name or event.actor1.code} ➔ {event.actor2.name or event.actor2.code}")
        print(f"  📊 Goldstein: {event.goldstein_scale} | QuadClass: {event.quad_class}")
        print(f"  💬 提及次数: {event.num_mentions} | 来源数: {event.num_sources}")
        
        print(f"\n  📰 报道详情 ({len(mentions)} 条):")
        for j, mention in enumerate(mentions[:3], 1):  # 只显示前3条
            print(f"     {j}. {mention.mention_source_name} | 置信度: {mention.confidence}%")
            print(f"        URL: {mention.mention_identifier[:80]}...")
        if len(mentions) > 3:
            print(f"     ... 以及其他 {len(mentions) - 3} 条报道")
        
        print(f"\n  🔍 深度分析 ({len(gkg_data)} 条):")
        for j, gkg in enumerate(gkg_data[:2], 1):  # 只显示前2条
            print(f"     {j}. {gkg.article_title[:60]}")
            print(f"        作者: {gkg.authors or '未知'}")
            print(f"        主题: {', '.join(gkg.v2_themes[:5])}")
            print(f"        人物: {', '.join([p.name for p in gkg.persons[:5]])}")
            print(f"        组织: {', '.join(gkg.organizations[:3])}")
            print(f"        情感基调: {gkg.tone.avg_tone:.2f}")
        if len(gkg_data) > 2:
            print(f"     ... 以及其他 {len(gkg_data) - 2} 条分析")
    
    print("\n" + "=" * 100 + "\n")


# ========== 使用示例 ==========
if __name__ == "__main__":
    # 示例：获取中国最近24小时的5个事件
    results = fetch_complete_gdelt_data(
        country_code="CH"
    )
