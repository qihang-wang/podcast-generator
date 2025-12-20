"""
GDELT 新闻数据获取主程序
从 BigQuery 获取 GDELT 数据并生成结构化报告
"""

import os
import pandas as pd
from datetime import datetime

# 导入 GDELT 数据获取模块
from gdelt_fetcher import fetch_gdelt_data, load_local_data, save_data

# 导入数据解析模块
from gdelt_parser import process_narrative

# 导入新闻合并模块
from news_merger import merge_related_news

# 导入报告解析模块
from parse_report import (
    parse_report,
    get_report_summary,
    search_reports,
    filter_by_criteria,
)

# 导入 LLM 新闻生成模块
from llm_generator import generate_news_from_record, LLMNewsGenerator


# ================= 配置区 =================
import pathlib
_SCRIPT_DIR = pathlib.Path(__file__).parent
KEY_PATH = str(_SCRIPT_DIR.parent.parent / 'gdelt_config' / 'my-gdelt-key.json')
PROJECT_ID = 'gdelt-analysis-480906'

# GDELT 查询配置已移至 gdelt_fetcher.py


def print_preview(result_df: pd.DataFrame):
    """打印全量报告预览"""
    total = len(result_df)
    print(f"\n📊 共 {total} 条记录")
    
    for i, row in result_df.iterrows():
        print(f"\n--- 记录 {i+1}/{total} ---")
        print(f"📌 标题: {row['Title']}")
        print(f"📰 来源: {row['Source_Name']}")
        print(f"📰 源URL: {row['Source_URL']}")
        print(f"🕐 时间: {row['Time']}")
        print(f"📍 地点: {row['Locations']}")
        print(f"🏢 机构: {row['Organizations']}")
        print(f"👤 人物: {row['Key_Persons']}")
        print(f"🎭 情感: {row['Emotions']}")
        print(f"📊 基调: {row['Tone']}")
        print(f"🏷️ 主题: {row['Themes']}")
        quotes = row['Quotes'][:500] if len(str(row['Quotes'])) > 500 else row['Quotes']
        print(f"💬 引用:\n{quotes}...")
        print(f"📈 数据: {row['Data_Facts']}")
        images = row['Images'][:100] if len(str(row['Images'])) > 100 else row['Images']
        print(f"🖼️ 图片: {images}...")
        summary = row['Article_Summary'][:1000] if len(str(row['Article_Summary'])) > 1000 else row['Article_Summary']
        print(f"📰 原文摘要:\n{summary}...")


def analyze_report(filename: str):
    """解析并分析生成的报告"""
    print("\n" + "="*60)
    print("📊 正在解析生成的报告...")
    print("="*60)
    
    try:
        report_result = parse_report(filename)
        summary = get_report_summary(filename)
        
        print(f"\n📋 报告摘要:")
        print(f"  - 文件名: {summary['file_name']}")
        print(f"  - 记录总数: {summary['record_count']}")
        print(f"  - 唯一来源数: {summary['source_count']}")
        
        print(f"\n🎭 情感分布:")
        for tone, count in summary['tone_stats'].items():
            if count > 0:
                percentage = summary.get('tone_percentages', {}).get(tone, 0)
                print(f"  - {tone}: {count} ({percentage}%)")
        
        print(f"\n📰 主要来源:")
        for source in summary['top_sources'][:5]:
            print(f"  - {source}")
        
        crisis_records = search_reports("crisis", filename)
        if crisis_records:
            print(f"\n🔍 包含 'crisis' 关键词的记录: {len(crisis_records)} 条")
        
        negative_records = filter_by_criteria(filename, tone="Negative")
        positive_records = filter_by_criteria(filename, tone="Positive")
        print(f"\n📈 情感筛选结果:")
        print(f"  - 负面报道: {len(negative_records)} 条")
        print(f"  - 正面报道: {len(positive_records)} 条")
        
        print("\n" + "="*60)
        print("✅ 报告解析完成！")
        print("="*60)
        
    except Exception as parse_error:
        print(f"⚠️ 报告解析时出现错误: {parse_error}")


def generate_bilingual_news(record: dict) -> tuple:
    """
    生成双语新闻（英文 + 中文）
    
    Args:
        record: 解析后的新闻记录字典
    
    Returns:
        (英文新闻, 中文新闻) 元组
    """
    print(f"\n📝 输入数据:")
    print(f"  - 标题: {record.get('Title')}")
    print(f"  - 来源: {record.get('Source_Name')}")
    locations = record.get('Locations', '')
    print(f"  - 地点: {locations[:80]}..." if len(locations) > 80 else f"  - 地点: {locations}")
    
    # 生成英文新闻
    print(f"\n🔤 生成英文新闻...")
    try:
        english_news = generate_news_from_record(record, language="en")
        print(f"\n📰 English News:")
        print("-" * 60)
        print(english_news)
        print("-" * 60)
    except Exception as e:
        english_news = f"[Error] {str(e)}"
        print(f"  ⚠️ 英文生成失败: {e}")
    
    # 生成中文新闻
    print(f"\n🔤 生成中文新闻...")
    try:
        chinese_news = generate_news_from_record(record, language="zh")
        print(f"\n📰 中文新闻:")
        print("-" * 60)
        print(chinese_news)
        print("-" * 60)
    except Exception as e:
        chinese_news = f"[Error] {str(e)}"
        print(f"  ⚠️ 中文生成失败: {e}")
    
    return english_news, chinese_news


def main():
    """主函数"""
    # 方式1: 从 BigQuery 获取数据（消耗额度，谨慎使用）
    # raw_df = fetch_gdelt_data(key_path=KEY_PATH, project_id=PROJECT_ID)
    
    # 方式2: 从本地文件读取数据
    data_dir = _SCRIPT_DIR.parent.parent / '.data'
    raw_path = data_dir / "gdelt_raw_data.csv"
    raw_df = load_local_data(str(raw_path))
    
    if raw_df.empty:
        print("错误: 找不到数据文件或数据为空")
        return
    
    try:
        # 数据保存目录
        data_dir = _SCRIPT_DIR.parent.parent / '.data'
        data_dir.mkdir(exist_ok=True)
        
        # 保存原始数据
        raw_path = data_dir / "gdelt_raw_data.csv"
        save_data(raw_df, str(raw_path))
        
        # 处理数据
        narratives = raw_df.apply(process_narrative, axis=1).tolist()
        
        # 合并相关/重复的新闻记录
        merged_narratives = merge_related_news(narratives, similarity_threshold=0.6)
        
        # 使用合并后的数据创建 DataFrame
        result_df = pd.DataFrame(merged_narratives)
        
        # 打印全量预览
        print_preview(result_df)
        
        # 保存结果
        filename = f"gdelt_report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        report_path = data_dir / filename
        save_data(result_df, str(report_path))
        
        # 解析报告
        analyze_report(str(report_path))
        
        # ================= LLM 生成双语新闻 =================
        print(f"\n{'='*60}")
        print(f"🤖 开始生成双语新闻（英文 + 中文）")
        print(f"📊 共 {len(merged_narratives)} 条新闻待生成")
        print(f"{'='*60}")
        
        if merged_narratives:
            all_news = []
            for i, record in enumerate(merged_narratives, 1):
                print(f"\n{'='*60}")
                print(f"🤖 正在生成第 {i}/{len(merged_narratives)} 条双语新闻...")
                print(f"{'='*60}")
                en_news, zh_news = generate_bilingual_news(record)
                all_news.append({
                    'title': record.get('Title'),
                    'source': record.get('Source_Name'),
                    'english': en_news,
                    'chinese': zh_news
                })
            
            # 汇总
            print(f"\n{'='*60}")
            print(f"✅ 双语新闻生成完成！")
            print(f"📊 共生成 {len(all_news)} 条双语新闻")
            print(f"{'='*60}")
        else:
            print("\n⚠️ 没有可用的数据记录")
        
    except Exception as e:
        print(f"数据解析错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()