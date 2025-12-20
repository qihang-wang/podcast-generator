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

# 新闻生成范围配置（支持分批处理）
NEWS_START_INDEX = 0   # 起始索引（从0开始）
NEWS_END_INDEX = 5    # 结束索引（不包含）


def process_and_generate(record: dict, index: int, total: int) -> dict:
    """
    打印单条数据预览并生成双语新闻
    将预览和生成放在一起，更直观
    """
    print(f"\n{'='*60}")
    print(f"📰 第 {index}/{total} 条新闻")
    print(f"{'='*60}")
    
    # === 完整数据预览 ===
    print(f"\n📋 原始数据:")
    print(f"  📌 标题: {record.get('Title')}")
    print(f"  📰 来源: {record.get('Source_Name')}")
    print(f"  � 源URL: {record.get('Source_URL')}")
    print(f"  🕐 时间: {record.get('Time')}")
    print(f"  �📍 地点: {record.get('Locations')}")
    print(f"  🏢 机构: {record.get('Organizations')}")
    print(f"  👤 人物: {record.get('Key_Persons')}")
    print(f"  🎭 情感: {record.get('Emotions')}")
    print(f"  📊 基调: {record.get('Tone')}")
    print(f"  🏷️ 主题: {record.get('Themes')}")
    
    quotes = record.get('Quotes', '')
    quotes = quotes[:500] if len(str(quotes)) > 500 else quotes
    print(f"  💬 引用:\n{quotes}...")
    
    print(f"  📈 数据: {record.get('Data_Facts')}")
    
    images = record.get('Images', '')
    images = images[:100] if len(str(images)) > 100 else images
    print(f"  🖼️ 图片: {images}...")
    
    summary = record.get('Article_Summary', '')
    summary = summary[:1000] if len(str(summary)) > 1000 else summary
    print(f"  📰 原文摘要:\n{summary}...")
    
    # === 生成英文新闻 ===
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
    
    # === 生成中文新闻 ===
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
    
    return {
        'title': record.get('Title'),
        'source': record.get('Source_Name'),
        'english': english_news,
        'chinese': chinese_news
    }


def main():
    """主函数"""
    data_dir = _SCRIPT_DIR.parent.parent / '.data'
    raw_path = data_dir / "gdelt_raw_data.csv"
    raw_df = load_local_data(str(raw_path))
    
    if raw_df.empty:
        print("错误: 找不到数据文件或数据为空")
        return
    
    try:
        data_dir.mkdir(exist_ok=True)
        save_data(raw_df, str(raw_path))
        
        # 处理和合并数据
        narratives = raw_df.apply(process_narrative, axis=1).tolist()
        merged_narratives = merge_related_news(narratives, similarity_threshold=0.6)
        
        # 保存报告
        result_df = pd.DataFrame(merged_narratives)
        filename = f"gdelt_report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        report_path = data_dir / filename
        save_data(result_df, str(report_path))
        
        # ================= LLM 生成双语新闻 =================
        total_count = len(merged_narratives)
        start_idx = min(NEWS_START_INDEX, total_count)
        end_idx = min(NEWS_END_INDEX, total_count)
        news_to_generate = merged_narratives[start_idx:end_idx]
        
        print(f"\n{'='*60}")
        print(f"🤖 开始生成双语新闻")
        print(f"📊 总记录: {total_count} 条，生成范围: [{start_idx}, {end_idx})")
        print(f"{'='*60}")
        
        if news_to_generate:
            all_news = []
            for i, record in enumerate(news_to_generate, 1):
                result = process_and_generate(record, i, len(news_to_generate))
                all_news.append(result)
            
            print(f"\n{'='*60}")
            print(f"✅ 完成！共生成 {len(all_news)} 条双语新闻")
            print(f"{'='*60}")
        else:
            print("\n⚠️ 没有可用的数据")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
