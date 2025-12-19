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

# 新闻生成语言配置: "zh" = 中文, "en" = 英文
NEWS_LANGUAGE = "zh"  # 可选: "zh" 或 "en"

# GDELT 查询配置已移至 gdelt_fetcher.py


def print_preview(result_df: pd.DataFrame, offset: int = 0, count: int = 5):
    """
    打印报告预览
    
    Args:
        result_df: 结果 DataFrame
        offset: 预览记录起始位置
        count: 预览记录数量
    """

    for i, row in result_df.iloc[offset:offset+count].iterrows():
        print(f"\n--- 记录 {i+1} ---")
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
        print(f"💬 引用:\n{row['Quotes'][:500]}...")
        print(f"📈 数据: {row['Data_Facts']}")
        print(f"🖼️ 图片: {row['Images'][:100]}...")
        print(f"📰 原文摘要:\n{row['Article_Summary'][:1000]}...")


def analyze_report(filename: str):
    """
    解析并分析生成的报告
    
    Args:
        filename: 报告文件路径
    """
    print("\n" + "="*60)
    print("📊 正在解析生成的报告...")
    print("="*60)
    
    try:
        # 解析报告
        report_result = parse_report(filename)
        
        # 获取摘要信息
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
        
        # 搜索示例：查找包含特定关键词的记录
        crisis_records = search_reports("crisis", filename)
        if crisis_records:
            print(f"\n🔍 包含 'crisis' 关键词的记录: {len(crisis_records)} 条")
        
        # 按情感筛选示例
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


def generate_news_with_llm(record: dict, language: str = "zh"):
    """
    使用 LLM 生成新闻文本
    
    Args:
        record: 解析后的新闻记录字典
        language: 语言代码，"zh" 为中文，"en" 为英文
    """
    lang_name = "英文" if language == "en" else "中文"
    print("\n" + "="*60)
    print(f"🤖 正在使用 LLM 生成{lang_name}新闻文本...")
    print("="*60)
    
    print(f"\n📝 输入数据:")
    print(f"  - 标题: {record.get('Title')}")
    print(f"  - 来源: {record.get('Source_Name')}")
    print(f"  - 地点: {record.get('Locations')}")
    print(f"  - 主题: {record.get('Themes')}")
    
    try:
        news_text = generate_news_from_record(record, language=language)
        return news_text
    except Exception as e:
        error_msg = f"LLM 生成失败: {str(e)}"
        print(f"\n⚠️ {error_msg}")
        return error_msg


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
        
        # 打印预览
        print_preview(result_df, offset=0, count=10)
        
        # 保存结果
        filename = f"gdelt_report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        report_path = data_dir / filename
        save_data(result_df, str(report_path))
        
        # 解析报告
        analyze_report(str(report_path))
        
        # ================= LLM 生成新闻 =================
        print(f"\n📝 新闻生成语言: {'英文' if NEWS_LANGUAGE == 'en' else '中文'}")
        if merged_narratives:
            # 取前10条合并后的数据进行新闻生成
            news_count = min(10, len(merged_narratives))
            for i, record in enumerate(merged_narratives[0:news_count], 1):
                print(f"\n{'='*60}")
                print(f"🤖 正在生成第 {i}/{news_count} 条新闻...")
                print(f"{'='*60}")
                generate_news_with_llm(record, language=NEWS_LANGUAGE)
        else:
            print("\n⚠️ 没有可用的数据记录")
        
    except Exception as e:
        print(f"数据解析错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()