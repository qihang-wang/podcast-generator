"""
GDELT 新闻数据获取主程序
从 BigQuery 获取 GDELT 数据并生成结构化报告
"""

import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# 导入数据解析模块
from gdelt_parser import process_narrative

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
KEY_PATH = str(_SCRIPT_DIR.parent.parent / 'gdelt_config' / 'my-gdelt-key.json')  # config 在项目根目录
PROJECT_ID = 'gdelt-analysis-480906'

# ================= 优化版 SQL - 使用分区表减少扫描成本 =================
# 关键优化:
# 1. 使用 gkg_partitioned 分区表而非 gkg
# 2. 使用 _PARTITIONTIME 伪列进行分区裁剪 (Partition Pruning)
# 3. 这样 BigQuery 只扫描指定日期分区的数据，而非全表
# 4. 预计扫描量从数百GB降到几GB
QUERY = """
SELECT
  GKGRECORDID,
  DATE,
  SourceCommonName,
  DocumentIdentifier AS SourceURL,
  CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64) AS AvgTone,
  V2Themes,
  V2Locations,
  V2Persons,
  V2Organizations,
  GCAM,
  Amounts,        
  Quotations,
  SocialImageEmbeds,
  SocialVideoEmbeds
FROM
  `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE
  -- 使用 _PARTITIONTIME 进行分区裁剪，只扫描今天的分区
  _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  -- 在分区内再按 DATE 字段精确过滤到最近2小时
  AND DATE >= CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)) AS INT64)
  AND (V2Themes LIKE '%ENV_CLIMATECHANGE%' OR V2Themes LIKE '%CRISIS%')
  AND ABS(CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64)) > 3
  AND Quotations IS NOT NULL
ORDER BY
  ABS(AvgTone) DESC
LIMIT 50
"""


def fetch_gdelt_data() -> pd.DataFrame:
    """
    从 BigQuery 获取 GDELT 数据
    
    Returns:
        包含 GDELT 数据的 DataFrame，如果失败则返回空 DataFrame
    """
    if not os.path.exists(KEY_PATH):
        print(f"错误: 找不到密钥文件 {KEY_PATH}")
        return pd.DataFrame()

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_PATH
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        print(f"[{datetime.now()}] 开始查询 BigQuery (使用分区表优化)...")
        
        # 预估查询成本
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_run_job = client.query(QUERY, job_config=job_config)
        bytes_processed = dry_run_job.total_bytes_processed
        gb_processed = bytes_processed / (1024**3)
        print(f"[预估扫描量] {gb_processed:.2f} GB")
        
        # 执行实际查询
        query_job = client.query(QUERY) 
        results = query_job.result()

        df = results.to_dataframe()
        print(f"[{datetime.now()}] 查询完成，获取到 {len(df)} 条记录。")
        return df
        
    except Exception as e:
        print(f"BigQuery 连接或查询错误: {e}")
        return pd.DataFrame()


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


def generate_news_with_llm(record: dict):
    """
    使用 LLM 生成新闻文本
    
    Args:
        record: 解析后的新闻记录字典
    """
    print("\n" + "="*60)
    print("🤖 正在使用 LLM 生成新闻文本...")
    print("="*60)
    
    print(f"\n📝 输入数据:")
    print(f"  - 标题: {record.get('Title')}")
    print(f"  - 来源: {record.get('Source_Name')}")
    print(f"  - 地点: {record.get('Locations')}")
    print(f"  - 主题: {record.get('Themes')}")
    
    try:
        news_text = generate_news_from_record(record)
        
        print(f"\n📰 生成的新闻文本:")
        print("-" * 60)
        print(news_text)
        print("-" * 60)
        
        return news_text
    except Exception as e:
        error_msg = f"LLM 生成失败: {str(e)}"
        print(f"\n⚠️ {error_msg}")
        return error_msg


def main():
    """主函数"""
    # raw_df = fetch_gdelt_data()  # 注释掉避免消耗 BigQuery 额度
    
    # 从现有 CSV 文件读取数据
    data_dir = _SCRIPT_DIR.parent.parent / '.data'
    raw_path = data_dir / "gdelt_raw_data.csv"
    if raw_path.exists():
        raw_df = pd.read_csv(raw_path)
        print(f"从本地文件加载数据: {raw_path}, 共 {len(raw_df)} 条记录")
    else:
        print(f"错误: 找不到数据文件 {raw_path}")
        return
    
    if not raw_df.empty:
        try:
            # 数据保存目录
            data_dir = _SCRIPT_DIR.parent.parent / '.data'
            data_dir.mkdir(exist_ok=True)
            
            # 保存原始数据
            raw_path = data_dir / "gdelt_raw_data.csv"
            raw_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
            print(f"原始数据已保存至: {raw_path}")
            
            # 处理数据
            narratives = raw_df.apply(process_narrative, axis=1).tolist()
            result_df = pd.DataFrame(narratives)
            
            # 打印预览
            print_preview(result_df, offset=0, count=10)
            
            # 保存结果
            filename = f"gdelt_report_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            report_path = data_dir / filename
            result_df.to_csv(report_path, index=False, encoding='utf-8-sig')
            print(f"\n✅ 完整报告已保存至: {report_path}")
            
            # 解析报告
            analyze_report(str(report_path))
            
            # ================= LLM 生成新闻 =================
            if narratives:
                # 取第0条到第10条数据进行新闻生成
                for i, record in enumerate(narratives[0:10], 1):
                    print(f"\n{'='*60}")
                    print(f"🤖 正在生成第 {i}/10 条新闻...")
                    print(f"{'='*60}")
                    generate_news_with_llm(record)
            else:
                print("\n⚠️ 没有可用的数据记录")
            
        except Exception as e:
            print(f"数据解析错误: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("未获取到数据，请检查 SQL 筛选条件或网络连接。")


if __name__ == "__main__":
    main()