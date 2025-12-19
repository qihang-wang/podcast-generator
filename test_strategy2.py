"""
测试策略2: 英文优先生成 + 翻译
"""
import sys
sys.path.insert(0, 'src/podcast_generator')

import pandas as pd
import pathlib

from llm_generator import LLMNewsGenerator
from gdelt_parser import process_narrative
from news_merger import merge_related_news

# 加载数据
data_dir = pathlib.Path('.data')
raw_path = data_dir / "gdelt_raw_data.csv"
raw_df = pd.read_csv(raw_path)
print(f"加载数据: {len(raw_df)} 条记录")

# 处理并合并数据
parsed = raw_df.apply(process_narrative, axis=1).tolist()
merged = merge_related_news(parsed)
print(f"合并后: {len(merged)} 条记录")

# 测试一条记录
if merged:
    record = merged[10]  # 取第11条
    print(f"\n📝 测试记录:")
    print(f"  - 标题: {record.get('Title')}")
    print(f"  - 来源: {record.get('Source_Name')}")
    
    generator = LLMNewsGenerator()
    
    # 测试中文生成（策略2: 先英文后翻译）
    print("\n" + "="*60)
    print("🧪 测试策略2: 英文生成 → 中文翻译")
    print("="*60)
    
    chinese_news = generator.generate_news(record, language="zh")
    
    print("\n✅ 最终中文新闻:")
    print("-"*60)
    print(chinese_news)
    print("-"*60)
