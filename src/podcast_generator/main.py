"""
GDELT 新闻数据获取主程序
"""

import json
from gdelt.data_fetcher import fetch_gdelt_data
from gdelt.data_loader import load_gdelt_data
from gdelt_parse import parse_gdelt_article


def main():
    """主函数"""
    print("=" * 60)
    print("🚀 GDELT 新闻数据获取")
    print("=" * 60)
    
    # 方式1：从 BigQuery 获取新数据（自动保存到 CSV）
    # fetch_gdelt_data(country_code="CH")
    
    # 方式2：从本地 CSV 加载已保存的数据
    gkg_models, event_models = load_gdelt_data(country_code="CH")
    print(f"\n加载完成: {len(gkg_models)} 篇文章, {len(event_models)} 个事件")
    
    # 建立 Event 映射
    events_dict = {e.global_event_id: e for e in event_models}
    
    # 逐条解析并打印 JSON
    print("\n" + "=" * 60)
    print("📝 解析后的 GDELT 数据")
    print("=" * 60)
    
    for i, gkg in enumerate(gkg_models, 1):
        event = events_dict.get(gkg.event_id)
        params = parse_gdelt_article(gkg, event)
        
        print(f"\n--- 文章 [{i}] ---")
        print(json.dumps(params, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
