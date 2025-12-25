"""
GDELT 新闻数据获取主程序
"""

from gdelt.data_fetcher import fetch_gdelt_data
from gdelt.data_loader import load_gdelt_data


def main():
    """主函数"""
    print("=" * 60)
    print("🚀 GDELT 新闻数据获取")
    print("=" * 60)
    
    # 方式1：从 BigQuery 获取新数据（自动保存到 CSV）
    fetch_gdelt_data(country_code="CH")
    
    # 方式2：从本地 CSV 加载已保存的数据
    gkg_models, event_models = load_gdelt_data(country_code="CH")
    print(f"\n加载完成: {len(gkg_models)} 篇文章, {len(event_models)} 个事件")



if __name__ == "__main__":
    main()
