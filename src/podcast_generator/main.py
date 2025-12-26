"""
GDELT 新闻数据获取主程序
"""

import json
import logging
import os
from gdelt.data_fetcher import fetch_gdelt_data
from gdelt.data_loader import load_gdelt_data
from gdelt_parse import parse_gdelt_article


def main():
    """主函数"""
    # 配置 logging
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/output.log", mode='w', encoding="utf-8"),  # mode='w' 覆盖旧内容
            logging.StreamHandler()  # 同时输出到终端
        ]
    )
    
    logging.info("=" * 60)
    logging.info("🚀 GDELT 新闻数据获取")
    logging.info("=" * 60)

    # GDELT 使用 FIPS 10-4 国家代码（非 ISO）:
    # CH=中国, JA=日本, KS=韩国, US=美国, RS=俄罗斯
    # UP=乌克兰, UK=英国, GM=德国, FR=法国, IN=印度
    # TW=台湾, AS=澳大利亚, CA=加拿大, BR=巴西, IS=以色列
    
    # fetch_gdelt_data(country_code="JA")  # 日本 FIPS 代码是 JA

    gkg_models, event_models = load_gdelt_data(country_code="CH")


    logging.info(f"\n加载完成: {len(gkg_models)} 篇文章, {len(event_models)} 个事件")
    
    # 建立 Event 映射
    events_dict = {e.global_event_id: e for e in event_models}
    
    # 逐条解析并打印 JSON
    logging.info("\n" + "=" * 60)
    logging.info("📝 解析后的 GDELT 数据")
    logging.info("=" * 60)
    
    for i, gkg in enumerate(gkg_models, 1):
        logging.info(f"\n--- 文章 [{i}] ---")
        event = events_dict.get(gkg.event_id)
        params = parse_gdelt_article(gkg, event)
        logging.info(json.dumps(params, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
