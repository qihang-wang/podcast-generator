"""
GDELT 新闻数据获取主程序
"""

import json
import logging
import os
from gdelt.data_fetcher import fetch_gdelt_data, fetch_gkg_data
from gdelt.data_loader import load_gdelt_data
from gdelt.gdelt_parse import parse_gdelt_article
from llm.llm_generator import generate_news_from_record


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
    logging.info("🚀 GDELT 新闻数据获取与生成")
    logging.info("=" * 60)

    # GDELT 使用 FIPS 10-4 国家代码（非 ISO）:
    # CH=中国, JA=日本, KS=韩国, US=美国, RS=俄罗斯
    # UP=乌克兰, UK=英国, GM=德国, FR=法国, IN=印度
    # TW=台湾, AS=澳大利亚, CA=加拿大, BR=巴西, IS=以色列
    
    fetch_gkg_data(country_code="CH")  # 美国 FIPS 代码是 US

    gkg_models, event_models = load_gdelt_data(country_code="CH")


    logging.info(f"\n加载完成: {len(gkg_models)} 篇文章, {len(event_models)} 个事件")
    
    # 建立 Event 映射
    events_dict = {e.global_event_id: e for e in event_models}
    
    # 逐条解析并生成新闻
    for i, gkg in enumerate(gkg_models, 1):
        logging.info(f"\n\n\n")
        logging.info(f"----------------------------------- 文章 [{i}] -----------------------------------")
        event = events_dict.get(gkg.event_id)
        params = parse_gdelt_article(gkg, event)
        
        logging.info("📋 原始参数:")
        logging.info(json.dumps(params, ensure_ascii=False, indent=2))
        
        # 检查摘要是否有效，无效则跳过LLM生成（正文暂不使用）
        article_content = params.get("article_content", {})
        summary_valid = article_content.get("summary_valid", False)
        
        if not summary_valid:
            logging.warning(f"⚠️ 跳过文章 [{i}]: 摘要无效")
            logging.warning(f"   - URL: {params.get('url', 'N/A')}")
            logging.warning(f"   - 来源: {params.get('source', 'N/A')}")
            logging.warning(f"   - 错误: {article_content.get('error', '未知')}")
            logging.info("-" * 40)
            continue
        
        # 生成中文新闻
        logging.info("🤖 正在生成中文新闻...")
        news_zh = generate_news_from_record(params, language="zh")
        logging.info("📰 中文新闻:")
        logging.info(news_zh)
        
        # 生成英文新闻
        logging.info("🤖 正在生成英文新闻...")
        news_en = generate_news_from_record(params, language="en")
        logging.info("📰 English News:")
        logging.info(news_en)
        
        logging.info("-" * 40)


if __name__ == "__main__":
    main()
