from fastapi import APIRouter, Query, HTTPException
from podcast_generator.gdelt.data_loader import load_gdelt_data
from podcast_generator.gdelt.gdelt_parse import parse_gdelt_article

router = APIRouter(prefix="/api/articles", tags=["文章数据"])

@router.get("/")
async def get_articles(
    country_code: str = Query("CH", description="国家代码 (FIPS 10-4)"),
    fetch_content: bool = Query(False, description="是否获取文章全文")
):
    """
    获取指定国家的文章数据
    
    - **country_code**: 国家代码，如 "CH"=中国, "US"=美国
    - **fetch_content**: 是否获取文章原文（耗时较长，默认 False）
    
    返回格式：
    ```json
    {
        "success": true,
        "data": [
            {
                "title": "文章标题",
                "source": "example.com",
                "url": "https://...",
                "persons": ["人物1", "人物2"],
                "organizations": ["组织1"],
                "themes": ["主题1", "主题2"],
                "quotations": [{"speaker": "...", "quote": "..."}],
                "tone": {"avg_tone": -2.5, ...},
                "emotion": {"positivity": 3.2, ...},
                "event": {"action": "发表声明", ...}
            }
        ],
        "total": 50
    }
    ```
    """
    try:
        # 加载数据
        gkg_models, event_models = load_gdelt_data(country_code=country_code)
        
        # 建立 Event 映射
        events_dict = {e.global_event_id: e for e in event_models}
        
        # 解析每篇文章
        articles = []
        for gkg in gkg_models:
            event = events_dict.get(gkg.event_id)
            params = parse_gdelt_article(gkg, event, fetch_content=fetch_content)
            articles.append(params)
        
        return {
            "success": True,
            "data": articles,
            "total": len(articles)
        }
    
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"未找到国家代码 {country_code} 的数据文件"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"服务器错误: {str(e)}"
        )
