from fastapi import FastAPI
from fastapi.responses import Response, HTMLResponse
from .pipeline import run_podcast_pipeline
# 导入其他可能需要的模块...

# 创建应用实例
app = FastAPI(title="Podcast Generator API")

@app.get("/", response_class=HTMLResponse)
async def home_page():
    # 这是一个示例性的首页或健康检查点
    return "<h1>Podcast Pipeline is running!</h1>"

@app.post("/generate-podcast/")
async def generate_podcast_endpoint(query: str):
    """
    接收查询，运行播客生成管道，并返回语音数据。
    """
    try:
        # 调用异步管道，FastAPI 和 Uvicorn 负责管理事件循环 [19]
        audio_data = await run_podcast_pipeline(query)
        
        # 返回音频数据流，设置正确的 Content-Type 供前端播放
        return Response(content=audio_data, media_type="audio/mpeg") 
    except Exception as e:
        # 生产环境应使用更优雅的错误处理和日志记录 [4]
        return Response(content=f"Error: {str(e)}", status_code=500, media_type="text/plain")