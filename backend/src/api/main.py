from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import articles
from .scheduler import lifespan_scheduler
from .logging_config import setup_logging

# 配置日志（支持轮转，防止文件爆炸）
setup_logging(level="INFO")

app = FastAPI(
    title="Podcast Generator API",
    description="GDELT 新闻数据接口 - 提供结构化的文章数据",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan_scheduler  # 集成定时任务调度器
)

# 配置 CORS（允许前端跨域访问）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(articles.router)

@app.get("/")
async def root():
    """API 根路径"""
    from .response import success_response
    
    return success_response(
        data={
            "message": "Podcast Generator API",
            "docs": "/docs",
            "endpoints": {
                "articles": "/api/articles?country_code=CH",
                "stats": "/api/articles/stats"
            },
            "scheduler": {
                "maintenance": "每天凌晨0点执行数据清理和预热"
            }
        }
    )

@app.get("/health")
async def health_check():
    """健康检查端点"""
    from .response import success_response
    
    return success_response(
        data={"status": "healthy"}
    )
