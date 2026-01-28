# Podcast Generator

基于 GDELT 数据的新闻播客生成系统，提供 RESTful API 接口用于获取结构化的新闻数据。

## 📋 项目简介

本项目从 GDELT（全球事件、语言和语调数据库）获取新闻数据，进行结构化解析，并提供 HTTP API 接口供前端调用。支持：

- 📰 获取多国新闻数据（基于 FIPS 10-4 国家代码）
- 🎭 情感分析（正负面情绪、焦虑度、唤醒度等）
- 👥 实体提取（人物、组织、地点）
- 💬 引语提取
- 📊 事件分类（CAMEO 编码系统）

## 🚀 快速开始

### 安装依赖

````bash
# 克隆项目
git clone <repository-url>
cd podcast-generator

# 安装依赖
poetry install

### 启动 API 服务器

```bash
# 启动服务器（端口 8888）
poetry run uvicorn podcast_generator.api.main:app --host 127.0.0.1 --port 8888 --reload
````

启动成功后，你会看到：

```
INFO:     Uvicorn running on http://127.0.0.1:8888 (Press CTRL+C to quit)
INFO:     Started server process
INFO:     Application startup complete.
```

```bash
# 测试调度器
poetry run python  .\tests\test_scheduler.py
```

### 访问 API 文档

在浏览器打开 **[http://localhost:8888/docs](http://localhost:8888/docs)** 即可查看交互式 API 文档（Swagger UI）。

---

## 📍 API 端点

| 端点                                 | 说明                      |
| ------------------------------------ | ------------------------- |
| `http://localhost:8888`              | API 根路径，返回欢迎信息  |
| `http://localhost:8888/docs`         | **Swagger 交互式文档** ⭐ |
| `http://localhost:8888/redoc`        | ReDoc 文档                |
| `http://localhost:8888/health`       | 健康检查                  |
| `http://localhost:8888/api/articles` | 获取文章数据              |

---

## 📖 使用 API

### 1. Swagger UI（推荐）

1. 打开 `http://localhost:8888/docs`
2. 找到 `GET /api/articles` 端点
3. 点击 **"Try it out"**
4. 设置参数：
   - `country_code`: `CH` (中国)
   - `fetch_content`: `false` (不获取全文，速度快)
5. 点击 **"Execute"**
6. 查看返回的 JSON 数据


### API 参数

| 参数           | 默认值 | 说明                     |
| -------------- | ------ | ------------------------ |
| `country_code` | `CH`   | 国家代码                 |
| `days`         | `1`    | 获取最近N天数据（1-7天） |
| `page`         | `1`    | 页码                     |
| `page_size`    | `20`   | 每页数量                 |

### 数据管理

```bash
# 查看数据库统计
curl http://localhost:8888/api/articles/stats


## 📄 License

MIT License

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！
