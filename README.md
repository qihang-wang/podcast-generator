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

### 2. 前端调用示例

#### Vue.js

```vue
<template>
  <div>
    <button @click="loadArticles">加载文章</button>
    <div v-for="article in articles" :key="article.url">
      <h3>{{ article.title }}</h3>
      <p>来源: {{ article.source }}</p>
      <p>情感: {{ article.tone.avg_tone }}</p>
    </div>
  </div>
</template>

<script>
export default {
  data() {
    return { articles: [] };
  },
  methods: {
    async loadArticles() {
      const res = await fetch(
        "http://localhost:8888/api/articles?country_code=CH",
      );
      const data = await res.json();
      if (data.success) this.articles = data.data;
    },
  },
};
</script>
```

---

## 💾 数据缓存策略（CSV + Supabase）

本项目采用**双层数据源**设计，实现高效的数据缓存和查询。

### 架构概览

```
BigQuery (GDELT) → CSV 文件 → Supabase PostgreSQL
     ↓                ↓              ↓
  实时数据        本地缓存       云端持久化
```

### 数据源角色

| 数据源       | 角色                | 保留策略           | 特点                    |
| ------------ | ------------------- | ------------------ | ----------------------- |
| **CSV 文件** | 写入缓冲 + 本地备份 | 仅当天（覆盖写入） | 离线可用、便于调试      |
| **Supabase** | 持久存储 + 查询服务 | 7 天滚动           | 云端存储、支持分页/过滤 |

### 数据流程

#### 场景 1：首次请求

```
1. 前端请求：GET /api/articles?country=CH&days=3
2. API 检查 Supabase 是否有缓存
3. 无缓存 → 从 BigQuery 获取数据
4. 保存到 CSV → 同步到 Supabase（按时间排序）
5. 返回数据
```

#### 场景 2：缓存命中

```
1. 前端请求：GET /api/articles?country=CH&days=3
2. Supabase 已有数据
3. 直接查询返回（毫秒级）
```

### API 参数

| 参数           | 默认值 | 说明                     |
| -------------- | ------ | ------------------------ |
| `country_code` | `CH`   | 国家代码                 |
| `days`         | `1`    | 获取最近N天数据（1-7天） |
| `page`         | `1`    | 页码                     |
| `page_size`    | `20`   | 每页数量                 |
| `use_database` | `true` | 是否优先使用数据库       |

### 数据管理

```bash
# 查看数据库统计
curl http://localhost:8888/api/articles/stats

# 清理 7 天前的数据
curl -X POST "http://localhost:8888/api/articles/cleanup?days=7"
```

## 📄 License

MIT License

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！
