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

```bash
# 克隆项目
git clone <repository-url>
cd podcast-generator

# 安装依赖
poetry install

# 安装 Supabase 和 dotenv
poetry run pip install supabase python-dotenv
```

### 启动 API 服务器

```bash
# 启动服务器（端口 8888）
poetry run uvicorn podcast_generator.api.main:app --host 127.0.0.1 --port 8888 --reload
```

启动成功后，你会看到：

```
INFO:     Uvicorn running on http://127.0.0.1:8888 (Press CTRL+C to quit)
INFO:     Started server process
INFO:     Application startup complete.
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

#### JavaScript Fetch

```javascript
// 获取中国文章数据
async function getArticles() {
  const response = await fetch(
    "http://localhost:8888/api/articles?country_code=CH&fetch_content=false",
  );
  const data = await response.json();

  if (data.success) {
    console.log(`获取到 ${data.total} 篇文章`);
    console.log(data.data); // 文章数组
  }
  return data;
}
```

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

## 📦 API 参数

| 参数            | 类型    | 默认值  | 说明                      |
| --------------- | ------- | ------- | ------------------------- |
| `country_code`  | string  | `CH`    | 国家代码 (FIPS 10-4 标准) |
| `fetch_content` | boolean | `false` | 是否获取文章全文          |

### 常用国家代码

| 代码 | 国家 | 代码 | 国家 |
| ---- | ---- | ---- | ---- |
| `CH` | 中国 | `US` | 美国 |
| `JA` | 日本 | `KS` | 韩国 |
| `UK` | 英国 | `GM` | 德国 |
| `FR` | 法国 | `IN` | 印度 |

---

## 📦 响应数据结构

### 成功响应

```json
{
  "success": true,
  "total": 50,
  "data": [
    {
      "title": "新闻标题",
      "source": "example.com",
      "url": "https://example.com/article",
      "authors": "记者名",
      "persons": ["人物A", "人物B"],
      "organizations": ["组织名称"],
      "themes": ["主题1", "主题2"],
      "locations": ["Beijing (CH)"],
      "quotations": [
        {
          "speaker": "发言人",
          "quote": "引语内容",
          "verb": "表示"
        }
      ],
      "tone": {
        "avg_tone": -2.5,
        "positive_score": 3.2,
        "negative_score": 5.7,
        "polarity": 2.5
      },
      "emotion": {
        "positivity": 3.2,
        "negativity": 5.7,
        "anxiety": 4.1,
        "arousal": 6.3
      },
      "emotion_instruction": "保持中立但略带担忧的语气",
      "event": {
        "event_id": 123456789,
        "action": "发表声明",
        "quad_class": "口头合作",
        "goldstein_scale": 1.0,
        "actor1": "CHINA",
        "actor2": "UNITED STATES",
        "location": "Beijing, China"
      }
    }
  ]
}
```

---

## 📁 项目结构

```
src/podcast_generator/
├── api/                  # HTTP API 接口
│   ├── main.py          # FastAPI 应用入口
│   └── routes/
│       └── articles.py  # 文章数据接口
├── gdelt/               # GDELT 数据处理模块
│   ├── gdelt_parse.py   # 数据解析
│   ├── data_loader.py   # CSV 数据加载
│   ├── data_fetcher.py  # BigQuery 数据获取
│   └── model/           # 数据模型
├── utils/               # 通用工具模块
│   └── article_fetcher.py  # 文章抓取
├── llm/                 # LLM 新闻生成
└── generate_news.py     # 新闻生成脚本
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

### 配置 Supabase

1. **注册并创建项目**：访问 [supabase.com](https://supabase.com)

2. **创建数据库表**：在 SQL Editor 中执行：

```sql
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(10) NOT NULL,
    gkg_record_id VARCHAR(100) UNIQUE NOT NULL,
    date_added BIGINT NOT NULL,
    title TEXT,
    source VARCHAR(255),
    url TEXT,
    authors TEXT,
    persons JSONB DEFAULT '[]',
    organizations JSONB DEFAULT '[]',
    themes JSONB DEFAULT '[]',
    locations JSONB DEFAULT '[]',
    quotations JSONB DEFAULT '[]',
    tone JSONB,
    emotion JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_articles_country_date ON articles(country_code, date_added DESC);
```

3. **配置环境变量**：创建 `.env` 文件：

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key
ENABLE_DATABASE_SYNC=true
```

4. **安装依赖**：

```bash
poetry run pip install supabase python-dotenv
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

### 存储容量

- **Supabase 免费版**：500 MB
- **预估使用量**：270-300 MB（150 国家 × 100 篇/天 × 7 天）
- **剩余空间**：~200 MB

详细配置步骤请参考：[Supabase 设置指南](./docs/supabase_setup_guide.md)

---

## ⚙️ 服务器配置

### 修改端口

```bash
poetry run uvicorn podcast_generator.api.main:app --port 9000
```

### 允许外网访问

```bash
# 绑定所有网络接口（生产环境慎用）
poetry run uvicorn podcast_generator.api.main:app --host 0.0.0.0 --port 8888
```

### 生产环境部署

```bash
# 不使用 --reload，增加 workers
poetry run uvicorn podcast_generator.api.main:app --host 0.0.0.0 --port 8888 --workers 4
```

---

## 🔧 常见问题

### Q: 如何停止服务器？

A: 在终端按 `Ctrl+C`

### Q: 端口被占用怎么办？

A: 更换端口或停止占用该端口的程序：

```bash
# 查看占用端口 8888 的进程（PowerShell）
Get-NetTCPConnection -LocalPort 8888

# 杀死进程
Stop-Process -Id <进程ID>
```

### Q: 为什么 `fetch_content=true` 很慢？

A: 获取文章全文需要爬取网页，每篇 2-5 秒。如果只需元数据（标题、人物、主题），设为 `false` 即可秒级返回。

### Q: 找不到数据文件？

A: 确保已运行数据获取脚本生成 CSV 文件：

```bash
poetry run python -m podcast_generator.generate_news
```

CSV 文件位于：`src/podcast_generator/gdelt/gdelt_csv/`

---

## 🛠️ 故障排除

### 导入错误

如遇 `ModuleNotFoundError`，检查导入路径使用绝对导入：

```python
from podcast_generator.gdelt import parse_gdelt_article
```

### CORS 错误

已配置 `allow_origins=["*"]`，如仍有问题，检查前端请求 URL 是否正确。

### 数据返回为空

1. 检查 CSV 文件是否存在
2. 确认 `country_code` 参数正确
3. 运行数据获取脚本生成数据

---

## 📚 相关命令

```bash
# 启动 API 服务
poetry run uvicorn podcast_generator.api.main:app --port 8888 --reload

# 运行新闻生成脚本
poetry run python -m podcast_generator.generate_news

# 测试 API（PowerShell）
Invoke-RestMethod -Uri "http://localhost:8888/api/articles?country_code=CH"

# 查看 API 文档
# 浏览器访问 http://localhost:8888/docs
```

---

## 📄 License

MIT License

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！
