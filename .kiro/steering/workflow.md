---
inclusion: always
---

# 项目工作流规则

## 文件创建规范

### ❌ 禁止创建
- 额外的文档文件（.md），除非用户明确要求
- 临时验证脚本（在对话中提供验证命令即可）
- 总结文档（在对话中进行总结）

### ✅ 允许创建/修改
- 核心代码文件
- 测试文件 → 统一放在 `tests/` 目录下
- README.md（项目文档更新）
- 配置文件（.env, pyproject.toml 等）

## 测试规范

### 单元测试位置
所有测试文件统一放在 `tests/` 目录：
```
tests/
├── test_gdelt_integration.py
├── test_data_pipeline.py
├── test_articles_helpers.py
└── test_gdelt_query_service.py
```

### 运行测试
```bash
poetry run pytest tests/ -v
poetry run pytest tests/test_xxx.py -v  # 单个文件
```

## 验证方式

### 代码验证
在对话中提供验证命令，而非创建脚本：

### 功能验证
建议用户手动执行或使用 Agent Hooks 自动化。

## 原则

**最小化文件创建，测试集中管理，验证命令化。**
