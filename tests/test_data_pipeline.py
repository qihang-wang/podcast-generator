import pytest
import asyncio
# 引入 mock 库 (pytest 默认集成或使用 unittest.mock)
from unittest.mock import AsyncMock, patch
from podcast_generator.pipeline import run_podcast_pipeline

# 单元测试示例：确保数据解析逻辑的正确性（假设存在一个解析函数）
def test_data_parsing_logic():
    # 确保解析函数处理边界情况的正确性
    # assert parse_data(...) == expected_output
    pass

# 集成测试示例：使用 Mock 确保管道逻辑的正确性 (I/O 隔离)
# 使用 patch 模拟 I/O 密集型函数
@pytest.mark.asyncio
@patch('podcast_generator.pipeline.fetch_gdelt_data', new_callable=AsyncMock)
@patch('podcast_generator.pipeline.generate_llm_text', new_callable=AsyncMock)
@patch('podcast_generator.pipeline.synthesize_speech', new_callable=AsyncMock)
async def test_full_pipeline_success(mock_tts, mock_llm, mock_fetch):
    # 模拟外部 API 返回的预期值
    mock_fetch.return_value = [{"article": "content"}]
    mock_llm.return_value = "Summary Text."
    mock_tts.return_value = b"Mock_Audio_Bytes"
    
    # 运行管道
    result = await run_podcast_pipeline("test_query")
    
    # 验证每个步骤是否按预期被调用
    mock_fetch.assert_called_once_with("test_query")
    mock_llm.assert_called_once()
    assert result == b"Mock_Audio_Bytes"