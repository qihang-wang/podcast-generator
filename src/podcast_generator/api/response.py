"""
统一 API 响应格式
确保所有端点返回一致的响应结构
"""

from typing import Any, Optional, TypeVar, Generic
from pydantic import BaseModel
from datetime import datetime
import uuid


T = TypeVar("T")


class APIResponse(BaseModel, Generic[T]):
    """
    统一 API 响应格式
    
    成功响应示例：
    {
        "success": true,
        "data": {...},
        "error": null,
        "meta": {
            "request_id": "abc-123",
            "timestamp": "2026-01-23T10:30:00Z"
        }
    }
    
    错误响应示例：
    {
        "success": false,
        "data": null,
        "error": {
            "code": "DATABASE_UNAVAILABLE",
            "message": "数据库服务不可用"
        },
        "meta": {...}
    }
    """
    success: bool
    data: Optional[T] = None
    error: Optional[dict] = None
    meta: dict = {}
    
    class Config:
        # 允许任意类型（因为 data 可能是任何类型）
        arbitrary_types_allowed = True


def success_response(
    data: Any = None,
    request_id: str = None,
    **extra_meta
) -> dict:
    """
    创建成功响应
    
    Args:
        data: 响应数据
        request_id: 请求ID（可选，自动生成）
        **extra_meta: 额外的元数据
        
    Returns:
        统一格式的响应字典
    """
    return {
        "success": True,
        "data": data,
        "error": None,
        "meta": {
            "request_id": request_id or str(uuid.uuid4())[:8],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            **extra_meta
        }
    }


def error_response(
    code: str,
    message: str,
    request_id: str = None,
    details: Any = None,
    **extra_meta
) -> dict:
    """
    创建错误响应
    
    Args:
        code: 错误代码（如 "DATABASE_UNAVAILABLE"）
        message: 错误信息
        request_id: 请求ID（可选，自动生成）
        details: 错误详情（可选）
        **extra_meta: 额外的元数据
        
    Returns:
        统一格式的错误响应字典
        
    常用错误代码：
        - VALIDATION_ERROR: 参数验证失败
        - DATABASE_UNAVAILABLE: 数据库不可用
        - RESOURCE_NOT_FOUND: 资源不存在
        - INTERNAL_ERROR: 内部错误
        - TIMEOUT: 请求超时
    """
    error_info = {
        "code": code,
        "message": message
    }
    if details:
        error_info["details"] = details
    
    return {
        "success": False,
        "data": None,
        "error": error_info,
        "meta": {
            "request_id": request_id or str(uuid.uuid4())[:8],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            **extra_meta
        }
    }


# 常用错误代码常量
class ErrorCode:
    VALIDATION_ERROR = "VALIDATION_ERROR"
    DATABASE_UNAVAILABLE = "DATABASE_UNAVAILABLE"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    TIMEOUT = "TIMEOUT"
    RATE_LIMITED = "RATE_LIMITED"
