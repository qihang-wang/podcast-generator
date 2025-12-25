"""
LLM 提供商抽象层
支持多种 LLM API（SiliconFlow、Google Gemini、自部署模型等）
"""

import os
import requests
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List


class LLMProvider(ABC):
    """LLM 提供商抽象基类"""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key
        self.model = model
    
    @abstractmethod
    def generate(self, 
                 system_prompt: str,
                 user_prompt: str,
                 temperature: float = 0.7,
                 max_tokens: int = 1024) -> str:
        """
        生成文本
        
        Args:
            system_prompt: 系统提示词
            user_prompt: 用户提示词
            temperature: 温度参数
            max_tokens: 最大 token 数
            
        Returns:
            生成的文本
        """
        pass
    
    @abstractmethod
    def get_provider_name(self) -> str:
        """获取提供商名称"""
        pass


# ================= SiliconFlow 实现 =================

class SiliconFlowProvider(LLMProvider):
    """SiliconFlow API 提供商"""
    
    DEFAULT_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
    DEFAULT_MODEL = "Qwen/Qwen2.5-7B-Instruct"
    # 默认 API Key（仅用于开发测试，生产环境请使用环境变量）
    DEFAULT_API_KEY = "sk-swhapsnwwfkevosxdwqwcojoclkgfnhdswfpfcizxjviprwb"
    
    def __init__(self, api_key: Optional[str] = None, 
                 model: Optional[str] = None,
                 api_url: Optional[str] = None):
        """
        初始化 SiliconFlow 提供商
        
        Args:
            api_key: API 密钥
                    优先级：参数 > 环境变量 SILICONFLOW_API_KEY > 默认 key
            model: 模型名称（默认 Qwen/Qwen2.5-7B-Instruct）
            api_url: API URL（默认 SiliconFlow 官方地址）
        """
        # API Key 优先级：参数 > 环境变量 > 默认值
        final_api_key = api_key or os.getenv("SILICONFLOW_API_KEY") or self.DEFAULT_API_KEY
        
        super().__init__(final_api_key, model or self.DEFAULT_MODEL)
        self.api_url = api_url or self.DEFAULT_API_URL
        
        if not self.api_key:
            raise ValueError(
                "SiliconFlow API Key 未设置！\n"
                "请通过以下任一方式提供：\n"
                "1. 参数: SiliconFlowProvider(api_key='your_key')\n"
                "2. 环境变量: export SILICONFLOW_API_KEY='your_key'\n"
                "3. 使用默认 key（仅开发环境）"
            )
    
    def generate(self, 
                 system_prompt: str,
                 user_prompt: str,
                 temperature: float = 0.7,
                 max_tokens: int = 1024) -> str:
        """使用 SiliconFlow API 生成文本"""
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False
        }
        
        try:
            response = requests.post(
                self.api_url, 
                headers=headers, 
                json=payload, 
                timeout=90
            )
            response.raise_for_status()
            result = response.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                return result['choices'][0]['message']['content']
            else:
                raise ValueError(f"API 返回格式错误: {result}")
                
        except requests.exceptions.Timeout:
            raise TimeoutError("API 请求超时 (90秒)")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"API 请求失败: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"生成文本时发生异常: {str(e)}")
    
    def get_provider_name(self) -> str:
        return "SiliconFlow"


# ================= Google Gemini 实现 =================

class GeminiProvider(LLMProvider):
    """Google Gemini API 提供商"""
    
    DEFAULT_MODEL = "gemini-2.0-flash-exp"
    DEFAULT_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    # 默认 API Key（仅用于开发测试，生产环境请使用环境变量）
    DEFAULT_API_KEY = "AIzaSyAbmYxNaA5F6Ky4rCoqw_wxebJVv34FAls"
    
    def __init__(self, api_key: Optional[str] = None, 
                 model: Optional[str] = None):
        """
        初始化 Gemini 提供商
        
        Args:
            api_key: Google API 密钥
                    优先级：参数 > 环境变量 GEMINI_API_KEY > 默认 key
            model: 模型名称（默认 gemini-2.0-flash-exp）
        """
        # API Key 优先级：参数 > 环境变量 > 默认值
        final_api_key = api_key or os.getenv("GEMINI_API_KEY") or self.DEFAULT_API_KEY
        
        super().__init__(final_api_key, model or self.DEFAULT_MODEL)
        
        if not self.api_key:
            raise ValueError(
                "Gemini API Key 未设置！\n"
                "请通过以下任一方式提供：\n"
                "1. 参数: GeminiProvider(api_key='your_key')\n"
                "2. 环境变量: export GEMINI_API_KEY='your_key'\n"
                "3. 使用默认 key（仅开发环境）"
            )
        
        self.api_url = self.DEFAULT_API_URL.format(model=self.model)

    def generate(self, 
                 system_prompt: str,
                 user_prompt: str,
                 temperature: float = 0.7,
                 max_tokens: int = 1024) -> str:
        """使用 Gemini API 生成文本"""
        
        # 合并 system 和 user prompt（Gemini 的格式）
        combined_prompt = f"{system_prompt}\n\n{user_prompt}"
        
        payload = {
            "contents": [{
                "parts": [{
                    "text": combined_prompt
                }]
            }],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            }
        }
        
        try:
            response = requests.post(
                f"{self.api_url}?key={self.api_key}",
                json=payload,
                timeout=90
            )
            response.raise_for_status()
            result = response.json()
            
            if 'candidates' in result and len(result['candidates']) > 0:
                return result['candidates'][0]['content']['parts'][0]['text']
            else:
                raise ValueError(f"API 返回格式错误: {result}")
                
        except requests.exceptions.Timeout:
            raise TimeoutError("API 请求超时 (90秒)")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"API 请求失败: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"生成文本时发生异常: {str(e)}")
    
    def get_provider_name(self) -> str:
        return "Google Gemini"


# ================= 自部署模型实现 =================

class SelfHostedProvider(LLMProvider):
    """自部署模型提供商（OpenAI 兼容 API）"""
    
    def __init__(self, api_url: str,
                 api_key: Optional[str] = None,
                 model: str = "default"):
        """
        初始化自部署模型提供商
        
        Args:
            api_url: 自部署模型的 API 地址
            api_key: API 密钥（可选）
            model: 模型名称
        """
        super().__init__(api_key, model)
        self.api_url = api_url
        
        if not self.api_url:
            raise ValueError("自部署模型的 API URL 未设置！")
    
    def generate(self, 
                 system_prompt: str,
                 user_prompt: str,
                 temperature: float = 0.7,
                 max_tokens: int = 1024) -> str:
        """使用自部署模型生成文本（OpenAI 兼容格式）"""
        
        headers = {
            "Content-Type": "application/json"
        }
        
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        
        try:
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=90
            )
            response.raise_for_status()
            result = response.json()
            
            # 兼容 OpenAI 格式
            if 'choices' in result and len(result['choices']) > 0:
                return result['choices'][0]['message']['content']
            else:
                raise ValueError(f"API 返回格式错误: {result}")
                
        except requests.exceptions.Timeout:
            raise TimeoutError("API 请求超时 (90秒)")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"API 请求失败: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"生成文本时发生异常: {str(e)}")
    
    def get_provider_name(self) -> str:
        return f"SelfHosted ({self.api_url})"


# ================= 工厂方法 =================

def create_llm_provider(provider_type: str = "siliconflow", **kwargs) -> LLMProvider:
    """
    创建 LLM 提供商实例
    
    Args:
        provider_type: 提供商类型 ("siliconflow", "gemini", "selfhosted")
        **kwargs: 提供商特定参数
        
    Returns:
        LLM 提供商实例
    """
    provider_type = provider_type.lower()
    
    if provider_type == "siliconflow":
        return SiliconFlowProvider(**kwargs)
    elif provider_type == "gemini":
        return GeminiProvider(**kwargs)
    elif provider_type == "selfhosted":
        return SelfHostedProvider(**kwargs)
    else:
        raise ValueError(f"不支持的提供商类型: {provider_type}。"
                         f"支持: siliconflow, gemini, selfhosted")
