"""
新闻翻译模块
使用 LLM 将英文新闻翻译成中文
"""

import os
import requests
from typing import Optional


# ================= 配置 =================
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
SILICONFLOW_MODEL = "Qwen/Qwen2.5-7B-Instruct"


# ================= 翻译提示词模板 =================
"""
翻译提示词中文说明（仅供参考）：

你是一名专业的新闻翻译员。将以下英文新闻翻译成中文。

翻译规则：
1. 保持相同结构（标题、导语、正文、来源）
2. 保持新闻专业语调 - 正式客观
3. 只输出中文，不输出英文
4. 人名翻译：知名人物使用常用中文译名，否则保留原文加音译
5. 地名翻译：使用标准中文名（如 Sydney → 悉尼, Melbourne → 墨尔本）
6. 保持所有数字和日期准确
7. 精确保留引语的含义
"""

TRANSLATION_PROMPT_TEMPLATE = """
You are a professional news translator. Translate the following English news article to Chinese.

## Translation Rules:
1. Keep the same structure (title, lead, body, sources)
2. Maintain journalistic tone - formal and objective
3. Output ONLY Chinese, no English text
4. For person names: use common Chinese translations if well-known, otherwise keep original with phonetic translation
5. For location names: use standard Chinese names (e.g., Sydney → 悉尼, Melbourne → 墨尔本)
6. Keep all numbers and dates accurate
7. Preserve the meaning of quotes exactly

## English Article to Translate:
{english_article}

Please translate to Chinese (output Chinese only):
"""


class NewsTranslator:
    """新闻翻译器 - 使用 LLM 将英文新闻翻译成中文"""
    
    def __init__(self, 
                 api_key: Optional[str] = None,
                 api_url: str = SILICONFLOW_API_URL,
                 model: str = SILICONFLOW_MODEL):
        """
        初始化翻译器
        
        Args:
            api_key: API Key，如果不提供则从环境变量获取
            api_url: API URL
            model: 使用的模型
        """
        self.api_key = api_key or os.environ.get('SILICONFLOW_API_KEY') or "sk-rufxmuzljylovtepourxbutettstqbggozkexzpzvpjwilwb"
        self.api_url = api_url
        self.model = model
        
        if not self.api_key:
            raise ValueError("未找到 API Key，请设置 SILICONFLOW_API_KEY 环境变量")
    
    def translate_to_chinese(self, english_text: str, 
                              temperature: float = 0.3,
                              print_comparison: bool = True) -> str:
        """
        将英文新闻翻译成中文
        
        Args:
            english_text: 英文新闻文本
            temperature: 翻译温度（建议较低以保证准确性）
            print_comparison: 是否打印翻译前后对比
            
        Returns:
            中文新闻文本
        """
        # 打印翻译前的英文文本
        if print_comparison:
            print("\n" + "="*60)
            print("📝 翻译前 (English):")
            print("-"*60)
            print(english_text)
            print("-"*60)
        
        translation_prompt = TRANSLATION_PROMPT_TEMPLATE.format(
            english_article=english_text
        )
        
        system_prompt = "你是一名专业的新闻翻译员，擅长将英文新闻准确翻译成中文，保持新闻的专业风格。"
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": translation_prompt}
            ],
            "temperature": temperature,
            "max_tokens": 2048,
            "stream": False
        }
        
        try:
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            
            result = response.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                chinese_text = result['choices'][0]['message']['content']
                
                # 打印翻译后的中文文本
                if print_comparison:
                    print("\n📝 翻译后 (Chinese):")
                    print("-"*60)
                    print(chinese_text)
                    print("="*60)
                
                return chinese_text
            else:
                return f"翻译API返回格式错误: {result}"
                
        except requests.exceptions.Timeout:
            return "错误: 翻译API请求超时"
        except requests.exceptions.RequestException as e:
            return f"错误: 翻译API请求失败 - {str(e)}"
        except Exception as e:
            return f"错误: 翻译时发生异常 - {str(e)}"


# ================= 便捷方法 =================

def translate_news(english_text: str, 
                   api_key: Optional[str] = None,
                   print_comparison: bool = True) -> str:
    """
    翻译英文新闻的便捷方法
    
    Args:
        english_text: 英文新闻文本
        api_key: API Key (可选)
        print_comparison: 是否打印翻译前后对比
        
    Returns:
        中文新闻文本
    """
    try:
        translator = NewsTranslator(api_key=api_key)
        return translator.translate_to_chinese(english_text, print_comparison=print_comparison)
    except ValueError as e:
        return f"错误: {str(e)}"
