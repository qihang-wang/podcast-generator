"""
LLM 新闻生成模块
使用硅基流动 SiliconFlow API 调用 Qwen 模型生成新闻文本
直接生成中文新闻，无需翻译
"""

import os
import requests
import re
from typing import Dict, Any, Optional

# 导入人物职位模块
try:
    from person_positions import translate_persons_string, enrich_persons_list
except ImportError as e:
    print(f"⚠️ 导入模块失败: {e}")
    def translate_persons_string(s: str, language: str = "zh") -> str:
        return s
    def enrich_persons_list(persons: list, language: str = "zh") -> list:
        return persons


# ================= 配置 =================
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
SILICONFLOW_MODEL = "Qwen/Qwen2.5-7B-Instruct"
SILICONFLOW_API_KEY = "sk-rufxmuzljylovtepourxbutettstqbggozkexzpzvpjwilwb"


# ================= 中文新闻提示词模板 =================
NEWS_PROMPT_TEMPLATE_ZH = """
你是一名专业的国际新闻记者。根据以下 GDELT 数据，撰写一段简洁的中文新闻报道（200-300字）。

## 核心规则：
1. **全部用中文输出**
2. **只使用提供的数据** - 禁止编造
3. **数字从数据事实获取** - 标题中的数字可能不准确
4. **引用完整使用** - 每条引语都要包含说话人姓名和职位
5. **人物职位使用关键人物字段中的信息**

## 新闻素材：
- 标题线索: {title}
- 信源: {source_name}
- 时间: {time}
- 地点: {locations}
- 关键人物: {key_persons}
- 机构: {organizations}
- 基调: {tone}
- 主题: {themes}

## 引语素材：
{quotes}

## 数据事实：
{data_facts}

## 输出要求：
写一段流畅的中文新闻：
1. 开头点明时间地点和主要事件
2. 自然融入引语和数据
3. 结尾标注信源

直接输出新闻正文（不要标题、不要分段标记）：
"""


# ================= 英文新闻提示词模板 =================
NEWS_PROMPT_TEMPLATE_EN = """
You are a professional news journalist. Based on the GDELT data below, write a concise news paragraph (150-250 words).

## Core Rules:
1. **English only** - Output everything in English
2. **Use only provided data** - No fabrication
3. **Numbers from Data Facts only** - Title numbers may be inaccurate
4. **Include all quotes** - Use every quote with speaker name and title
5. **Person titles from Key Persons field**

## News Data:
- Title hint: {title}
- Source: {source_name}
- Time: {time}
- Location: {locations}
- Key Persons: {key_persons}
- Organizations: {organizations}
- Tone: {tone}
- Themes: {themes}

## Quotes:
{quotes}

## Data Facts:
{data_facts}

## Output:
Write ONE continuous paragraph (no headers, just the text):
"""


def get_prompt_template(language: str = "zh") -> str:
    """获取提示词模板"""
    if language == "zh":
        return NEWS_PROMPT_TEMPLATE_ZH
    return NEWS_PROMPT_TEMPLATE_EN


def post_process_news(news_text: str, record: Dict[str, Any]) -> str:
    """后处理：清理格式"""
    processed = news_text.strip()
    
    # 清理 markdown 标记
    processed = re.sub(r'^#+\s*', '', processed)
    processed = re.sub(r'\n#+\s*', '\n', processed)
    processed = re.sub(r'^\*\*.*?\*\*\s*', '', processed)
    
    # 确保来源标注
    source_name = record.get('Source_Name', '')
    if source_name and source_name not in processed:
        if '来源' not in processed and '信源' not in processed and 'Source' not in processed:
            processed = processed.rstrip('。.') + f"。（来源：{source_name}）"
    
    return processed


class LLMNewsGenerator:
    """LLM 新闻生成器 - 直接生成目标语言"""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or SILICONFLOW_API_KEY
        self.model = model or SILICONFLOW_MODEL
        self.api_url = SILICONFLOW_API_URL
        
        if not self.api_key:
            raise ValueError("未设置 API Key！")
    
    def _build_prompt(self, record: Dict[str, Any], language: str = "zh") -> str:
        """构建提示词"""
        template = get_prompt_template(language)
        
        locations = record.get('Locations', 'Unknown')
        key_persons = record.get('Key_Persons', 'Unknown')
        data_facts = record.get('Data_Facts', 'No specific data')
        quotes = record.get('Quotes', 'No quotes available')
        
        # 过滤广告数据
        data_facts = self._filter_ad_data(data_facts)
        
        # 处理人物：使用数据库中的职位信息
        key_persons = translate_persons_string(key_persons, language)
        
        # 处理引语中的人物职位
        if language == "zh":
            # 替换引语中的人物为带职位的版本
            for name in key_persons.split(', '):
                if name in quotes:
                    enriched = translate_persons_string(name, "zh")
                    if enriched != name:
                        quotes = quotes.replace(f"{name} 表示", f"{enriched}表示")
        
        return template.format(
            title=record.get('Title', 'Unknown'),
            source_name=record.get('Source_Name', 'Unknown'),
            time=record.get('Time', 'Unknown'),
            locations=locations,
            key_persons=key_persons,
            organizations=record.get('Organizations', 'Unknown'),
            tone=record.get('Tone', 'Neutral'),
            themes=record.get('Themes', 'General'),
            quotes=quotes,
            data_facts=data_facts
        )
    
    def _filter_ad_data(self, data_facts: str) -> str:
        """过滤广告数据"""
        if not data_facts or data_facts == 'No specific data':
            return data_facts
        
        ad_keywords = [
            'premium domains', 'advertising', 'subscribe', 'newsletter',
            'click here', 'sign up', 'download', 'promotion', 'discount',
            'buy now', 'free trial', 'offer', 'sponsored'
        ]
        
        items = [item.strip() for item in data_facts.split(';')]
        filtered_items = []
        
        for item in items:
            item_lower = item.lower()
            is_ad = any(keyword in item_lower for keyword in ad_keywords)
            if not is_ad and item:
                filtered_items.append(item)
        
        return '; '.join(filtered_items) if filtered_items else 'No specific data'
    
    def generate_news(self, record: Dict[str, Any], 
                      temperature: float = 0.7,
                      max_tokens: int = 1024,
                      language: str = "zh") -> str:
        """直接生成目标语言的新闻"""
        prompt = self._build_prompt(record, language)
        
        if language == "zh":
            system_prompt = (
                "你是专业的中文新闻记者。"
                "根据提供的数据撰写简洁流畅的中文新闻。"
                "只使用提供的数据，不要编造。"
            )
        else:
            system_prompt = (
                "You are a professional news journalist. "
                "Write concise, factual news based on provided data only. "
                "Output English only."
            )
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False
        }
        
        try:
            response = requests.post(
                self.api_url, headers=headers, json=payload, timeout=90
            )
            response.raise_for_status()
            result = response.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                raw_news = result['choices'][0]['message']['content']
                return post_process_news(raw_news, record)
            else:
                return f"API 返回格式错误: {result}"
                
        except requests.exceptions.Timeout:
            return "错误: API 请求超时 (90秒)"
        except requests.exceptions.RequestException as e:
            return f"错误: API 请求失败 - {str(e)}"
        except Exception as e:
            return f"错误: 生成新闻时发生异常 - {str(e)}"


# ================= 便捷方法 =================

def generate_news_from_record(record: Dict[str, Any], 
                               api_key: Optional[str] = None,
                               language: str = "zh") -> str:
    """根据记录生成新闻"""
    try:
        generator = LLMNewsGenerator(api_key=api_key)
        return generator.generate_news(record, language=language)
    except ValueError as e:
        return f"错误: {str(e)}"


def format_prompt(record: Dict[str, Any], language: str = "zh") -> str:
    """格式化提示词（不调用 API）"""
    template = get_prompt_template(language)
    return template.format(
        title=record.get('Title', 'Unknown'),
        source_name=record.get('Source_Name', 'Unknown'),
        time=record.get('Time', 'Unknown'),
        locations=record.get('Locations', 'Unknown'),
        key_persons=record.get('Key_Persons', 'Unknown'),
        organizations=record.get('Organizations', 'Unknown'),
        tone=record.get('Tone', 'Neutral'),
        themes=record.get('Themes', 'General'),
        quotes=record.get('Quotes', 'No quotes available'),
        data_facts=record.get('Data_Facts', 'No specific data')
    )
