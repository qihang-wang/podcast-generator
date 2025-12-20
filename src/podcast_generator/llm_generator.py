"""
LLM 新闻生成模块
使用硅基流动 SiliconFlow API 调用 Qwen 模型生成新闻文本
"""

import os
import requests
import re
from typing import Dict, Any, Optional

# 导入翻译模块
try:
    from person_positions import translate_persons_string, enrich_persons_list
    from news_translator import NewsTranslator
except ImportError as e:
    print(f"⚠️ 导入模块失败: {e}")
    def translate_persons_string(s: str, language: str = "zh") -> str:
        return s
    def enrich_persons_list(persons: list, language: str = "zh") -> list:
        return persons
    NewsTranslator = None


# ================= 配置 =================
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
SILICONFLOW_MODEL = "Qwen/Qwen2.5-7B-Instruct"
SILICONFLOW_API_KEY = "sk-rufxmuzljylovtepourxbutettstqbggozkexzpzvpjwilwb"


# ================= 提示词中文翻译（仅供参考，不发送给LLM）=================
"""
提示词中文翻译：

你是一名专业的新闻记者。根据以下 GDELT 数据，撰写一段简洁的新闻文本（150-250词）。

## 核心规则：
1. 只用英文 - 所有输出必须是英文
2. 只使用提供的数据 - 禁止编造，禁止推断
3. 数字只从数据事实中获取 - 标题中的数字可能不准确
4. 包含所有引语 - 每条引语都要包含说话人姓名和职位
5. 人物职位必须使用 - 使用 "US President Joe Biden"、"UN Secretary-General Antonio Guterres" 格式
6. 第一个地点是事件发生地 - 使用地点字段中第一个城市/地点

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

## 输出格式：
写一段连续的新闻文本，要求：
1. 以日期、地点、主要事件开头
2. 包含关键人物及其职位
3. 自然地融入所有引语
4. 准确引用数据事实
5. 以信源归属结尾

只输出新闻段落（无标题、无分节，只有文本）
"""

# ================= 简化版英文新闻提示词 =================
NEWS_PROMPT_TEMPLATE_EN = """
You are a professional news journalist. Based on the GDELT data below, write a concise news paragraph (150-250 words).

## Core Rules:
1. **English only** - Output everything in English
2. **Use only provided data** - No fabrication, no inference
3. **Numbers from Data Facts only** - Title numbers may be inaccurate
4. **Include all quotes** - Use every quote with speaker name and title
5. **Person titles required** - Use "US President Joe Biden", "UN Secretary-General Antonio Guterres" format
6. **First location is event location** - Use the first city/place listed

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

## Output Format:
Write ONE continuous paragraph that:
1. Starts with date, location, and main event
2. Includes key persons WITH their titles
3. Integrates all quotes naturally
4. Cites data facts accurately
5. Ends with source attribution

Output ONLY the news paragraph (no headers, no sections, just the text):
"""


def get_prompt_template(language: str = "zh") -> str:
    """获取提示词模板"""
    return NEWS_PROMPT_TEMPLATE_EN


def post_process_news(news_text: str, record: Dict[str, Any]) -> str:
    """后处理校验层：验证并修复 LLM 生成的新闻文本"""
    import re
    processed = news_text
    
    # 1. 过滤不合理的大数字
    unreasonable_patterns = [
        (r'\d{6,}名?(?:证人|观察者|目击者|witnesses?)', '多名目击者'),
        (r'\d{6,}\s*(?:people|persons?)\s*(?:living|affected|noticed|observed)', '大量民众'),
    ]
    for pattern, replacement in unreasonable_patterns:
        processed = re.sub(pattern, replacement, processed, flags=re.IGNORECASE)
    
    # 2. 修复性别称呼
    quotes = record.get('Quotes', '')
    if 'Mr.' in quotes or 'Mr ' in quotes:
        if any(term in processed for term in ['Ms.', '女士', '母亲', 'her son', 'she ']):
            processed = processed.replace('Ms.', 'Mr.')
            processed = processed.replace('女士', '先生')
            processed = processed.replace('母亲', '父亲')
            processed = re.sub(r'\bher son\b', 'his son', processed, flags=re.IGNORECASE)
            processed = re.sub(r'\bshe\b', 'he', processed, flags=re.IGNORECASE)
    
    # 3. 版权保护：截断过长引语
    def truncate_quote(match):
        quote = match.group(1)
        if len(quote) > 80:
            truncated = quote[:75]
            for sep in ['. ', '。', ', ', '，', '; ', '；']:
                last_sep = truncated.rfind(sep)
                if last_sep > 40:
                    truncated = truncated[:last_sep+1]
                    break
            return '"' + truncated.strip() + '..."'
        return match.group(0)
    
    processed = re.sub(r'"([^"]{81,})"', truncate_quote, processed)
    processed = re.sub(r'"([^"]{81,})"', truncate_quote, processed)
    
    # 4. 确保来源标注
    source_name = record.get('Source_Name', '')
    if source_name and source_name not in processed:
        if not any(marker in processed for marker in ['信源', '来源', 'Source', '信息来源', 'based on', 'according to']):
            processed = processed.rstrip() + f" (Source: {source_name})"
    
    return processed


def _extract_person_names(persons_str: str) -> list:
    """从人物字符串中提取纯人名"""
    if not persons_str or persons_str == 'Unknown':
        return []
    
    names = []
    for part in persons_str.split(', '):
        part = part.strip()
        import re
        match = re.match(r'^[\u4e00-\u9fff]+([A-Za-z][\w\s\.\-\']+)$', part)
        if match:
            names.append(match.group(1).strip())
        else:
            names.append(part)
    return names


class LLMNewsGenerator:
    """LLM 新闻生成器"""
    
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
        
        # 过滤广告数据
        data_facts = self._filter_ad_data(data_facts)
        
        # 优化地点顺序
        locations = self._optimize_location_order(locations)
        
        # 处理引语
        quotes = record.get('Quotes', 'No quotes available')
        quotes = self._preprocess_quotes(quotes, language)
        
        # 根据语言处理人物职位
        if language == "zh":
            key_persons = translate_persons_string(key_persons, "zh")
        else:
            # 英文模式：提取纯人名并添加英文职位
            person_names = _extract_person_names(key_persons)
            if person_names:
                key_persons = translate_persons_string(', '.join(person_names), "en")
        
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
    
    def _preprocess_quotes(self, quotes: str, language: str = "zh") -> str:
        """预处理引语"""
        if not quotes or quotes == 'No quotes available':
            return quotes
        
        accusation_indicators = [
            'engaged in rebuilding', 'rebuilding the terrorist',
            'terror', 'terrorist organization', 'attack', 'violence',
            'killed', 'murder', 'massacre', 'crimes', 'criminal'
        ]
        
        unlikely_self_statements = [
            'engaged in', 'was involved in', 'participated in',
            'responsible for', 'carried out', 'committed'
        ]
        
        lines = quotes.split('\n')
        processed_lines = []
        
        for line in lines:
            line_lower = line.lower()
            has_accusation = any(ind in line_lower for ind in accusation_indicators)
            has_unlikely = any(phrase in line_lower for phrase in unlikely_self_statements)
            
            if has_accusation and has_unlikely:
                if '表示' in line:
                    line = line.replace('表示', 'is accused of')
                elif 'said' not in line.lower() and 'stated' not in line.lower():
                    line = f"[ACCUSATION] {line}"
            
            processed_lines.append(line)
        
        return '\n'.join(processed_lines)
    
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
    
    def _optimize_location_order(self, locations: str) -> str:
        """优化地点顺序"""
        if not locations or locations == 'Unknown':
            return locations
        
        priority_locations = [
            'Bondi Beach', 'Sydney', 'Melbourne', 'Brisbane', 'Perth',
            'London', 'Paris', 'Berlin', 'Tokyo', 'Beijing', 'Jerusalem',
            'Tel Aviv', 'Gaza City', 'Kiev', 'Kyiv', 'Moscow', 'Washington',
            'New York', 'Los Angeles'
        ]
        
        parts = [p.strip() for p in locations.split(',')]
        priority_parts = []
        other_parts = []
        
        for part in parts:
            is_priority = any(loc in part for loc in priority_locations)
            if is_priority:
                priority_parts.append(part)
            else:
                other_parts.append(part)
        
        reordered = priority_parts + other_parts
        return ', '.join(reordered) if reordered else locations
    
    def generate_news(self, record: Dict[str, Any], 
                      temperature: float = 0.7,
                      max_tokens: int = 1024,
                      language: str = "zh") -> str:
        """生成新闻（英文优先+翻译策略）"""
        # 始终用英文生成
        english_news = self._generate_english_news(record, temperature, max_tokens)
        
        if english_news.startswith("错误") or english_news.startswith("API"):
            return english_news
        
        # 如果需要中文则翻译
        if language == "zh":
            return self._translate_to_chinese(english_news, temperature)
        
        return english_news
    
    def _generate_english_news(self, record: Dict[str, Any], 
                                temperature: float, 
                                max_tokens: int) -> str:
        """生成英文新闻"""
        prompt = self._build_prompt(record, language="en")
        
        system_prompt = (
            "You are a professional news journalist. "
            "Write concise, factual news in one continuous paragraph. "
            "Always include person titles (e.g., 'US President Joe Biden'). "
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
                self.api_url, headers=headers, json=payload, timeout=60
            )
            response.raise_for_status()
            result = response.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                raw_news = result['choices'][0]['message']['content']
                return post_process_news(raw_news, record)
            else:
                return f"API 返回格式错误: {result}"
                
        except requests.exceptions.Timeout:
            return "错误: API 请求超时"
        except requests.exceptions.RequestException as e:
            return f"错误: API 请求失败 - {str(e)}"
        except Exception as e:
            return f"错误: 生成新闻时发生异常 - {str(e)}"
    
    def _translate_to_chinese(self, english_text: str, temperature: float = 0.3) -> str:
        """翻译成中文"""
        if NewsTranslator is None:
            print("⚠️ 翻译模块未加载，返回英文原文")
            return english_text
        
        translator = NewsTranslator(api_key=self.api_key)
        return translator.translate_to_chinese(english_text, temperature=temperature)


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


def format_prompt(record: Dict[str, Any]) -> str:
    """格式化提示词（不调用 API）"""
    return NEWS_PROMPT_TEMPLATE_EN.format(
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
