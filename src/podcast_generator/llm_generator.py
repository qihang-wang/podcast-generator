"""
LLM 新闻生成模块
使用硅基流动 SiliconFlow API 调用 Qwen 模型生成新闻文本
"""

import os
import requests
from typing import Dict, Any, Optional


# ================= 配置 =================
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
SILICONFLOW_MODEL = "Qwen/Qwen2.5-7B-Instruct"

# API Key 从环境变量获取，或在此处直接设置
# 请在环境变量中设置 SILICONFLOW_API_KEY，或替换下面的值
SILICONFLOW_API_KEY = "sk-rufxmuzljylovtepourxbutettstqbggozkexzpzvpjwilwb"


# ================= LLM 提示词模板 =================
NEWS_PROMPT_TEMPLATE = """
你是一名专业的国际新闻记者。根据以下 GDELT 提取的结构化数据，撰写一篇 250 字左右的新闻报道。

## 新闻素材
- 标题线索: {title}
- 信源: {source_name}
- 时间: {time}
- 地点: {locations}
- 关键人物: {key_persons}
- 涉及机构: {organizations}
- 情感基调: {emotions} ({tone})
- 主题: {themes}

## 核心引用 (必须使用)
{quotes}

## 关键数据 (必须核实后使用)
{data_facts}

## ⚠️ 原文摘要 (如有则优先参考)
{article_summary}
（注意：如果上述摘要为空，请完全依赖 GDELT 结构化数据生成新闻，不要编造细节）

## 生成要求
1. 标题: 一句话概括核心事件（如果提供的标题以 "Article from" 开头，请根据主题自行生成标题）
2. 导语: 回答 Who/What/When/Where
3. 正文: 使用至少 2 条引用，嵌入具体数据
4. 结尾: 注明信息来源
5. ⚠️ 重要: 如果数据看起来不完整或异常，请用"据报道"来模糊处理
6. 严禁编造任何未提供的信息
7. 如果数字看起来不合理（如人数超过100万或证人数量超过1000人），请直接省略该数字
8. 输出格式使用 Markdown（标题用 ###，正文分段清晰）
9. ⚠️ 严禁添加原始数据中未提及的国家或机构参与信息
10. 如果地点中只提到一个国家，不要编造其他国家的参与

## ⚠️ 地点准确性规则
- **Sudan (苏丹)** 和 **South Sudan (南苏丹)** 是两个不同的国家，绝不能混淆
- 如果地点包含 "Kordofan" (科尔多凡)，这是 **Sudan (苏丹)** 的州，不是南苏丹
- 如果地点包含 "Darfur" (达尔富尔)，这也是 **Sudan (苏丹)** 的地区
- 优先使用原文摘要中的国家名称

## ⚠️ 伤亡数据优先级规则
- 如果有多个伤亡数据来源，按以下优先级选择：
  1. 联合国 (UN) 官方声明
  2. 当事国政府声明
  3. 其他媒体报道
- 如果不同来源数据不一致，使用最低/最保守的数字
- 示例: UN说6人受伤, 孟加拉国说8人受伤 → 使用"至少6人受伤"

## ⚠️ 版权保护规则 (必须严格遵守)
1. **引语改写要求**:
   - 直接引语最多使用原文的15个字，超出部分必须改写
   - 格式: "XXX表示，他认为...(改写内容)"
   - 禁止: 连续复制超过20个字的原文句子

2. **事实重述要求**:
   - 所有事实描述必须用自己的语言重新表述
   - 数字可以保留原样，但描述语句必须改写
   - 示例: 原文 "At least 5,400 people were injured" → 改写为 "据报道，约有5400人在灾害中受伤"

3. **禁止推断对话**:
   - 严禁编造未提供的人物对话或想法
   - 如果引用数据中没有某人的具体话语，不要猜测他们说了什么

4. **来源标注**:
   - 每条新闻结尾必须标注信息来源
   - 格式: "*信息来源: [媒体名称]*"

## ⚠️ 关于人物称呼的重要规则
- 必须优先参考"原文摘要"中的称呼（如 Mr./Ms./Dr.）
- 示例：如果原文摘要写 "Mr. Mbaonu O. Mbaonu"，则必须使用"先生"或 "Mbaonu 先生"
- 如果原文摘要中没有性别信息，直接使用姓名，不要猜测性别

## 📝 生成示例

### 输入示例:
- 标题: Death Sentence Appeal Case
- 关键人物: Obadiah Mbaonu, Justice Agwu Umah Kalu
- 原文摘要: [摘要参考] An Abia indigene, Mr. Mbaonu O. Mbaonu, has appealed to Governor Alex Otti...

### 正确输出:
```
### 尼日利亚 Abia 州一父亲为儿子死刑案向州长求情

Mbaonu O. Mbaonu 先生呼吁 Abia 州州长介入其儿子 Obadiah Mbaonu 的死刑案件。Mbaonu 先生表示："我儿子是无辜的..."
```

### 错误输出 (不要这样写):
```
Ms. Mbaonu 作为母亲呼吁... ❌ (原文明确是 Mr.，不是 Ms.)
```

## ⚠️ 引语归属准确性
- 每条引语必须正确归属到原始发言人
- 如果"核心引用"中标注了发言人 (如 "Antonio Guterres 表示")，必须使用该发言人
- 严禁将 A 的话错误归属给 B
- 如果原文摘要中明确了人物角色（如"母亲 Hayley Peoples"），请使用正确角色描述

## ⚠️ 人物角色识别
- 优先从原文摘要中识别人物关系（父亲/母亲/儿子/女儿等）
- 如果 Key_Persons 只有姓名，请从原文摘要中查找其角色
- 示例：原文写 "Hayley Peoples, 21, contacted police" → 识别为母亲

请生成新闻:
"""


def post_process_news(news_text: str, record: Dict[str, Any]) -> str:
    """
    后处理校验层：验证并修复 LLM 生成的新闻文本
    
    Args:
        news_text: LLM 生成的新闻文本
        record: 原始记录数据（用于校验）
    
    Returns:
        修复后的新闻文本
    """
    import re
    
    processed = news_text
    
    # === 1. 过滤不合理的大数字 ===
    # 匹配 "18000000名证人" 或 "5000000 people" 等模式
    unreasonable_patterns = [
        (r'\d{6,}名?(?:证人|观察者|目击者|witnesses?)', '多名目击者'),
        (r'\d{6,}\s*(?:people|persons?)\s*(?:living|affected|noticed|observed)', '大量民众'),
    ]
    for pattern, replacement in unreasonable_patterns:
        processed = re.sub(pattern, replacement, processed, flags=re.IGNORECASE)
    
    # === 2. 修复性别称呼 (基于原文摘要) ===
    article_summary = record.get('Article_Summary', '')
    
    # 如果原文摘要中包含 "Mr." 但生成文本使用了 "Ms./女士/母亲"
    if 'Mr.' in article_summary or 'Mr ' in article_summary:
        # 检查是否有错误的女性称呼
        if any(term in processed for term in ['Ms.', '女士', '母亲', 'her son', 'she ']):
            # 尝试修复常见的性别错误
            processed = processed.replace('Ms.', 'Mr.')
            processed = processed.replace('女士', '先生')
            processed = processed.replace('母亲', '父亲')
            processed = re.sub(r'\bher son\b', 'his son', processed, flags=re.IGNORECASE)
            processed = re.sub(r'\bshe\b', 'he', processed, flags=re.IGNORECASE)
    
    # === 3. 地点修正: Sudan vs South Sudan ===
    locations = record.get('Locations', '')
    # 如果原始地点包含 Kordofan 或 Darfur (苏丹的地区)，但文本写成"南苏丹"
    sudan_regions = ['Kordofan', 'Darfur', 'Khartoum', 'Kadugli']
    if any(region in locations for region in sudan_regions):
        if '南苏丹' in processed and 'South Sudan' not in locations:
            processed = processed.replace('南苏丹', '苏丹')
        if 'South Sudan' in processed and 'South Sudan' not in locations:
            processed = processed.replace('South Sudan', 'Sudan')
    
    # === 3. 版权保护：截断过长的直接引语 ===
    # 匹配中文引号内的长引语 (更严格: 30字符)
    def truncate_quote(match):
        quote = match.group(1)
        if len(quote) > 30:  # 更严格: 超过30字符的引语截断
            return '"' + quote[:25] + '..."'
        return match.group(0)
    
    processed = re.sub(r'"([^"]{31,})"', truncate_quote, processed)
    processed = re.sub(r'"([^"]{31,})"', truncate_quote, processed)
    
    # === 4. 检测并标记潜在侵权风险 (英文长句) ===
    # 如果包含超过40个连续英文字符的句子，添加改写标记
    long_english = re.findall(r'[a-zA-Z\s,]{40,}', processed)
    if long_english:
        for phrase in long_english[:2]:  # 最多处理2个
            short_phrase = phrase[:35].rsplit(' ', 1)[0] + '...'
            processed = processed.replace(phrase, short_phrase)
    
    # === 5. 确保来源标注存在 ===
    source_name = record.get('Source_Name', '')
    if source_name and source_name not in processed:
        # 如果新闻末尾没有来源标注，添加一个
        if not any(marker in processed for marker in ['信源', '来源', 'Source', '信息来源']):
            processed = processed.rstrip() + f"\n\n*信息来源: {source_name}*"
    
    return processed


class LLMNewsGenerator:
    """
    LLM 新闻生成器
    使用硅基流动 API 调用 Qwen 模型
    """
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        """
        初始化生成器
        
        Args:
            api_key: SiliconFlow API Key，如果不提供则从环境变量获取
            model: 模型名称，默认使用 Qwen/Qwen2.5-7B-Instruct
        """
        self.api_key = api_key or SILICONFLOW_API_KEY
        self.model = model or SILICONFLOW_MODEL
        self.api_url = SILICONFLOW_API_URL
        
        if not self.api_key:
            raise ValueError(
                "未设置 API Key！请设置环境变量 SILICONFLOW_API_KEY 或在初始化时传入 api_key"
            )
    
    def _build_prompt(self, record: Dict[str, Any]) -> str:
        """
        根据记录数据构建提示词
        
        Args:
            record: 解析后的新闻记录字典
            
        Returns:
            格式化后的提示词
        """
        return NEWS_PROMPT_TEMPLATE.format(
            title=record.get('Title', 'Unknown'),
            source_name=record.get('Source_Name', 'Unknown'),
            time=record.get('Time', 'Unknown'),
            locations=record.get('Locations', 'Unknown'),
            key_persons=record.get('Key_Persons', 'Unknown'),
            organizations=record.get('Organizations', 'Unknown'),
            emotions=record.get('Emotions', 'Neutral'),
            tone=record.get('Tone', 'Neutral'),
            themes=record.get('Themes', 'General'),
            quotes=record.get('Quotes', 'No quotes available'),
            data_facts=record.get('Data_Facts', 'No specific data'),
            article_summary=record.get('Article_Summary', '(无法获取原文摘要，请基于上述结构化数据生成)')
        )
    
    def generate_news(self, record: Dict[str, Any], 
                      temperature: float = 0.7,
                      max_tokens: int = 1024) -> str:
        """
        根据记录数据生成新闻文本
        
        Args:
            record: 解析后的新闻记录字典
            temperature: 生成温度，越高越有创意
            max_tokens: 最大生成 token 数
            
        Returns:
            生成的新闻文本
        """
        prompt = self._build_prompt(record)
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "你是一名专业的国际新闻记者，擅长根据结构化数据撰写准确、客观的新闻报道。"
                },
                {
                    "role": "user",
                    "content": prompt
                }
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
                timeout=60
            )
            response.raise_for_status()
            
            result = response.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                raw_news = result['choices'][0]['message']['content']
                # 应用后处理校验层
                return post_process_news(raw_news, record)
            else:
                return f"API 返回格式错误: {result}"
                
        except requests.exceptions.Timeout:
            return "错误: API 请求超时"
        except requests.exceptions.RequestException as e:
            return f"错误: API 请求失败 - {str(e)}"
        except Exception as e:
            return f"错误: 生成新闻时发生异常 - {str(e)}"


# ================= 便捷方法 =================

def generate_news_from_record(record: Dict[str, Any], 
                               api_key: Optional[str] = None) -> str:
    """
    根据记录生成新闻的便捷方法
    
    Args:
        record: 解析后的新闻记录字典
        api_key: API Key (可选)
        
    Returns:
        生成的新闻文本
    """
    try:
        generator = LLMNewsGenerator(api_key=api_key)
        return generator.generate_news(record)
    except ValueError as e:
        return f"错误: {str(e)}"


def get_prompt_template() -> str:
    """获取提示词模板"""
    return NEWS_PROMPT_TEMPLATE


def format_prompt(record: Dict[str, Any]) -> str:
    """
    格式化提示词（不调用 API）
    
    Args:
        record: 解析后的新闻记录
        
    Returns:
        格式化后的提示词
    """
    return NEWS_PROMPT_TEMPLATE.format(
        title=record.get('Title', 'Unknown'),
        source_name=record.get('Source_Name', 'Unknown'),
        time=record.get('Time', 'Unknown'),
        locations=record.get('Locations', 'Unknown'),
        key_persons=record.get('Key_Persons', 'Unknown'),
        organizations=record.get('Organizations', 'Unknown'),
        emotions=record.get('Emotions', 'Neutral'),
        tone=record.get('Tone', 'Neutral'),
        themes=record.get('Themes', 'General'),
        quotes=record.get('Quotes', 'No quotes available'),
        data_facts=record.get('Data_Facts', 'No specific data')
    )
