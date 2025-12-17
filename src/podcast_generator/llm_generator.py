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
    from location_translator import translate_locations_string
    from person_positions import translate_persons_string
except ImportError:
    # 如果导入失败，提供空实现
    def translate_locations_string(s: str) -> str:
        return s
    def translate_persons_string(s: str) -> str:
        return s


# ================= 配置 =================
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
SILICONFLOW_MODEL = "Qwen/Qwen2.5-7B-Instruct"

# API Key 从环境变量获取，或在此处直接设置
# 请在环境变量中设置 SILICONFLOW_API_KEY，或替换下面的值
SILICONFLOW_API_KEY = "sk-rufxmuzljylovtepourxbutettstqbggozkexzpzvpjwilwb"


# ================= LLM 提示词模板 =================
NEWS_PROMPT_TEMPLATE = """
你是一名专业的国际新闻记者。根据以下 GDELT 提取的结构化数据，撰写一篇 350-450 字的新闻报道。

## ⛔ 核心规则（必须严格遵守）

### 1. 事实核验 - 零容忍编造
- **只使用提供的数据**：不添加任何数据中没有的信息
- **地点必须精确**：只使用"地点"字段中列出的地名，不要推断、扩展或概括
- **人物必须精确**：只提及"关键人物"字段中的人名和职位，不要添加未列出的人
- **禁止推断因果**：不解释数据中未明确说明的原因或动机


### 2. 数字处理 - 保守原则
- **只使用"关键数据"中的数字**，不要使用标题中的数字（标题可能不准确）
- **数据冲突时选最小值**：如果有"11人死亡"和"15人死亡"两个数据，使用"至少11人死亡"
- **不要四舍五入或估算**：直接使用原始数字
- **标注数据来源**：如"据报道有42人被送往医院"
- **必须使用所有数据**：不要遗漏任何一条数据，包括"2 police officers"这类细节
- **正确理解数据单位**：
  - "X people/persons" = 人数
  - "X tall/high/meters" = 高度/长度，不是人数
  - "X dollars/万" = 金额，不是人数
  - "X firearms/guns" = 武器数量
- **过滤无关数据**：
  - 忽略包含 "premium domains"、"advertising"、"subscribe" 等广告相关数据
  - 忽略明显与新闻事件无关的数字


### 3. 时间处理 - 使用精确时间戳
- 时间字段格式为 YYYYMMDDHHMMSS（如 20251215010000 = 2025年12月15日01:00:00）
- 在新闻中使用"当地时间X月X日"格式，不要写"今天"、"昨天"等相对时间
- 不要自行推断"圣诞节前夜"等节日描述，除非数据中明确提及

### 4. 引语使用 - 必须全部使用 ⚠️
- **强制要求：必须使用下方提供的每一条引语**
- 每条引语都要标注说话人姓名和职位（如有）
- 引语格式：「XXX表示："引语内容"」或「XXX指出：...」
- 直接引用控制在20字以内，超出部分改写为间接引语，但核心意思必须保留
- 如果引语较多，可分散到不同段落中使用

### 5. 引语归属判断 - 区分直接发言与指控 ⚠️
- **关键判断**：引语内容是本人直接发言，还是他人对其的指控/转述？
- **负面内容警惕**：如果引语内容是负面的（如"参与恐怖活动"、"重建恐怖组织"），很可能是他人的指控，不是本人发言
- **正确处理方式**：
  - 如果"A表示：X参与了恐怖活动"，应写成"据报道，A被指控参与恐怖活动"
  - 如果"以色列称X正在重建恐怖组织"，应写成"以色列方面指控X重建恐怖组织"
- **禁止错误归属**：不要将对某人的指控写成该人自己的发言

---

## 新闻素材

### 基本信息
- 标题线索: {title}（注意：标题中的数字可能不准确，以关键数据为准）
- 信源: {source_name}
- 精确时间: {time}
- 地点（仅使用这些）: {locations}
- 关键人物: {key_persons}
- 涉及机构: {organizations}
- 情感基调: {emotions} ({tone})
- 主题标签: {themes}

### 引语素材（请全部使用）
{quotes}

### 数据事实（请全部使用）
{data_facts}

---

## 输出格式（必须遵守）

```
### [新闻标题]

#### 导语
[一句话：时间+地点+人物+事件核心]

#### 正文
[段落1：核心事件描述，引用关键数据中的数字]
[段落2：第一组引语，2-3条相关人物的表态]
[段落3：第二组引语，其他人物的表态]
[段落4：背景或影响，使用剩余引语]

#### 信息来源
*本文信息综合自 {source_name}。报道引用了[列出所有引语来源人名]的公开表态。*
```

## 特别注意
- 标题简洁有力，不超过 25 字
- 如标题是UUID格式（如 A019Ffb1...）则根据内容自拟标题
- Sudan（苏丹）≠ South Sudan（南苏丹），严格区分
- 如数据中人名标注了职位（如"澳大利亚总理Anthony Albanese"），直接使用完整表述
- 禁止写"据悉"、"据了解"等模糊表达，要么有出处要么不写

### 文章类型识别
- 如果数据中包含**多个不同日期的事件**（如2025年、2022年、2019年...），说明这是**历史回顾/盘点类文章**
- 历史回顾类文章应聚焦**最新/最主要的事件**，不要把多个历史事件混为一谈
- 如机构字段包含多个不相关地点（如Darwin、Port Arthur、Lindt Cafe），说明是多事件盘点

### 地点使用优先级
- **优先使用地点字段中排在最前面的具体地点**作为事件发生地
- 如果地点字段包含 "Sydney" 或 "Bondi Beach"，事件地点就是悉尼，不是墨尔本
- 地点字段中可能包含多个地点（如当事人国籍、相关国家），但**事件发生地通常是第一个具体城市/地区**
- Melbourne（墨尔本）和 Sydney（悉尼）是不同的城市，严格区分

### 数据单位再次强调
- "X firearms" = X支枪械/武器，不是X人受伤
- "X shooters/gunmen" = X名枪手，不是X人死亡
- "X people at/gathered" = X人在场/聚集，不是伤亡数
- "X people dead/killed/murdered" = X人死亡


- **检查清单**：生成后自查是否使用了所有引语，如遗漏请补充

请生成新闻:
"""


# ================= English Prompt Template =================
NEWS_PROMPT_TEMPLATE_EN = """
You are a professional international news journalist. Based on the following GDELT extracted structured data, write a 300-400 word news article.

## ⛔ Core Rules (Must Strictly Follow)

### 1. Fact Verification - Zero Tolerance for Fabrication
- **Use ONLY the provided data**: Do not add any information not in the data
- **Locations must be exact**: Only use place names listed in "Locations" field
- **People must be exact**: Only mention names and positions listed in "Key Persons" field
- **No causal inference**: Do not explain reasons not explicitly stated in data

### 2. Number Handling - Conservative Principle
- **Only use numbers from "Data Facts"**, not from the title (titles may be inaccurate)
- **Use minimum value when data conflicts**: If "11 dead" and "15 dead" both appear, use "at least 11 dead"
- **Use all provided data**: Do not omit any data point, including "2 police officers" etc.
- **Understand data units correctly**:
  - "X people/persons" = number of people
  - "X tall/high/meters" = height/length, NOT people count
  - "X dollars" = money amount, NOT people count
  - "X firearms/guns" = weapon count

### 3. Time Handling - Use Exact Timestamps
- Time format is YYYYMMDDHHMMSS (e.g., 20251215010000 = December 15, 2025 01:00:00)
- Use "on [Month] [Day]" format, avoid "today", "yesterday"
- Do not infer holidays unless explicitly mentioned

### 4. Quote Usage - Must Use All ⚠️
- **Required: Use every quote provided below**
- Each quote must include speaker's name and title (if available)
- Format: [Name] said: "[quote content]"
- If many quotes, distribute across different paragraphs

### 5. Quote Attribution - Distinguish Statements vs Accusations ⚠️
- **Key judgment**: Is the quote content the person's own statement, or someone else's accusation?
- **Negative content alert**: If quote content is negative (e.g., "engaged in terror"), it's likely an accusation, not the person's own words
- **Correct handling**:
  - If "Israel said X was engaged in rebuilding terror" → Write: "Israel accused X of rebuilding terror organization"
  - Do NOT write: "X said: 'We are rebuilding terror organization'"

---

## News Materials

### Basic Information
- Title hint: {title} (Note: numbers in title may be inaccurate, use Data Facts)
- Source: {source_name}
- Exact time: {time}
- Locations (use ONLY these): {locations}
- Key Persons: {key_persons}
- Organizations: {organizations}
- Emotional tone: {emotions} ({tone})
- Theme tags: {themes}

### Quotes (Must use ALL)
{quotes}

### Data Facts (Must use ALL)
{data_facts}

---

## Output Format (Must Follow)

```
### [News Title]

#### Lead
[One sentence: time + location + person + core event]

#### Body
[Paragraph 1: Core event description, cite data facts]
[Paragraph 2: First group of quotes, 2-3 related statements]
[Paragraph 3: Second group of quotes, other statements]
[Paragraph 4: Background or impact, use remaining quotes]

#### Sources
*This article is based on {source_name}. The report cites statements from [list all quote sources].*
```

## Special Notes
- Title should be concise, no more than 15 words
- If title is UUID format (e.g., A019Ffb1...), create appropriate title based on content
- Sudan ≠ South Sudan, strictly distinguish
- Use full names with titles when provided (e.g., "Australian PM Anthony Albanese")

### Article Type Recognition
- If data contains **events from multiple different dates** (2025, 2022, 2019...), this is a **historical review article**
- For historical reviews, focus on the **most recent/main event**, don't mix multiple historical events

Please generate the news article:
"""


def get_prompt_template(language: str = "zh") -> str:
    """
    获取指定语言的提示词模板
    
    Args:
        language: 语言代码，"zh" 为中文，"en" 为英文
        
    Returns:
        对应语言的提示词模板
    """
    if language.lower() == "en":
        return NEWS_PROMPT_TEMPLATE_EN
    return NEWS_PROMPT_TEMPLATE


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
    
    # === 2. 修复性别称呼 (基于引用中的称呼) ===
    quotes = record.get('Quotes', '')
    
    # 如果引用中包含 "Mr." 但生成文本使用了 "Ms./女士/母亲"
    if 'Mr.' in quotes or 'Mr ' in quotes:
        # 检查是否有错误的女性称呼
        if any(term in processed for term in ['Ms.', '女士', '母亲', 'her son', 'she ']):
            # 尝试修复常见的性别错误
            processed = processed.replace('Ms.', 'Mr.')
            processed = processed.replace('女士', '先生')
            processed = processed.replace('母亲', '父亲')
            processed = re.sub(r'\bher son\b', 'his son', processed, flags=re.IGNORECASE)
            processed = re.sub(r'\bshe\b', 'he', processed, flags=re.IGNORECASE)
    
    # === 3. 地点修正 ===
    # 注：Sudan vs South Sudan 的区分现在由 location_translator.py 在预处理阶段完成
    
    # === 4. 版权保护：截断过长的直接引语 ===
    # 匹配中文引号内的长引语 (更严格: 30字符)
    def truncate_quote(match):
        quote = match.group(1)
        if len(quote) > 30:  # 更严格: 超过30字符的引语截断
            return '"' + quote[:25] + '..."'
        return match.group(0)
    
    processed = re.sub(r'"([^"]{31,})"', truncate_quote, processed)
    processed = re.sub(r'"([^"]{31,})"', truncate_quote, processed)
    
    # === 5. 检测并标记潜在侵权风险 (英文长句) ===
    # 如果包含超过40个连续英文字符的句子，添加改写标记
    long_english = re.findall(r'[a-zA-Z\s,]{40,}', processed)
    if long_english:
        for phrase in long_english[:2]:  # 最多处理2个
            short_phrase = phrase[:35].rsplit(' ', 1)[0] + '...'
            processed = processed.replace(phrase, short_phrase)
    
    # === 6. 确保来源标注存在 ===
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
    
    def _build_prompt(self, record: Dict[str, Any], language: str = "zh") -> str:
        """
        根据记录数据构建提示词
        
        Args:
            record: 解析后的新闻记录字典
            language: 语言代码，"zh" 为中文，"en" 为英文
            
        Returns:
            格式化后的提示词
        """
        template = get_prompt_template(language)
        
        # 获取原始数据
        locations = record.get('Locations', 'Unknown')
        key_persons = record.get('Key_Persons', 'Unknown')
        
        # 如果是中文模式，预翻译地点和人物
        if language == "zh":
            locations = translate_locations_string(locations)
            key_persons = translate_persons_string(key_persons)
        
        return template.format(
            title=record.get('Title', 'Unknown'),
            source_name=record.get('Source_Name', 'Unknown'),
            time=record.get('Time', 'Unknown'),
            locations=locations,
            key_persons=key_persons,
            organizations=record.get('Organizations', 'Unknown'),
            emotions=record.get('Emotions', 'Neutral'),
            tone=record.get('Tone', 'Neutral'),
            themes=record.get('Themes', 'General'),
            quotes=record.get('Quotes', 'No quotes available'),
            data_facts=record.get('Data_Facts', 'No specific data')
        )
    
    def generate_news(self, record: Dict[str, Any], 
                      temperature: float = 0.7,
                      max_tokens: int = 1024,
                      language: str = "zh") -> str:
        """
        根据记录数据生成新闻文本
        
        Args:
            record: 解析后的新闻记录字典
            temperature: 生成温度，越高越有创意
            max_tokens: 最大生成 token 数
            language: 语言代码，"zh" 为中文，"en" 为英文
            
        Returns:
            生成的新闻文本
        """
        prompt = self._build_prompt(record, language)
        
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
                               api_key: Optional[str] = None,
                               language: str = "zh") -> str:
    """
    根据记录生成新闻的便捷方法
    
    Args:
        record: 解析后的新闻记录字典
        api_key: API Key (可选)
        language: 语言代码，"zh" 为中文，"en" 为英文
        
    Returns:
        生成的新闻文本
    """
    try:
        generator = LLMNewsGenerator(api_key=api_key)
        return generator.generate_news(record, language=language)
    except ValueError as e:
        return f"错误: {str(e)}"



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
