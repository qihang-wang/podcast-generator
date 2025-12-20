"""
新闻翻译模块
使用 LLM 将英文新闻翻译成中文
翻译后自动使用 person_positions 数据库替换人名和职位（无需 LLM 翻译）
"""

import os
import re
import requests
from typing import Optional

# 导入人物数据库
try:
    from person_positions import KNOWN_PERSONS_FULL, NAME_VARIANTS, enrich_person_with_position
except ImportError:
    KNOWN_PERSONS_FULL = {}
    NAME_VARIANTS = {}
    def enrich_person_with_position(name: str, language: str = "zh") -> str:
        return name


# ================= 配置 =================
SILICONFLOW_API_URL = "https://api.siliconflow.cn/v1/chat/completions"
SILICONFLOW_MODEL = "Qwen/Qwen2.5-7B-Instruct"


# ================= 翻译提示词中文说明（仅供参考，不发送给LLM）=================
"""
翻译提示词中文说明：

你是一名专业的新闻翻译员。将以下英文新闻翻译成中文。

## 翻译规则：
1. 保持相同结构（如果有标题、导语、正文、来源）
2. 保持新闻专业语调 - 正式客观
3. 只输出中文，英文专有名词除外
4. 【人名+职位】：保留英文原文不翻译！
   - 后处理会自动从数据库替换
   - 例如：保持 "US President Joe Biden" 不变
   - 后处理会自动替换为 "美国总统乔·拜登"
5. 地名：使用标准中文名（如 Sydney → 悉尼, Gaza → 加沙）
6. 保持所有数字和日期准确
7. 精确保留引语的含义
8. 【关键】年份必须保持原样
   - "December 14, 2025" → "2025年12月14日"
   - 不得以任何理由修改年份

## 后处理自动替换（无需 LLM 翻译）：
- "US President Joe Biden" → "美国总统乔·拜登"
- "UN Secretary-General Antonio Guterres" → "联合国秘书长安东尼奥·古特雷斯"
- "Israeli Prime Minister Benjamin Netanyahu" → "以色列总理本雅明·内塔尼亚胡"
"""


# ================= 翻译提示词模板 =================
TRANSLATION_PROMPT_TEMPLATE = """
You are a professional news translator. Translate the following English news article to Chinese.

## Translation Rules:
1. Keep the same structure (title, lead, body, sources if present)
2. Maintain journalistic tone - formal and objective
3. Output ONLY Chinese, no English text except for person names with titles
4. **For person names with titles: DO NOT translate, keep English as-is**
   - Keep "US President Joe Biden" exactly as written
   - Keep "UN Secretary-General Antonio Guterres" exactly as written
   - They will be auto-replaced with Chinese versions later
5. For location names: use standard Chinese names (e.g., Sydney → 悉尼, Gaza → 加沙)
6. Keep all numbers and dates accurate
7. Preserve the meaning of quotes exactly
8. **CRITICAL - Year Preservation**: The year MUST remain exactly as stated.
   - "December 14, 2025" → "2025年12月14日"
   - DO NOT change the year under any circumstances

## English Article to Translate:
{english_article}

Please translate to Chinese (keep person names with titles in English):
"""


# ================= 构建英文职位+人名的替换映射 =================
def _build_replacement_map():
    """
    构建从 "英文职位 英文人名" 到 "中文职位中文人名" 的替换映射
    
    使用三元组完整数据：
    例如: "US President Joe Biden" → "美国总统乔·拜登"
    """
    replacement_map = {}
    
    for en_name, (en_pos, cn_pos, cn_name) in KNOWN_PERSONS_FULL.items():
        # 完整格式: "English Position Name" → "中文职位中文人名"
        en_full = f"{en_pos} {en_name}"
        cn_full = f"{cn_pos}{cn_name}"
        replacement_map[en_full] = cn_full
        
        # 也支持只有人名的情况
        replacement_map[en_name] = cn_name
    
    # 添加变体
    for variant, full_name in NAME_VARIANTS.items():
        if full_name in KNOWN_PERSONS_FULL:
            _, _, cn_name = KNOWN_PERSONS_FULL[full_name]
            replacement_map[variant] = cn_name
    
    return replacement_map


# ================= 翻译后处理函数 =================
def post_process_translation(chinese_text: str, original_english: str) -> str:
    """
    翻译后处理：修复常见翻译错误 + 自动替换人名和职位
    
    使用三元组完整数据（无需 LLM 翻译人名职位）：
    - "US President Joe Biden" → "美国总统乔·拜登"
    - "UN Secretary-General Antonio Guterres" → "联合国秘书长安东尼奥·古特雷斯"
    """
    processed = chinese_text
    
    # === 1. 修复年份错误 ===
    en_years = re.findall(r'\b(202[0-9])\b', original_english)
    cn_years = re.findall(r'(20[12][0-9])年', processed)
    
    if en_years and cn_years:
        expected_year = en_years[0]
        for wrong_year in cn_years:
            if wrong_year != expected_year:
                processed = processed.replace(f'{wrong_year}年', f'{expected_year}年')
                print(f"  ⚠️ 年份修正: {wrong_year} → {expected_year}")
    
    # === 2. 自动替换人名+职位（直接从数据库获取，无需 LLM 翻译）===
    replacement_map = _build_replacement_map()
    
    # 按长度倒序排列，优先替换长的完整格式
    sorted_keys = sorted(replacement_map.keys(), key=len, reverse=True)
    
    for en_text in sorted_keys:
        if en_text in processed:
            cn_text = replacement_map[en_text]
            if cn_text != en_text:
                processed = processed.replace(en_text, cn_text)
                print(f"  ✓ 替换: {en_text} → {cn_text}")
    
    # === 3. 修复中英混合问题 - 常见地名 ===
    location_fixes = {
        'Gaza': '加沙', 'Sudan': '苏丹', 'South Sudan': '南苏丹',
        'Israel': '以色列', 'Palestine': '巴勒斯坦', 'Jerusalem': '耶路撒冷',
        'Tel Aviv': '特拉维夫', 'Bangladesh': '孟加拉国', 'Pakistan': '巴基斯坦',
        'Germany': '德国', 'Bavaria': '巴伐利亚', 'Munich': '慕尼黑',
        'Nigeria': '尼日利亚', 'Ukraine': '乌克兰', 'Russia': '俄罗斯',
        'Moscow': '莫斯科', 'Kyiv': '基辅', 'Beijing': '北京',
        'Shanghai': '上海', 'Tokyo': '东京', 'Seoul': '首尔',
        'Sydney': '悉尼', 'Melbourne': '墨尔本', 'London': '伦敦',
        'Paris': '巴黎', 'Berlin': '柏林', 'Washington': '华盛顿',
        'New York': '纽约', 'Los Angeles': '洛杉矶', 'Toronto': '多伦多',
        'Canada': '加拿大', 'Australia': '澳大利亚', 'United Kingdom': '英国',
        'United States': '美国', 'France': '法国', 'Italy': '意大利',
        'Spain': '西班牙', 'Japan': '日本', 'China': '中国', 'India': '印度',
        'Brazil': '巴西', 'Mexico': '墨西哥', 'Iran': '伊朗', 'Tehran': '德黑兰',
        'Iraq': '伊拉克', 'Baghdad': '巴格达', 'Syria': '叙利亚',
        'Damascus': '大马士革', 'Turkey': '土耳其', 'Ankara': '安卡拉',
        'Istanbul': '伊斯坦布尔', 'Egypt': '埃及', 'Cairo': '开罗',
        'Saudi Arabia': '沙特阿拉伯', 'Riyadh': '利雅得', 'UAE': '阿联酋',
        'Dubai': '迪拜', 'Poland': '波兰', 'Warsaw': '华沙',
        'Hungary': '匈牙利', 'Budapest': '布达佩斯', 'Netherlands': '荷兰',
        'Belgium': '比利时', 'Brussels': '布鲁塞尔', 'Switzerland': '瑞士',
        'Geneva': '日内瓦', 'Vienna': '维也纳', 'Austria': '奥地利',
        'Sweden': '瑞典', 'Stockholm': '斯德哥尔摩', 'Norway': '挪威',
        'Denmark': '丹麦', 'Copenhagen': '哥本哈根', 'Finland': '芬兰',
        'Singapore': '新加坡', 'Malaysia': '马来西亚', 'Indonesia': '印度尼西亚',
        'Thailand': '泰国', 'Bangkok': '曼谷', 'Vietnam': '越南',
        'Philippines': '菲律宾', 'South Korea': '韩国', 'North Korea': '朝鲜',
        'Taiwan': '台湾', 'Hong Kong': '香港', 'Afghanistan': '阿富汗',
        'Myanmar': '缅甸', 'Sri Lanka': '斯里兰卡', 'Argentina': '阿根廷',
        'Chile': '智利', 'Colombia': '哥伦比亚', 'Venezuela': '委内瑞拉',
        'Cuba': '古巴', 'South Africa': '南非', 'Kenya': '肯尼亚',
        'Ethiopia': '埃塞俄比亚', 'Morocco': '摩洛哥', 'New Zealand': '新西兰',
    }
    
    # 按长度倒序替换地名
    for en_loc in sorted(location_fixes.keys(), key=len, reverse=True):
        cn_loc = location_fixes[en_loc]
        pattern = rf'\b{re.escape(en_loc)}\b'
        if re.search(pattern, processed):
            processed = re.sub(pattern, cn_loc, processed)
    
    # === 4. 修复格式问题 ===
    processed = processed.replace('领导语', '导语')
    
    return processed


class NewsTranslator:
    """新闻翻译器 - LLM翻译 + 自动人名职位替换（使用三元组数据）"""
    
    def __init__(self, 
                 api_key: Optional[str] = None,
                 api_url: str = SILICONFLOW_API_URL,
                 model: str = SILICONFLOW_MODEL):
        self.api_key = api_key or os.environ.get('SILICONFLOW_API_KEY') or "sk-rufxmuzljylovtepourxbutettstqbggozkexzpzvpjwilwb"
        self.api_url = api_url
        self.model = model
        
        if not self.api_key:
            raise ValueError("未找到 API Key")
    
    def translate_to_chinese(self, english_text: str, 
                              temperature: float = 0.3,
                              print_comparison: bool = True) -> str:
        """将英文新闻翻译成中文"""
        if print_comparison:
            print("\n" + "="*60)
            print("📝 翻译前 (English):")
            print("-"*60)
            print(english_text)
            print("-"*60)
        
        translation_prompt = TRANSLATION_PROMPT_TEMPLATE.format(
            english_article=english_text
        )
        
        # 系统提示词
        system_prompt = (
            "你是专业新闻翻译员。年份必须与原文一致。"
            "人名和职位保留英文原文，系统会自动替换为中文。"
        )
        
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
                self.api_url, headers=headers, json=payload, timeout=60
            )
            response.raise_for_status()
            result = response.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                chinese_text = result['choices'][0]['message']['content']
                
                # 应用后处理（自动替换人名+职位，直接从数据库获取）
                chinese_text = post_process_translation(chinese_text, english_text)
                
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


def translate_news(english_text: str, 
                   api_key: Optional[str] = None,
                   print_comparison: bool = True) -> str:
    """翻译英文新闻的便捷方法"""
    try:
        translator = NewsTranslator(api_key=api_key)
        return translator.translate_to_chinese(english_text, print_comparison=print_comparison)
    except ValueError as e:
        return f"错误: {str(e)}"
