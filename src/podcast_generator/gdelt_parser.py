"""
GDELT 原始数据解析模块
用于解析 BigQuery 返回的 GDELT 原始数据并转换为结构化叙事格式
"""

import re
from urllib.parse import urlparse
from collections import Counter
from typing import List, Dict, Any, Optional
import requests

# 导入人物职位数据库
try:
    from person_positions import enrich_person_with_position, get_person_position
except ImportError:
    # 如果导入失败，提供空实现
    def enrich_person_with_position(name: str) -> str:
        return name
    def get_person_position(name: str) -> Optional[str]:
        return None


def fetch_article_summary(url: str, max_chars: int = 1000) -> str:
    """
    抓取原文摘要作为 LLM 补充信息
    
    注意：
    - 仅抓取前1000字作为摘要参考，避免版权问题
    - 抓取失败时返回空字符串，使用 GDELT 数据作为兜底
    - 添加 User-Agent 模拟正常浏览器请求
    
    Args:
        url: 原文 URL
        max_chars: 最大抓取字符数（默认1000字，避免侵权）
    
    Returns:
        原文摘要文本，失败时返回空字符串
    """
    if not url or str(url) == 'nan':
        return ""
    
    try:
        from bs4 import BeautifulSoup
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        resp = requests.get(url, timeout=10, headers=headers)
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # 移除脚本和样式标签
        for script in soup(["script", "style", "nav", "header", "footer"]):
            script.decompose()
        
        # 提取正文段落（前5段）
        paragraphs = soup.find_all('p')
        text_parts = []
        current_len = 0
        
        for p in paragraphs:
            p_text = p.get_text(strip=True)
            if len(p_text) > 30:  # 过滤太短的段落
                text_parts.append(p_text)
                current_len += len(p_text)
                if current_len >= max_chars:
                    break
        
        summary = ' '.join(text_parts)[:max_chars]
        
        # 添加版权提示
        if summary:
            summary = f"[摘要参考] {summary}..."
        
        return summary
        
    except Exception as e:
        # 抓取失败时静默返回空字符串，使用 GDELT 数据作为兜底
        return ""


def format_chinese_number(num_str: str) -> str:
    """
    将大数字转换为中文单位表示
    
    示例:
        6000000 → "600万"
        100000000 → "1亿"
        1500000 → "150万"
    """
    try:
        # 移除逗号
        num = int(num_str.replace(',', ''))
        
        if num >= 100000000:  # 1亿及以上
            return f"{num // 100000000}亿"
        elif num >= 10000:  # 1万及以上
            wan = num / 10000
            if wan == int(wan):
                return f"{int(wan)}万"
            else:
                return f"{wan:.1f}万"
        else:
            return num_str
    except:
        return num_str


# ================= GCAM 情感代码映射 =================
GCAM_CODES = {
    'c1.1': 'Anger (愤怒)',
    'c1.2': 'Fear (恐惧)',
    'c1.3': 'Sadness (悲伤)',
    'c1.4': 'Joy (喜悦)',
    'c1.5': 'Disgust (厌恶)',
    'c1.6': 'Surprise (惊讶)',
    'c2.1': 'Trust (信任)',
    'c2.2': 'Anticipation (期待)',
    'c9.1': 'Negative (消极)',
    'c9.2': 'Positive (积极)',
    'c12.1': 'Hate (仇恨)',
    'c12.3': 'Violence (暴力)',
    'c12.5': 'Disaster (灾难)',
    'c12.10': 'Death (死亡)',
    'c12.14': 'Conflict (冲突)',
    'c18.1': 'Economic (经济)',
    'c18.4': 'Political (政治)',
}


# ================= 地点类型优先级映射 =================
# GDELT V2Locations 类型代码:
# 1 = COUNTRY, 2 = USSTATE/ADM1, 3 = USCITY/ADM2, 4 = WORLDCITY, 5 = WORLDSTATE
# 数字越小优先级越高（更具体的地点）
LOCATION_TYPE_PRIORITY = {
    '3': 1,   # 城市 - 最精确
    '4': 1,   # 世界城市
    '5': 2,   # 州/省级
    '2': 2,   # 美国州/行政区
    '1': 3,   # 国家 - 最泛化
}

# 仅作为来源/国籍提及的地点（可能与事件发生地无关）
SOURCE_ONLY_COUNTRIES = {
    'Chad', 'Bangladesh', 'Pakistan', 'Russia', 'China', 
    'Japan', 'South Korea', 'North Korea', 'Vietnam',
    'Philippines', 'Indonesia', 'Thailand', 'Malaysia',
}


class GdeltDataParser:
    """
    GDELT 原始数据解析器
    提供一系列静态方法用于解析 GDELT 各字段数据
    """
    
    @staticmethod
    def extract_title_from_url(url: str) -> str:
        """
        从 URL 中提取可能的文章标题
        改进版：尝试从URL路径的多个部分提取有意义的标题
        """
        if not url or str(url) == 'nan':
            return "Unknown Title"
        
        try:
            parsed = urlparse(str(url))
            path = parsed.path.strip('/')
            
            if not path:
                return "Unknown Title"
            
            # 将路径分割成多个段
            segments = [s for s in path.split('/') if s]
            
            # 从后往前尝试每个段
            for i in range(len(segments) - 1, -1, -1):
                slug = segments[i]
                
                # 移除常见后缀
                slug = re.sub(r'\.(html|htm|php|asp|aspx)$', '', slug, flags=re.IGNORECASE)
                
                # 新增：移除 "数字." 前缀 (如 "24851476.bishopbriggs-...")
                slug = re.sub(r'^\d+\.', '', slug)
                
                # 跳过纯数字或ID格式的段 (如 a242880323, 1649572)
                if re.match(r'^[a-z]?\d+$', slug, re.IGNORECASE):
                    continue
                
                # 跳过常见非标题的段
                skip_patterns = [
                    r'^(news|article|story|post|page|index|posts|focus)$',
                    r'^(syndicated-article|embed|amp|mobile|f-news)$',
                    r'^\d{4,}$',  # 日期或ID
                ]
                if any(re.match(p, slug, re.IGNORECASE) for p in skip_patterns):
                    continue
                
                # 替换分隔符为空格
                title = slug.replace('-', ' ').replace('_', ' ')
                
                # 如果标题太短，跳过
                if len(title) < 10:
                    continue
                
                # 标题格式化
                title = title.title()
                
                return title
            
            # 如果所有段都不适合，返回来源域名
            domain = parsed.netloc.replace('www.', '')
            return f"Article from {domain}"
        except:
            return "Unknown Title"


    @staticmethod
    def parse_quotations(raw_quotations: str) -> List[Dict[str, str]]:
        """解析引语字段"""
        if not raw_quotations or str(raw_quotations) == 'nan':
            return []
        
        quotes = []
        entries = str(raw_quotations).split('#')
        
        for entry in entries:
            parts = entry.split('|')
            if len(parts) >= 4:
                verb = parts[2].strip() if parts[2] else "said"
                quote_text = parts[3].strip() if parts[3] else ""
                if quote_text and len(quote_text) > 20:
                    quotes.append({"verb": verb, "text": quote_text})
            elif len(parts) == 1 and len(parts[0]) > 20:
                quotes.append({"verb": "said", "text": parts[0].strip()})
        
        return quotes

    @staticmethod
    def parse_amounts(raw_amounts: str) -> List[str]:
        """
        解析数量字段 - 优化版
        GDELT Amounts 格式: amount,type,context,offset;...
        注意: 数字可能包含逗号 (如 1,221)，需要特殊处理
        """
        if not raw_amounts or str(raw_amounts) == 'nan':
            return []
        
        facts = []
        entries = str(raw_amounts).split(';')
        
        for entry in entries[:10]:  # 增加扫描数量
            # 使用正则表达式匹配: 数字(可能带逗号) + 描述 + 位置
            match = re.match(r'^([\d,]+)\s*,\s*(.+?)(?:,(\d+))?$', entry.strip())
            if match:
                amount = match.group(1).strip()
                rest = match.group(2).strip()
                
                # 跳过时间相关的数据
                skip_types = ['weeks', 'days', 'months', 'years', 'hours', 'minutes', 'seconds', 'week', 'day', 'month', 'year']
                if any(skip in rest.lower() for skip in skip_types):
                    continue
                
                # 清理描述文本，移除末尾的数字（通常是 offset）
                desc = re.sub(r',\s*\d+$', '', rest).strip()
                
                # === 新增: 数字合理性验证 ===
                try:
                    num_value = int(amount.replace(',', ''))
                    # 过滤不合理的数字
                    invalid_contexts = ['witness', 'noticed', 'observed', 'saw', 'people living']
                    if any(ctx in desc.lower() for ctx in invalid_contexts):
                        if num_value > 10000:  # 超过1万人的证人/观察者数量不合理
                            continue
                    if num_value > 100000000:  # 超过1亿的数字通常不合理
                        continue
                except:
                    pass
                # === 验证结束 ===
                
                if amount and desc and len(desc) > 3:
                    # 使用中文数字格式化
                    formatted_amount = format_chinese_number(amount)
                    facts.append(f"{formatted_amount} {desc}")
            else:
                # 备用解析方式
                parts = entry.split(',')
                if len(parts) >= 2:
                    try:
                        amount = parts[0].strip()
                        desc = parts[1].strip() if len(parts) > 1 else ""
                        
                        skip_types = ['weeks', 'days', 'months', 'years', 'hours', 'minutes', 'seconds']
                        if not any(skip in desc.lower() for skip in skip_types):
                            if amount and desc:
                                formatted_amount = format_chinese_number(amount)
                                facts.append(f"{formatted_amount} {desc}")
                    except:
                        continue
        
        # 去重
        return list(dict.fromkeys(facts))[:8]

    @staticmethod
    def parse_themes(raw_themes: str) -> List[str]:
        """解析主题字段 - 优化版，提取更多主题"""
        if not raw_themes or str(raw_themes) == 'nan':
            return []
        
        themes = []
        entries = str(raw_themes).split(';')
        
        # 扫描更多主题
        for entry in entries[:30]:
            parts = entry.split(',')
            if parts:
                theme = parts[0].strip()
                # 跳过 WB_ 前缀的世界银行代码，但保留其他重要主题
                if theme and not theme.startswith('WB_') and not theme.startswith('TAX_FNCACT'):
                    themes.append(theme)
        
        # 统计频率，优先返回高频主题
        theme_counts = Counter(themes)
        unique_themes = [t for t, _ in theme_counts.most_common(10)]
        
        return unique_themes[:8]  # 返回更多主题

    @staticmethod
    def parse_persons(raw_persons: str) -> List[str]:
        """解析人物字段 - 优化版，提取更多人物并过滤媒体机构名称"""
        if not raw_persons or str(raw_persons) == 'nan':
            return []
        
        # 媒体机构名称列表 - 这些不应被识别为人物
        MEDIA_NAMES = {
            'Deutsche Welle', 'BBC', 'CNN', 'Reuters', 'AFP', 'AP',
            'Associated Press', 'Al Jazeera', 'Fox News', 'NBC', 'CBS', 'ABC',
            'The Guardian', 'New York Times', 'Washington Post', 'Wall Street Journal',
            'Times of Israel', 'Haaretz', 'Jerusalem Post', 'Sky News',
            'France 24', 'RT', 'TASS', 'Xinhua', 'Bloomberg', 'CNBC',
            'The Telegraph', 'Daily Mail', 'The Independent', 'Mirror',
        }
        
        persons = []
        entries = str(raw_persons).split(';')
        
        # 扫描更多人物
        for entry in entries[:20]:
            parts = entry.split(',')
            if parts:
                person = parts[0].strip()
                # 过滤条件：长度>2，不是媒体机构名称
                if person and len(person) > 2 and person not in MEDIA_NAMES:
                    persons.append(person)
        
        # 统计频率，优先返回高频人物
        person_counts = Counter(persons)
        unique_persons = [p for p, _ in person_counts.most_common(10)]
        
        return unique_persons  # 返回 10 个人物

    @staticmethod
    def parse_locations(raw_locations: str) -> List[str]:
        """
        解析地理位置字段 - 优化版
        格式: type#name#countrycode#adm1code#lat#long#featureid;...
        
        优化：
        1. 按地点类型优先级排序（城市 > 州/省 > 国家）
        2. 过滤仅作为国籍/来源的无关国家
        3. 限制返回数量，避免噪音
        """
        if not raw_locations or str(raw_locations) == 'nan':
            return []
        
        # 国籍/民族标记（不是实际地点，应过滤）
        NATIONALITY_MARKERS = {
            'South Korean', 'North Korean', 'Korean', 'Nigerian', 'Nigerians',
            'Chinese', 'Americans', 'American', 'Russian', 'Russians',
            'British', 'French', 'German', 'Japanese', 'Indian', 'Indians',
            'Pakistani', 'Bangladeshi', 'Egyptian', 'Iranian', 'Iraqi',
            'Syrian', 'Sudanese', 'Ethiopian', 'Filipino', 'Indonesian',
            'Canadian', 'Australian', 'Mexican', 'Brazilian', 'Moroccans',
            'Moroccan', 'Tunisian', 'Yemeni', 'Lebanese', 'Jordanian',
        }
        
        locations_with_priority = []
        entries = str(raw_locations).split(';')
        
        for idx, entry in enumerate(entries[:15]):  # 扫描更多条目
            parts = entry.split('#')
            if len(parts) >= 3:
                location_type = parts[0].strip()
                name = parts[1].strip()
                country_code = parts[2].strip() if len(parts) > 2 else ""
                adm1_code = parts[3].strip() if len(parts) > 3 else ""
                
                # 跳过国籍/民族标记
                if name in NATIONALITY_MARKERS:
                    continue
                
                # 跳过空名称
                if not name:
                    continue
                
                # 获取优先级（默认为3=国家级）
                priority = LOCATION_TYPE_PRIORITY.get(location_type, 3)
                
                # 对于仅作为来源的国家，降低优先级
                if name in SOURCE_ONLY_COUNTRIES and priority >= 3:
                    # 如果不是第一个地点，且是来源国家，跳过
                    if idx > 0:
                        continue
                
                # 构建地点字符串
                if country_code and country_code != name:
                    location_str = f"{name} ({country_code})"
                else:
                    location_str = name
                
                # 添加带优先级的元组 (优先级, 原始顺序, 地点字符串)
                # 原始顺序确保同优先级时保持GDELT给出的顺序（通常第一个最相关）
                locations_with_priority.append((priority, idx, location_str))
        
        # 按优先级和原始顺序排序
        locations_with_priority.sort(key=lambda x: (x[0], x[1]))
        
        # 提取地点字符串并去重
        seen = set()
        unique_locations = []
        for _, _, loc in locations_with_priority:
            if loc not in seen:
                seen.add(loc)
                unique_locations.append(loc)
        
        # 返回前4个最相关的地点
        return unique_locations[:4]

    @staticmethod
    def parse_organizations(raw_orgs: str) -> List[str]:
        """
        解析组织机构字段 - 优化版
        格式: org_name,offset;org_name,offset;...
        按出现频率排序，提取更多组织
        """
        if not raw_orgs or str(raw_orgs) == 'nan':
            return []
        
        orgs = []
        entries = str(raw_orgs).split(';')
        
        # 扫描更多组织
        for entry in entries[:30]:
            parts = entry.split(',')
            if parts:
                org = parts[0].strip()
                if org and len(org) > 2:
                    orgs.append(org)
        
        # 统计频率，优先返回高频组织
        org_counts = Counter(orgs)
        unique_orgs = [o for o, _ in org_counts.most_common(10)]
        
        return unique_orgs  # 返回 10 个组织

    @staticmethod
    def parse_gcam(raw_gcam: str) -> List[str]:
        """
        解析 GCAM 情感字段
        格式: code:value,code:value,...
        返回最显著的情感标签
        """
        if not raw_gcam or str(raw_gcam) == 'nan':
            return []
        
        entries = str(raw_gcam).split(',')
        
        emotion_scores = []
        for entry in entries:
            parts = entry.split(':')
            if len(parts) == 2:
                code = parts[0].strip()
                try:
                    score = float(parts[1].strip())
                    if code in GCAM_CODES and score > 0:
                        emotion_scores.append((GCAM_CODES[code], score))
                except:
                    continue
        
        emotion_scores.sort(key=lambda x: x[1], reverse=True)
        return [e[0] for e in emotion_scores[:5]]  # 增加到 5 个

    @staticmethod
    def parse_images(raw_images: str) -> List[str]:
        """解析社交媒体图片嵌入"""
        if not raw_images or str(raw_images) == 'nan':
            return []
        
        images = str(raw_images).split(';')
        return [img.strip() for img in images[:5] if img.strip()]
    
    @staticmethod
    def parse_videos(raw_videos: str) -> List[str]:
        """解析社交媒体视频嵌入"""
        if not raw_videos or str(raw_videos) == 'nan':
            return []
        
        videos = str(raw_videos).split(';')
        return [video.strip() for video in videos[:3] if video.strip()]
    
    @staticmethod
    def parse_full_tone(raw_tone: str) -> Dict[str, Any]:
        """
        解析完整的 V2Tone 字段
        格式: AvgTone,PositiveScore,NegativeScore,Polarity,ActivityReferenceDensity,SelfGroupReferenceDensity,WordCount
        """
        if not raw_tone or str(raw_tone) == 'nan':
            return {}
        
        parts = str(raw_tone).split(',')
        result = {}
        
        try:
            if len(parts) >= 1:
                result['avg_tone'] = float(parts[0])
            if len(parts) >= 2:
                result['positive_score'] = float(parts[1])
            if len(parts) >= 3:
                result['negative_score'] = float(parts[2])
            if len(parts) >= 4:
                result['polarity'] = float(parts[3])
            if len(parts) >= 5:
                result['activity_density'] = float(parts[4])
            if len(parts) >= 6:
                result['self_group_density'] = float(parts[5])
            if len(parts) >= 7:
                result['word_count'] = int(parts[6])
        except (ValueError, IndexError):
            pass
        
        return result


def process_narrative(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    数据清洗与叙事构建函数 - 完整优化版
    
    Args:
        row: 包含 GDELT 原始数据的字典
        
    Returns:
        结构化的叙事数据字典
    """
    parser = GdeltDataParser
    
    # 1. 新闻来源
    source_name = str(row.get('SourceCommonName', '')) if row.get('SourceCommonName') else "Unknown Source"
    
    # 2. 从 URL 提取标题
    title = parser.extract_title_from_url(row.get('SourceURL'))
    
    # 3. 解析引语
    quotes = parser.parse_quotations(row.get('Quotations'))
    quotes_sorted = sorted(quotes, key=lambda x: len(x['text']), reverse=True)[:15]
    
    # 解析关键人物用于引语归属
    persons = parser.parse_persons(row.get('V2Persons'))
    
    # 格式化引语：如果有人物信息，尝试关联；否则使用通用格式
    formatted_quotes = []
    for idx, q in enumerate(quotes_sorted):
        verb = q['verb'] if q['verb'] and q['verb'] != 'said' else ''
        if persons and idx < len(persons):
            # 有人物信息时关联
            speaker = persons[idx]
            if verb:
                formatted_quotes.append(f'{speaker} {verb}: "{q["text"]}"')
            else:
                formatted_quotes.append(f'{speaker} 表示: "{q["text"]}"')
        else:
            # 无人物信息时使用通用格式
            if verb:
                formatted_quotes.append(f'有关人士 {verb}: "{q["text"]}"')
            else:
                formatted_quotes.append(f'据报道: "{q["text"]}"')
    
    all_quotes = "\n---\n".join(formatted_quotes) if formatted_quotes else "No quotes available"
    
    # 4. 解析数据事实
    facts = parser.parse_amounts(row.get('Amounts'))
    facts_str = "; ".join(facts) if facts else "No specific data"
    
    # 5. 解析主题
    themes = parser.parse_themes(row.get('V2Themes'))
    themes_str = ", ".join(themes) if themes else "General"
    
    # 6. 解析人物（带职位信息）
    persons = parser.parse_persons(row.get('V2Persons'))
    # 为已知人物添加职位
    persons_with_positions = [enrich_person_with_position(p) for p in persons]
    persons_str = ", ".join(persons_with_positions) if persons_with_positions else "Unknown"
    
    # 7. 解析地点（优化后的版本）
    locations = parser.parse_locations(row.get('V2Locations'))
    locations_str = ", ".join(locations) if locations else "Unknown Location"
    
    # 8. 解析组织
    orgs = parser.parse_organizations(row.get('V2Organizations'))
    orgs_str = ", ".join(orgs) if orgs else "No organizations mentioned"
    
    # 9. 解析情感分类
    emotions = parser.parse_gcam(row.get('GCAM'))
    emotions_str = ", ".join(emotions) if emotions else "Neutral"
    
    # 10. 解析图片
    images = parser.parse_images(row.get('SocialImageEmbeds'))
    images_str = "; ".join(images) if images else "No images"
    
    # 11. 确定基调
    tone_val = row.get('AvgTone', 0) or 0
    if tone_val > 3:
        tone_label = "Very Positive"
    elif tone_val > 0:
        tone_label = "Positive"
    elif tone_val > -3:
        tone_label = "Negative"
    else:
        tone_label = "Very Negative"
    
    # 12. 解析视频嵌入
    videos = parser.parse_videos(row.get('SocialVideoEmbeds'))
    videos_str = "; ".join(videos) if videos else "No videos"
    
    # 13. 解析完整情感维度
    full_tone = parser.parse_full_tone(row.get('V2Tone')) if row.get('V2Tone') else {}
    tone_details = ""
    if full_tone:
        tone_parts = []
        if 'positive_score' in full_tone:
            tone_parts.append(f"正面情感强度: {full_tone['positive_score']:.1f}")
        if 'negative_score' in full_tone:
            tone_parts.append(f"负面情感强度: {full_tone['negative_score']:.1f}")
        if 'polarity' in full_tone:
            tone_parts.append(f"极性: {full_tone['polarity']:.2f}")
        if 'word_count' in full_tone:
            tone_parts.append(f"词数: {full_tone['word_count']}")
        tone_details = "; ".join(tone_parts)
    
    source_url = row.get('SourceURL')
    article_summary = fetch_article_summary(source_url)
    
    return {
        "Time": row.get('DATE'),
        "GKGRECORDID": row.get('GKGRECORDID', ''),  # 记录ID用于追溯
        "Title": title,
        "Source_Name": source_name,
        "Tone": f"{tone_label} ({tone_val:.2f})",
        "Tone_Details": tone_details,  # 新增：详细情感维度
        "Emotions": emotions_str,
        "Themes": themes_str,
        "Locations": locations_str,
        "Key_Persons": persons_str,
        "Organizations": orgs_str,
        "Quotes": all_quotes,
        "Data_Facts": facts_str,
        "Images": images_str,
        "Videos": videos_str,  # 新增：视频嵌入
        "Source_URL": source_url,
        "Article_Summary": article_summary  # 新增：原文摘要
    }


# ================= 便捷方法 =================

def extract_title_from_url(url: str) -> str:
    """从 URL 提取标题的便捷方法"""
    return GdeltDataParser.extract_title_from_url(url)


def parse_quotations(raw_quotations: str) -> List[Dict[str, str]]:
    """解析引语的便捷方法"""
    return GdeltDataParser.parse_quotations(raw_quotations)


def parse_amounts(raw_amounts: str) -> List[str]:
    """解析数量的便捷方法"""
    return GdeltDataParser.parse_amounts(raw_amounts)


def parse_themes(raw_themes: str) -> List[str]:
    """解析主题的便捷方法"""
    return GdeltDataParser.parse_themes(raw_themes)


def parse_persons(raw_persons: str) -> List[str]:
    """解析人物的便捷方法"""
    return GdeltDataParser.parse_persons(raw_persons)


def parse_locations(raw_locations: str) -> List[str]:
    """解析地点的便捷方法"""
    return GdeltDataParser.parse_locations(raw_locations)


def parse_organizations(raw_orgs: str) -> List[str]:
    """解析组织的便捷方法"""
    return GdeltDataParser.parse_organizations(raw_orgs)


def parse_gcam(raw_gcam: str) -> List[str]:
    """解析情感的便捷方法"""
    return GdeltDataParser.parse_gcam(raw_gcam)


def parse_images(raw_images: str) -> List[str]:
    """解析图片的便捷方法"""
    return GdeltDataParser.parse_images(raw_images)
