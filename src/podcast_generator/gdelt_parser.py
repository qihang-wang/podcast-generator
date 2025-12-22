"""
GDELT 原始数据解析模块
用于解析 BigQuery 返回的 GDELT 原始数据并转换为结构化叙事格式
"""

import re
from urllib.parse import urlparse
from collections import Counter
from typing import List, Dict, Any, Optional

# 导入人物职位数据库
try:
    from person_positions import enrich_person_with_position, get_person_position
except ImportError:
    # 如果导入失败，提供空实现
    def enrich_person_with_position(name: str) -> str:
        return name
    def get_person_position(name: str) -> Optional[str]:
        return None




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
        """
        解析引语字段 - 增强版
        GDELT Quotations 格式: offset|length|verb|quote|speaker
        现在提取真实说话人信息
        """
        if not raw_quotations or str(raw_quotations) == 'nan':
            return []
        
        quotes = []
        entries = str(raw_quotations).split('#')
        
        for entry in entries:
            parts = entry.split('|')
            if len(parts) >= 4:
                verb = parts[2].strip() if parts[2] else "said"
                quote_text = parts[3].strip() if parts[3] else ""
                # 提取真实说话人（如果存在）
                speaker = parts[4].strip() if len(parts) >= 5 and parts[4].strip() else None
                
                if quote_text and len(quote_text) > 20:
                    quotes.append({
                        "verb": verb, 
                        "text": quote_text,
                        "speaker": speaker  # 新增：真实说话人
                    })
            elif len(parts) == 1 and len(parts[0]) > 20:
                quotes.append({"verb": "said", "text": parts[0].strip(), "speaker": None})
        
        return quotes

    @staticmethod
    def parse_enhanced_persons(raw_enhanced_persons: str) -> List[Dict[str, Any]]:
        """
        解析增强版人物字段 V2EnhancedPersons
        格式: 姓名,字符偏移量;姓名,字符偏移量;...
        
        Returns:
            List of dicts with 'name' and 'offset' keys
        """
        if not raw_enhanced_persons or str(raw_enhanced_persons) == 'nan':
            return []
        
        persons = []
        entries = str(raw_enhanced_persons).split(';')
        
        for entry in entries[:30]:  # 限制解析数量
            parts = entry.split(',')
            if len(parts) >= 2:
                name = parts[0].strip()
                try:
                    offset = int(parts[1].strip())
                except ValueError:
                    offset = 0
                
                if name and len(name) > 2:
                    persons.append({
                        'name': name,
                        'offset': offset
                    })
        
        return persons

    @staticmethod
    def infer_person_role(person_name: str, quotations: str, offset: int = 0) -> str:
        """
        根据上下文推断人物角色
        
        角色类型：
        - prosecutor: 检察官/起诉方律师
        - defense: 辩护律师
        - judge: 法官
        - defendant: 被告
        - victim: 受害者
        - witness: 证人
        - official: 官员
        - speaker: 发言人/信源
        - unknown: 未知
        
        Args:
            person_name: 人物姓名
            quotations: 引语原始字段
            offset: 人物在原文中的偏移量
            
        Returns:
            角色标签
        """
        if not quotations or str(quotations) == 'nan':
            return "unknown"
        
        quotations_lower = quotations.lower()
        person_lower = person_name.lower()
        
        # 检查人物是否在引语中作为说话人出现
        if person_lower in quotations_lower:
            # 检查是否是律师角色
            if 'prosecuting' in quotations_lower or 'prosecutor' in quotations_lower:
                # 检查名字是否紧跟在 prosecuting 后面
                if f"{person_lower}, prosecuting" in quotations_lower or f"prosecuting, {person_lower}" in quotations_lower:
                    return "prosecutor"
            
            if 'mitigating' in quotations_lower or 'defense' in quotations_lower or 'defending' in quotations_lower:
                if f"{person_lower}, mitigating" in quotations_lower or f"mitigating for" in quotations_lower:
                    return "defense"
            
            if 'judge' in quotations_lower or 'sentencing' in quotations_lower:
                if f"judge {person_lower}" in quotations_lower:
                    return "judge"
        
        # 检查是否是被告
        defendant_patterns = ['sentenced', 'jailed', 'convicted', 'pleaded guilty', 'admitted', 'arrested']
        for pattern in defendant_patterns:
            if pattern in quotations_lower and person_lower in quotations_lower:
                # 人名出现在判刑上下文中
                return "defendant"
        
        # 检查是否是官员
        official_patterns = ['minister', 'secretary', 'president', 'governor', 'mayor', 'chief']
        for pattern in official_patterns:
            if pattern in quotations_lower and person_lower in quotations_lower:
                return "official"
        
        return "speaker"

    @staticmethod
    def parse_counts(raw_counts: str) -> List[Dict[str, Any]]:
        """
        解析 V2Counts 字段 - 结构化计数数据
        格式: COUNT_TYPE#数量#OBJECT_TYPE#地点#ADM1#国家#LAT#LONG#FeatureId;...
        
        常见 COUNT_TYPE:
        - KILL: 死亡人数
        - WOUND: 受伤人数
        - ARREST: 逮捕人数
        - PROTEST: 抗议人数
        
        Returns:
            List of dicts with 'type', 'count', 'object' keys
        """
        if not raw_counts or str(raw_counts) == 'nan':
            return []
        
        counts = []
        seen_counts = set()  # 用于去重
        entries = str(raw_counts).split(';')
        
        for entry in entries[:10]:
            parts = entry.split('#')
            if len(parts) >= 3:
                count_type = parts[0].strip()
                try:
                    count_value = int(parts[1].strip())
                except ValueError:
                    continue
                
                object_type = parts[2].strip() if len(parts) > 2 else ""
                
                # 过滤 CRISISLEX 技术标签（不应出现在最终输出中）
                if 'CRISISLEX' in count_type.upper():
                    continue
                
                # 只保留有效的人类可读类型
                valid_types = ['KILL', 'WOUND', 'ARREST', 'PROTEST', 'REFUGEE', 'AFFECT']
                if any(vt in count_type.upper() for vt in valid_types):
                    # 去重
                    key = (count_type.upper(), count_value)
                    if key not in seen_counts:
                        seen_counts.add(key)
                        counts.append({
                            'type': count_type,
                            'count': count_value,
                            'object': object_type
                        })
        
        return counts

    @staticmethod
    def format_counts_for_prompt(counts: List[Dict[str, Any]]) -> str:
        """
        将解析后的计数数据格式化为提示词可用的字符串
        """
        if not counts:
            return ""
        
        formatted = []
        type_labels = {
            'KILL': '死亡',
            'WOUND': '受伤',
            'ARREST': '逮捕',
            'PROTEST': '抗议',
            'REFUGEE': '难民',
            'AFFECT': '受影响',
        }
        
        seen = set()  # 再次去重
        for c in counts:
            ctype = c['type'].upper()
            
            # 再次过滤 CRISISLEX 标签（以防万一）
            if 'CRISISLEX' in ctype:
                continue
            
            label = type_labels.get(ctype, ctype)
            output = f"{c['count']} {label}"
            
            if output not in seen:
                seen.add(output)
                formatted.append(output)
        
        return '; '.join(formatted)

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
            entry_stripped = entry.strip()
            
            # === 新增: 过滤时间格式被误读的情况 ===
            # 如 "2.40am on his way out" 被误解析
            if re.search(r'\d+\.\d+[ap]m', entry_stripped, re.IGNORECASE):
                continue
            if 'his way out' in entry_stripped.lower() or 'her way out' in entry_stripped.lower():
                continue
            if 'on his way' in entry_stripped.lower() or 'on her way' in entry_stripped.lower():
                continue
            # === 过滤结束 ===
            
            # 使用正则表达式匹配: 数字(可能带逗号) + 描述 + 位置
            match = re.match(r'^([\d,]+)\s*,\s*(.+?)(?:,(\d+))?$', entry_stripped)
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
        """解析主题字段 - 只提取前10个高频主题（与SQL过滤保持一致）"""
        if not raw_themes or str(raw_themes) == 'nan':
            return []
        
        themes = []
        entries = str(raw_themes).split(';')
        
        # 只扫描前10个主题条目（与SQL过滤一致）
        for entry in entries[:10]:
            parts = entry.split(',')
            if parts:
                theme = parts[0].strip()
                # 不再过滤 WB_ 前缀，保留所有主题
                if theme:
                    themes.append(theme)
        
        # 去重并保持顺序，严格限制为前10个
        seen = set()
        unique_themes = []
        for t in themes:
            if t not in seen and len(unique_themes) < 10:
                seen.add(t)
                unique_themes.append(t)
        
        return unique_themes

    @staticmethod
    def parse_persons(raw_persons: str) -> List[str]:
        """解析人物字段 - 优化版，提取更多人物并过滤媒体机构名称和非人物实体"""
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
        
        # 非人物实体 - 产品/品牌名称等常被误识别为人物
        NON_PERSON_ENTITIES = {
            # 手机品牌和产品
            'Nokia Lumia', 'Nokia', 'Huawei', 'Samsung Galaxy', 'Samsung', 
            'iPhone', 'Xiaomi', 'Oppo', 'Vivo', 'OnePlus', 'Motorola', 
            'LG', 'Sony Xperia', 'Google Pixel', 'Redmi', 'Realme',
            # 地名（可能被误识为人物）
            'Nagorno Karabakh', 'Gaza Strip', 'West Bank', 'Gaza',
            'Gaza Christians', 'Gazan Christians',
            # 其他非人物实体
            'Crown Court', 'High Court', 'Supreme Court',
        }
        
        persons = []
        entries = str(raw_persons).split(';')
        
        # 扫描更多人物
        for entry in entries[:20]:
            parts = entry.split(',')
            if parts:
                person = parts[0].strip()
                # 过滤条件：长度>2，不是媒体机构名称，不是非人物实体
                is_media = person in MEDIA_NAMES
                is_non_person = any(np.lower() in person.lower() for np in NON_PERSON_ENTITIES)
                
                if person and len(person) > 2 and not is_media and not is_non_person:
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
    
    # 2. 获取标题：优先使用 Extras 中的真实标题，否则从 URL 提取
    article_title = row.get('Article_Title')
    if article_title and str(article_title) != 'nan' and len(str(article_title).strip()) > 5:
        title = str(article_title).strip()
    else:
        title = parser.extract_title_from_url(row.get('SourceURL'))
    
    # 2.1 获取作者信息
    authors = row.get('Authors')
    authors_str = str(authors).strip() if authors and str(authors) != 'nan' else ""
    
    # 2.2 获取 AMP URL（备用链接）
    amp_url = row.get('AMP_URL')
    amp_url_str = str(amp_url).strip() if amp_url and str(amp_url) != 'nan' else ""
    
    # 3. 解析引语（现在包含真实说话人信息）
    quotes = parser.parse_quotations(row.get('Quotations'))
    quotes_sorted = sorted(quotes, key=lambda x: len(x['text']), reverse=True)[:15]
    
    # 解析关键人物用于无说话人时的兜底
    persons = parser.parse_persons(row.get('V2Persons'))
    
    # 格式化引语：优先使用真实说话人，否则使用人物列表兜底
    # 注意：如果引语包含第一人称，可能是文章主角说的，不应随意归属给名人
    first_person_patterns = [' I ', ' I\'m ', ' I\'ve ', ' I\'ll ', ' my ', ' me ', ' myself ']
    
    formatted_quotes = []
    for idx, q in enumerate(quotes_sorted):
        verb = q['verb'] if q['verb'] and q['verb'] != 'said' else ''
        quote_text = q['text']
        
        # 检测是否为第一人称引语
        is_first_person = any(pattern.lower() in f" {quote_text.lower()} " for pattern in first_person_patterns)
        
        # 优先使用引语中的真实说话人
        if q.get('speaker'):
            speaker = q['speaker']
            if verb:
                formatted_quotes.append(f'{speaker} {verb}: "{quote_text}"')
            else:
                formatted_quotes.append(f'{speaker} 表示: "{quote_text}"')
        elif is_first_person:
            # 第一人称引语：不要用名人列表猜测，使用通用格式
            if verb:
                formatted_quotes.append(f'受访者 {verb}: "{quote_text}"')
            else:
                formatted_quotes.append(f'受访者表示: "{quote_text}"')
        elif persons and idx < len(persons):
            # 兜底：使用人物列表关联（可能不准确）
            speaker = persons[idx]
            if verb:
                formatted_quotes.append(f'{speaker} {verb}: "{quote_text}"')
            else:
                formatted_quotes.append(f'{speaker} 表示: "{quote_text}"')
        else:
            # 无说话人信息时使用通用格式
            if verb:
                formatted_quotes.append(f'有关人士 {verb}: "{quote_text}"')
            else:
                formatted_quotes.append(f'据报道: "{quote_text}"')
    
    all_quotes = "\n---\n".join(formatted_quotes) if formatted_quotes else "No quotes available"


    
    # 4. 解析数据事实
    facts = parser.parse_amounts(row.get('Amounts'))
    
    # 4.1 解析 V2Counts（结构化计数数据：死亡、受伤等）
    counts = parser.parse_counts(row.get('V2Counts'))
    counts_str = parser.format_counts_for_prompt(counts)
    
    # 合并 Amounts 和 V1Counts 数据
    if counts_str:
        facts_str = "; ".join(facts) + "; " + counts_str if facts else counts_str
    else:
        facts_str = "; ".join(facts) if facts else "No specific data"
    
    # 4.2 解析增强版人物字段（V2Persons 包含偏移量信息）
    # 格式: 姓名,偏移量;姓名,偏移量;...
    enhanced_persons = parser.parse_enhanced_persons(row.get('V2Persons'))
    quotations_raw = row.get('Quotations', '')
    
    # 为每个人物推断角色
    person_roles = {}
    for ep in enhanced_persons[:10]:
        role = parser.infer_person_role(ep['name'], quotations_raw, ep['offset'])
        if role != "unknown":
            person_roles[ep['name']] = role


    
    # 5. 解析主题
    themes = parser.parse_themes(row.get('V2Themes'))
    themes_str = ", ".join(themes) if themes else "General"
    
    # 6. 解析人物（带职位信息和角色标签）
    persons = parser.parse_persons(row.get('V2Persons'))
    # 为已知人物添加职位
    persons_with_positions = [enrich_person_with_position(p) for p in persons]
    
    # 添加角色标签
    role_labels = {
        'prosecutor': '[检察官/起诉方]',
        'defense': '[辩护律师]',
        'judge': '[法官]',
        'defendant': '[被告]',
        'victim': '[受害者]',
        'official': '[官员]',
        'speaker': '',
    }
    
    persons_with_roles = []
    for p in persons_with_positions:
        # 检查是否有推断的角色
        role_tag = ""
        for name, role in person_roles.items():
            if name.lower() in p.lower():
                role_tag = role_labels.get(role, '')
                break
        persons_with_roles.append(f"{p} {role_tag}".strip() if role_tag else p)
    
    persons_str = ", ".join(persons_with_roles) if persons_with_roles else "Unknown"

    
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
    
    # 11.1 情感极性校验 - 检测矛盾情况
    # 如果基调显著正面但情感标签包含负面词，可能是误判
    tone_warning = ""
    negative_emotions = ['hate', 'death', 'disaster', 'conflict', 'violence']
    positive_emotions = ['trust', 'positive']
    
    if tone_val > 5:  # 强正面基调
        emotions_lower = emotions_str.lower() if emotions_str else ""
        has_negative = any(neg in emotions_lower for neg in negative_emotions)
        if has_negative:
            tone_warning = "⚠️ 注意：文章基调为强正面，情感标签可能不准确。请根据标题和引语判断真实倾向。"
    elif tone_val < -5:  # 强负面基调
        emotions_lower = emotions_str.lower() if emotions_str else ""
        has_only_positive = all(pos in emotions_lower for pos in positive_emotions)
        if has_only_positive and emotions_lower:
            tone_warning = "⚠️ 注意：文章基调为强负面，情感标签可能不准确。"
    
    # 12. 解析视频嵌入
    videos = parser.parse_videos(row.get('SocialVideoEmbeds'))
    videos_str = "; ".join(videos) if videos else "No videos"
    
    # 13. 解析完整情感维度（优先使用 BigQuery 返回的字段）
    positive_score = row.get('PositiveScore')
    negative_score = row.get('NegativeScore')
    word_count = row.get('WordCount')
    
    # 如果 BigQuery 没有返回这些字段，则从 V2Tone 解析
    if positive_score is None or negative_score is None:
        full_tone = parser.parse_full_tone(row.get('V2Tone')) if row.get('V2Tone') else {}
        positive_score = full_tone.get('positive_score')
        negative_score = full_tone.get('negative_score')
        word_count = full_tone.get('word_count') if word_count is None else word_count
    
    tone_details = ""
    tone_parts = []
    if positive_score is not None:
        tone_parts.append(f"正面: {positive_score:.1f}")
    if negative_score is not None:
        tone_parts.append(f"负面: {negative_score:.1f}")
    if word_count is not None:
        tone_parts.append(f"词数: {int(word_count)}")
    tone_details = "; ".join(tone_parts)
    
    source_url = row.get('SourceURL')

    
    return {
        "Time": row.get('DATE'),
        "GKGRECORDID": row.get('GKGRECORDID', ''),  # 记录ID用于追溯
        "Title": title,
        "Source_Name": source_name,
        "Tone": f"{tone_label} ({tone_val:.2f})",
        "Tone_Details": tone_details,  # 新增：详细情感维度
        "Tone_Warning": tone_warning,  # 新增：情感极性警告
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
        "Authors": authors_str,  # 新增：文章作者
        "AMP_URL": amp_url_str,  # 新增：AMP替代链接
    }

