"""
地名中英文映射数据库
用于将GDELT英文地名转换为中文

数据更新说明：
- 包含常见国家、城市、地区的中英文对照
- 可通过 add_location() 函数动态添加
"""

from typing import Dict, Optional, Tuple
import re

# ================= 国家名称映射 =================
COUNTRIES = {
    # 东亚
    "China": "中国",
    "Japan": "日本",
    "South Korea": "韩国",
    "North Korea": "朝鲜",
    "Korea": "韩国",
    "Taiwan": "台湾",
    "Mongolia": "蒙古",
    
    # 东南亚
    "Vietnam": "越南",
    "Thailand": "泰国",
    "Singapore": "新加坡",
    "Indonesia": "印度尼西亚",
    "Malaysia": "马来西亚",
    "Philippines": "菲律宾",
    "Myanmar": "缅甸",
    "Cambodia": "柬埔寨",
    "Laos": "老挝",
    "Brunei": "文莱",
    
    # 南亚
    "India": "印度",
    "Pakistan": "巴基斯坦",
    "Bangladesh": "孟加拉国",
    "Sri Lanka": "斯里兰卡",
    "Nepal": "尼泊尔",
    "Afghanistan": "阿富汗",
    
    # 中亚
    "Kazakhstan": "哈萨克斯坦",
    "Uzbekistan": "乌兹别克斯坦",
    "Turkmenistan": "土库曼斯坦",
    "Tajikistan": "塔吉克斯坦",
    "Kyrgyzstan": "吉尔吉斯斯坦",
    
    # 西亚/中东
    "Israel": "以色列",
    "Palestine": "巴勒斯坦",
    "Palestinian": "巴勒斯坦",
    "Iran": "伊朗",
    "Iraq": "伊拉克",
    "Syria": "叙利亚",
    "Lebanon": "黎巴嫩",
    "Jordan": "约旦",
    "Saudi Arabia": "沙特阿拉伯",
    "United Arab Emirates": "阿联酋",
    "UAE": "阿联酋",
    "Qatar": "卡塔尔",
    "Kuwait": "科威特",
    "Bahrain": "巴林",
    "Oman": "阿曼",
    "Yemen": "也门",
    "Turkey": "土耳其",
    
    # 欧洲
    "United Kingdom": "英国",
    "UK": "英国",
    "Britain": "英国",
    "England": "英格兰",
    "Scotland": "苏格兰",
    "Wales": "威尔士",
    "France": "法国",
    "Germany": "德国",
    "Italy": "意大利",
    "Spain": "西班牙",
    "Portugal": "葡萄牙",
    "Netherlands": "荷兰",
    "Belgium": "比利时",
    "Switzerland": "瑞士",
    "Austria": "奥地利",
    "Poland": "波兰",
    "Ukraine": "乌克兰",
    "Ukrainian": "乌克兰",
    "Russia": "俄罗斯",
    "Russian": "俄罗斯",
    "Belarus": "白俄罗斯",
    "Sweden": "瑞典",
    "Norway": "挪威",
    "Denmark": "丹麦",
    "Finland": "芬兰",
    "Iceland": "冰岛",
    "Ireland": "爱尔兰",
    "Greece": "希腊",
    "Czech Republic": "捷克",
    "Czechia": "捷克",
    "Hungary": "匈牙利",
    "Romania": "罗马尼亚",
    "Bulgaria": "保加利亚",
    "Serbia": "塞尔维亚",
    "Croatia": "克罗地亚",
    "Slovenia": "斯洛文尼亚",
    "Slovakia": "斯洛伐克",
    "Estonia": "爱沙尼亚",
    "Latvia": "拉脱维亚",
    "Lithuania": "立陶宛",
    "Moldova": "摩尔多瓦",
    "Albania": "阿尔巴尼亚",
    "North Macedonia": "北马其顿",
    "Montenegro": "黑山",
    "Kosovo": "科索沃",
    "Bosnia and Herzegovina": "波斯尼亚和黑塞哥维那",
    
    # 北美
    "United States": "美国",
    "US": "美国",
    "USA": "美国",
    "America": "美国",
    "American": "美国",
    "Americans": "美国人",
    "Canada": "加拿大",
    "Mexico": "墨西哥",
    
    # 南美
    "Brazil": "巴西",
    "Argentina": "阿根廷",
    "Chile": "智利",
    "Colombia": "哥伦比亚",
    "Peru": "秘鲁",
    "Venezuela": "委内瑞拉",
    "Ecuador": "厄瓜多尔",
    "Bolivia": "玻利维亚",
    "Paraguay": "巴拉圭",
    "Uruguay": "乌拉圭",
    
    # 大洋洲
    "Australia": "澳大利亚",
    "Australian": "澳大利亚",
    "Australians": "澳大利亚人",
    "New Zealand": "新西兰",
    "New Zealanders": "新西兰人",
    
    # 非洲
    "Egypt": "埃及",
    "South Africa": "南非",
    "Nigeria": "尼日利亚",
    "Kenya": "肯尼亚",
    "Ethiopia": "埃塞俄比亚",
    "Morocco": "摩洛哥",
    "Algeria": "阿尔及利亚",
    "Tunisia": "突尼斯",
    "Libya": "利比亚",
    "Sudan": "苏丹",
    "South Sudan": "南苏丹",
    "Somalia": "索马里",
    "Ghana": "加纳",
    "Senegal": "塞内加尔",
    "Congo": "刚果",
    "Democratic Republic of Congo": "刚果民主共和国",
    "DRC": "刚果民主共和国",
    "Zimbabwe": "津巴布韦",
    "Tanzania": "坦桑尼亚",
    "Uganda": "乌干达",
    "Rwanda": "卢旺达",
    "Mali": "马里",
    "Niger": "尼日尔",
    "Chad": "乍得",
    "Cameroon": "喀麦隆",
    "Ivory Coast": "科特迪瓦",
    "Mozambique": "莫桑比克",
    "Angola": "安哥拉",
}

# ================= 主要城市映射 =================
CITIES = {
    # 美国
    "Washington": "华盛顿",
    "Washington D.C.": "华盛顿特区",
    "New York": "纽约",
    "Los Angeles": "洛杉矶",
    "Chicago": "芝加哥",
    "Houston": "休斯顿",
    "Phoenix": "菲尼克斯",
    "Philadelphia": "费城",
    "San Francisco": "旧金山",
    "Seattle": "西雅图",
    "Boston": "波士顿",
    "Miami": "迈阿密",
    "Atlanta": "亚特兰大",
    "Denver": "丹佛",
    "Dallas": "达拉斯",
    "Silicon Valley": "硅谷",
    
    # 英国
    "London": "伦敦",
    "Manchester": "曼彻斯特",
    "Birmingham": "伯明翰",
    "Edinburgh": "爱丁堡",
    "Glasgow": "格拉斯哥",
    "Liverpool": "利物浦",
    "Oxford": "牛津",
    "Cambridge": "剑桥",
    
    # 法国
    "Paris": "巴黎",
    "Marseille": "马赛",
    "Lyon": "里昂",
    "Nice": "尼斯",
    "Strasbourg": "斯特拉斯堡",
    
    # 德国
    "Berlin": "柏林",
    "Munich": "慕尼黑",
    "Frankfurt": "法兰克福",
    "Hamburg": "汉堡",
    "Cologne": "科隆",
    
    # 意大利
    "Rome": "罗马",
    "Milan": "米兰",
    "Venice": "威尼斯",
    "Florence": "佛罗伦萨",
    "Naples": "那不勒斯",
    "Turin": "都灵",
    
    # 俄罗斯
    "Moscow": "莫斯科",
    "St. Petersburg": "圣彼得堡",
    "Saint Petersburg": "圣彼得堡",
    
    # 中国
    "Beijing": "北京",
    "Shanghai": "上海",
    "Hong Kong": "香港",
    "Macau": "澳门",
    "Shenzhen": "深圳",
    "Guangzhou": "广州",
    "Chengdu": "成都",
    "Wuhan": "武汉",
    "Hangzhou": "杭州",
    "Nanjing": "南京",
    "Xi'an": "西安",
    "Chongqing": "重庆",
    "Tianjin": "天津",
    
    # 日本
    "Tokyo": "东京",
    "Osaka": "大阪",
    "Kyoto": "京都",
    "Yokohama": "横滨",
    "Nagoya": "名古屋",
    "Hiroshima": "广岛",
    "Nagasaki": "长崎",
    "Fukuoka": "福冈",
    
    # 韩国
    "Seoul": "首尔",
    "Busan": "釜山",
    "Incheon": "仁川",
    
    # 印度
    "New Delhi": "新德里",
    "Delhi": "德里",
    "Mumbai": "孟买",
    "Bangalore": "班加罗尔",
    "Kolkata": "加尔各答",
    "Chennai": "金奈",
    
    # 中东
    "Jerusalem": "耶路撒冷",
    "Tel Aviv": "特拉维夫",
    "Gaza": "加沙",
    "Gaza City": "加沙城",
    "Ramallah": "拉姆安拉",
    "Tehran": "德黑兰",
    "Baghdad": "巴格达",
    "Damascus": "大马士革",
    "Beirut": "贝鲁特",
    "Amman": "安曼",
    "Riyadh": "利雅得",
    "Dubai": "迪拜",
    "Abu Dhabi": "阿布扎比",
    "Doha": "多哈",
    "Ankara": "安卡拉",
    "Istanbul": "伊斯坦布尔",
    "Cairo": "开罗",
    
    # 东南亚
    "Bangkok": "曼谷",
    "Hanoi": "河内",
    "Ho Chi Minh City": "胡志明市",
    "Saigon": "西贡",
    "Jakarta": "雅加达",
    "Kuala Lumpur": "吉隆坡",
    "Manila": "马尼拉",
    "Yangon": "仰光",
    "Phnom Penh": "金边",
    
    # 澳大利亚
    "Sydney": "悉尼",
    "Melbourne": "墨尔本",
    "Brisbane": "布里斯班",
    "Perth": "珀斯",
    "Canberra": "堪培拉",
    "Adelaide": "阿德莱德",
    
    # 非洲
    "Johannesburg": "约翰内斯堡",
    "Cape Town": "开普敦",
    "Lagos": "拉各斯",
    "Nairobi": "内罗毕",
    "Addis Ababa": "亚的斯亚贝巴",
    "Khartoum": "喀土穆",
    
    # 南美
    "Sao Paulo": "圣保罗",
    "Rio de Janeiro": "里约热内卢",
    "Buenos Aires": "布宜诺斯艾利斯",
    "Lima": "利马",
    "Bogota": "波哥大",
    "Santiago": "圣地亚哥",
    "Caracas": "加拉加斯",
    
    # 加拿大
    "Toronto": "多伦多",
    "Vancouver": "温哥华",
    "Montreal": "蒙特利尔",
    "Ottawa": "渥太华",
    
    # 乌克兰
    "Kyiv": "基辅",
    "Kiev": "基辅",
    "Kharkiv": "哈尔科夫",
    "Odesa": "敖德萨",
    "Odessa": "敖德萨",
    "Mariupol": "马里乌波尔",
    "Zaporizhzhia": "扎波罗热",
    "Kherson": "赫尔松",
    "Donetsk": "顿涅茨克",
    "Luhansk": "卢甘斯克",
    "Crimea": "克里米亚",
}

# ================= 地区/州/省映射 =================
REGIONS = {
    # 美国州
    "California": "加利福尼亚州",
    "Texas": "德克萨斯州",
    "Florida": "佛罗里达州",
    "New York State": "纽约州",
    "Pennsylvania": "宾夕法尼亚州",
    "Illinois": "伊利诺伊州",
    "Ohio": "俄亥俄州",
    "Georgia": "佐治亚州",
    "North Carolina": "北卡罗来纳州",
    "Michigan": "密歇根州",
    "Arizona": "亚利桑那州",
    "Massachusetts": "马萨诸塞州",
    "New Jersey": "新泽西州",
    "Virginia": "弗吉尼亚州",
    "Hawaii": "夏威夷",
    "Alaska": "阿拉斯加",
    
    # 澳大利亚州
    "New South Wales": "新南威尔士州",
    "Victoria": "维多利亚州",
    "Queensland": "昆士兰州",
    "Tasmania": "塔斯马尼亚州",
    "Bondi Beach": "邦迪海滩",
    
    # 中东地区
    "West Bank": "约旦河西岸",
    "Gaza Strip": "加沙地带",
    "Golan Heights": "戈兰高地",
    
    # 苏丹地区（重点区分苏丹和南苏丹）
    "Kordofan": "科尔多凡",
    "Darfur": "达尔富尔",
    "Kadugli": "卡杜格利",
    "South Kordofan": "南科尔多凡",
    "Juba": "朱巴",  # 南苏丹首都
    
    # 其他
    "Kashmir": "克什米尔",
    "Punjab": "旁遮普",
    "Tibet": "西藏",
    "Xinjiang": "新疆",
    "Hong Kong Island": "香港岛",
    "Kowloon": "九龙",
}

# ================= 特殊地名处理 =================
# 需要保持英文或有特殊处理的地名
SPECIAL_LOCATIONS = {
    "Israel (General)": "以色列",
    "Israel (IS)": "以色列",
    "Palestinian (IS)": "巴勒斯坦",
    "Israeli (IS)": "以色列",
    "Australia (AS)": "澳大利亚",
    "United States (US)": "美国",
    "Ukraine (UP)": "乌克兰",
    "Russia (RS)": "俄罗斯",
    "China (CH)": "中国",
    "Iran (IR)": "伊朗",
    "New Zealand (NZ)": "新西兰",
    "France (FR)": "法国",
    "Thailand (TH)": "泰国",
    "South Korea (KS)": "韩国",
    "Hong Kong (HK)": "香港",
    "Qatar (QA)": "卡塔尔",
}


# 合并所有映射
ALL_LOCATIONS: Dict[str, str] = {}
ALL_LOCATIONS.update(COUNTRIES)
ALL_LOCATIONS.update(CITIES)
ALL_LOCATIONS.update(REGIONS)
ALL_LOCATIONS.update(SPECIAL_LOCATIONS)


def translate_location(location: str) -> str:
    """
    将英文地名翻译为中文
    
    Args:
        location: 英文地名
        
    Returns:
        中文地名，如果未找到则返回原地名
    """
    # 直接匹配
    if location in ALL_LOCATIONS:
        return ALL_LOCATIONS[location]
    
    # 去除括号后缀再匹配（如 "Australia (AS)" -> "Australia"）
    clean_location = re.sub(r'\s*\([^)]*\)\s*$', '', location).strip()
    if clean_location in ALL_LOCATIONS:
        return ALL_LOCATIONS[clean_location]
    
    # 部分匹配
    for en, zh in ALL_LOCATIONS.items():
        if en in location:
            return zh
    
    return location


def translate_locations_string(locations_str: str) -> str:
    """
    翻译逗号分隔的地点字符串
    
    Args:
        locations_str: 逗号分隔的英文地名，如 "Sydney, New South Wales, Australia (AS)"
        
    Returns:
        翻译后的中文地名字符串
    """
    if not locations_str:
        return locations_str
    
    parts = [p.strip() for p in locations_str.split(',')]
    translated = []
    seen = set()  # 去重
    
    for part in parts:
        zh = translate_location(part)
        if zh not in seen:
            translated.append(zh)
            seen.add(zh)
    
    return ', '.join(translated)


def add_location(english: str, chinese: str) -> None:
    """
    动态添加地名映射
    
    Args:
        english: 英文地名
        chinese: 中文地名
    """
    ALL_LOCATIONS[english] = chinese


def get_all_locations() -> Dict[str, str]:
    """获取所有地名映射"""
    return ALL_LOCATIONS.copy()


def get_stats() -> Dict[str, int]:
    """获取数据库统计信息"""
    return {
        "总地名数": len(ALL_LOCATIONS),
        "国家": len(COUNTRIES),
        "城市": len(CITIES),
        "地区": len(REGIONS),
        "特殊地名": len(SPECIAL_LOCATIONS),
    }


# 测试
if __name__ == "__main__":
    print("=== 地名映射数据库统计 ===")
    stats = get_stats()
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    print("\n=== 测试翻译 ===")
    test_locations = [
        "Sydney",
        "New South Wales",
        "Australia (AS)",
        "Gaza City",
        "Jerusalem",
        "Tel Aviv",
        "Sudan",
        "South Sudan",
        "Bondi Beach",
        "Unknown Location",
    ]
    for loc in test_locations:
        result = translate_location(loc)
        print(f"  {loc} → {result}")
    
    print("\n=== 测试字符串翻译 ===")
    test_str = "Sydney, New South Wales, Australia (AS), Israel (IS)"
    print(f"输入: {test_str}")
    print(f"输出: {translate_locations_string(test_str)}")
