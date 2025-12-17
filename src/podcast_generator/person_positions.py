"""
公众人物职位映射数据库
用于在新闻生成时提供准确的人物职位信息

数据更新说明：
- 职位信息截至 2024 年底
- 如有变动请及时更新
- 可通过 add_person() 函数动态添加
"""

from typing import Dict, Optional, List

# ================= 世界主要国家领导人 =================
WORLD_LEADERS = {
    # 联合国及国际组织
    "Antonio Guterres": "联合国秘书长",
    "Tedros Adhanom Ghebreyesus": "世界卫生组织总干事",
    "Kristalina Georgieva": "国际货币基金组织总裁",
    "Ajay Banga": "世界银行行长",
    "Ngozi Okonjo-Iweala": "世界贸易组织总干事",
    "Fatou Bensouda": "国际刑事法院前检察官",
    "Karim Khan": "国际刑事法院检察官",
    
    # 美国
    "Joe Biden": "美国总统",
    "Kamala Harris": "美国副总统",
    "Antony Blinken": "美国国务卿",
    "Lloyd Austin": "美国国防部长",
    "Janet Yellen": "美国财政部长",
    "Merrick Garland": "美国司法部长",
    "Donald Trump": "美国前总统",
    "Mike Pence": "美国前副总统",
    "Mike Pompeo": "美国前国务卿",
    "Barack Obama": "美国前总统",
    "Hillary Clinton": "美国前国务卿",
    "John Kerry": "美国气候特使",
    "Jake Sullivan": "美国国家安全顾问",
    "Avril Haines": "美国国家情报总监",
    "William Burns": "美国中央情报局局长",
    "Christopher Wray": "美国联邦调查局局长",
    "Jerome Powell": "美联储主席",
    
    # 中国
    "Xi Jinping": "中国国家主席",
    "Li Qiang": "中国国务院总理",
    "Wang Yi": "中国外交部长",
    "Qin Gang": "中国前外交部长",
    "Li Shangfu": "中国前国防部长",
    "He Lifeng": "中国国务院副总理",
    "Cai Qi": "中共中央书记处书记",
    
    # 俄罗斯
    "Vladimir Putin": "俄罗斯总统",
    "Mikhail Mishustin": "俄罗斯总理",
    "Sergei Lavrov": "俄罗斯外交部长",
    "Sergei Shoigu": "俄罗斯安全会议秘书",
    "Andrei Belousov": "俄罗斯国防部长",
    "Dmitry Medvedev": "俄罗斯安全会议副主席",
    "Nikolai Patrushev": "俄罗斯前安全会议秘书",
    
    # 欧盟及主要欧洲国家
    "Ursula von der Leyen": "欧盟委员会主席",
    "Charles Michel": "欧洲理事会主席",
    "Josep Borrell": "欧盟外交与安全政策高级代表",
    "Christine Lagarde": "欧洲央行行长",
    "Roberta Metsola": "欧洲议会议长",
    
    # 英国
    "Keir Starmer": "英国首相",
    "Rishi Sunak": "英国前首相",
    "Boris Johnson": "英国前首相",
    "Liz Truss": "英国前首相",
    "David Lammy": "英国外交大臣",
    "King Charles III": "英国国王",
    "Prince William": "英国威廉王子",
    
    # 法国
    "Emmanuel Macron": "法国总统",
    "Gabriel Attal": "法国总理",
    "Michel Barnier": "法国前欧盟首席谈判代表",
    "Catherine Colonna": "法国前外交部长",
    
    # 德国
    "Olaf Scholz": "德国总理",
    "Frank-Walter Steinmeier": "德国总统",
    "Annalena Baerbock": "德国外交部长",
    "Boris Pistorius": "德国国防部长",
    "Angela Merkel": "德国前总理",
    
    # 意大利
    "Giorgia Meloni": "意大利总理",
    "Sergio Mattarella": "意大利总统",
    
    # 日本
    "Fumio Kishida": "日本前首相",
    "Shigeru Ishiba": "日本首相",
    "Naruhito": "日本天皇",
    "Yoko Kamikawa": "日本外务大臣",
    
    # 韩国
    "Yoon Suk-yeol": "韩国总统",
    "Han Duck-soo": "韩国总理",
    "Cho Tae-yul": "韩国外交部长",
    
    # 印度
    "Narendra Modi": "印度总理",
    "Droupadi Murmu": "印度总统",
    "S. Jaishankar": "印度外交部长",
    
    # 澳大利亚
    "Anthony Albanese": "澳大利亚总理",
    "Penny Wong": "澳大利亚外交部长",
    "Richard Marles": "澳大利亚国防部长",
    "Scott Morrison": "澳大利亚前总理",
    
    # 加拿大
    "Justin Trudeau": "加拿大总理",
    "Melanie Joly": "加拿大外交部长",
    
    # 巴西
    "Luiz Inacio Lula da Silva": "巴西总统",
    "Lula": "巴西总统",
    "Jair Bolsonaro": "巴西前总统",
    
    # 墨西哥
    "Claudia Sheinbaum": "墨西哥总统",
    "Andres Manuel Lopez Obrador": "墨西哥前总统",
    
    # 以色列
    "Benjamin Netanyahu": "以色列总理",
    "Isaac Herzog": "以色列总统",
    "Yoav Gallant": "以色列前国防部长",
    "Gideon Saar": "以色列外交部长",
    "Benny Gantz": "以色列前国防部长",
    "Itamar Ben-Gvir": "以色列国家安全部长",
    "Bezalel Smotrich": "以色列财政部长",
    
    # 巴勒斯坦
    "Mahmoud Abbas": "巴勒斯坦总统",
    "Mohammad Shtayyeh": "巴勒斯坦前总理",
    "Yahya Sinwar": "哈马斯领导人",
    "Ismail Haniyeh": "哈马斯前政治局主席",
    "Khaled Mashal": "哈马斯政治局成员",
    
    # 伊朗
    "Masoud Pezeshkian": "伊朗总统",
    "Ebrahim Raisi": "伊朗前总统",
    "Ali Khamenei": "伊朗最高领袖",
    "Hossein Amir-Abdollahian": "伊朗前外交部长",
    "Mohammad Javad Zarif": "伊朗前外交部长",
    
    # 沙特阿拉伯
    "Mohammed bin Salman": "沙特王储",
    "Salman bin Abdulaziz": "沙特国王",
    "Faisal bin Farhan": "沙特外交大臣",
    
    # 阿联酋
    "Mohamed bin Zayed Al Nahyan": "阿联酋总统",
    "Mohammed bin Rashid Al Maktoum": "阿联酋副总统兼迪拜酋长",
    
    # 土耳其
    "Recep Tayyip Erdogan": "土耳其总统",
    "Hakan Fidan": "土耳其外交部长",
    
    # 埃及
    "Abdel Fattah el-Sisi": "埃及总统",
    "Sameh Shoukry": "埃及外交部长",
    
    # 南非
    "Cyril Ramaphosa": "南非总统",
    "Naledi Pandor": "南非外交部长",
    
    # 尼日利亚
    "Bola Tinubu": "尼日利亚总统",
    
    # 乌克兰
    "Volodymyr Zelensky": "乌克兰总统",
    "Denys Shmyhal": "乌克兰总理",
    "Dmytro Kuleba": "乌克兰前外交部长",
    "Andrii Sybiha": "乌克兰外交部长",
    "Valerii Zaluzhnyi": "乌克兰前武装部队总司令",
    "Oleksandr Syrskyi": "乌克兰武装部队总司令",
    "Rustem Umerov": "乌克兰国防部长",
    
    # 波兰
    "Andrzej Duda": "波兰总统",
    "Donald Tusk": "波兰总理",
    
    # 北约
    "Mark Rutte": "北约秘书长",
    "Jens Stoltenberg": "北约前秘书长",
    
    # 朝鲜
    "Kim Jong Un": "朝鲜最高领导人",
    "Kim Yo Jong": "朝鲜劳动党副部长",
    
    # 新加坡
    "Lawrence Wong": "新加坡总理",
    "Lee Hsien Loong": "新加坡前总理",
    
    # 印度尼西亚
    "Prabowo Subianto": "印度尼西亚总统",
    "Joko Widodo": "印度尼西亚前总统",
    
    # 菲律宾
    "Ferdinand Marcos Jr.": "菲律宾总统",
    "Bongbong Marcos": "菲律宾总统",
    
    # 越南
    "To Lam": "越南国家主席",
    "Pham Minh Chinh": "越南总理",
    
    # 泰国
    "Paetongtarn Shinawatra": "泰国总理",
    "Srettha Thavisin": "泰国前总理",
    
    # 马来西亚
    "Anwar Ibrahim": "马来西亚总理",
    
    # 巴基斯坦
    "Shehbaz Sharif": "巴基斯坦总理",
    "Asif Ali Zardari": "巴基斯坦总统",
    "Imran Khan": "巴基斯坦前总理",
    
    # 阿根廷
    "Javier Milei": "阿根廷总统",
    
    # 智利
    "Gabriel Boric": "智利总统",
    
    # 哥伦比亚
    "Gustavo Petro": "哥伦比亚总统",
    
    # 委内瑞拉
    "Nicolas Maduro": "委内瑞拉总统",
    
    # 古巴
    "Miguel Diaz-Canel": "古巴国家主席",
    
    # 叙利亚
    "Bashar al-Assad": "叙利亚前总统",
    
    # 阿富汗
    "Hibatullah Akhundzada": "阿富汗塔利班最高领导人",
    
    # 缅甸
    "Min Aung Hlaing": "缅甸军政府领导人",
    
    # 斯里兰卡
    "Anura Kumara Dissanayake": "斯里兰卡总统",
    
    # 孟加拉国
    "Muhammad Yunus": "孟加拉国临时政府首席顾问",
    
    # 瑞士
    "Viola Amherd": "瑞士联邦主席",
    
    # 奥地利
    "Karl Nehammer": "奥地利总理",
    
    # 荷兰
    "Dick Schoof": "荷兰首相",
    "Mark Rutte": "荷兰前首相",
    
    # 比利时
    "Alexander De Croo": "比利时首相",
    
    # 西班牙
    "Pedro Sanchez": "西班牙首相",
    "Felipe VI": "西班牙国王",
    
    # 葡萄牙
    "Luis Montenegro": "葡萄牙总理",
    
    # 希腊
    "Kyriakos Mitsotakis": "希腊总理",
    
    # 匈牙利
    "Viktor Orban": "匈牙利总理",
    
    # 捷克
    "Petr Fiala": "捷克总理",
    
    # 瑞典
    "Ulf Kristersson": "瑞典首相",
    
    # 挪威
    "Jonas Gahr Store": "挪威首相",
    
    # 丹麦
    "Mette Frederiksen": "丹麦首相",
    
    # 芬兰
    "Petteri Orpo": "芬兰总理",
    
    # 新西兰
    "Christopher Luxon": "新西兰总理",
    "Jacinda Ardern": "新西兰前总理",
    
    # 梵蒂冈
    "Pope Francis": "天主教教皇",
    "Francis": "天主教教皇",
}

# ================= 科技商业领袖 =================
TECH_BUSINESS_LEADERS = {
    "Elon Musk": "特斯拉和SpaceX首席执行官",
    "Tim Cook": "苹果公司首席执行官",
    "Satya Nadella": "微软首席执行官",
    "Sundar Pichai": "谷歌首席执行官",
    "Mark Zuckerberg": "Meta首席执行官",
    "Jeff Bezos": "亚马逊创始人",
    "Jensen Huang": "英伟达首席执行官",
    "Sam Altman": "OpenAI首席执行官",
    "Jack Ma": "阿里巴巴创始人",
    "Pony Ma": "腾讯首席执行官",
    "Lei Jun": "小米创始人",
    "Ren Zhengfei": "华为创始人",
    "Warren Buffett": "伯克希尔哈撒韦董事长",
    "Bill Gates": "微软联合创始人",
    "Larry Fink": "贝莱德首席执行官",
    "Jamie Dimon": "摩根大通首席执行官",
    "Mary Barra": "通用汽车首席执行官",
    "Bob Iger": "迪士尼首席执行官",
    "Reed Hastings": "Netflix联合创始人",
}

# ================= 媒体人物 =================
MEDIA_FIGURES = {
    "Tucker Carlson": "美国政治评论员",
    "Sean Hannity": "福克斯新闻主持人",
    "Rachel Maddow": "MSNBC新闻主持人",
    "Anderson Cooper": "CNN主持人",
    "Joe Rogan": "播客主持人",
}

# ================= 军事领袖 =================
MILITARY_LEADERS = {
    "Charles Q. Brown Jr.": "美国参谋长联席会议主席",
    "Mark Milley": "美国前参谋长联席会议主席",
    "Valery Gerasimov": "俄罗斯武装力量总参谋长",
}

# ================= 合并所有数据库 =================
KNOWN_PERSONS: Dict[str, str] = {}
KNOWN_PERSONS.update(WORLD_LEADERS)
KNOWN_PERSONS.update(TECH_BUSINESS_LEADERS)
KNOWN_PERSONS.update(MEDIA_FIGURES)
KNOWN_PERSONS.update(MILITARY_LEADERS)


# ================= 人名中文翻译 =================
# 用于将英文人名翻译为中文（主要针对需要翻译的人名）
NAME_TRANSLATIONS = {
    # 西方领导人 - 通常保留原名但可加中文标注
    "Donald Trump": "唐纳德·特朗普",
    "Joe Biden": "乔·拜登",
    "Barack Obama": "巴拉克·奥巴马",
    "Hillary Clinton": "希拉里·克林顿",
    "Kamala Harris": "卡玛拉·哈里斯",
    "Antony Blinken": "安东尼·布林肯",
    "Marco Rubio": "马尔科·卢比奥",
    "Mike Huckabee": "迈克·赫卡比",
    
    # 欧洲领导人
    "Emmanuel Macron": "埃马纽埃尔·马克龙",
    "Olaf Scholz": "奥拉夫·朔尔茨",
    "Boris Johnson": "鲍里斯·约翰逊",
    "Keir Starmer": "基尔·斯塔默",
    "Rishi Sunak": "里希·苏纳克",
    "Ursula von der Leyen": "乌尔苏拉·冯德莱恩",
    
    # 俄罗斯
    "Vladimir Putin": "弗拉基米尔·普京",
    "Sergei Lavrov": "谢尔盖·拉夫罗夫",
    "Dmitry Medvedev": "德米特里·梅德韦杰夫",
    
    # 中东
    "Benjamin Netanyahu": "本雅明·内塔尼亚胡",
    "Isaac Herzog": "伊萨克·赫尔佐格",
    "Gideon Saar": "吉德翁·萨尔",
    "Yoav Gallant": "约阿夫·加兰特",
    "Mahmoud Abbas": "马哈茂德·阿巴斯",
    "Ali Khamenei": "阿里·哈梅内伊",
    "Mohammed bin Salman": "穆罕默德·本·萨勒曼",
    
    # 乌克兰
    "Volodymyr Zelensky": "弗拉基米尔·泽连斯基",
    "Dmytro Kuleba": "德米特罗·库列巴",
    
    # 联合国
    "Antonio Guterres": "安东尼奥·古特雷斯",
    
    # 澳大利亚/新西兰
    "Anthony Albanese": "安东尼·阿尔巴尼斯",
    "Scott Morrison": "斯科特·莫里森",
    "Christopher Luxon": "克里斯托弗·卢克森",
    "Chris Minns": "克里斯·明斯",
    
    # 亚洲领导人（通常有约定俗成的中文译名）
    "Narendra Modi": "纳伦德拉·莫迪",
    "Fumio Kishida": "岸田文雄",
    "Yoon Suk-yeol": "尹锡悦",
    
    # 科技领袖
    "Elon Musk": "埃隆·马斯克",
    "Mark Zuckerberg": "马克·扎克伯格",
    "Jeff Bezos": "杰夫·贝佐斯",
    "Sam Altman": "萨姆·奥特曼",
    "Tim Cook": "蒂姆·库克",
    "Jensen Huang": "黄仁勋",
    
    # 其他出现在最新新闻中的人物
    "Mal Lanyon": "马尔·拉尼翁",
    "Eli Schlanger": "伊莱·施兰格",
    "Amichai Chikli": "阿米哈伊·奇克利",
    "Alan Shatter": "艾伦·沙特",
    "Micheal Martin": "迈克尔·马丁",
    "Tony Abbott": "托尼·阿博特",
    "Friedrich Merz": "弗里德里希·默茨",
    "Raed Saad": "拉埃德·萨阿德",
    "Timothy Brant-Coles": "蒂莫西·布兰特-科尔斯",
    "Camilo Diaz": "卡米洛·迪亚兹",
    
    # 以色列领导人
    "Gideon Saar": "吉德翁·萨尔",
    "Isaac Herzog": "伊萨克·赫尔佐格",
    "Benjamin Netanyahu": "本雅明·内塔尼亚胡",
    "Yoav Gallant": "约阿夫·加兰特",
    
    # 哈马斯/巴勒斯坦
    "Yahya Sinwar": "叶海亚·辛瓦尔",
    "Ismail Haniyeh": "伊斯梅尔·哈尼亚",
}

# 创建姓名变体映射（处理不同写法）
NAME_VARIANTS = {
    # 常见变体
    "Biden": "Joe Biden",
    "Trump": "Donald Trump",
    "Putin": "Vladimir Putin",
    "Xi": "Xi Jinping",
    "Zelensky": "Volodymyr Zelensky",
    "Zelenskyy": "Volodymyr Zelensky",  # 另一种拼写
    "Netanyahu": "Benjamin Netanyahu",
    "Bibi Netanyahu": "Benjamin Netanyahu",
    "MBS": "Mohammed bin Salman",
    "MBZ": "Mohamed bin Zayed Al Nahyan",
    "Erdoğan": "Recep Tayyip Erdogan",
    "Macron": "Emmanuel Macron",
    "Scholz": "Olaf Scholz",
    "Starmer": "Keir Starmer",
    "Sunak": "Rishi Sunak",
    "Modi": "Narendra Modi",
    "Kishida": "Fumio Kishida",
    "Albanese": "Anthony Albanese",
    "Trudeau": "Justin Trudeau",
    "Guterres": "Antonio Guterres",
    "von der Leyen": "Ursula von der Leyen",
    "Stoltenberg": "Jens Stoltenberg",
    "Rutte": "Mark Rutte",
    "Musk": "Elon Musk",
    "Bezos": "Jeff Bezos",
    "Zuckerberg": "Mark Zuckerberg",
    "Cook": "Tim Cook",
    "Pichai": "Sundar Pichai",
    "Nadella": "Satya Nadella",
    "Altman": "Sam Altman",
    "Pope": "Pope Francis",
    "the Pope": "Pope Francis",
}


def get_person_position(name: str) -> Optional[str]:
    """
    获取人物职位
    
    Args:
        name: 人物姓名
        
    Returns:
        职位字符串，如果未知则返回 None
    """
    # 直接匹配
    if name in KNOWN_PERSONS:
        return KNOWN_PERSONS[name]
    
    # 尝试变体匹配
    if name in NAME_VARIANTS:
        full_name = NAME_VARIANTS[name]
        if full_name in KNOWN_PERSONS:
            return KNOWN_PERSONS[full_name]
    
    # 尝试部分匹配（姓氏）
    for known_name, position in KNOWN_PERSONS.items():
        # 如果输入的名字是已知名字的一部分
        if name in known_name or known_name in name:
            return position
    
    return None


def enrich_person_with_position(name: str) -> str:
    """
    为人物名字添加职位信息（如果已知）
    
    Args:
        name: 人物姓名
        
    Returns:
        带职位的人物描述，如 "联合国秘书长 Antonio Guterres"
        如果职位未知，返回原姓名
    """
    position = get_person_position(name)
    if position:
        return f"{position}{name}"
    return name


def translate_person_name(name: str) -> str:
    """
    将英文人名翻译为中文
    
    Args:
        name: 英文人名
        
    Returns:
        中文人名，如果未找到翻译则返回原名
    """
    # 直接匹配
    if name in NAME_TRANSLATIONS:
        return NAME_TRANSLATIONS[name]
    
    # 尝试变体匹配
    if name in NAME_VARIANTS:
        full_name = NAME_VARIANTS[name]
        if full_name in NAME_TRANSLATIONS:
            return NAME_TRANSLATIONS[full_name]
    
    return name


def translate_person_with_position(name: str) -> str:
    """
    获取人物的中文职位和中文姓名
    
    Args:
        name: 英文人名
        
    Returns:
        中文职位+中文姓名，如 "美国总统唐纳德·特朗普"
    """
    position = get_person_position(name)
    chinese_name = translate_person_name(name)
    
    if position:
        return f"{position}{chinese_name}"
    return chinese_name


def translate_persons_list(persons: List[str]) -> List[str]:
    """
    批量翻译人物列表为中文（带职位）
    
    Args:
        persons: 英文人物姓名列表
        
    Returns:
        中文人物描述列表
    """
    return [translate_person_with_position(p) for p in persons]


def translate_persons_string(persons_str: str) -> str:
    """
    翻译以分隔符分隔的人物字符串
    
    Args:
        persons_str: 分隔符分隔的英文人名，如 "Donald Trump, Joe Biden"
        
    Returns:
        翻译后的中文人物字符串
    """
    if not persons_str:
        return persons_str
    
    # 处理常见分隔符
    for sep in [', ', '; ', ' | ']:
        if sep in persons_str:
            parts = [p.strip() for p in persons_str.split(sep)]
            translated = [translate_person_with_position(p) for p in parts]
            return sep.join(translated)
    
    # 单个人名
    return translate_person_with_position(persons_str)


def enrich_persons_list(persons: List[str]) -> List[str]:
    """
    批量为人物列表添加职位信息
    
    Args:
        persons: 人物姓名列表
        
    Returns:
        带职位的人物描述列表
    """
    return [enrich_person_with_position(p) for p in persons]


def add_person(name: str, position: str) -> None:
    """
    动态添加人物到数据库
    
    Args:
        name: 人物姓名
        position: 职位
    """
    KNOWN_PERSONS[name] = position


def get_all_known_persons() -> Dict[str, str]:
    """获取所有已知人物"""
    return KNOWN_PERSONS.copy()


def get_stats() -> Dict[str, int]:
    """获取数据库统计信息"""
    return {
        "总人数": len(KNOWN_PERSONS),
        "世界领导人": len(WORLD_LEADERS),
        "科技商业领袖": len(TECH_BUSINESS_LEADERS),
        "媒体人物": len(MEDIA_FIGURES),
        "军事领袖": len(MILITARY_LEADERS),
        "姓名变体": len(NAME_VARIANTS),
    }


# 测试
if __name__ == "__main__":
    print("=== 人物职位数据库统计 ===")
    stats = get_stats()
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    print("\n=== 测试查询 ===")
    test_names = [
        "Antonio Guterres",
        "Gideon Saar",
        "Isaac Herzog",
        "Putin",
        "Biden",
        "Unknown Person",
    ]
    for name in test_names:
        result = enrich_person_with_position(name)
        print(f"  {name} → {result}")
