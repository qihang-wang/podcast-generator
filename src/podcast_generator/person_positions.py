"""
公众人物职位映射数据库 - 完整双语版
用于在新闻生成时提供准确的人物职位和人名翻译（无需 LLM 翻译）

数据更新说明：
- 职位信息截至 2024 年底
- 如有变动请及时更新
- 可通过 add_person() 函数动态添加
"""

from typing import Dict, Optional, List, Tuple

# ================= 完整双语映射 (英文职位, 中文职位, 中文人名) =================
# 格式: "英文人名": ("English Position", "中文职位", "中文人名")

WORLD_LEADERS_FULL: Dict[str, Tuple[str, str, str]] = {
    # 联合国及国际组织
    "Antonio Guterres": ("UN Secretary-General", "联合国秘书长", "安东尼奥·古特雷斯"),
    "Tedros Adhanom Ghebreyesus": ("WHO Director-General", "世界卫生组织总干事", "谭德塞"),
    "Kristalina Georgieva": ("IMF Managing Director", "国际货币基金组织总裁", "克里斯塔利娜·格奥尔基耶娃"),
    "Ajay Banga": ("World Bank President", "世界银行行长", "阿贾伊·班加"),
    "Ngozi Okonjo-Iweala": ("WTO Director-General", "世界贸易组织总干事", "恩戈齐·奥孔乔-伊维拉"),
    "Fatou Bensouda": ("Former ICC Prosecutor", "国际刑事法院前检察官", "法图·本苏达"),
    "Karim Khan": ("ICC Prosecutor", "国际刑事法院检察官", "卡里姆·汗"),
    
    # 美国
    "Joe Biden": ("US President", "美国总统", "乔·拜登"),
    "Kamala Harris": ("US Vice President", "美国副总统", "卡玛拉·哈里斯"),
    "Antony Blinken": ("US Secretary of State", "美国国务卿", "安东尼·布林肯"),
    "Lloyd Austin": ("US Defense Secretary", "美国国防部长", "劳埃德·奥斯汀"),
    "Janet Yellen": ("US Treasury Secretary", "美国财政部长", "珍妮特·耶伦"),
    "Merrick Garland": ("US Attorney General", "美国司法部长", "梅里克·加兰"),
    "Donald Trump": ("Former US President", "美国前总统", "唐纳德·特朗普"),
    "Mike Pence": ("Former US Vice President", "美国前副总统", "迈克·彭斯"),
    "Mike Pompeo": ("Former US Secretary of State", "美国前国务卿", "迈克·蓬佩奥"),
    "Barack Obama": ("Former US President", "美国前总统", "巴拉克·奥巴马"),
    "Hillary Clinton": ("Former US Secretary of State", "美国前国务卿", "希拉里·克林顿"),
    "John Kerry": ("US Climate Envoy", "美国气候特使", "约翰·克里"),
    "Jake Sullivan": ("US National Security Advisor", "美国国家安全顾问", "杰克·沙利文"),
    "Avril Haines": ("US Director of National Intelligence", "美国国家情报总监", "艾薇儿·海恩斯"),
    "William Burns": ("CIA Director", "美国中央情报局局长", "威廉·伯恩斯"),
    "Christopher Wray": ("FBI Director", "美国联邦调查局局长", "克里斯托弗·雷"),
    "Jerome Powell": ("Federal Reserve Chairman", "美联储主席", "杰罗姆·鲍威尔"),
    "Marco Rubio": ("US Senator", "美国参议员", "马尔科·卢比奥"),
    
    # 中国
    "Xi Jinping": ("Chinese President", "中国国家主席", "习近平"),
    "Li Qiang": ("Chinese Premier", "中国国务院总理", "李强"),
    "Wang Yi": ("Chinese Foreign Minister", "中国外交部长", "王毅"),
    "Qin Gang": ("Former Chinese Foreign Minister", "中国前外交部长", "秦刚"),
    "Li Shangfu": ("Former Chinese Defense Minister", "中国前国防部长", "李尚福"),
    "He Lifeng": ("Chinese Vice Premier", "中国国务院副总理", "何立峰"),
    "Cai Qi": ("CPC Central Secretariat", "中共中央书记处书记", "蔡奇"),
    
    # 俄罗斯
    "Vladimir Putin": ("Russian President", "俄罗斯总统", "弗拉基米尔·普京"),
    "Mikhail Mishustin": ("Russian Prime Minister", "俄罗斯总理", "米哈伊尔·米舒斯京"),
    "Sergei Lavrov": ("Russian Foreign Minister", "俄罗斯外交部长", "谢尔盖·拉夫罗夫"),
    "Sergei Shoigu": ("Russian Security Council Secretary", "俄罗斯安全会议秘书", "谢尔盖·绍伊古"),
    "Andrei Belousov": ("Russian Defense Minister", "俄罗斯国防部长", "安德烈·别洛乌索夫"),
    "Dmitry Medvedev": ("Russian Security Council Deputy Chairman", "俄罗斯安全会议副主席", "德米特里·梅德韦杰夫"),
    "Nikolai Patrushev": ("Former Russian Security Council Secretary", "俄罗斯前安全会议秘书", "尼古拉·帕特鲁舍夫"),
    
    # 欧盟及主要欧洲国家
    "Ursula von der Leyen": ("European Commission President", "欧盟委员会主席", "乌尔苏拉·冯德莱恩"),
    "Charles Michel": ("European Council President", "欧洲理事会主席", "夏尔·米歇尔"),
    "Josep Borrell": ("EU High Representative for Foreign Affairs", "欧盟外交与安全政策高级代表", "何塞普·博雷利"),
    "Christine Lagarde": ("ECB President", "欧洲央行行长", "克里斯蒂娜·拉加德"),
    "Roberta Metsola": ("European Parliament President", "欧洲议会议长", "罗伯塔·梅索拉"),
    
    # 英国
    "Keir Starmer": ("UK Prime Minister", "英国首相", "基尔·斯塔默"),
    "Rishi Sunak": ("Former UK Prime Minister", "英国前首相", "里希·苏纳克"),
    "Boris Johnson": ("Former UK Prime Minister", "英国前首相", "鲍里斯·约翰逊"),
    "Liz Truss": ("Former UK Prime Minister", "英国前首相", "利兹·特拉斯"),
    "David Lammy": ("UK Foreign Secretary", "英国外交大臣", "戴维·拉米"),
    "King Charles III": ("King of the United Kingdom", "英国国王", "查尔斯三世"),
    "Prince William": ("Prince of Wales", "英国威廉王子", "威廉王子"),
    
    # 法国
    "Emmanuel Macron": ("French President", "法国总统", "埃马纽埃尔·马克龙"),
    "Gabriel Attal": ("French Prime Minister", "法国总理", "加布里埃尔·阿塔尔"),
    "Michel Barnier": ("Former EU Chief Negotiator", "法国前欧盟首席谈判代表", "米歇尔·巴尼耶"),
    "Catherine Colonna": ("Former French Foreign Minister", "法国前外交部长", "卡特琳娜·科隆纳"),
    
    # 德国
    "Olaf Scholz": ("German Chancellor", "德国总理", "奥拉夫·朔尔茨"),
    "Frank-Walter Steinmeier": ("German President", "德国总统", "弗兰克-瓦尔特·施泰因迈尔"),
    "Annalena Baerbock": ("German Foreign Minister", "德国外交部长", "安娜莱娜·贝尔伯克"),
    "Boris Pistorius": ("German Defense Minister", "德国国防部长", "鲍里斯·皮斯托里乌斯"),
    "Angela Merkel": ("Former German Chancellor", "德国前总理", "安格拉·默克尔"),
    
    # 意大利
    "Giorgia Meloni": ("Italian Prime Minister", "意大利总理", "乔治娅·梅洛尼"),
    "Sergio Mattarella": ("Italian President", "意大利总统", "塞尔吉奥·马塔雷拉"),
    
    # 日本
    "Fumio Kishida": ("Former Japanese Prime Minister", "日本前首相", "岸田文雄"),
    "Shigeru Ishiba": ("Japanese Prime Minister", "日本首相", "石破茂"),
    "Naruhito": ("Emperor of Japan", "日本天皇", "德仁天皇"),
    "Yoko Kamikawa": ("Japanese Foreign Minister", "日本外务大臣", "上川�的子"),
    
    # 韩国
    "Yoon Suk-yeol": ("South Korean President", "韩国总统", "尹锡悦"),
    "Han Duck-soo": ("South Korean Prime Minister", "韩国总理", "韩德洙"),
    "Cho Tae-yul": ("South Korean Foreign Minister", "韩国外交部长", "赵兑烈"),
    
    # 印度
    "Narendra Modi": ("Indian Prime Minister", "印度总理", "纳伦德拉·莫迪"),
    "Droupadi Murmu": ("Indian President", "印度总统", "德劳帕迪·慕尔穆"),
    "S. Jaishankar": ("Indian Foreign Minister", "印度外交部长", "苏杰生"),
    
    # 澳大利亚
    "Anthony Albanese": ("Australian Prime Minister", "澳大利亚总理", "安东尼·阿尔巴尼斯"),
    "Penny Wong": ("Australian Foreign Minister", "澳大利亚外交部长", "黄英贤"),
    "Richard Marles": ("Australian Defense Minister", "澳大利亚国防部长", "理查德·马尔斯"),
    "Scott Morrison": ("Former Australian Prime Minister", "澳大利亚前总理", "斯科特·莫里森"),
    
    # 加拿大
    "Justin Trudeau": ("Canadian Prime Minister", "加拿大总理", "贾斯汀·特鲁多"),
    "Melanie Joly": ("Canadian Foreign Minister", "加拿大外交部长", "梅拉妮·乔利"),
    
    # 巴西
    "Luiz Inacio Lula da Silva": ("Brazilian President", "巴西总统", "路易斯·伊纳西奥·卢拉·达席尔瓦"),
    "Lula": ("Brazilian President", "巴西总统", "卢拉"),
    "Jair Bolsonaro": ("Former Brazilian President", "巴西前总统", "雅伊尔·博索纳罗"),
    
    # 墨西哥
    "Claudia Sheinbaum": ("Mexican President", "墨西哥总统", "克劳迪娅·谢因鲍姆"),
    "Andres Manuel Lopez Obrador": ("Former Mexican President", "墨西哥前总统", "安德烈斯·曼努埃尔·洛佩斯·奥夫拉多尔"),
    
    # 以色列
    "Benjamin Netanyahu": ("Israeli Prime Minister", "以色列总理", "本雅明·内塔尼亚胡"),
    "Isaac Herzog": ("Israeli President", "以色列总统", "伊萨克·赫尔佐格"),
    "Yoav Gallant": ("Former Israeli Defense Minister", "以色列前国防部长", "约阿夫·加兰特"),
    "Gideon Saar": ("Israeli Foreign Minister", "以色列外交部长", "吉德翁·萨尔"),
    "Benny Gantz": ("Former Israeli Defense Minister", "以色列前国防部长", "本尼·甘茨"),
    "Itamar Ben-Gvir": ("Israeli National Security Minister", "以色列国家安全部长", "伊塔马尔·本-格维尔"),
    "Bezalel Smotrich": ("Israeli Finance Minister", "以色列财政部长", "贝扎莱尔·斯莫特里奇"),
    
    # 巴勒斯坦
    "Mahmoud Abbas": ("Palestinian President", "巴勒斯坦总统", "马哈茂德·阿巴斯"),
    "Mohammad Shtayyeh": ("Former Palestinian Prime Minister", "巴勒斯坦前总理", "穆罕默德·什塔耶"),
    "Yahya Sinwar": ("Hamas Leader", "哈马斯领导人", "叶海亚·辛瓦尔"),
    "Ismail Haniyeh": ("Former Hamas Political Bureau Chief", "哈马斯前政治局主席", "伊斯梅尔·哈尼亚"),
    "Khaled Mashal": ("Hamas Political Bureau Member", "哈马斯政治局成员", "哈立德·马沙阿勒"),
    
    # 伊朗
    "Masoud Pezeshkian": ("Iranian President", "伊朗总统", "马苏德·佩泽什基安"),
    "Ebrahim Raisi": ("Former Iranian President", "伊朗前总统", "易卜拉欣·莱希"),
    "Ali Khamenei": ("Supreme Leader of Iran", "伊朗最高领袖", "阿里·哈梅内伊"),
    "Hossein Amir-Abdollahian": ("Former Iranian Foreign Minister", "伊朗前外交部长", "侯赛因·阿米尔-阿卜杜拉希扬"),
    "Mohammad Javad Zarif": ("Former Iranian Foreign Minister", "伊朗前外交部长", "穆罕默德·贾瓦德·扎里夫"),
    
    # 沙特阿拉伯
    "Mohammed bin Salman": ("Saudi Crown Prince", "沙特王储", "穆罕默德·本·萨勒曼"),
    "Salman bin Abdulaziz": ("King of Saudi Arabia", "沙特国王", "萨勒曼·本·阿卜杜勒阿齐兹"),
    "Faisal bin Farhan": ("Saudi Foreign Minister", "沙特外交大臣", "费萨尔·本·法尔汉"),
    
    # 阿联酋
    "Mohamed bin Zayed Al Nahyan": ("UAE President", "阿联酋总统", "穆罕默德·本·扎耶德·阿勒纳哈扬"),
    "Mohammed bin Rashid Al Maktoum": ("UAE Vice President and Dubai Ruler", "阿联酋副总统兼迪拜酋长", "穆罕默德·本·拉希德·阿勒马克图姆"),
    
    # 土耳其
    "Recep Tayyip Erdogan": ("Turkish President", "土耳其总统", "雷杰普·塔伊普·埃尔多安"),
    "Hakan Fidan": ("Turkish Foreign Minister", "土耳其外交部长", "哈坎·菲丹"),
    
    # 埃及
    "Abdel Fattah el-Sisi": ("Egyptian President", "埃及总统", "阿卜杜勒·法塔赫·塞西"),
    "Sameh Shoukry": ("Egyptian Foreign Minister", "埃及外交部长", "萨梅赫·舒克里"),
    
    # 南非
    "Cyril Ramaphosa": ("South African President", "南非总统", "西里尔·拉马福萨"),
    "Naledi Pandor": ("South African Foreign Minister", "南非外交部长", "纳莱迪·潘多尔"),
    
    # 尼日利亚
    "Bola Tinubu": ("Nigerian President", "尼日利亚总统", "博拉·蒂努布"),
    "Alex Otti": ("Abia State Governor", "阿比亚州州长", "亚历克斯·奥蒂"),
    
    # 乌克兰
    "Volodymyr Zelensky": ("Ukrainian President", "乌克兰总统", "弗拉基米尔·泽连斯基"),
    "Denys Shmyhal": ("Ukrainian Prime Minister", "乌克兰总理", "杰尼斯·什梅加尔"),
    "Dmytro Kuleba": ("Former Ukrainian Foreign Minister", "乌克兰前外交部长", "德米特罗·库列巴"),
    "Andrii Sybiha": ("Ukrainian Foreign Minister", "乌克兰外交部长", "安德里·西比哈"),
    "Valerii Zaluzhnyi": ("Former Ukrainian Armed Forces Commander-in-Chief", "乌克兰前武装部队总司令", "瓦列里·扎卢日尼"),
    "Oleksandr Syrskyi": ("Ukrainian Armed Forces Commander-in-Chief", "乌克兰武装部队总司令", "亚历山大·瑟尔斯基"),
    "Rustem Umerov": ("Ukrainian Defense Minister", "乌克兰国防部长", "鲁斯捷姆·乌梅罗夫"),
    
    # 波兰
    "Andrzej Duda": ("Polish President", "波兰总统", "安杰伊·杜达"),
    "Donald Tusk": ("Polish Prime Minister", "波兰总理", "唐纳德·图斯克"),
    
    # 北约
    "Mark Rutte": ("NATO Secretary General", "北约秘书长", "马克·吕特"),
    "Jens Stoltenberg": ("Former NATO Secretary General", "北约前秘书长", "延斯·斯托尔滕贝格"),
    
    # 朝鲜
    "Kim Jong Un": ("North Korean Supreme Leader", "朝鲜最高领导人", "金正恩"),
    "Kim Yo Jong": ("North Korean Workers' Party Vice Department Director", "朝鲜劳动党副部长", "金与正"),
    
    # 新加坡
    "Lawrence Wong": ("Singaporean Prime Minister", "新加坡总理", "黄循财"),
    "Lee Hsien Loong": ("Former Singaporean Prime Minister", "新加坡前总理", "李显龙"),
    
    # 印度尼西亚
    "Prabowo Subianto": ("Indonesian President", "印度尼西亚总统", "普拉博沃·苏比安托"),
    "Joko Widodo": ("Former Indonesian President", "印度尼西亚前总统", "佐科·维多多"),
    
    # 菲律宾
    "Ferdinand Marcos Jr.": ("Philippine President", "菲律宾总统", "小费迪南德·马科斯"),
    "Bongbong Marcos": ("Philippine President", "菲律宾总统", "邦邦·马科斯"),
    
    # 越南
    "To Lam": ("Vietnamese President", "越南国家主席", "苏林"),
    "Pham Minh Chinh": ("Vietnamese Prime Minister", "越南总理", "范明政"),
    
    # 泰国
    "Paetongtarn Shinawatra": ("Thai Prime Minister", "泰国总理", "贝东丹·西那瓦"),
    "Srettha Thavisin": ("Former Thai Prime Minister", "泰国前总理", "赛塔·他威信"),
    
    # 马来西亚
    "Anwar Ibrahim": ("Malaysian Prime Minister", "马来西亚总理", "安瓦尔·易卜拉欣"),
    
    # 巴基斯坦
    "Shehbaz Sharif": ("Pakistani Prime Minister", "巴基斯坦总理", "夏巴兹·谢里夫"),
    "Asif Ali Zardari": ("Pakistani President", "巴基斯坦总统", "阿西夫·阿里·扎尔达里"),
    "Imran Khan": ("Former Pakistani Prime Minister", "巴基斯坦前总理", "伊姆兰·汗"),
    
    # 阿根廷
    "Javier Milei": ("Argentine President", "阿根廷总统", "哈维尔·米莱"),
    
    # 智利
    "Gabriel Boric": ("Chilean President", "智利总统", "加夫列尔·博里奇"),
    
    # 哥伦比亚
    "Gustavo Petro": ("Colombian President", "哥伦比亚总统", "古斯塔沃·佩特罗"),
    
    # 委内瑞拉
    "Nicolas Maduro": ("Venezuelan President", "委内瑞拉总统", "尼古拉斯·马杜罗"),
    
    # 古巴
    "Miguel Diaz-Canel": ("Cuban President", "古巴国家主席", "米格尔·迪亚斯-卡内尔"),
    
    # 叙利亚
    "Bashar al-Assad": ("Former Syrian President", "叙利亚前总统", "巴沙尔·阿萨德"),
    
    # 阿富汗
    "Hibatullah Akhundzada": ("Taliban Supreme Leader", "阿富汗塔利班最高领导人", "希巴图拉·阿洪扎达"),
    
    # 缅甸
    "Min Aung Hlaing": ("Myanmar Military Leader", "缅甸军政府领导人", "敏昂莱"),
    
    # 斯里兰卡
    "Anura Kumara Dissanayake": ("Sri Lankan President", "斯里兰卡总统", "阿努拉·库马拉·迪萨纳亚克"),
    
    # 孟加拉国
    "Muhammad Yunus": ("Bangladesh Interim Government Chief Adviser", "孟加拉国临时政府首席顾问", "穆罕默德·尤努斯"),
    
    # 瑞士
    "Viola Amherd": ("Swiss Federal President", "瑞士联邦主席", "维奥拉·阿姆赫德"),
    
    # 奥地利
    "Karl Nehammer": ("Austrian Chancellor", "奥地利总理", "卡尔·内哈默"),
    
    # 荷兰
    "Dick Schoof": ("Dutch Prime Minister", "荷兰首相", "迪克·斯霍夫"),
    
    # 比利时
    "Alexander De Croo": ("Belgian Prime Minister", "比利时首相", "亚历山大·德克罗"),
    
    # 西班牙
    "Pedro Sanchez": ("Spanish Prime Minister", "西班牙首相", "佩德罗·桑切斯"),
    "Felipe VI": ("King of Spain", "西班牙国王", "费利佩六世"),
    
    # 葡萄牙
    "Luis Montenegro": ("Portuguese Prime Minister", "葡萄牙总理", "路易斯·蒙特内格罗"),
    
    # 希腊
    "Kyriakos Mitsotakis": ("Greek Prime Minister", "希腊总理", "基里亚科斯·米佐塔基斯"),
    
    # 匈牙利
    "Viktor Orban": ("Hungarian Prime Minister", "匈牙利总理", "维克托·欧尔班"),
    
    # 捷克
    "Petr Fiala": ("Czech Prime Minister", "捷克总理", "彼得·菲亚拉"),
    
    # 瑞典
    "Ulf Kristersson": ("Swedish Prime Minister", "瑞典首相", "乌尔夫·克里斯特松"),
    
    # 挪威
    "Jonas Gahr Store": ("Norwegian Prime Minister", "挪威首相", "约纳斯·加尔·斯特勒"),
    
    # 丹麦
    "Mette Frederiksen": ("Danish Prime Minister", "丹麦首相", "梅特·弗雷泽里克森"),
    
    # 芬兰
    "Petteri Orpo": ("Finnish Prime Minister", "芬兰总理", "佩特里·奥尔波"),
    
    # 新西兰
    "Christopher Luxon": ("New Zealand Prime Minister", "新西兰总理", "克里斯托弗·卢克森"),
    "Jacinda Ardern": ("Former New Zealand Prime Minister", "新西兰前总理", "杰辛达·阿德恩"),
    
    # 梵蒂冈
    "Pope Francis": ("Pope", "天主教教皇", "方济各教皇"),
    "Francis": ("Pope", "天主教教皇", "方济各"),
}

# ================= 科技商业领袖 =================
TECH_BUSINESS_LEADERS_FULL: Dict[str, Tuple[str, str, str]] = {
    "Elon Musk": ("Tesla and SpaceX CEO", "特斯拉和SpaceX首席执行官", "埃隆·马斯克"),
    "Tim Cook": ("Apple CEO", "苹果公司首席执行官", "蒂姆·库克"),
    "Satya Nadella": ("Microsoft CEO", "微软首席执行官", "萨蒂亚·纳德拉"),
    "Sundar Pichai": ("Google CEO", "谷歌首席执行官", "桑达尔·皮查伊"),
    "Mark Zuckerberg": ("Meta CEO", "Meta首席执行官", "马克·扎克伯格"),
    "Jeff Bezos": ("Amazon Founder", "亚马逊创始人", "杰夫·贝佐斯"),
    "Jensen Huang": ("NVIDIA CEO", "英伟达首席执行官", "黄仁勋"),
    "Sam Altman": ("OpenAI CEO", "OpenAI首席执行官", "萨姆·奥特曼"),
    "Jack Ma": ("Alibaba Founder", "阿里巴巴创始人", "马云"),
    "Pony Ma": ("Tencent CEO", "腾讯首席执行官", "马化腾"),
    "Lei Jun": ("Xiaomi Founder", "小米创始人", "雷军"),
    "Ren Zhengfei": ("Huawei Founder", "华为创始人", "任正非"),
    "Warren Buffett": ("Berkshire Hathaway Chairman", "伯克希尔哈撒韦董事长", "沃伦·巴菲特"),
    "Bill Gates": ("Microsoft Co-founder", "微软联合创始人", "比尔·盖茨"),
    "Larry Fink": ("BlackRock CEO", "贝莱德首席执行官", "拉里·芬克"),
    "Jamie Dimon": ("JPMorgan Chase CEO", "摩根大通首席执行官", "杰米·戴蒙"),
    "Mary Barra": ("General Motors CEO", "通用汽车首席执行官", "玛丽·博拉"),
    "Bob Iger": ("Disney CEO", "迪士尼首席执行官", "鲍勃·艾格"),
    "Reed Hastings": ("Netflix Co-founder", "Netflix联合创始人", "里德·黑斯廷斯"),
}

# ================= 媒体人物 =================
MEDIA_FIGURES_FULL: Dict[str, Tuple[str, str, str]] = {
    "Tucker Carlson": ("American Political Commentator", "美国政治评论员", "塔克·卡尔森"),
    "Sean Hannity": ("Fox News Host", "福克斯新闻主持人", "肖恩·汉尼提"),
    "Rachel Maddow": ("MSNBC Host", "MSNBC新闻主持人", "蕾切尔·马多"),
    "Anderson Cooper": ("CNN Host", "CNN主持人", "安德森·库珀"),
    "Joe Rogan": ("Podcast Host", "播客主持人", "乔·罗根"),
}

# ================= 军事领袖 =================
MILITARY_LEADERS_FULL: Dict[str, Tuple[str, str, str]] = {
    "Charles Q. Brown Jr.": ("US Joint Chiefs of Staff Chairman", "美国参谋长联席会议主席", "小查尔斯·布朗"),
    "Mark Milley": ("Former US Joint Chiefs of Staff Chairman", "美国前参谋长联席会议主席", "马克·米利"),
    "Valery Gerasimov": ("Russian Armed Forces Chief of Staff", "俄罗斯武装力量总参谋长", "瓦列里·格拉西莫夫"),
}

# ================= 合并所有数据库 =================
KNOWN_PERSONS_FULL: Dict[str, Tuple[str, str, str]] = {}
KNOWN_PERSONS_FULL.update(WORLD_LEADERS_FULL)
KNOWN_PERSONS_FULL.update(TECH_BUSINESS_LEADERS_FULL)
KNOWN_PERSONS_FULL.update(MEDIA_FIGURES_FULL)
KNOWN_PERSONS_FULL.update(MILITARY_LEADERS_FULL)

# 创建姓名变体映射
NAME_VARIANTS = {
    "Biden": "Joe Biden",
    "Trump": "Donald Trump",
    "Putin": "Vladimir Putin",
    "Xi": "Xi Jinping",
    "Zelensky": "Volodymyr Zelensky",
    "Zelenskyy": "Volodymyr Zelensky",
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


def _get_person_info(name: str) -> Optional[Tuple[str, str, str]]:
    """
    获取人物完整信息（英文职位, 中文职位, 中文人名）
    
    Args:
        name: 人物姓名
        
    Returns:
        三元组 (英文职位, 中文职位, 中文人名)，未知则返回 None
    """
    # 直接匹配
    if name in KNOWN_PERSONS_FULL:
        return KNOWN_PERSONS_FULL[name]
    
    # 尝试变体匹配
    if name in NAME_VARIANTS:
        full_name = NAME_VARIANTS[name]
        if full_name in KNOWN_PERSONS_FULL:
            return KNOWN_PERSONS_FULL[full_name]
    
    # 尝试部分匹配（姓氏）
    for known_name, info in KNOWN_PERSONS_FULL.items():
        if name in known_name or known_name in name:
            return info
    
    return None


def get_person_position(name: str, language: str = "zh") -> Optional[str]:
    """
    获取人物职位（支持双语）
    
    Args:
        name: 人物姓名
        language: 语言代码，"zh" 中文, "en" 英文
        
    Returns:
        职位字符串，如果未知则返回 None
    """
    info = _get_person_info(name)
    if info:
        if language.lower() == "en":
            return info[0]  # 英文职位
        else:
            return info[1]  # 中文职位
    return None


def translate_person_name(name: str) -> str:
    """
    将英文人名翻译为中文（直接从数据库获取，无需 LLM）
    
    Args:
        name: 英文人名
        
    Returns:
        中文人名，如果未找到则返回原名
    """
    info = _get_person_info(name)
    if info:
        return info[2]  # 中文人名
    return name


def enrich_person_with_position(name: str, language: str = "zh") -> str:
    """
    为人物名字添加职位信息（支持双语，无需 LLM 翻译）
    
    Args:
        name: 人物姓名
        language: 语言代码，"zh" 中文, "en" 英文
        
    Returns:
        带职位的人物描述
        英文示例: "US President Joe Biden"
        中文示例: "美国总统乔·拜登"
    """
    info = _get_person_info(name)
    if info:
        en_pos, cn_pos, cn_name = info
        if language.lower() == "en":
            return f"{en_pos} {name}"       # 英文：英文职位 + 英文人名
        else:
            return f"{cn_pos}{cn_name}"     # 中文：中文职位 + 中文人名
    
    # 无职位信息时
    if language.lower() == "zh":
        # 即使没有职位，也尝试翻译人名
        return translate_person_name(name)
    return name


def translate_person_with_position(name: str) -> str:
    """
    获取人物的中文职位和中文姓名（便捷方法）
    
    Args:
        name: 英文人名
        
    Returns:
        中文职位+中文姓名，如 "美国总统乔·拜登"
    """
    return enrich_person_with_position(name, "zh")


def translate_persons_list(persons: List[str]) -> List[str]:
    """批量翻译人物列表为中文（带职位）"""
    return [translate_person_with_position(p) for p in persons]


def translate_persons_string(persons_str: str, language: str = "zh") -> str:
    """
    翻译以分隔符分隔的人物字符串（支持双语，无需 LLM）
    
    Args:
        persons_str: 分隔符分隔的人名
        language: 语言代码
        
    Returns:
        翻译后的人物字符串
    """
    if not persons_str:
        return persons_str
    
    # 处理常见分隔符
    for sep in [', ', '; ', ' | ']:
        if sep in persons_str:
            parts = [p.strip() for p in persons_str.split(sep)]
            translated = [enrich_person_with_position(p, language) for p in parts]
            return sep.join(translated)
    
    # 单个人名
    return enrich_person_with_position(persons_str, language)


def enrich_persons_list(persons: List[str], language: str = "zh") -> List[str]:
    """批量为人物列表添加职位信息（支持双语）"""
    return [enrich_person_with_position(p, language) for p in persons]


def add_person(name: str, en_position: str, cn_position: str, cn_name: str) -> None:
    """
    动态添加人物到数据库
    
    Args:
        name: 英文人名
        en_position: 英文职位
        cn_position: 中文职位
        cn_name: 中文人名
    """
    KNOWN_PERSONS_FULL[name] = (en_position, cn_position, cn_name)


def get_all_known_persons() -> Dict[str, Tuple[str, str, str]]:
    """获取所有已知人物"""
    return KNOWN_PERSONS_FULL.copy()


def get_stats() -> Dict[str, int]:
    """获取数据库统计信息"""
    return {
        "总人数": len(KNOWN_PERSONS_FULL),
        "世界领导人": len(WORLD_LEADERS_FULL),
        "科技商业领袖": len(TECH_BUSINESS_LEADERS_FULL),
        "媒体人物": len(MEDIA_FIGURES_FULL),
        "军事领袖": len(MILITARY_LEADERS_FULL),
        "姓名变体": len(NAME_VARIANTS),
    }


# 测试
if __name__ == "__main__":
    print("=== 人物职位数据库统计 ===")
    stats = get_stats()
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    print("\n=== 双语测试（无需 LLM 翻译）===")
    test_names = [
        "Antonio Guterres",
        "Joe Biden",
        "Benjamin Netanyahu",
        "Putin",
        "Xi Jinping",
        "Elon Musk",
    ]
    for name in test_names:
        en_result = enrich_person_with_position(name, "en")
        zh_result = enrich_person_with_position(name, "zh")
        print(f"  {name}")
        print(f"    EN: {en_result}")
        print(f"    ZH: {zh_result}")
