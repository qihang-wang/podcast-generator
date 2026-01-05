"""
GDELT CAMEO (Conflict and Mediation Event Observations) 编码映射表

基于官方 CAMEO Codebook (Version 0.9b6) 和 CAMEO Scale

CAMEO 是一个三级金字塔式分类体系：
- QuadClass (4类): 事件性质的最宏观分类
- EventRootCode (20类): 战略类别
- EventCode (~300类): 具体行为

注意：EventRootCode 和 EventCode 应作为字符串处理，以保留前导零
"""

from typing import Dict, Tuple, Optional

# ============================================================================
# 第一级：QuadClass - 事件性质（4个基本象限）
# ============================================================================

QUAD_CLASS_MAP: Dict[int, Tuple[str, str]] = {
    1: ("口头合作", "Verbal Cooperation"),
    2: ("物质合作", "Material Cooperation"),
    3: ("口头冲突", "Verbal Conflict"),
    4: ("物质冲突", "Material Conflict"),
}


def get_quad_class_name(quad_class: int, lang: str = "zh") -> str:
    """获取 QuadClass 名称"""
    if quad_class not in QUAD_CLASS_MAP:
        return "未知"
    return QUAD_CLASS_MAP[quad_class][0 if lang == "zh" else 1]


# ============================================================================
# 第二级:EventRootCode - 战略类别(20个核心大类)
# 格式: {code: (中文名, 英文名)}
# ============================================================================

EVENT_ROOT_CODE_MAP: Dict[str, Tuple[str, str]] = {
    "01": ("发表声明", "Make statement"),
    "02": ("呼吁", "Appeal"),
    "03": ("表达合作意向", "Express intent to cooperate"),
    "04": ("磋商", "Consult"),
    "05": ("外交合作", "Engage in diplomatic cooperation"),
    "06": ("物质合作", "Engage in material cooperation"),
    "07": ("提供援助", "Provide aid"),
    "08": ("让步", "Yield"),
    "09": ("调查", "Investigate"),
    "10": ("要求", "Demand"),
    "11": ("反对", "Disapprove"),
    "12": ("拒绝", "Reject"),
    "13": ("威胁", "Threaten"),
    "14": ("抗议", "Engage in political protest"),
    "15": ("展示武力", "Demonstrate military or police power"),
    "16": ("降低关系", "Reduce relations"),
    "17": ("施加强制", "Coerce"),
    "18": ("攻击", "Use unconventional violence"),
    "19": ("战斗", "Use conventional military force"),
    "20": ("大规模暴力", "Engage in unconventional mass violence"),
}



def get_event_root_name(root_code: str, lang: str = "zh") -> str:
    """获取 EventRootCode 名称"""
    root_code = str(root_code).zfill(2)
    if root_code not in EVENT_ROOT_CODE_MAP:
        return "未知"
    return EVENT_ROOT_CODE_MAP[root_code][0 if lang == "zh" else 1]


def get_event_root_goldstein(root_code: str) -> float:
    """获取 EventRootCode 的典型 Goldstein 值（已不再提供，返回 0.0）"""
    # 现在 EVENT_ROOT_CODE_MAP 只包含名称，不包含 Goldstein
    # 为兼容旧代码，统一返回 0.0 表示未知
    return 0.0


# ============================================================================
# 第三级：EventCode - 具体行为
# 格式: {code: (中文名, 英文名, Goldstein值)}
# 基于官方 CAMEO Scale (Philip Schrodt)
# ============================================================================

EVENT_CODE_MAP: Dict[str, Tuple[str, str, float]] = {
    # 01X - 发表声明
    "010": ("发表一般声明", "Make statement, not specified", 0.0),
    "011": ("拒绝评论", "Decline comment", -0.1),
    "012": ("发表悲观评论", "Make pessimistic comment", -0.4),
    "013": ("发表乐观评论", "Make optimistic comment", 0.4),
    "014": ("考虑政策选项", "Consider policy option", 0.0),
    "015": ("承认或声称责任", "Acknowledge or claim responsibility", 0.0),
    "016": ("发表同情评论", "Make empathetic comment", 3.4),
    "017": ("进行象征性行为", "Engage in symbolic act", 0.0),
    "018": ("表达一致", "Express accord", 3.4),
    
    # 02X - 呼吁/请求
    "020": ("呼吁", "Appeal, not specified", 3.0),
    "021": ("呼吁合作", "Appeal for cooperation", 3.4),
    "022": ("呼吁政策支持", "Appeal for policy support", 3.4),
    "023": ("呼吁援助", "Appeal for aid", 3.4),
    "024": ("呼吁政治改革", "Appeal for political reform", -0.3),
    "025": ("呼吁让步", "Appeal to yield", -0.3),
    "026": ("呼吁会面或谈判", "Appeal to meet or negotiate", 4.0),
    "027": ("呼吁解决争端", "Appeal to settle dispute", 4.0),
    "028": ("呼吁进行调解", "Appeal to engage in mediation", 4.0),
    
    # 03X - 表达合作意向
    "030": ("表达合作意向", "Express intent to cooperate", 4.0),
    "031": ("表达物质合作意向", "Express intent to material cooperation", 5.2),
    "032": ("表达政策支持意向", "Express intent to provide policy support", 4.5),
    "033": ("表达援助意向", "Express intent to provide aid", 5.2),
    "034": ("表达政治改革意向", "Express intent to bring political reform", 7.0),
    "035": ("表达让步意向", "Express intent to yield", 7.0),
    "036": ("表达会谈意向", "Express intent to meet or negotiate", 4.0),
    "037": ("表达解决争端意向", "Express intent to settle dispute", 5.0),
    "038": ("表达接受调解意向", "Express intent to accept mediation", 7.0),
    "039": ("表达调解意向", "Express intent to mediate", 5.0),
    
    # 04X - 磋商
    "040": ("磋商", "Consult, not specified", 1.0),
    "041": ("电话讨论", "Discuss by telephone", 1.0),
    "042": ("进行访问", "Make a visit", 1.9),
    "043": ("接待访问", "Host a visit", 2.8),
    "044": ("在第三地会面", "Meet at a third location", 2.5),
    "045": ("进行调解", "Mediate", 5.0),
    "046": ("进行谈判", "Engage in negotiation", 7.0),
    
    # 05X - 外交合作
    "050": ("外交合作", "Engage in diplomatic cooperation", 3.5),
    "051": ("赞扬/认可", "Praise or endorse", 3.4),
    "052": ("口头辩护", "Defend verbally", 3.5),
    "053": ("代表他人集结支持", "Rally support on behalf of", 3.8),
    "054": ("给予外交承认", "Grant diplomatic recognition", 6.0),
    "055": ("道歉", "Apologize", 7.0),
    "056": ("原谅", "Forgive", 7.0),
    "057": ("签署正式协议", "Sign formal agreement", 8.0),
    
    # 06X - 物质合作
    "060": ("物质合作", "Engage in material cooperation", 6.0),
    "061": ("经济合作", "Cooperate economically", 6.4),
    "062": ("军事合作", "Cooperate militarily", 7.4),
    "063": ("司法合作", "Engage in judicial cooperation", 7.4),
    "064": ("情报/信息共享", "Share intelligence or information", 7.0),
    
    # 07X - 提供援助
    "070": ("提供援助", "Provide aid, not specified", 7.0),
    "071": ("提供经济援助", "Provide economic aid", 7.4),
    "072": ("提供军事援助", "Provide military aid", 8.3),
    "073": ("提供人道主义援助", "Provide humanitarian aid", 7.4),
    "074": ("提供军事保护或维和", "Provide military protection or peacekeeping", 8.5),
    "075": ("给予庇护", "Grant asylum", 7.0),
    
    # 08X - 屈服/让步
    "080": ("让步", "Yield, not specified", 5.0),
    "081": ("放宽行政制裁", "Ease administrative sanctions", 5.0),
    "082": ("平息抗议", "Ease popular protest", 5.0),
    "083": ("接受政治改革要求", "Accede to demands for political reform", 5.0),
    "084": ("归还/释放人员或财产", "Return, release persons or property", 7.0),
    "085": ("放宽经济制裁/禁运", "Ease economic sanctions, boycott, embargo", 7.0),
    "086": ("允许国际介入", "Allow international involvement", 9.0),
    "087": ("降低军事对抗", "De-escalate military engagement", 9.0),
    
    # 09X - 调查
    "090": ("调查", "Investigate, not specified", -2.0),
    "091": ("调查犯罪/腐败", "Investigate crime, corruption", -2.0),
    "092": ("调查人权侵犯", "Investigate human rights abuses", -2.0),
    "093": ("调查军事行动", "Investigate military action", -2.0),
    "094": ("调查战争罪行", "Investigate war crimes", -2.0),
    
    # 10X - 要求
    "100": ("要求", "Demand, not specified", -5.0),
    "101": ("要求调查/信息", "Demand information, investigation", -5.0),
    "102": ("要求政策支持", "Demand policy support", -5.0),
    "103": ("要求援助/保护", "Demand aid, protection, or peacekeeping", -5.0),
    "104": ("要求政治改革", "Demand political reform", -5.0),
    "105": ("要求调解", "Demand mediation", -5.0),
    "106": ("要求撤离", "Demand withdrawal", -5.0),
    "107": ("要求停火", "Demand ceasefire", -5.0),
    "108": ("要求会面/谈判", "Demand meeting, negotiation", -5.0),
    
    # 11X - 反对/批评
    "110": ("反对", "Disapprove, not specified", -2.0),
    "111": ("批评/谴责", "Criticize or denounce", -2.0),
    "112": ("指控", "Accuse", -2.0),
    "113": ("集结反对力量", "Rally opposition against", -2.0),
    "114": ("正式投诉", "Complain officially", -2.0),
    "115": ("提起诉讼", "Bring lawsuit against", -2.0),
    
    # 12X - 拒绝
    "120": ("拒绝", "Reject, not specified", -4.0),
    "121": ("拒绝提案", "Reject proposal", -4.0),
    "122": ("拒绝物质援助请求", "Reject request for material aid", -4.0),
    "123": ("拒绝政治改革要求", "Reject demands for political reform", -4.0),
    "124": ("拒绝会面/谈判", "Reject proposal to meet, negotiate", -5.0),
    "125": ("拒绝调解", "Reject mediation", -5.0),
    "126": ("违反规范/法律", "Defy norms, law", -5.0),
    "127": ("否认指控/责任", "Reject accusation, deny responsibility", -5.0),
    "128": ("行使否决权", "Veto", -5.0),
    
    # 13X - 威胁
    "130": ("威胁", "Threaten, not specified", -4.4),
    "131": ("非武力威胁", "Threaten non-force", -5.8),
    "132": ("威胁行政制裁", "Threaten with administrative sanctions", -5.8),
    "133": ("威胁集体异议", "Threaten collective dissent", -5.8),
    "134": ("威胁中止谈判", "Threaten to halt negotiations", -5.8),
    "135": ("威胁中止调解", "Threaten to halt mediation", -5.8),
    "136": ("威胁驱逐维和人员", "Threaten to expel peacekeepers", -7.0),
    "137": ("威胁暴力镇压", "Threaten with violent repression", -7.0),
    "138": ("威胁使用军事力量", "Threaten to use military force", -7.0),
    "139": ("发出最后通牒", "Give ultimatum", -7.0),
    
    # 14X - 抗议
    "140": ("抗议", "Engage in political protest", -6.5),
    "141": ("示威游行", "Demonstrate or rally", -6.5),
    "142": ("进行绝食抗议", "Conduct hunger strike", -6.5),
    "143": ("罢工/抵制", "Conduct strike or boycott", -6.5),
    "144": ("阻塞通道", "Obstruct passage, block", -7.5),
    "145": ("暴力抗议/骚乱", "Protest violently, riot", -7.5),
    
    # 15X - 展示武力
    "150": ("展示军事/警察力量", "Demonstrate military or police power", -7.2),
    "151": ("提高警察警戒状态", "Increase police alert status", -7.2),
    "152": ("提高军事警戒状态", "Increase military alert status", -7.2),
    "153": ("动员/增加警察力量", "Mobilize or increase police power", -7.2),
    "154": ("动员/增加武装力量", "Mobilize or increase armed forces", -7.2),
    
    # 16X - 降低关系
    "160": ("降低关系", "Reduce relations, not specified", -4.0),
    "161": ("降低/断绝外交关系", "Reduce or break diplomatic relations", -4.0),
    "162": ("减少/停止援助", "Reduce or stop aid", -5.6),
    "163": ("中止谈判", "Halt negotiations", -6.5),
    "164": ("驱逐/撤离人员", "Expel or withdraw personnel", -7.0),
    "165": ("中止调解", "Halt mediation", -7.0),
    "166": ("实施禁运/制裁", "Impose embargo, boycott, or sanctions", -8.0),
    
    # 17X - 施加强制
    "170": ("施加强制", "Coerce, not specified", -7.0),
    "171": ("没收/破坏财产", "Seize or damage property", -9.2),
    "172": ("实施行政制裁", "Impose administrative sanctions", -5.0),
    "173": ("逮捕/拘留", "Arrest, detain, or charge with legal action", -5.0),
    "174": ("驱逐/遣返个人", "Expel or deport individuals", -5.0),
    "175": ("暴力镇压", "Use violent repression", -9.0),
    
    # 18X - 攻击/袭击
    "180": ("非常规暴力", "Use unconventional violence", -9.0),
    "181": ("绑架/劫持", "Abduct, hijack, or take hostage", -9.0),
    "182": ("人身攻击", "Physically assault", -9.5),
    "183": ("非军事爆炸袭击", "Conduct bombing", -10.0),
    "184": ("用作人质盾牌", "Use as human shield", -8.0),
    "185": ("企图暗杀", "Attempt to assassinate", -8.0),
    "186": ("暗杀", "Assassinate", -10.0),
    
    # 19X - 战斗/交战
    "190": ("使用常规军事力量", "Use conventional military force", -10.0),
    "191": ("实施封锁/限制通行", "Impose blockade, restrict movement", -9.5),
    "192": ("占领领土", "Occupy territory", -9.5),
    "193": ("使用轻武器战斗", "Fight with small arms and light weapons", -10.0),
    "194": ("使用火炮和坦克战斗", "Fight with artillery and tanks", -10.0),
    "195": ("使用空中武器", "Employ aerial weapons", -10.0),
    "196": ("违反停火", "Violate ceasefire", -9.5),
    
    # 20X - 大规模暴力
    "200": ("非常规大规模暴力", "Engage in unconventional mass violence", -10.0),
    "201": ("大规模驱逐", "Engage in mass expulsion", -9.5),
    "202": ("大规模杀戮", "Engage in mass killings", -10.0),
    "203": ("种族清洗", "Engage in ethnic cleansing", -10.0),
    "204": ("使用大规模杀伤性武器", "Use weapons of mass destruction", -10.0),
}


def get_event_code_name(event_code: str, lang: str = "zh") -> str:
    """获取 EventCode 名称"""
    event_code = str(event_code).zfill(3)
    
    if event_code in EVENT_CODE_MAP:
        return EVENT_CODE_MAP[event_code][0 if lang == "zh" else 1]
    
    # 回退到 RootCode
    root_code = event_code[:2]
    return get_event_root_name(root_code, lang)


def get_event_code_goldstein(event_code: str) -> float:
    """获取 EventCode 的 Goldstein 值"""
    event_code = str(event_code).zfill(3)
    if event_code in EVENT_CODE_MAP:
        return EVENT_CODE_MAP[event_code][2]
    return get_event_root_goldstein(event_code[:2])


# ============================================================================
# 工具函数
# ============================================================================

def get_event_classification(quad_class: int, event_root_code: str, event_code: str, 
                             lang: str = "zh") -> Dict[str, str]:
    """获取事件的完整三级分类信息"""
    return {
        "quad_class": get_quad_class_name(quad_class, lang),
        "event_root": get_event_root_name(event_root_code, lang),
        "event_code": get_event_code_name(event_code, lang),
    }


def format_event_description(quad_class: int, event_root_code: str, event_code: str,
                             goldstein_scale: float = None) -> str:
    """格式化事件描述（中文）"""
    classification = get_event_classification(quad_class, event_root_code, event_code, "zh")
    
    parts = [
        f"[{classification['quad_class']}]",
        classification['event_root'],
        f"- {classification['event_code']}"
    ]
    
    if goldstein_scale is not None:
        impact = "稳定" if goldstein_scale >= 0 else "不稳定"
        parts.append(f"(影响: {goldstein_scale:+.1f}, {impact})")
    
    return " ".join(parts)


if __name__ == "__main__":
    # 示例: 你提到的两个事件
    print(format_event_description(4, "19", "193", -10.0))
    # 输出: [物质冲突] 战斗/交战 - 使用轻武器战斗 (影响: -10.0, 不稳定)
    
    print(format_event_description(2, "07", "071", 7.4))
    # 输出: [物质合作] 提供援助 - 提供经济援助 (影响: +7.4, 稳定)
