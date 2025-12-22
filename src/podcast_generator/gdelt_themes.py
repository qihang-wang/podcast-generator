"""
GDELT GKG 2.0 主题分类模块

基于 GDELT GKG 2.0 主题代码深度分类研究报告构建。
实现六大领域分类与双层校验逻辑。

六大领域：
1. 突发/危机 (Breaking/Crisis)
2. 政治 (Politics)
3. 军事 (Military)
4. 经济 (Economy)
5. 科技 (Technology)
6. 社会 (Society)
"""

from typing import Dict, List, Optional
from dataclasses import dataclass


# ============================================================================
# 停用主题 - 过于泛化，不应单独使用
# ============================================================================
STOP_THEMES = [
    "GENERAL_GOVERNMENT",
    "LEADER",
    "GENERAL_HEALTH",
]


# ============================================================================
# 六大领域主题代码定义
# ============================================================================

@dataclass
class ThemePreset:
    """主题预设配置 - 支持双层校验逻辑"""
    primary: List[str]           # 主代码（必须匹配）
    validator: Optional[List[str]] = None   # 验证代码（可选共现验证）
    exclude: Optional[List[str]] = None      # 排除代码（必须不存在）


class GDELTThemePresets:
    """GDELT 主题预设 - 六大领域分类"""
    
    # ========================================================================
    # 1. 突发/危机 (Breaking/Crisis)
    # ========================================================================
    BREAKING = ThemePreset(
        primary=[
            # CrisisLex 体系 - 核心危机标识
            "CRISISLEX_T03_DEAD",               # 死亡提及（金标准）
            "CRISISLEX_T02_INJURED",            # 受伤提及
            "CRISISLEX_T09_DISPLACED",          # 流离失所
            "CRISISLEX_C02_NEEDSPROVIDE_FOOD",  # 食品需求
            "CRISISLEX_C06_WATER_SANITATION",   # 水资源危机
            "CRISISLEX_C05_NEED_OF_SHELTERS",   # 避难所需求
            "CRISISLEX_T01_CAUTION_ADVICE",     # 警告建议
            "CRISISLEX_O01_WEATHER",            # 极端天气
            
            # 自然灾害
            "NATURAL_DISASTER_EARTHQUAKE",
            "NATURAL_DISASTER_FLOOD",
            "NATURAL_DISASTER_FLOODWATER",
            "NATURAL_DISASTER_TSUNAMI",
            "NATURAL_DISASTER_HURRICANE",
            "NATURAL_DISASTER_CYCLONE",
            "NATURAL_DISASTER_TORNADO",
            "NATURAL_DISASTER_WILDFIRE",
            "ENV_WILDFIRE",
            "NATURAL_DISASTER_LANDSLIDE",
            "NATURAL_DISASTER",
        ],
        validator=[
            "KILL",
            "WOUND",
            "AFFECT",
        ],
    )
    
    # ========================================================================
    # 2. 政治 (Politics)
    # ========================================================================
    POLITICS = ThemePreset(
        primary=[
            # 功能性角色 (Functional Actors)
            "TAX_FNCACT_PRESIDENT",
            "TAX_FNCACT_PRIME_MINISTER",
            "TAX_FNCACT_MINISTER",
            "TAX_FNCACT_OPPOSITION",
            "TAX_FNCACT_LAWMAKERS",
            "TAX_FNCACT_CONGRESSMAN",
            "TAX_POLITICAL_PARTY",
            
            # 治理结构
            "WB_831_GOVERNANCE",
            "WB_832_ANTI_CORRUPTION",
            "WB_2432_FRAGILITY_CONFLICT_AND_VIOLENCE",
            "WB_696_PUBLIC_SECTOR_MANAGEMENT",
            "WB_723_PUBLIC_ADMINISTRATION",
            
            # 选举与民主
            "ELECTION",
            "DEMOCRACY",
            "LEGISLATION",
            "CORRUPTION",
        ],
        validator=[
            "WB_831_GOVERNANCE",
            "WB_832_ANTI_CORRUPTION",
            "WB_2432_FRAGILITY_CONFLICT_AND_VIOLENCE",
            "WB_696_PUBLIC_SECTOR_MANAGEMENT",
            "LEGISLATION",
        ],
        exclude=[
            "WB_698_TRADE",  # 排除礼节性外交
        ],
    )
    
    # ========================================================================
    # 3. 军事 (Military)
    # ========================================================================
    MILITARY = ThemePreset(
        primary=[
            "MILITARY",
            "ARMEDCONFLICT",
            "TAX_FNCACT_SOLDIER",
            "TAX_FNCACT_PILOT",
            "TAX_MILITARY_TITLE",
            
            # 非对称威胁
            "TERROR",
            "TAX_TERROR_GROUP",
            "INSURGENCY",
            
            # 网络战
            "CYBER_ATTACK",
            
            # 武器扩散
            "MIL_WEAPONS_PROLIFERATION",
            "TAX_WEAPONS",
        ],
        validator=[
            "KILL",
            "WOUND",
            "WB_2433_CONFLICT_AND_VIOLENCE",
            "TAX_FNCACT_SOLDIER",
        ],
        exclude=[
            "HISTORY",
            "ANNIVERSARY",
            "MOVIE",
            "FILM",
            "ARTS",
        ],
    )
    
    # ========================================================================
    # 4. 经济 (Economy)
    # ========================================================================
    ECONOMY = ThemePreset(
        primary=[
            # 宏观经济与金融市场
            "ECON_STOCKMARKET",
            "ECON_BANKRUPTCY",
            "ECON_INFLATION",
            "WB_470_INFLATION",
            "ECON_INTEREST_RATES",
            "ECON_WORLDCURRENCIES",
            "ECON_CENTRALBANK",
            
            # 贸易与全球化
            "WB_698_TRADE",
            "ECON_ENTREPRENEURSHIP",
            "ECON_FREETRADE",
            
            # 能源与资源
            "ENV_OIL",
            "ENV_GAS",
            "ENV_COAL",
            "ENV_MINING",
            "ECON_OILPRICE",
            
            # 基础设施与发展
            "WB_137_WATER",
            "WB_451_TRANSPORTATION",
            "WB_135_TRANSPORT",
        ],
        validator=[
            "WB_1104_MACROECONOMIC_VULNERABILITY_AND_DEBT",
            "ECON_CENTRALBANK",
            "ECON_WORLDCURRENCIES",
        ],
    )
    
    # ========================================================================
    # 5. 科技 (Technology)
    # ========================================================================
    TECH = ThemePreset(
        primary=[
            # 核心科技与研发
            "SCIENCE",
            "SOC_TECHNOLOGYSECTOR",
            "WB_376_INNOVATION_TECHNOLOGY_AND_ENTREPRENEURSHIP",
            
            # 前沿技术领域
            "WB_1331_HEALTH_TECHNOLOGIES",  # 医疗科技
            "WB_825_INFORMATION_AND_COMMUNICATION_TECHNOLOGIES",  # ICT
            "WB_133_INFORMATION_AND_COMMUNICATION_TECHNOLOGIES",  # ICT（另一代码）
            "CYBER_ATTACK",  # 网络安全（兼具军事属性）
            "ENV_CLIMATECHANGE",  # 气候技术
            
            # 科研角色
            "TAX_FNCACT_SCIENTIST",
            "TAX_FNCACT_RESEARCHER",
            "TAX_FNCACT_PROFESSOR",
        ],
        validator=[
            "TAX_FNCACT_SCIENTIST",
            "TAX_FNCACT_RESEARCHER",
            "TAX_FNCACT_PROFESSOR",
            "ECON_ENTREPRENEURSHIP",
            "ECON_STOCKMARKET",
        ],
    )
    
    # ========================================================================
    # 6. 社会 (Society)
    # ========================================================================
    SOCIETY = ThemePreset(
        primary=[
            # 公共卫生
            "HEALTH",
            "GENERAL_HEALTH",
            "MEDICAL",
            "WB_621_HEALTH_NUTRITION_AND_POPULATION",
            "WB_1406_DISEASES",
            
            # 教育
            "EDUCATION",
            "WB_470_EDUCATION",
            
            # 人权与社会正义
            "HUMAN_RIGHTS",
            "REFUGEES",
            "WB_1012_GENDER",
            "DISCRIMINATION_RACE",
            
            # 犯罪与公共安全
            "CRIME_ILLEGAL_DRUGS",
            "SOC_GENERALCRIME",
            "ARREST",
            "TRIAL",
            "TAX_FNCACT_POLICE",
            "TAX_FNCACT_CRIMINAL",
        ],
        validator=[
            "TAX_FNCACT_TEACHER",
            "TAX_FNCACT_DOCTOR",
            "TAX_FNCACT_POLICE",
            "WB_1406_DISEASES",
            "TAX_FNCACT_STUDENT",
        ],
    )


# ============================================================================
# 双层校验逻辑构建函数
# ============================================================================

def build_theme_condition(
    preset: ThemePreset,
    offset_limit: int = 10,
    use_validator: bool = True,
    use_exclude: bool = True
) -> str:
    """
    构建双层校验的 SQL WHERE 条件
    
    Args:
        preset: 主题预设配置
        offset_limit: 主代码检查的主题数量限制（前N个主题）
        use_validator: 是否启用验证代码校验
        use_exclude: 是否启用排除代码校验
    
    Returns:
        SQL WHERE 条件字符串
    
    逻辑：
    1. 主代码必须在前N个主题中出现（核心匹配）
    2. 验证代码必须在全文主题中共现（可选，增强精度）
    3. 排除代码不能出现在任何位置（可选，减少噪音）
    """
    conditions = []
    
    # 1. 主代码匹配（必须）
    if preset.primary:
        primary_checks = " OR ".join([
            f"SPLIT(theme, ',')[OFFSET(0)] = '{code}'"
            for code in preset.primary
        ])
        primary_condition = f"""EXISTS (
    SELECT 1 FROM UNNEST(SPLIT(V2Themes, ';')) AS theme WITH OFFSET
    WHERE OFFSET < {offset_limit}
    AND ({primary_checks})
  )"""
        conditions.append(primary_condition)
    
    # 2. 验证代码共现（可选）
    if use_validator and preset.validator:
        validator_checks = " OR ".join([
            f"SPLIT(theme, ',')[OFFSET(0)] = '{code}'"
            for code in preset.validator
        ])
        validator_condition = f"""EXISTS (
    SELECT 1 FROM UNNEST(SPLIT(V2Themes, ';')) AS theme
    WHERE ({validator_checks})
  )"""
        conditions.append(validator_condition)
    
    # 3. 排除代码过滤（可选）
    if use_exclude and preset.exclude:
        exclude_checks = " OR ".join([
            f"SPLIT(theme, ',')[OFFSET(0)] = '{code}'"
            for code in preset.exclude
        ])
        exclude_condition = f"""NOT EXISTS (
    SELECT 1 FROM UNNEST(SPLIT(V2Themes, ';')) AS theme
    WHERE ({exclude_checks})
  )"""
        conditions.append(exclude_condition)
    
    # 组合所有条件
    return " AND ".join(conditions)


def get_preset_by_name(name: str) -> Optional[ThemePreset]:
    """
    根据名称获取主题预设
    
    Args:
        name: 预设名称（BREAKING, POLITICS, MILITARY, ECONOMY, TECH, SOCIETY）
    
    Returns:
        ThemePreset 对象或 None
    """
    preset_map = {
        "BREAKING": GDELTThemePresets.BREAKING,
        "POLITICS": GDELTThemePresets.POLITICS,
        "MILITARY": GDELTThemePresets.MILITARY,
        "ECONOMY": GDELTThemePresets.ECONOMY,
        "TECH": GDELTThemePresets.TECH,
        "SOCIETY": GDELTThemePresets.SOCIETY,
    }
    return preset_map.get(name.upper())
