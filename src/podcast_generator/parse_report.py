"""
GDELT报告CSV解析器
用于解析生成的GDELT报告CSV文件并提取所有字段信息
"""

import csv
import os
import glob
from datetime import datetime
from typing import List, Dict, Any, Optional


class GdeltReportParser:
    """GDELT报告解析器类"""
    
    # CSV文件的字段列表
    FIELDS = [
        'Time',           # 时间
        'Title',          # 标题
        'Source_Name',    # 来源名称
        'Tone',           # 情感
        'Emotions',       # 情绪
        'Themes',         # 主题
        'Locations',      # 地点
        'Key_Persons',    # 关键人物
        'Organizations',  # 组织
        'Quotes',         # 引用
        'Data_Facts',     # 数据事实
        'Images',         # 图片
        'Source_URL'      # 来源链接
    ]
    
    def __init__(self, file_path: str):
        """
        初始化解析器
        
        Args:
            file_path: CSV文件路径
        """
        self.file_path = file_path
        self.data: List[Dict[str, Any]] = []
        
    def parse(self) -> List[Dict[str, Any]]:
        """
        解析CSV文件
        
        Returns:
            包含所有记录的列表，每条记录是一个字典
        """
        with open(self.file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.data = list(reader)
        return self.data
    
    def get_fields(self) -> List[str]:
        """
        获取所有字段名称
        
        Returns:
            字段名称列表
        """
        return self.FIELDS.copy()
    
    def get_record_count(self) -> int:
        """
        获取记录数量
        
        Returns:
            记录数量
        """
        return len(self.data)
    
    def get_field_values(self, field_name: str) -> List[Any]:
        """
        获取指定字段的所有值
        
        Args:
            field_name: 字段名称
            
        Returns:
            该字段的所有值列表
        """
        if field_name not in self.FIELDS:
            raise ValueError(f"无效的字段名: {field_name}")
        return [record.get(field_name) for record in self.data]
    
    def filter_by_tone(self, tone_type: str) -> List[Dict[str, Any]]:
        """
        按情感类型筛选记录
        
        Args:
            tone_type: 情感类型，如 "Very Negative", "Negative", "Positive", "Very Positive"
            
        Returns:
            符合条件的记录列表
        """
        return [record for record in self.data if tone_type in record.get('Tone', '')]
    
    def filter_by_source(self, source_name: str) -> List[Dict[str, Any]]:
        """
        按来源筛选记录
        
        Args:
            source_name: 来源名称
            
        Returns:
            符合条件的记录列表
        """
        return [record for record in self.data 
                if source_name.lower() in record.get('Source_Name', '').lower()]
    
    def get_unique_sources(self) -> List[str]:
        """
        获取所有唯一的来源
        
        Returns:
            唯一来源列表
        """
        sources = set()
        for record in self.data:
            source = record.get('Source_Name', '')
            if source:
                sources.add(source)
        return sorted(list(sources))
    
    def get_tone_statistics(self) -> Dict[str, int]:
        """
        获取情感统计
        
        Returns:
            各情感类型的数量统计
        """
        stats = {
            'Very Negative': 0,
            'Negative': 0,
            'Neutral': 0,
            'Positive': 0,
            'Very Positive': 0
        }
        for record in self.data:
            tone = record.get('Tone', '')
            for tone_type in stats.keys():
                if tone_type in tone:
                    stats[tone_type] += 1
                    break
        return stats
    
    def search_by_keyword(self, keyword: str, fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        按关键词搜索记录
        
        Args:
            keyword: 搜索关键词
            fields: 要搜索的字段列表，默认搜索所有字段
            
        Returns:
            符合条件的记录列表
        """
        if fields is None:
            fields = self.FIELDS
        
        results = []
        keyword_lower = keyword.lower()
        for record in self.data:
            for field in fields:
                value = record.get(field, '')
                if keyword_lower in str(value).lower():
                    results.append(record)
                    break
        return results
    
    def export_to_dict(self, record_index: int) -> Dict[str, Any]:
        """
        将指定索引的记录导出为字典
        
        Args:
            record_index: 记录索引
            
        Returns:
            记录字典
        """
        if 0 <= record_index < len(self.data):
            return self.data[record_index]
        raise IndexError(f"记录索引超出范围: {record_index}")
    
    def print_summary(self):
        """打印报告摘要"""
        print(f"\n{'='*60}")
        print(f"GDELT报告摘要: {os.path.basename(self.file_path)}")
        print(f"{'='*60}")
        print(f"记录总数: {self.get_record_count()}")
        print(f"\n字段列表 ({len(self.FIELDS)}个):")
        for i, field in enumerate(self.FIELDS, 1):
            print(f"  {i:2}. {field}")
        
        print(f"\n来源统计:")
        sources = self.get_unique_sources()
        print(f"  唯一来源数量: {len(sources)}")
        
        print(f"\n情感分布:")
        tone_stats = self.get_tone_statistics()
        for tone, count in tone_stats.items():
            if count > 0:
                print(f"  {tone}: {count}")
        print(f"{'='*60}\n")


def find_latest_report(directory: str = ".") -> Optional[str]:
    """
    查找最新的GDELT报告文件
    
    Args:
        directory: 搜索目录
        
    Returns:
        最新报告文件的路径，如果没有找到则返回None
    """
    pattern = os.path.join(directory, "gdelt_report_*.csv")
    files = glob.glob(pattern)
    if not files:
        return None
    # 按文件名排序，取最新的
    return max(files)


# ================= 供 main.py 调用的便捷方法 =================

def parse_report(file_path: str) -> Dict[str, Any]:
    """
    解析指定的报告文件
    
    Args:
        file_path: CSV文件路径
        
    Returns:
        包含解析结果的字典，包括:
        - data: 所有记录列表
        - record_count: 记录数量
        - fields: 字段列表
        - sources: 唯一来源列表
        - tone_stats: 情感统计
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    parser = GdeltReportParser(file_path)
    parser.parse()
    
    return {
        'file_path': file_path,
        'data': parser.data,
        'record_count': parser.get_record_count(),
        'fields': parser.get_fields(),
        'sources': parser.get_unique_sources(),
        'tone_stats': parser.get_tone_statistics(),
        'parser': parser  # 返回解析器实例，便于进一步操作
    }


def parse_latest_report(directory: str = ".") -> Optional[Dict[str, Any]]:
    """
    解析最新的报告文件
    
    Args:
        directory: 搜索目录
        
    Returns:
        解析结果字典，如果没有找到报告则返回None
    """
    file_path = find_latest_report(directory)
    if file_path is None:
        return None
    return parse_report(file_path)


def get_report_summary(file_path: str) -> Dict[str, Any]:
    """
    获取报告摘要信息
    
    Args:
        file_path: CSV文件路径
        
    Returns:
        报告摘要字典
    """
    result = parse_report(file_path)
    
    summary = {
        'file_name': os.path.basename(file_path),
        'record_count': result['record_count'],
        'source_count': len(result['sources']),
        'tone_stats': result['tone_stats'],
        'top_sources': result['sources'][:10] if result['sources'] else [],
    }
    
    # 统计情感分布百分比
    total = sum(result['tone_stats'].values())
    if total > 0:
        summary['tone_percentages'] = {
            k: round(v / total * 100, 1) 
            for k, v in result['tone_stats'].items()
        }
    
    return summary


def search_reports(keyword: str, file_path: Optional[str] = None, 
                   fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    在报告中搜索关键词
    
    Args:
        keyword: 搜索关键词
        file_path: CSV文件路径，如果不提供则使用最新报告
        fields: 要搜索的字段列表，默认搜索所有字段
        
    Returns:
        符合条件的记录列表
    """
    if file_path is None:
        file_path = find_latest_report()
        if file_path is None:
            return []
    
    parser = GdeltReportParser(file_path)
    parser.parse()
    return parser.search_by_keyword(keyword, fields)


def filter_by_criteria(file_path: Optional[str] = None,
                       tone: Optional[str] = None,
                       source: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    按条件筛选报告记录
    
    Args:
        file_path: CSV文件路径，如果不提供则使用最新报告
        tone: 情感类型筛选
        source: 来源筛选
        
    Returns:
        符合条件的记录列表
    """
    if file_path is None:
        file_path = find_latest_report()
        if file_path is None:
            return []
    
    parser = GdeltReportParser(file_path)
    parser.parse()
    
    results = parser.data
    
    if tone:
        results = [r for r in results if tone in r.get('Tone', '')]
    if source:
        results = [r for r in results if source.lower() in r.get('Source_Name', '').lower()]
    
    return results

