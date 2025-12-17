"""
新闻合并模块
用于合并标题相似的重复新闻记录，整合多来源信息
"""

import re
from typing import List, Dict, Any


def normalize_title(title: str) -> str:
    """
    标题归一化处理，用于相似度比较
    
    - 转换为小写
    - 移除标点符号
    - 移除日期数字（如 2025 12 14）
    - 保留核心关键词
    """
    if not title:
        return ""
    
    normalized = title.lower()
    
    # 移除日期格式 (如 2025 12 14, 2025-12-14)
    normalized = re.sub(r'\b20\d{2}[\s\-/]?\d{1,2}[\s\-/]?\d{1,2}\b', '', normalized)
    
    # 移除标点符号
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    
    # 合并多个空格
    normalized = ' '.join(normalized.split())
    
    return normalized.strip()


def calculate_title_similarity(title1: str, title2: str) -> float:
    """
    计算两个标题的相似度
    
    使用 Jaccard 系数计算词集合的相似度
    返回 0.0-1.0 之间的相似度分数
    """
    if not title1 or not title2:
        return 0.0
    
    # 归一化标题
    norm1 = normalize_title(title1)
    norm2 = normalize_title(title2)
    
    if not norm1 or not norm2:
        return 0.0
    
    # 分词
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    
    # 过滤停用词
    stopwords = {'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or', 'is', 'are', 'was', 'were', 'from'}
    words1 = words1 - stopwords
    words2 = words2 - stopwords
    
    if not words1 or not words2:
        return 0.0
    
    # Jaccard 相似度
    intersection = len(words1 & words2)
    union = len(words1 | words2)
    
    return intersection / union if union > 0 else 0.0


def merge_two_records(primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
    """
    合并两条相关新闻记录
    
    Args:
        primary: 主记录（保留标题和基本信息）
        secondary: 次要记录（合并其独特内容）
    
    Returns:
        合并后的记录
    """
    merged = primary.copy()
    
    # 合并来源信息
    sources = [primary.get('Source_Name', '')]
    if secondary.get('Source_Name') and secondary['Source_Name'] not in sources:
        sources.append(secondary['Source_Name'])
    merged['Source_Name'] = ' | '.join(filter(None, sources))
    
    # 合并来源URL
    urls = [primary.get('Source_URL', '')]
    if secondary.get('Source_URL') and secondary['Source_URL'] not in urls:
        urls.append(secondary['Source_URL'])
    merged['Source_URL'] = ' ; '.join(filter(None, urls))
    
    # 合并引语（去重）
    primary_quotes = primary.get('Quotes', '') or ''
    secondary_quotes = secondary.get('Quotes', '') or ''
    if secondary_quotes and secondary_quotes != 'No quotes available':
        if primary_quotes == 'No quotes available':
            merged['Quotes'] = secondary_quotes
        else:
            # 分割引语并合并
            all_quotes = set()
            for q in primary_quotes.split('\n---\n'):
                if q.strip():
                    all_quotes.add(q.strip())
            for q in secondary_quotes.split('\n---\n'):
                if q.strip():
                    all_quotes.add(q.strip())
            merged['Quotes'] = '\n---\n'.join(list(all_quotes)[:15])  # 最多15条
    
    # 合并地点（去重）
    primary_loc = primary.get('Locations', '') or ''
    secondary_loc = secondary.get('Locations', '') or ''
    if secondary_loc and secondary_loc != 'Unknown Location':
        if primary_loc == 'Unknown Location':
            merged['Locations'] = secondary_loc
        else:
            all_locs = set(primary_loc.split(', ')) | set(secondary_loc.split(', '))
            all_locs.discard('Unknown Location')
            merged['Locations'] = ', '.join(list(all_locs)[:8])
    
    # 合并人物（去重）
    primary_persons = primary.get('Key_Persons', '') or ''
    secondary_persons = secondary.get('Key_Persons', '') or ''
    if secondary_persons and secondary_persons != 'Unknown':
        if primary_persons == 'Unknown':
            merged['Key_Persons'] = secondary_persons
        else:
            all_persons = set(primary_persons.split(', ')) | set(secondary_persons.split(', '))
            all_persons.discard('Unknown')
            merged['Key_Persons'] = ', '.join(list(all_persons)[:10])
    
    # 合并组织（去重）
    primary_orgs = primary.get('Organizations', '') or ''
    secondary_orgs = secondary.get('Organizations', '') or ''
    if secondary_orgs and secondary_orgs != 'No organizations mentioned':
        if primary_orgs == 'No organizations mentioned':
            merged['Organizations'] = secondary_orgs
        else:
            all_orgs = set(primary_orgs.split(', ')) | set(secondary_orgs.split(', '))
            all_orgs.discard('No organizations mentioned')
            merged['Organizations'] = ', '.join(list(all_orgs)[:10])
    
    # 合并主题（去重）
    primary_themes = primary.get('Themes', '') or ''
    secondary_themes = secondary.get('Themes', '') or ''
    if secondary_themes and secondary_themes != 'General':
        if primary_themes == 'General':
            merged['Themes'] = secondary_themes
        else:
            all_themes = set(primary_themes.split(', ')) | set(secondary_themes.split(', '))
            all_themes.discard('General')
            merged['Themes'] = ', '.join(list(all_themes)[:10])
    
    # 合并数据事实（去重）
    primary_facts = primary.get('Data_Facts', '') or ''
    secondary_facts = secondary.get('Data_Facts', '') or ''
    if secondary_facts and secondary_facts != 'No specific data':
        if primary_facts == 'No specific data':
            merged['Data_Facts'] = secondary_facts
        else:
            all_facts = set(primary_facts.split('; ')) | set(secondary_facts.split('; '))
            all_facts.discard('No specific data')
            merged['Data_Facts'] = '; '.join(list(all_facts)[:10])
    
    # 合并图片（去重）
    primary_imgs = primary.get('Images', '') or ''
    secondary_imgs = secondary.get('Images', '') or ''
    if secondary_imgs and secondary_imgs != 'No images':
        if primary_imgs == 'No images':
            merged['Images'] = secondary_imgs
        else:
            all_imgs = set(primary_imgs.split('; ')) | set(secondary_imgs.split('; '))
            all_imgs.discard('No images')
            merged['Images'] = '; '.join(list(all_imgs)[:8])
    
    # 合并原文摘要
    primary_summary = primary.get('Article_Summary', '') or ''
    secondary_summary = secondary.get('Article_Summary', '') or ''
    if secondary_summary and not primary_summary:
        merged['Article_Summary'] = secondary_summary
    elif secondary_summary and primary_summary:
        # 如果两边都有摘要，保留较长的
        if len(secondary_summary) > len(primary_summary):
            merged['Article_Summary'] = secondary_summary
    
    return merged


def merge_related_news(narratives: List[Dict[str, Any]], similarity_threshold: float = 0.6) -> List[Dict[str, Any]]:
    """
    合并相关的新闻记录
    
    将标题相似度超过阈值的新闻合并成一条更丰富的记录，
    保留多来源的引用、数据、图片等信息。
    
    Args:
        narratives: 新闻记录列表
        similarity_threshold: 相似度阈值（0.0-1.0），默认0.6
    
    Returns:
        合并后的新闻记录列表（去重且内容丰富）
    """
    if not narratives:
        return []
    
    # 标记每条记录是否已被合并
    merged_indices = set()
    result = []
    
    print(f"\n📊 开始合并相关新闻...")
    print(f"   原始记录数: {len(narratives)}")
    
    for i, record in enumerate(narratives):
        if i in merged_indices:
            continue
        
        # 找出所有与当前记录相似的记录
        similar_records = [record]
        similar_sources = [record.get('Source_Name', 'Unknown')]
        
        for j in range(i + 1, len(narratives)):
            if j in merged_indices:
                continue
            
            other_record = narratives[j]
            similarity = calculate_title_similarity(
                record.get('Title', ''),
                other_record.get('Title', '')
            )
            
            if similarity >= similarity_threshold:
                similar_records.append(other_record)
                similar_sources.append(other_record.get('Source_Name', 'Unknown'))
                merged_indices.add(j)
        
        # 如果有多条相似记录，进行合并
        if len(similar_records) > 1:
            merged_record = similar_records[0]
            for other in similar_records[1:]:
                merged_record = merge_two_records(merged_record, other)
            
            print(f"   🔗 合并 {len(similar_records)} 条相关新闻: {record.get('Title', '')[:50]}...")
            print(f"      来源: {', '.join(similar_sources)}")
            
            result.append(merged_record)
        else:
            result.append(record)
    
    print(f"   合并后记录数: {len(result)}")
    print(f"   减少重复: {len(narratives) - len(result)} 条")
    
    return result
