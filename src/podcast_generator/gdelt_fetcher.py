"""
GDELT BigQuery 数据获取模块
从 Google BigQuery 获取 GDELT 新闻数据
"""

import os
import pandas as pd
from datetime import datetime
from typing import Optional

# BigQuery 客户端（延迟导入以避免环境无此依赖时报错）
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None


# ================= 配置 =================

# 默认查询配置
DEFAULT_PROJECT_ID = 'gdelt-analysis-480906'

# 优化版 SQL - 使用分区表减少扫描成本
# 关键优化:
# 1. 使用 gkg_partitioned 分区表而非 gkg
# 2. 使用 _PARTITIONTIME 伪列进行分区裁剪 (Partition Pruning)
# 3. 这样 BigQuery 只扫描指定日期分区的数据，而非全表
# 4. 预计扫描量从数百GB降到几GB

DEFAULT_QUERY = """
SELECT
  GKGRECORDID,
  DATE,
  SourceCommonName,
  DocumentIdentifier AS SourceURL,
  CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64) AS AvgTone,
  V2Themes,
  V2Locations,
  V2Persons,
  V2Organizations,
  GCAM,
  Amounts,        
  Quotations,
  SocialImageEmbeds,
  SocialVideoEmbeds
FROM
  `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE
  -- 使用 _PARTITIONTIME 进行分区裁剪，只扫描今天的分区
  _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  -- 在分区内再按 DATE 字段精确过滤到最近2小时
  AND DATE >= CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)) AS INT64)
  AND (V2Themes LIKE '%ENV_CLIMATECHANGE%' OR V2Themes LIKE '%CRISIS%')
  AND ABS(CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64)) > 3
  AND Quotations IS NOT NULL
ORDER BY
  ABS(AvgTone) DESC
LIMIT 50
"""


class GDELTFetcher:
    """GDELT 数据获取器 - 从 BigQuery 获取 GDELT 新闻数据"""
    
    def __init__(self, 
                 key_path: Optional[str] = None,
                 project_id: str = DEFAULT_PROJECT_ID):
        """
        初始化 GDELT 数据获取器
        
        Args:
            key_path: Google Cloud 服务账号密钥文件路径
            project_id: Google Cloud 项目 ID
        """
        self.key_path = key_path
        self.project_id = project_id
        self.client = None
        
        if bigquery is None:
            raise ImportError("未找到 google-cloud-bigquery 库，请安装: pip install google-cloud-bigquery")
    
    def _init_client(self) -> bool:
        """
        初始化 BigQuery 客户端
        
        Returns:
            是否初始化成功
        """
        if self.client is not None:
            return True
            
        # 设置认证
        if self.key_path:
            if not os.path.exists(self.key_path):
                print(f"错误: 找不到密钥文件 {self.key_path}")
                return False
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.key_path
        
        try:
            self.client = bigquery.Client(project=self.project_id)
            return True
        except Exception as e:
            print(f"BigQuery 客户端初始化失败: {e}")
            return False
    
    def estimate_query_cost(self, query: str = DEFAULT_QUERY) -> float:
        """
        预估查询成本（扫描的数据量 GB）
        
        Args:
            query: SQL 查询语句
            
        Returns:
            预估扫描量（GB）
        """
        if not self._init_client():
            return -1
        
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            dry_run_job = self.client.query(query, job_config=job_config)
            bytes_processed = dry_run_job.total_bytes_processed
            gb_processed = bytes_processed / (1024**3)
            return gb_processed
        except Exception as e:
            print(f"预估查询成本失败: {e}")
            return -1
    
    def fetch(self, 
              query: str = DEFAULT_QUERY,
              print_progress: bool = True) -> pd.DataFrame:
        """
        执行查询并获取 GDELT 数据
        
        Args:
            query: SQL 查询语句
            print_progress: 是否打印进度信息
            
        Returns:
            包含 GDELT 数据的 DataFrame，如果失败则返回空 DataFrame
        """
        if not self._init_client():
            return pd.DataFrame()
        
        try:
            if print_progress:
                print(f"[{datetime.now()}] 开始查询 BigQuery (使用分区表优化)...")
                
                # 预估查询成本
                gb_processed = self.estimate_query_cost(query)
                if gb_processed >= 0:
                    print(f"[预估扫描量] {gb_processed:.2f} GB")
            
            # 执行实际查询
            query_job = self.client.query(query)
            results = query_job.result()
            
            df = results.to_dataframe()
            
            if print_progress:
                print(f"[{datetime.now()}] 查询完成，获取到 {len(df)} 条记录。")
            
            return df
            
        except Exception as e:
            print(f"BigQuery 查询错误: {e}")
            return pd.DataFrame()


# ================= 便捷方法 =================

def fetch_gdelt_data(key_path: Optional[str] = None,
                     project_id: str = DEFAULT_PROJECT_ID,
                     query: str = DEFAULT_QUERY) -> pd.DataFrame:
    """
    获取 GDELT 数据的便捷方法
    
    Args:
        key_path: Google Cloud 服务账号密钥文件路径
        project_id: Google Cloud 项目 ID
        query: SQL 查询语句
        
    Returns:
        包含 GDELT 数据的 DataFrame
    """
    try:
        fetcher = GDELTFetcher(key_path=key_path, project_id=project_id)
        return fetcher.fetch(query=query)
    except ImportError as e:
        print(f"错误: {e}")
        return pd.DataFrame()


def load_local_data(file_path: str) -> pd.DataFrame:
    """
    从本地 CSV 文件加载数据
    
    Args:
        file_path: CSV 文件路径
        
    Returns:
        DataFrame
    """
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        print(f"从本地文件加载数据: {file_path}, 共 {len(df)} 条记录")
        return df
    else:
        print(f"错误: 找不到数据文件 {file_path}")
        return pd.DataFrame()


def save_data(df: pd.DataFrame, file_path: str) -> bool:
    """
    保存数据到 CSV 文件
    
    Args:
        df: 要保存的 DataFrame
        file_path: 保存路径
        
    Returns:
        是否保存成功
    """
    try:
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"数据已保存至: {file_path}")
        return True
    except Exception as e:
        print(f"保存数据失败: {e}")
        return False
