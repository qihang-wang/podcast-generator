"""
GDELT 配置模块
统一管理 BigQuery 相关配置
"""

import os
from typing import Optional

# ================= BigQuery 配置 =================

# 默认项目 ID
DEFAULT_PROJECT_ID = 'gdelt-analysis-480906'

# BigQuery 表名
EVENTS_TABLE = 'gdelt-bq.gdeltv2.events_partitioned'
MENTIONS_TABLE = 'gdelt-bq.gdeltv2.mentionsCc'
GKG_TABLE = 'gdelt-bq.gdeltv2.gkg_partitioned'


class GDELTConfig:
    """GDELT BigQuery 配置类"""
    
    def __init__(self,
                 project_id: str = DEFAULT_PROJECT_ID,
                 key_path: Optional[str] = None):
        """
        初始化配置
        
        Args:
            project_id: Google Cloud 项目 ID
            key_path: 服务账号密钥文件路径
        """
        self.project_id = project_id
        self.key_path = key_path
    
    def setup_credentials(self) -> bool:
        """设置认证凭据"""
        if self.key_path:
            if not os.path.exists(self.key_path):
                print(f"错误: 找不到密钥文件 {self.key_path}")
                return False
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.key_path
        return True
    
    @classmethod
    def from_env(cls) -> 'GDELTConfig':
        """从环境变量创建配置"""
        return cls(
            project_id=os.environ.get('GDELT_PROJECT_ID', DEFAULT_PROJECT_ID),
            key_path=os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        )


# 全局默认配置实例
default_config = GDELTConfig()
