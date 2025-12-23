"""
GDELT 配置模块
统一管理 BigQuery 相关配置
"""

import os
import pathlib
from typing import Optional

# ================= BigQuery 配置 =================

# 默认项目 ID
DEFAULT_PROJECT_ID = 'gdelt-analysis-480906'

# 自动检测密钥路径（与 main.py 保持一致）
_SCRIPT_DIR = pathlib.Path(__file__).parent
DEFAULT_KEY_PATH = str(_SCRIPT_DIR.parent.parent.parent / 'gdelt_config' / 'my-gdelt-key.json')


class GDELTConfig:
    """GDELT BigQuery 配置类"""
    
    def __init__(self,
                 project_id: str = DEFAULT_PROJECT_ID,
                 key_path: Optional[str] = None):
        """
        初始化配置
        
        Args:
            project_id: Google Cloud 项目 ID
            key_path: 服务账号密钥文件路径（None 则使用默认路径）
        """
        self.project_id = project_id
        # 如果未指定 key_path，使用默认路径
        self.key_path = key_path or DEFAULT_KEY_PATH
    
    def setup_credentials(self) -> bool:
        """设置认证凭据"""
        if self.key_path:
            if not os.path.exists(self.key_path):
                print(f"⚠️ 找不到密钥文件: {self.key_path}")
                print(f"   请确保文件存在或设置 GOOGLE_APPLICATION_CREDENTIALS 环境变量")
                return False
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.key_path
            print(f"✓ 使用密钥文件: {self.key_path}")
        return True
    
    @classmethod
    def from_env(cls) -> 'GDELTConfig':
        """从环境变量创建配置"""
        return cls(
            project_id=os.environ.get('GDELT_PROJECT_ID', DEFAULT_PROJECT_ID),
            key_path=os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        )


# 全局默认配置实例（自动使用默认密钥路径）
default_config = GDELTConfig()

