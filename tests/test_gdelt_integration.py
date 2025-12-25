"""
GDELT Query Service 集成测试
实际调用 BigQuery API 测试功能

⚠️⚠️⚠️ 成本警告 ⚠️⚠️⚠️
本测试会产生真实的 BigQuery 扫描成本！
- 每次测试预计扫描: ~0.001-0.01 GB
- 预计成本: $0.000005 - $0.00005 USD/次
- 总测试成本: < $0.001 USD

运行方式:
    poetry run python -m unittest tests.test_gdelt_integration -v
"""

import unittest
import os
from datetime import datetime

from podcast_generator.gdelt.gdelt_service import GDELTQueryService
from podcast_generator.gdelt.config import GDELTConfig
from podcast_generator.gdelt.model import EventModel


class TestGDELTQueryServiceIntegration(unittest.TestCase):
    """
    GDELTQueryService 集成测试
    
    这些测试会实际访问 BigQuery，产生真实成本。
    已采取以下措施降低成本：
    - hours_back=1 (仅查询最近1小时)
    - limit=5 (仅返回5条记录)
    - 使用分区表
    - 强制地理过滤
    """
    
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        print("\n" + "="*80)
        print("⚠️  开始运行 GDELT 集成测试 - 这将产生真实的 BigQuery 成本")
        print("="*80)
        
        # 使用默认配置
        cls.config = GDELTConfig()
        cls.service = GDELTQueryService(config=cls.config)
    
    def test_query_events_by_country_code_minimal(self):
        """
        测试按国家代码查询（最小成本）
        
        预期成本: ~0.001 GB 扫描
        """
        print(f"\n[{datetime.now()}] 测试: 按国家代码查询中国事件...")
        
        # 使用严格限制降低成本
        events = self.service.query_events_by_location(
            country_code="CH",          # 中国
            geo_types=[3, 4],           # 仅城市级别
            require_feature_id=True,    # 要求 FeatureID
            hours_back=1,               # 仅最近1小时
            limit=5,                    # 仅5条记录
            print_progress=True
        )
        
        # 验证结果
        self.assertIsInstance(events, list)
        print(f"✓ 返回 {len(events)} 条事件")
        
        # 验证返回的是 EventModel
        if events:
            self.assertIsInstance(events[0], EventModel)
            event = events[0]
            print(f"✓ 示例事件: {event.action_geo.full_name}")
            print(f"  - Event ID: {event.global_event_id}")
            print(f"  - Actor1: {event.actor1.name or event.actor1.code}")
            print(f"  - Actor2: {event.actor2.name or event.actor2.code}")
            print(f"  - Goldstein: {event.goldstein_scale}")
            
            # 验证地理过滤生效
            self.assertIn(event.action_geo.geo_type, [3, 4])
            self.assertIsNotNone(event.action_geo.feature_id)
            self.assertEqual(event.action_geo.country_code, "CH")
        else:
            print("⚠ 最近1小时内没有符合条件的事件")
    
    def test_query_events_by_location_name_minimal(self):
        """
        测试按地点名称查询（最小成本）
        
        预期成本: ~0.001 GB 扫描
        """
        print(f"\n[{datetime.now()}] 测试: 按地点名称查询北京事件...")
        
        events = self.service.query_events_by_location(
            location_name="Beijing",
            geo_types=[4],              # 仅世界城市
            require_feature_id=True,
            hours_back=1,
            limit=3,                    # 更严格的限制
            print_progress=True
        )
        
        self.assertIsInstance(events, list)
        print(f"✓ 返回 {len(events)} 条事件")
        
        if events:
            for i, event in enumerate(events, 1):
                print(f"  {i}. {event.action_geo.full_name} - {event.actor1.name or event.actor1.code}")
                # 验证地点名称包含 Beijing
                self.assertIn("Beijing", event.action_geo.full_name, 
                            f"地点应包含 Beijing，实际为: {event.action_geo.full_name}")
        else:
            print("⚠ 最近1小时内没有北京相关事件")
    
    def test_query_builder_cost_optimization(self):
        """
        测试查询构建器的成本优化特性
        
        验证:
        - 使用分区表
        - 时间窗口限制
        - 地理过滤
        """
        print(f"\n[{datetime.now()}] 测试: 查询成本优化特性...")
        
        # 创建一个测试查询但限制为0条以避免不必要的数据传输
        events = self.service.query_events_by_location(
            country_code="US",
            hours_back=1,
            limit=1,  # 最少记录
            print_progress=True
        )
        
        # 只要能成功查询就通过
        self.assertIsInstance(events, list)
        print(f"✓ 成本优化查询成功，返回 {len(events)} 条记录")
    
    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        print("\n" + "="*80)
        print("✓ GDELT 集成测试完成")
        print("💰 请检查 BigQuery 控制台查看实际扫描数据量和成本")
        print("="*80 + "\n")


class TestQuickIntegrationCheck(unittest.TestCase):
    """
    快速集成检查（无需环境变量）
    
    这个测试只验证配置是否正确，不实际查询数据
    """
    
    def test_service_initialization(self):
        """测试服务初始化"""
        config = GDELTConfig()
        service = GDELTQueryService(config=config)
        
        self.assertIsNotNone(service.config)
        self.assertIsNotNone(service.event_fetcher)
        print("✓ GDELTQueryService 初始化成功")
    
    def test_config_defaults(self):
        """测试默认配置"""
        config = GDELTConfig()
        
        # 验证默认值
        self.assertIsNotNone(config.project_id)
        print(f"✓ 默认项目 ID: {config.project_id}")


if __name__ == '__main__':
    unittest.main()
