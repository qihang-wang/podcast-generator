"""
GDELT Query Service 单元测试
测试 GDELTQueryService 的地理位置查询方法

⚠️ 成本安全说明：
本测试文件使用完全 Mock 的方式测试 GDELT 查询功能，不会产生任何 BigQuery 成本：
- 所有测试使用 @patch 装饰器 Mock BigQuery 客户端
- 不会建立真实的网络连接
- 不会扫描任何实际数据
- total_bytes_processed 统计为手动设置的模拟值

运行这些测试完全免费且安全。
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from podcast_generator.gdelt.query_service import GDELTQueryService
from podcast_generator.gdelt.gdelt_event import EventQueryBuilder
from podcast_generator.gdelt.model import EventModel


class TestGDELTQueryService(unittest.TestCase):
    """测试 GDELTQueryService 类"""
    
    def setUp(self):
        """测试前准备"""
        # 使用 mock 避免实际连接 BigQuery
        self.mock_config = Mock()
        self.mock_config.project_id = "test-project"
        self.mock_config.setup_credentials = Mock(return_value=True)
    
    @patch('podcast_generator.gdelt.gdelt_event.bigquery')
    def test_query_events_by_location_default_params(self, mock_bigquery):
        """测试默认参数调用"""
        # Mock BigQuery 返回空 DataFrame
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_query_job = Mock()
        mock_query_job.result.return_value.to_dataframe.return_value = pd.DataFrame()
        mock_query_job.total_bytes_processed = 0
        mock_client.query.return_value = mock_query_job
        
        service = GDELTQueryService(config=self.mock_config)
        result = service.query_events_by_location(print_progress=False)
        
        # 验证返回空列表
        self.assertEqual(result, [])
        # 验证 BigQuery 被调用
        mock_client.query.assert_called_once()
    
    @patch('podcast_generator.gdelt.gdelt_event.bigquery')
    def test_query_events_by_location_with_country_code(self, mock_bigquery):
        """测试按国家代码查询"""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_query_job = Mock()
        mock_query_job.result.return_value.to_dataframe.return_value = pd.DataFrame()
        mock_query_job.total_bytes_processed = 1000
        mock_client.query.return_value = mock_query_job
        
        service = GDELTQueryService(config=self.mock_config)
        service.query_events_by_location(country_code="CH", print_progress=False)
        
        # 获取实际调用的 SQL
        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        
        # 验证 SQL 包含国家代码条件
        self.assertIn("ActionGeo_CountryCode IN ('CH')", sql)
    
    @patch('podcast_generator.gdelt.gdelt_event.bigquery')
    def test_query_events_by_location_with_location_name(self, mock_bigquery):
        """测试按地点名称查询"""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_query_job = Mock()
        mock_query_job.result.return_value.to_dataframe.return_value = pd.DataFrame()
        mock_query_job.total_bytes_processed = 1000
        mock_client.query.return_value = mock_query_job
        
        service = GDELTQueryService(config=self.mock_config)
        service.query_events_by_location(location_name="Beijing", print_progress=False)
        
        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        
        # 验证 SQL 包含地点名称条件
        self.assertIn("ActionGeo_FullName LIKE '%Beijing%'", sql)
    
    @patch('podcast_generator.gdelt.gdelt_event.bigquery')
    def test_query_events_by_location_geo_filtering(self, mock_bigquery):
        """测试地理类型过滤"""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_query_job = Mock()
        mock_query_job.result.return_value.to_dataframe.return_value = pd.DataFrame()
        mock_query_job.total_bytes_processed = 0
        mock_client.query.return_value = mock_query_job
        
        service = GDELTQueryService(config=self.mock_config)
        # 使用默认参数，应该包含 geo_types=[3, 4] 和 require_feature_id=True
        service.query_events_by_location(print_progress=False)
        
        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        
        # 验证默认地理过滤条件
        self.assertIn("ActionGeo_Type IN (3, 4)", sql)
        self.assertIn("ActionGeo_FeatureID IS NOT NULL", sql)
    
    @patch('podcast_generator.gdelt.gdelt_event.bigquery')
    def test_query_events_by_location_custom_geo_types(self, mock_bigquery):
        """测试自定义地理类型"""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_query_job = Mock()
        mock_query_job.result.return_value.to_dataframe.return_value = pd.DataFrame()
        mock_query_job.total_bytes_processed = 0
        mock_client.query.return_value = mock_query_job
        
        service = GDELTQueryService(config=self.mock_config)
        service.query_events_by_location(geo_types=[4], require_feature_id=False, print_progress=False)
        
        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        
        # 验证自定义地理类型
        self.assertIn("ActionGeo_Type IN (4)", sql)
        # 不要求 FeatureID
        self.assertNotIn("ActionGeo_FeatureID IS NOT NULL", sql)
    
    @patch('podcast_generator.gdelt.gdelt_event.bigquery')
    def test_query_events_by_location_uses_partitioned_table(self, mock_bigquery):
        """测试使用分区表以控制成本"""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_query_job = Mock()
        mock_query_job.result.return_value.to_dataframe.return_value = pd.DataFrame()
        mock_query_job.total_bytes_processed = 0
        mock_client.query.return_value = mock_query_job
        
        service = GDELTQueryService(config=self.mock_config)
        service.query_events_by_location(print_progress=False)
        
        call_args = mock_client.query.call_args
        sql = call_args[0][0]
        
        # 验证使用分区表
        self.assertIn("events_partitioned", sql)
        # 验证使用分区时间过滤
        self.assertIn("_PARTITIONTIME", sql)
    
    @patch('podcast_generator.gdelt.gdelt_event.bigquery')
    def test_query_events_by_location_returns_models(self, mock_bigquery):
        """测试返回 EventModel 对象列表"""
        # 模拟返回一条数据
        mock_df = pd.DataFrame([{
            'GLOBALEVENTID': 12345,
            'SQLDATE': 20241223,
            'Actor1Code': 'CHN',
            'Actor1Name': 'CHINA',
            'Actor1CountryCode': 'CHN',
            'Actor1Type1Code': 'GOV',
            'Actor2Code': 'USA',
            'Actor2Name': 'UNITED STATES',
            'Actor2CountryCode': 'USA',
            'Actor2Type1Code': 'GOV',
            'EventCode': '010',
            'EventBaseCode': '01',
            'EventRootCode': '01',
            'QuadClass': 1,
            'GoldsteinScale': 3.4,
            'NumMentions': 10,
            'NumSources': 5,
            'NumArticles': 3,
            'AvgTone': 2.5,
            'ActionGeo_Type': 4,
            'ActionGeo_FullName': 'Beijing, China',
            'ActionGeo_CountryCode': 'CH',
            'ActionGeo_ADM1Code': 'CH11',
            'ActionGeo_Lat': 39.9,
            'ActionGeo_Long': 116.4,
            'ActionGeo_FeatureID': '-1234567',
            'SOURCEURL': 'https://example.com/news',
            'DATEADDED': 20241223150000
        }])
        
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_query_job = Mock()
        mock_query_job.result.return_value.to_dataframe.return_value = mock_df
        mock_query_job.total_bytes_processed = 1024
        mock_client.query.return_value = mock_query_job
        
        service = GDELTQueryService(config=self.mock_config)
        result = service.query_events_by_location(print_progress=False)
        
        # 验证返回 EventModel 列表
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], EventModel)
        self.assertEqual(result[0].global_event_id, 12345)
        self.assertEqual(result[0].action_geo.full_name, 'Beijing, China')
        self.assertEqual(result[0].actor1.name, 'CHINA')


class TestEventQueryBuilder(unittest.TestCase):
    """测试 EventQueryBuilder 的地理过滤功能"""
    
    def test_set_geo_types(self):
        """测试设置地理类型"""
        builder = EventQueryBuilder()
        builder.set_geo_types([3, 4])
        sql = builder.build()
        
        self.assertIn("ActionGeo_Type IN (3, 4)", sql)
    
    def test_set_require_feature_id(self):
        """测试要求 FeatureID"""
        builder = EventQueryBuilder()
        builder.set_require_feature_id(True)
        sql = builder.build()
        
        self.assertIn("ActionGeo_FeatureID IS NOT NULL", sql)
    
    def test_set_location_name(self):
        """测试设置地点名称"""
        builder = EventQueryBuilder()
        builder.set_location_name("Shanghai")
        sql = builder.build()
        
        self.assertIn("ActionGeo_FullName LIKE '%Shanghai%'", sql)
    
    def test_set_country_codes(self):
        """测试设置国家代码"""
        builder = EventQueryBuilder()
        builder.set_country_codes(["CH", "US"])
        sql = builder.build()
        
        self.assertIn("ActionGeo_CountryCode IN ('CH', 'US')", sql)
    
    def test_default_time_window(self):
        """测试默认时间窗口"""
        builder = EventQueryBuilder()
        sql = builder.build()
        
        # 默认 24 小时
        self.assertIn("INTERVAL 24 HOUR", sql)
        self.assertIn("_PARTITIONTIME", sql)


if __name__ == '__main__':
    unittest.main()
