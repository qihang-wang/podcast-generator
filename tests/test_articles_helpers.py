"""
articles_helpers.py 辅助函数单元测试
测试按天缓存策略的核心逻辑
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

# 导入被测试的函数和常量
from podcast_generator.api.routes.articles_helpers import (
    get_day_range,
    datetime_to_int,
    get_days_list,
    check_day_cached,
    fetch_day_data,
    EXPECTED_ARTICLES_PER_DAY,
    CACHE_COMPLETENESS_THRESHOLD
)


class TestGetDayRange:
    """测试 get_day_range 函数"""
    
    def test_basic_date(self):
        """测试基本日期转换"""
        date = datetime(2026, 1, 22, 15, 30, 45)
        start, end = get_day_range(date)
        
        assert start == datetime(2026, 1, 22, 0, 0, 0)
        assert end == datetime(2026, 1, 22, 23, 59, 59)
    
    def test_midnight(self):
        """测试午夜时间"""
        date = datetime(2026, 1, 22, 0, 0, 0)
        start, end = get_day_range(date)
        
        assert start == datetime(2026, 1, 22, 0, 0, 0)
        assert end == datetime(2026, 1, 22, 23, 59, 59)
    
    def test_end_of_day(self):
        """测试接近午夜的时间"""
        date = datetime(2026, 1, 22, 23, 59, 59)
        start, end = get_day_range(date)
        
        assert start == datetime(2026, 1, 22, 0, 0, 0)
        assert end == datetime(2026, 1, 22, 23, 59, 59)
    
    def test_year_boundary(self):
        """测试跨年边界"""
        date = datetime(2025, 12, 31, 12, 0, 0)
        start, end = get_day_range(date)
        
        assert start == datetime(2025, 12, 31, 0, 0, 0)
        assert end == datetime(2025, 12, 31, 23, 59, 59)
    
    def test_new_year(self):
        """测试新年第一天"""
        date = datetime(2026, 1, 1, 8, 0, 0)
        start, end = get_day_range(date)
        
        assert start == datetime(2026, 1, 1, 0, 0, 0)
        assert end == datetime(2026, 1, 1, 23, 59, 59)
    
    def test_return_type(self):
        """测试返回值类型"""
        date = datetime(2026, 1, 22, 15, 30, 45)
        start, end = get_day_range(date)
        
        assert isinstance(start, datetime)
        assert isinstance(end, datetime)
    
    def test_start_before_end(self):
        """测试开始时间小于结束时间"""
        date = datetime(2026, 1, 22, 15, 30, 45)
        start, end = get_day_range(date)
        
        assert start < end


class TestDatetimeToInt:
    """测试 datetime_to_int 函数"""
    
    def test_basic_conversion(self):
        """测试基本转换"""
        dt = datetime(2026, 1, 22, 15, 30, 45)
        result = datetime_to_int(dt)
        
        assert result == 20260122153045
    
    def test_midnight(self):
        """测试午夜"""
        dt = datetime(2026, 1, 22, 0, 0, 0)
        result = datetime_to_int(dt)
        
        assert result == 20260122000000
    
    def test_end_of_day(self):
        """测试一天结束"""
        dt = datetime(2026, 1, 22, 23, 59, 59)
        result = datetime_to_int(dt)
        
        assert result == 20260122235959
    
    def test_return_type(self):
        """测试返回类型"""
        dt = datetime(2026, 1, 22, 15, 30, 45)
        result = datetime_to_int(dt)
        
        assert isinstance(result, int)


class TestGetDaysList:
    """测试 get_days_list 函数"""
    
    @patch('podcast_generator.api.routes.articles_helpers.datetime')
    def test_one_day(self, mock_datetime):
        """测试获取1天的数据（昨天）"""
        mock_datetime.now.return_value = datetime(2026, 1, 22, 15, 30, 0)
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        dates = get_days_list(1)
        
        assert len(dates) == 1
        assert dates[0].date() == datetime(2026, 1, 21).date()
    
    @patch('podcast_generator.api.routes.articles_helpers.datetime')
    def test_three_days(self, mock_datetime):
        """测试获取3天的数据"""
        mock_datetime.now.return_value = datetime(2026, 1, 22, 15, 30, 0)
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        dates = get_days_list(3)
        
        assert len(dates) == 3
        # 从最早到最新: 1/19, 1/20, 1/21
        assert dates[0].date() == datetime(2026, 1, 19).date()
        assert dates[1].date() == datetime(2026, 1, 20).date()
        assert dates[2].date() == datetime(2026, 1, 21).date()
    
    @patch('podcast_generator.api.routes.articles_helpers.datetime')
    def test_seven_days(self, mock_datetime):
        """测试获取7天的数据"""
        mock_datetime.now.return_value = datetime(2026, 1, 22, 15, 30, 0)
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        dates = get_days_list(7)
        
        assert len(dates) == 7
        # 第一天是 1/15，最后一天是 1/21
        assert dates[0].date() == datetime(2026, 1, 15).date()
        assert dates[-1].date() == datetime(2026, 1, 21).date()
    
    @patch('podcast_generator.api.routes.articles_helpers.datetime')
    def test_order_oldest_first(self, mock_datetime):
        """测试日期顺序：从最早到最新"""
        mock_datetime.now.return_value = datetime(2026, 1, 22, 15, 30, 0)
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        dates = get_days_list(3)
        
        # 验证顺序递增
        for i in range(len(dates) - 1):
            assert dates[i] < dates[i + 1]
    
    @patch('podcast_generator.api.routes.articles_helpers.datetime')
    def test_excludes_today(self, mock_datetime):
        """测试不包含今天"""
        today = datetime(2026, 1, 22, 15, 30, 0)
        mock_datetime.now.return_value = today
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        dates = get_days_list(3)
        
        # 确保今天不在列表中
        today_date = today.date()
        for date in dates:
            assert date.date() != today_date
    
    @patch('podcast_generator.api.routes.articles_helpers.datetime')
    def test_cross_month_boundary(self, mock_datetime):
        """测试跨月边界"""
        # 假设今天是 2 月 3 日，获取 5 天
        mock_datetime.now.return_value = datetime(2026, 2, 3, 10, 0, 0)
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        dates = get_days_list(5)
        
        # 应该包含 1/29, 1/30, 1/31, 2/1, 2/2
        assert dates[0].date() == datetime(2026, 1, 29).date()
        assert dates[-1].date() == datetime(2026, 2, 2).date()


class TestCheckDayCached:
    """测试 check_day_cached 函数（含完整性检查）"""
    
    def test_full_cache_returns_true(self):
        """测试 100% 数据时返回 True"""
        mock_repo = Mock()
        mock_repo.query_by_country_and_time.return_value = {
            "data": [],
            "total": 100,  # 100% of 100
            "page": 1,
            "page_size": 1
        }
        
        date = datetime(2026, 1, 21, 12, 0, 0)
        result = check_day_cached(mock_repo, "CH", date)
        
        assert result is True
    
    def test_80_percent_cache_returns_true(self):
        """测试 80% 数据时返回 True（刚好达到阈值）"""
        mock_repo = Mock()
        mock_repo.query_by_country_and_time.return_value = {
            "data": [],
            "total": 80,  # 80% of 100
            "page": 1,
            "page_size": 1
        }
        
        date = datetime(2026, 1, 21, 12, 0, 0)
        result = check_day_cached(mock_repo, "CH", date)
        
        assert result is True
    
    def test_79_percent_cache_returns_false(self):
        """测试 79% 数据时返回 False（未达到阈值）"""
        mock_repo = Mock()
        mock_repo.query_by_country_and_time.return_value = {
            "data": [],
            "total": 79,  # 79% of 100
            "page": 1,
            "page_size": 1
        }
        
        date = datetime(2026, 1, 21, 12, 0, 0)
        result = check_day_cached(mock_repo, "CH", date)
        
        assert result is False
    
    def test_empty_cache_returns_false(self):
        """测试无数据时返回 False"""
        mock_repo = Mock()
        mock_repo.query_by_country_and_time.return_value = {
            "data": [],
            "total": 0,
            "page": 1,
            "page_size": 1
        }
        
        date = datetime(2026, 1, 21, 12, 0, 0)
        result = check_day_cached(mock_repo, "CH", date)
        
        assert result is False
    
    def test_partial_cache_returns_false(self):
        """测试少量数据时返回 False（如只有5条）"""
        mock_repo = Mock()
        mock_repo.query_by_country_and_time.return_value = {
            "data": [],
            "total": 5,  # 5% of 100
            "page": 1,
            "page_size": 1
        }
        
        date = datetime(2026, 1, 21, 12, 0, 0)
        result = check_day_cached(mock_repo, "CH", date)
        
        assert result is False
    
    def test_custom_expected_count(self):
        """测试自定义期望数量"""
        mock_repo = Mock()
        mock_repo.query_by_country_and_time.return_value = {
            "data": [],
            "total": 40,  # 40 articles
            "page": 1,
            "page_size": 1
        }
        
        date = datetime(2026, 1, 21, 12, 0, 0)
        
        # 期望 50 条，阈值 80%，即需要 40 条
        result = check_day_cached(mock_repo, "CH", date, expected_count=50, threshold=0.8)
        assert result is True
        
        # 期望 50 条，阈值 90%，即需要 45 条
        result = check_day_cached(mock_repo, "CH", date, expected_count=50, threshold=0.9)
        assert result is False
    
    def test_custom_threshold(self):
        """测试自定义阈值"""
        mock_repo = Mock()
        mock_repo.query_by_country_and_time.return_value = {
            "data": [],
            "total": 50,  # 50% of 100
            "page": 1,
            "page_size": 1
        }
        
        date = datetime(2026, 1, 21, 12, 0, 0)
        
        # 阈值 50%，应该 True
        result = check_day_cached(mock_repo, "CH", date, threshold=0.5)
        assert result is True
        
        # 阈值 60%，应该 False
        result = check_day_cached(mock_repo, "CH", date, threshold=0.6)
        assert result is False
    
    def test_calls_repo_with_correct_params(self):
        """测试调用 repo 时传入正确的参数"""
        mock_repo = Mock()
        mock_repo.query_by_country_and_time.return_value = {
            "data": [],
            "total": 100,
            "page": 1,
            "page_size": 1
        }
        
        date = datetime(2026, 1, 21, 15, 30, 0)
        check_day_cached(mock_repo, "US", date)
        
        # 验证调用参数
        mock_repo.query_by_country_and_time.assert_called_once_with(
            "US",
            20260121000000,  # start_time
            20260121235959,  # end_time
            page=1,
            page_size=1
        )


class TestCacheConstants:
    """测试缓存配置常量"""
    
    def test_expected_articles_per_day(self):
        """测试默认期望文章数量"""
        assert EXPECTED_ARTICLES_PER_DAY == 100
    
    def test_cache_completeness_threshold(self):
        """测试默认完整性阈值"""
        assert CACHE_COMPLETENESS_THRESHOLD == 0.8


class TestFetchDayData:
    """测试 fetch_day_data 函数"""
    
    @patch('podcast_generator.gdelt.data_fetcher.fetch_gkg_data')
    def test_calls_fetch_gkg_data_with_time_range(self, mock_fetch):
        """测试使用精确时间范围调用 fetch_gkg_data"""
        date = datetime(2026, 1, 21, 0, 0, 0)
        fetch_day_data("CH", date)
        
        mock_fetch.assert_called_once()
        
        # 验证参数
        call_kwargs = mock_fetch.call_args[1]
        assert call_kwargs["country_code"] == "CH"
        assert call_kwargs["limit"] == EXPECTED_ARTICLES_PER_DAY
        
        # 验证时间范围是精确的一天
        assert call_kwargs["start_time"] == datetime(2026, 1, 21, 0, 0, 0)
        assert call_kwargs["end_time"] == datetime(2026, 1, 21, 23, 59, 59)
    
    @patch('podcast_generator.gdelt.data_fetcher.fetch_gkg_data')
    def test_custom_limit(self, mock_fetch):
        """测试自定义 limit 参数"""
        date = datetime(2026, 1, 21, 0, 0, 0)
        fetch_day_data("CH", date, limit=50)
        
        call_kwargs = mock_fetch.call_args[1]
        assert call_kwargs["limit"] == 50
    
    @patch('podcast_generator.gdelt.data_fetcher.fetch_gkg_data')
    def test_no_hours_back_parameter(self, mock_fetch):
        """测试不再使用 hours_back 参数"""
        date = datetime(2026, 1, 21, 0, 0, 0)
        fetch_day_data("CH", date)
        
        call_kwargs = mock_fetch.call_args[1]
        
        # 应该使用 start_time/end_time 而不是 hours_back
        assert "start_time" in call_kwargs
        assert "end_time" in call_kwargs
        assert call_kwargs.get("hours_back") is None


class TestIntegration:
    """集成测试：验证函数之间的协作"""
    
    @patch('podcast_generator.api.routes.articles_helpers.datetime')
    def test_get_days_list_with_get_day_range(self, mock_datetime):
        """测试 get_days_list 和 get_day_range 的配合"""
        mock_datetime.now.return_value = datetime(2026, 1, 22, 15, 30, 0)
        mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)
        
        dates = get_days_list(3)
        
        for date in dates:
            start, end = get_day_range(date)
            
            # 验证返回 datetime 类型
            assert isinstance(start, datetime)
            assert isinstance(end, datetime)
            
            # 验证同一天
            assert start.date() == end.date()
            
            # 验证开始是 00:00:00，结束是 23:59:59
            assert start.hour == 0 and start.minute == 0 and start.second == 0
            assert end.hour == 23 and end.minute == 59 and end.second == 59
    
    def test_datetime_to_int_with_day_range(self):
        """测试 datetime_to_int 与 get_day_range 的配合"""
        date = datetime(2026, 1, 21, 12, 0, 0)
        start, end = get_day_range(date)
        
        start_int = datetime_to_int(start)
        end_int = datetime_to_int(end)
        
        # 验证整数格式正确
        assert start_int == 20260121000000
        assert end_int == 20260121235959


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
