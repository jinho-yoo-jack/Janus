"""
Hive Reader 모듈 테스트
"""

import pytest
from unittest.mock import Mock, patch
from readers.hive_reader import HiveReader
from config import HiveConfig


def test_convert_date_format_yyyy_mm_dd(sample_hive_config):
    """yyyy-mm-dd 형식 날짜 변환 테스트"""
    spark_mock = Mock()
    reader = HiveReader(spark_mock, sample_hive_config)
    
    result = reader._convert_date_format("2024-01-15")
    
    assert result == "2024-01-15"


def test_convert_date_format_yyyymmdd(sample_hive_config):
    """yyyymmdd 형식 날짜 변환 테스트"""
    config = HiveConfig(
        database="test_db",
        table_name="test_table",
        date_column="dt",
        date_column_type="yyyymmdd"
    )
    
    spark_mock = Mock()
    reader = HiveReader(spark_mock, config)
    
    result = reader._convert_date_format("2024-01-15")
    
    assert result == "20240115"


def test_convert_date_format_datetime(sample_hive_config):
    """yyyymmdd HH24:mm:ss 형식 날짜 변환 테스트"""
    config = HiveConfig(
        database="test_db",
        table_name="test_table",
        date_column="dt",
        date_column_type="yyyymmdd HH24:mm:ss"
    )
    
    spark_mock = Mock()
    reader = HiveReader(spark_mock, config)
    
    result = reader._convert_date_format("2024-01-15")
    
    assert result == "20240115 00:00:00"


def test_convert_date_format_invalid(sample_hive_config):
    """잘못된 날짜 형식 테스트"""
    config = HiveConfig(
        database="test_db",
        table_name="test_table",
        date_column="dt",
        date_column_type="invalid_format"
    )
    
    spark_mock = Mock()
    reader = HiveReader(spark_mock, config)
    
    with pytest.raises(ValueError, match="지원하지 않는 날짜 형식"):
        reader._convert_date_format("2024-01-15")


@patch('readers.hive_reader.SparkSession')
def test_read_from_hive(mock_spark_session, sample_hive_config):
    """Hive 읽기 테스트 (모킹)"""
    # Mock DataFrame
    mock_df = Mock()
    mock_df.count.return_value = 5
    
    # Mock SparkSession
    mock_spark = Mock()
    mock_sql = Mock()
    mock_sql.return_value = mock_df
    mock_spark.sql = mock_sql
    
    reader = HiveReader(mock_spark, sample_hive_config)
    
    # 읽기 실행
    result_df = reader._read_from_hive("2024-01-15")
    
    # 결과 확인
    assert result_df == mock_df
    mock_sql.assert_called_once()
    assert "dt = '2024-01-15'" in mock_sql.call_args[0][0]

