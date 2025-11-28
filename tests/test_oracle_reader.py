"""
Oracle Reader 모듈 테스트
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from readers.oracle_reader import OracleReader
from config import OracleConfig


def test_build_where_clause_yyyy_mm_dd(sample_oracle_config):
    """yyyy-mm-dd 형식 WHERE 절 생성 테스트"""
    spark_mock = Mock()
    reader = OracleReader(spark_mock, sample_oracle_config)
    
    where_clause = reader._build_where_clause("2024-01-15")
    
    assert "TRUNC(CREATED_DATE)" in where_clause
    assert "TO_DATE('2024-01-15', 'YYYY-MM-DD')" in where_clause
    assert "2024-01-15" in where_clause


def test_build_where_clause_yyyymmdd(sample_oracle_config):
    """yyyymmdd 형식 WHERE 절 생성 테스트"""
    config = OracleConfig(
        jdbc_url="jdbc:oracle:thin:@localhost:1521/ORCL",
        user="test",
        password="test",
        schema="TEST",
        table_name="TEST",
        columns=["*"],
        primary_keys=["ID"],
        date_column="CREATED_DATE",
        date_column_type="yyyymmdd"
    )
    
    spark_mock = Mock()
    reader = OracleReader(spark_mock, config)
    
    where_clause = reader._build_where_clause("2024-01-15")
    
    assert "TO_CHAR(CREATED_DATE, 'YYYYMMDD')" in where_clause
    assert "20240115" in where_clause


def test_build_where_clause_datetime(sample_oracle_config):
    """yyyymmdd HH24:mm:ss 형식 WHERE 절 생성 테스트"""
    config = OracleConfig(
        jdbc_url="jdbc:oracle:thin:@localhost:1521/ORCL",
        user="test",
        password="test",
        schema="TEST",
        table_name="TEST",
        columns=["*"],
        primary_keys=["ID"],
        date_column="CREATED_DATE",
        date_column_type="yyyymmdd HH24:mm:ss"
    )
    
    spark_mock = Mock()
    reader = OracleReader(spark_mock, config)
    
    where_clause = reader._build_where_clause("2024-01-15")
    
    assert "TO_CHAR(CREATED_DATE, 'YYYYMMDD HH24:MI:SS')" in where_clause
    assert "20240115 00:00:00" in where_clause


def test_build_where_clause_invalid_format(sample_oracle_config):
    """잘못된 날짜 형식 테스트"""
    config = OracleConfig(
        jdbc_url="jdbc:oracle:thin:@localhost:1521/ORCL",
        user="test",
        password="test",
        schema="TEST",
        table_name="TEST",
        columns=["*"],
        primary_keys=["ID"],
        date_column="CREATED_DATE",
        date_column_type="invalid_format"
    )
    
    spark_mock = Mock()
    reader = OracleReader(spark_mock, config)
    
    with pytest.raises(ValueError, match="지원하지 않는 날짜 형식"):
        reader._build_where_clause("2024-01-15")


@patch('readers.oracle_reader.SparkSession')
def test_read_from_oracle(mock_spark_session, sample_oracle_config):
    """Oracle 읽기 테스트 (모킹)"""
    # Mock DataFrame
    mock_df = Mock()
    mock_df.count.return_value = 10
    
    # Mock SparkSession
    mock_spark = Mock()
    mock_read = Mock()
    mock_format = Mock()
    mock_options = Mock()
    mock_load = Mock()
    
    mock_load.return_value = mock_df
    mock_options.return_value = mock_load
    mock_format.return_value = mock_options
    mock_read.format.return_value = mock_format
    mock_spark.read = mock_read
    
    reader = OracleReader(mock_spark, sample_oracle_config)
    
    # 읽기 실행
    result_df = reader._read_from_oracle("2024-01-15")
    
    # 결과 확인
    assert result_df == mock_df
    mock_read.format.assert_called_once_with("jdbc")
    mock_options.assert_called_once()

