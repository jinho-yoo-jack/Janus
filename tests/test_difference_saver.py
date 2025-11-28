"""
차이점 저장 모듈 테스트
"""

import pytest
from unittest.mock import Mock, patch
from reporting.difference_saver import DifferenceSaver
from config import OracleConfig


def test_save_oracle_only_records(sample_oracle_config):
    """Oracle 전용 레코드 저장 테스트"""
    saver = DifferenceSaver(sample_oracle_config)
    
    # Mock DataFrame
    mock_df = Mock()
    mock_df.write = Mock()
    mock_mode = Mock()
    mock_parquet = Mock()
    mock_df.write.mode.return_value = mock_parquet
    
    comparison_result = {
        "oracle_only_count": 5,
        "hive_only_count": 0,
        "oracle_only_records": mock_df,
        "hive_only_records": Mock(),
        "different_records": None,
    }
    
    # 저장 실행
    saver.save(comparison_result, "2024-01-15", "/tmp/output")
    
    # Parquet 저장이 호출되었는지 확인
    mock_parquet.assert_called_once()
    assert "oracle_only" in str(mock_parquet.call_args)


def test_save_hive_only_records(sample_oracle_config):
    """Hive 전용 레코드 저장 테스트"""
    saver = DifferenceSaver(sample_oracle_config)
    
    # Mock DataFrame
    mock_df = Mock()
    mock_df.write = Mock()
    mock_mode = Mock()
    mock_parquet = Mock()
    mock_df.write.mode.return_value = mock_parquet
    
    comparison_result = {
        "oracle_only_count": 0,
        "hive_only_count": 3,
        "oracle_only_records": Mock(),
        "hive_only_records": mock_df,
        "different_records": None,
    }
    
    # 저장 실행
    saver.save(comparison_result, "2024-01-15", "/tmp/output")
    
    # Parquet 저장이 호출되었는지 확인
    mock_parquet.assert_called_once()
    assert "hive_only" in str(mock_parquet.call_args)


def test_save_column_differences(sample_oracle_config):
    """컬럼 차이 레코드 저장 테스트"""
    saver = DifferenceSaver(sample_oracle_config)
    
    # Mock DataFrame
    mock_df = Mock()
    mock_df.write = Mock()
    mock_mode = Mock()
    mock_parquet = Mock()
    mock_df.write.mode.return_value = mock_parquet
    
    comparison_result = {
        "oracle_only_count": 0,
        "hive_only_count": 0,
        "oracle_only_records": Mock(),
        "hive_only_records": Mock(),
        "different_records": mock_df,
    }
    
    # 저장 실행
    saver.save(comparison_result, "2024-01-15", "/tmp/output")
    
    # Parquet 저장이 호출되었는지 확인
    mock_parquet.assert_called_once()
    assert "column_differences" in str(mock_parquet.call_args)


def test_save_no_differences(sample_oracle_config):
    """차이점이 없을 때 저장 테스트"""
    saver = DifferenceSaver(sample_oracle_config)
    
    comparison_result = {
        "oracle_only_count": 0,
        "hive_only_count": 0,
        "oracle_only_records": Mock(),
        "hive_only_records": Mock(),
        "different_records": None,
    }
    
    # 저장 실행 (에러가 발생하지 않아야 함)
    saver.save(comparison_result, "2024-01-15", "/tmp/output")

