"""
Janus 통합 테스트
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from janus import JanusValidator
from config import OracleConfig, HiveConfig, ValidationConfig


def test_get_target_date():
    """대상 날짜 계산 테스트"""
    from datetime import datetime, timedelta
    
    spark_mock = Mock()
    oracle_config = Mock()
    hive_config = Mock()
    validation_config = Mock()
    validation_config.date_offset_days = 2  # date_offset_days 설정
    
    validator = JanusValidator(spark_mock, oracle_config, hive_config, validation_config)
    
    target_date = validator.get_target_date()
    
    # 날짜 형식 확인
    assert len(target_date) == 10
    assert target_date.count("-") == 2
    
    # 날짜가 2일 전인지 확인
    expected_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    assert target_date == expected_date


@patch('janus.OracleReader')
@patch('janus.HiveReader')
@patch('janus.DataComparator')
@patch('janus.ReportGenerator')
@patch('janus.DifferenceSaver')
def test_run_success(
    mock_difference_saver,
    mock_report_generator,
    mock_comparator,
    mock_hive_reader,
    mock_oracle_reader,
    spark_session,
    sample_oracle_config,
    sample_hive_config,
    sample_validation_config
):
    """성공적인 검증 실행 테스트"""
    # Mock 설정
    mock_oracle_df = Mock()
    mock_oracle_df.count.return_value = 100
    
    mock_hive_df = Mock()
    mock_hive_df.count.return_value = 100
    
    mock_oracle_reader_instance = Mock()
    mock_oracle_reader_instance.read.return_value = mock_oracle_df
    mock_oracle_reader.return_value = mock_oracle_reader_instance
    
    mock_hive_reader_instance = Mock()
    mock_hive_reader_instance.read.return_value = mock_hive_df
    mock_hive_reader.return_value = mock_hive_reader_instance
    
    comparison_result = {
        "oracle_row_count": 100,
        "hive_row_count": 100,
        "oracle_only_count": 0,
        "hive_only_count": 0,
        "both_exist_count": 100,
        "common_columns": ["id", "name"],
        "oracle_only_columns": [],
        "hive_only_columns": [],
        "column_differences": [],
        "total_column_differences": 0,
    }
    
    mock_comparator_instance = Mock()
    mock_comparator_instance.compare.return_value = comparison_result
    mock_comparator.return_value = mock_comparator_instance
    
    mock_report_generator_instance = Mock()
    mock_report_generator_instance.generate.return_value = "Test Report"
    mock_report_generator.return_value = mock_report_generator_instance
    
    mock_difference_saver_instance = Mock()
    mock_difference_saver.return_value = mock_difference_saver_instance
    
    # Janus 검증 실행
    validator = JanusValidator(
        spark_session,
        sample_oracle_config,
        sample_hive_config,
        sample_validation_config
    )
    
    result = validator.run()
    
    # 결과 확인
    assert result is True
    mock_oracle_reader_instance.read.assert_called_once()
    mock_hive_reader_instance.read.assert_called_once()
    mock_comparator_instance.compare.assert_called_once()
    mock_report_generator_instance.generate.assert_called_once()


@patch('janus.OracleReader')
@patch('janus.HiveReader')
@patch('janus.DataComparator')
@patch('janus.ReportGenerator')
def test_run_with_differences(
    mock_report_generator,
    mock_comparator,
    mock_hive_reader,
    mock_oracle_reader,
    spark_session,
    sample_oracle_config,
    sample_hive_config,
    sample_validation_config
):
    """차이점이 있는 경우 테스트"""
    # Mock 설정
    mock_oracle_df = Mock()
    mock_hive_df = Mock()
    
    mock_oracle_reader_instance = Mock()
    mock_oracle_reader_instance.read.return_value = mock_oracle_df
    mock_oracle_reader.return_value = mock_oracle_reader_instance
    
    mock_hive_reader_instance = Mock()
    mock_hive_reader_instance.read.return_value = mock_hive_df
    mock_hive_reader.return_value = mock_hive_reader_instance
    
    comparison_result = {
        "oracle_row_count": 100,
        "hive_row_count": 95,
        "oracle_only_count": 5,
        "hive_only_count": 0,
        "both_exist_count": 95,
        "common_columns": ["id", "name"],
        "oracle_only_columns": [],
        "hive_only_columns": [],
        "column_differences": [],
        "total_column_differences": 0,
    }
    
    mock_comparator_instance = Mock()
    mock_comparator_instance.compare.return_value = comparison_result
    mock_comparator.return_value = mock_comparator_instance
    
    mock_report_generator_instance = Mock()
    mock_report_generator_instance.generate.return_value = "Test Report"
    mock_report_generator.return_value = mock_report_generator_instance
    
    # Janus 검증 실행
    validator = JanusValidator(
        spark_session,
        sample_oracle_config,
        sample_hive_config,
        sample_validation_config
    )
    
    result = validator.run()
    
    # 결과 확인 (차이점이 있으므로 False)
    assert result is False

