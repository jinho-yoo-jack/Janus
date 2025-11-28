"""
리포트 생성 모듈 테스트
"""

import pytest
import os
import tempfile
from reporting.report_generator import ReportGenerator
from config import OracleConfig, HiveConfig


def test_generate_report(sample_oracle_config, sample_hive_config):
    """리포트 생성 테스트"""
    generator = ReportGenerator(sample_oracle_config, sample_hive_config)
    
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
    
    report = generator.generate(comparison_result, "2024-01-15")
    
    # 리포트 내용 확인
    assert "Oracle vs Hive 테이블 데이터 비교 리포트" in report
    assert "2024-01-15" in report
    assert "Oracle 레코드 수: 100" in report
    assert "Hive 레코드 수: 100" in report
    assert "✅ 일치" in report or "❌ 차이점 발견" in report


def test_generate_report_with_differences(sample_oracle_config, sample_hive_config):
    """차이점이 있는 리포트 생성 테스트"""
    generator = ReportGenerator(sample_oracle_config, sample_hive_config)
    
    comparison_result = {
        "oracle_row_count": 100,
        "hive_row_count": 95,
        "oracle_only_count": 5,
        "hive_only_count": 0,
        "both_exist_count": 95,
        "common_columns": ["id", "name"],
        "oracle_only_columns": [],
        "hive_only_columns": [],
        "column_differences": [{"column": "name", "count": 3}],
        "total_column_differences": 1,
    }
    
    report = generator.generate(comparison_result, "2024-01-15")
    
    # 리포트 내용 확인
    assert "❌ 차이점 발견" in report
    assert "Oracle에만 존재: 5" in report
    assert "name: 3개 레코드 차이" in report


def test_save_report(sample_oracle_config, sample_hive_config):
    """리포트 저장 테스트"""
    generator = ReportGenerator(sample_oracle_config, sample_hive_config)
    
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
    
    report = generator.generate(comparison_result, "2024-01-15")
    
    # 임시 디렉토리에 저장
    with tempfile.TemporaryDirectory() as tmpdir:
        generator.save(report, "2024-01-15", tmpdir)
        
        # 파일이 생성되었는지 확인
        expected_file = os.path.join(tmpdir, f"validation_report_{sample_oracle_config.table_name}_2024-01-15.txt")
        assert os.path.exists(expected_file)
        
        # 파일 내용 확인
        with open(expected_file, "r", encoding="utf-8") as f:
            content = f.read()
            assert "Oracle vs Hive 테이블 데이터 비교 리포트" in content

