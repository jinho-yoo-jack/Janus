"""
설정 모듈 테스트
"""

import pytest
import yaml
import tempfile
import os
from config import TableConfig, DatabaseConfig, OracleConfig, HiveConfig, ValidationConfig


def test_database_config_from_yaml_oracle():
    """Oracle DB 접속 정보 읽기 테스트"""
    config_data = {
        "oracle": {
            "host": "localhost",
            "port": 1521,
            "service_name": "ORCL",
            "user": "test_user",
            "password": "test_password",
            "default_schema": "TEST_SCHEMA"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        config_path = f.name
    
    try:
        config = DatabaseConfig.from_yaml(config_path, "oracle")
        
        assert config.user == "test_user"
        assert config.password == "test_password"
        assert config.default_schema == "TEST_SCHEMA"
        assert "localhost" in config.jdbc_url
    finally:
        os.unlink(config_path)


def test_database_config_from_yaml_hive():
    """Hive DB 접속 정보 읽기 테스트"""
    config_data = {
        "hive": {
            "database": "test_db"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        config_path = f.name
    
    try:
        config = DatabaseConfig.from_yaml(config_path, "hive")
        
        assert config.default_database == "test_db"
    finally:
        os.unlink(config_path)


def test_table_config_from_yaml():
    """테이블 설정 파일 읽기 테스트"""
    config_data = {
        "source_db_name": "oracle",
        "target_db_name": "hive",
        "table_name": "TEST_TABLE",
        "date_column": "CREATED_DATE",
        "date_column_type": "yyyy-mm-dd",
        "exclude_columns": ["UPDATED_DATE", "VERSION"],
        "primary_keys": ["ID"]
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        config_path = f.name
    
    try:
        config = TableConfig.from_yaml(config_path)
        
        assert config.source_db_name == "oracle"
        assert config.target_db_name == "hive"
        assert config.table_name == "TEST_TABLE"
        assert config.date_column == "CREATED_DATE"
        assert config.date_column_type == "yyyy-mm-dd"
        assert config.exclude_columns == ["UPDATED_DATE", "VERSION"]
        assert config.primary_keys == ["ID"]
    finally:
        os.unlink(config_path)


def test_oracle_config_from_table_and_database_config():
    """TableConfig와 DatabaseConfig로부터 OracleConfig 생성 테스트"""
    table_config_data = {
        "source_db_name": "oracle",
        "target_db_name": "hive",
        "table_name": "TEST_TABLE",
        "date_column": "CREATED_DATE",
        "date_column_type": "yyyy-mm-dd",
        "exclude_columns": ["UPDATED_DATE"],
        "primary_keys": ["ID"],
        "oracle_schema": "TEST_SCHEMA"
    }
    
    database_config_data = {
        "oracle": {
            "host": "localhost",
            "port": 1521,
            "service_name": "ORCL",
            "user": "test_user",
            "password": "test_password",
            "default_schema": "DEFAULT_SCHEMA"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as table_f:
        yaml.dump(table_config_data, table_f)
        table_path = table_f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as db_f:
        yaml.dump(database_config_data, db_f)
        db_path = db_f.name
    
    try:
        table_config = TableConfig.from_yaml(table_path)
        db_config = DatabaseConfig.from_yaml(db_path, "oracle")
        
        oracle_config = OracleConfig.from_table_and_database_config(table_config, db_config)
        
        assert oracle_config.table_name == "TEST_TABLE"
        assert oracle_config.schema == "TEST_SCHEMA"  # table_config의 oracle_schema 사용
        assert oracle_config.user == "test_user"
        assert oracle_config.primary_keys == ["ID"]
    finally:
        os.unlink(table_path)
        os.unlink(db_path)


def test_hive_config_from_table_and_database_config():
    """TableConfig와 DatabaseConfig로부터 HiveConfig 생성 테스트"""
    table_config_data = {
        "source_db_name": "oracle",
        "target_db_name": "hive",
        "table_name": "test_table",
        "date_column": "dt",
        "date_column_type": "yyyy-mm-dd",
        "exclude_columns": ["updated_date"],
        "primary_keys": ["id"],
        "hive_database": "test_db"
    }
    
    database_config_data = {
        "hive": {
            "database": "default"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as table_f:
        yaml.dump(table_config_data, table_f)
        table_path = table_f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as db_f:
        yaml.dump(database_config_data, db_f)
        db_path = db_f.name
    
    try:
        table_config = TableConfig.from_yaml(table_path)
        db_config = DatabaseConfig.from_yaml(db_path, "hive")
        
        hive_config = HiveConfig.from_table_and_database_config(table_config, db_config)
        
        assert hive_config.table_name == "test_table"
        assert hive_config.database == "test_db"  # table_config의 hive_database 사용
        assert hive_config.date_column == "dt"
    finally:
        os.unlink(table_path)
        os.unlink(db_path)


def test_validation_config_from_yaml():
    """검증 설정 파일 읽기 테스트 (하위 호환성)"""
    config_data = {
        "validation": {
            "report_path": "/tmp/reports",
            "output_path": "/tmp/output",
            "date_offset_days": 2
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        config_path = f.name
    
    try:
        config = ValidationConfig.from_yaml(config_path)
        
        assert config.report_path == "/tmp/reports"
        assert config.output_path == "/tmp/output"
        assert config.date_offset_days == 2
    finally:
        os.unlink(config_path)


def test_validation_config_from_yaml_with_environment():
    """환경별 검증 설정 파일 읽기 테스트"""
    config_data = {
        "dev": {
            "validation": {
                "report_path": "/tmp/reports/dev",
                "output_path": "/tmp/output/dev",
                "date_offset_days": 2
            }
        },
        "prod": {
            "validation": {
                "report_path": "/tmp/reports/prod",
                "output_path": "/tmp/output/prod",
                "date_offset_days": 1
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        config_path = f.name
    
    try:
        # dev 환경 테스트
        config_dev = ValidationConfig.from_yaml(config_path, environment="dev")
        assert config_dev.report_path == "/tmp/reports/dev"
        assert config_dev.output_path == "/tmp/output/dev"
        assert config_dev.date_offset_days == 2
        
        # prod 환경 테스트
        config_prod = ValidationConfig.from_yaml(config_path, environment="prod")
        assert config_prod.report_path == "/tmp/reports/prod"
        assert config_prod.output_path == "/tmp/output/prod"
        assert config_prod.date_offset_days == 1
    finally:
        os.unlink(config_path)


def test_table_config_missing_primary_keys():
    """Primary Key 누락 테스트"""
    config_data = {
        "source_db_name": "oracle",
        "target_db_name": "hive",
        "table_name": "TEST_TABLE",
        "date_column": "CREATED_DATE",
        "date_column_type": "yyyy-mm-dd"
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        config_path = f.name
    
    try:
        with pytest.raises(ValueError, match="primary_keys가 설정되지 않았습니다"):
            TableConfig.from_yaml(config_path)
    finally:
        os.unlink(config_path)


def test_table_config_invalid_date_type():
    """잘못된 날짜 타입 테스트"""
    config_data = {
        "source_db_name": "oracle",
        "target_db_name": "hive",
        "table_name": "TEST_TABLE",
        "date_column": "CREATED_DATE",
        "date_column_type": "invalid_format",
        "primary_keys": ["ID"]
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(config_data, f)
        config_path = f.name
    
    try:
        with pytest.raises(ValueError, match="date_column_type은 다음 중 하나여야 합니다"):
            TableConfig.from_yaml(config_path)
    finally:
        os.unlink(config_path)
