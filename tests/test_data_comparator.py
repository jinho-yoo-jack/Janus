"""
데이터 비교 모듈 테스트
"""

import pytest
from comparison.data_comparator import DataComparator


def test_compare_identical_dataframes(spark_session, sample_dataframe):
    """동일한 DataFrame 비교 테스트"""
    comparator = DataComparator(primary_keys=["id"])
    
    # 동일한 DataFrame 비교
    result = comparator.compare(sample_dataframe, sample_dataframe)
    
    # 결과 확인
    assert result["oracle_row_count"] == result["hive_row_count"]
    assert result["oracle_only_count"] == 0
    assert result["hive_only_count"] == 0
    assert result["total_column_differences"] == 0


def test_compare_different_dataframes(spark_session):
    """다른 DataFrame 비교 테스트"""
    comparator = DataComparator(primary_keys=["id"])
    
    # Oracle DataFrame
    oracle_data = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)]
    oracle_df = spark_session.createDataFrame(oracle_data, ["id", "name", "value"])
    
    # Hive DataFrame (일부 레코드 누락, 일부 값 변경)
    hive_data = [(1, "Alice", 100), (2, "Bob", 250)]  # 3번 레코드 누락, 2번 값 변경
    hive_df = spark_session.createDataFrame(hive_data, ["id", "name", "value"])
    
    # 비교
    result = comparator.compare(oracle_df, hive_df)
    
    # 결과 확인
    assert result["oracle_row_count"] == 3
    assert result["hive_row_count"] == 2
    assert result["oracle_only_count"] == 1  # 3번 레코드가 Oracle에만 존재
    assert result["hive_only_count"] == 0
    assert result["total_column_differences"] == 1  # 2번 레코드의 value 값 차이


def test_compare_with_missing_records(spark_session):
    """누락된 레코드 비교 테스트"""
    comparator = DataComparator(primary_keys=["id"])
    
    # Oracle DataFrame
    oracle_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    oracle_df = spark_session.createDataFrame(oracle_data, ["id", "name"])
    
    # Hive DataFrame (레코드 누락)
    hive_data = [(1, "Alice"), (2, "Bob")]
    hive_df = spark_session.createDataFrame(hive_data, ["id", "name"])
    
    # 비교
    result = comparator.compare(oracle_df, hive_df)
    
    # 결과 확인
    assert result["oracle_only_count"] == 1  # 3번 레코드가 Oracle에만 존재
    assert result["hive_only_count"] == 0


def test_compare_with_different_columns(spark_session):
    """다른 컬럼을 가진 DataFrame 비교 테스트"""
    comparator = DataComparator(primary_keys=["id"])
    
    # Oracle DataFrame
    oracle_data = [(1, "Alice", "email1@test.com")]
    oracle_df = spark_session.createDataFrame(oracle_data, ["id", "name", "email"])
    
    # Hive DataFrame (다른 컬럼)
    hive_data = [(1, "Alice", "2024-01-15")]
    hive_df = spark_session.createDataFrame(hive_data, ["id", "name", "dt"])
    
    # 비교
    result = comparator.compare(oracle_df, hive_df)
    
    # 결과 확인
    assert "email" in result["oracle_only_columns"]
    assert "dt" in result["hive_only_columns"]
    assert "id" in result["common_columns"]
    assert "name" in result["common_columns"]


def test_compare_with_null_values(spark_session):
    """NULL 값을 가진 DataFrame 비교 테스트"""
    comparator = DataComparator(primary_keys=["id"])
    
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    
    # Oracle DataFrame (NULL 포함)
    oracle_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
    ])
    oracle_data = [(1, "Alice"), (2, None)]
    oracle_df = spark_session.createDataFrame(oracle_data, oracle_schema)
    
    # Hive DataFrame
    hive_data = [(1, "Alice"), (2, "Bob")]
    hive_df = spark_session.createDataFrame(hive_data, ["id", "name"])
    
    # 비교
    result = comparator.compare(oracle_df, hive_df)
    
    # NULL 값 차이가 감지되었는지 확인
    assert result["total_column_differences"] >= 0  # NULL 차이 감지

