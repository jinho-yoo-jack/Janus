"""
데이터 정규화 모듈 테스트
"""

import pytest
from pyspark.sql import SparkSession
from comparison.data_normalizer import DataNormalizer


def test_normalize_column_names(spark_session):
    """컬럼명 정규화 테스트"""
    normalizer = DataNormalizer()
    
    # 대문자 컬럼명을 가진 DataFrame
    data = [(1, "Alice"), (2, "Bob")]
    df = spark_session.createDataFrame(data, ["ID", "NAME"])
    
    # 정규화
    normalized_df = normalizer.normalize(df, "Test")
    
    # 컬럼명이 소문자로 변환되었는지 확인
    assert normalized_df.columns == ["id", "name"]


def test_normalize_null_values(spark_session):
    """NULL 값 처리 테스트"""
    normalizer = DataNormalizer()
    
    # NULL 값을 가진 DataFrame
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])
    data = [(1, None), (2, "Bob")]
    df = spark_session.createDataFrame(data, schema)
    
    # 정규화
    normalized_df = normalizer.normalize(df, "Test")
    
    # NULL 값이 처리되었는지 확인
    assert normalized_df.count() == 2


def test_create_composite_key(spark_session):
    """복합 키 생성 테스트"""
    normalizer = DataNormalizer()
    
    data = [(1, "A", 100), (2, "B", 200)]
    df = spark_session.createDataFrame(data, ["id", "code", "value"])
    
    # 복합 키 생성
    result_df = normalizer.create_composite_key(df, ["id", "code"])
    
    # 복합 키 컬럼이 추가되었는지 확인
    assert "_composite_key" in result_df.columns
    
    # 복합 키 값 확인
    keys = result_df.select("_composite_key").collect()
    assert len(keys) == 2
    assert "1||A" in [row["_composite_key"] for row in keys]
    assert "2||B" in [row["_composite_key"] for row in keys]


def test_create_composite_key_single_key(spark_session):
    """단일 키로 복합 키 생성 테스트"""
    normalizer = DataNormalizer()
    
    data = [(1, "Alice"), (2, "Bob")]
    df = spark_session.createDataFrame(data, ["id", "name"])
    
    # 단일 키로 복합 키 생성
    result_df = normalizer.create_composite_key(df, ["id"])
    
    # 복합 키 컬럼이 추가되었는지 확인
    assert "_composite_key" in result_df.columns
    
    # 복합 키 값 확인
    keys = result_df.select("_composite_key").collect()
    assert "1" in [row["_composite_key"] for row in keys]
    assert "2" in [row["_composite_key"] for row in keys]

