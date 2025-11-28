"""
pytest 설정 및 공통 픽스처
"""

import pytest
from pyspark.sql import SparkSession
from config import OracleConfig, HiveConfig, ValidationConfig


@pytest.fixture(scope="session")
def spark_session():
    """Spark 세션 픽스처"""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_oracle_config():
    """샘플 Oracle 설정"""
    return OracleConfig(
        jdbc_url="jdbc:oracle:thin:@localhost:1521/ORCL",
        user="test_user",
        password="test_password",
        schema="TEST_SCHEMA",
        table_name="TEST_TABLE",
        columns=["*"],
        primary_keys=["ID"],
        date_column="CREATED_DATE",
        date_column_type="yyyy-mm-dd"
    )


@pytest.fixture
def sample_hive_config():
    """샘플 Hive 설정"""
    return HiveConfig(
        database="test_db",
        table_name="test_table",
        date_column="dt",
        date_column_type="yyyy-mm-dd"
    )


@pytest.fixture
def sample_validation_config():
    """샘플 검증 설정"""
    return ValidationConfig(
        report_path="/tmp/reports",
        output_path="/tmp/output",
        date_offset_days=2
    )


@pytest.fixture
def sample_dataframe(spark_session):
    """샘플 DataFrame 생성"""
    data = [
        (1, "Alice", "2024-01-15", 100),
        (2, "Bob", "2024-01-15", 200),
        (3, "Charlie", "2024-01-15", 300),
    ]
    columns = ["id", "name", "dt", "value"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def sample_oracle_dataframe(spark_session):
    """샘플 Oracle DataFrame"""
    data = [
        (1, "Alice", "2024-01-15", 100),
        (2, "Bob", "2024-01-15", 200),
        (3, "Charlie", "2024-01-15", 300),
    ]
    columns = ["ID", "NAME", "CREATED_DATE", "VALUE"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def sample_hive_dataframe(spark_session):
    """샘플 Hive DataFrame"""
    data = [
        (1, "Alice", "2024-01-15", 100),
        (2, "Bob", "2024-01-15", 200),
    ]
    columns = ["id", "name", "dt", "value"]
    return spark_session.createDataFrame(data, columns)

