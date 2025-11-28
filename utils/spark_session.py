"""
Spark 세션 유틸리티 모듈
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """
    Spark 세션 생성
    
    Args:
        app_name: 애플리케이션 이름
    
    Returns:
        SparkSession 인스턴스
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # 로그 레벨 설정
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

