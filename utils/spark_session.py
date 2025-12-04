"""
Spark 세션 유틸리티 모듈
"""

import random
from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """
    Spark 세션 생성
    
    Yarn 클러스터 모드로 실행하며, hive 또는 hive2 큐를 랜덤하게 선택합니다.
    
    Args:
        app_name: 애플리케이션 이름
    
    Returns:
        SparkSession 인스턴스
    """
    # Yarn 큐 랜덤 선택 (hive, hive2)
    yarn_queues = ["hive", "hive2"]
    selected_queue = random.choice(yarn_queues)
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.yarn.queue", selected_queue) \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # 로그 레벨 설정
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

