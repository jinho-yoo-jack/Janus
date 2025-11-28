"""
데이터 정규화 모듈
"""

import logging
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, concat_ws, lit, md5, sha2

logger = logging.getLogger(__name__)


class DataNormalizer:
    """DataFrame 정규화 클래스"""
    
    @staticmethod
    def normalize(df: DataFrame, source: str) -> DataFrame:
        """
        DataFrame 정규화 (컬럼명, 데이터 타입, NULL 처리)
        
        Args:
            df: 정규화할 DataFrame
            source: 데이터 소스명 (로깅용)
        
        Returns:
            정규화된 DataFrame
        """
        logger.info(f"{source} 데이터 정규화 시작")
        
        # 컬럼명을 소문자로 변환
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())
        
        # NULL 값을 None으로 유지 (비교 시 NULL 처리를 위해)
        # 문자열 타입의 경우 빈 문자열로 변환
        for col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]
            if col_type.startswith("string"):
                df = df.withColumn(
                    col_name,
                    when(isnull(col(col_name)) | isnan(col(col_name)), lit("")).otherwise(col(col_name))
                )
        
        return df
    
    @staticmethod
    def create_composite_key(df: DataFrame, keys: List[str]) -> DataFrame:
        """
        복합 키 생성
        
        Args:
            df: DataFrame
            keys: 키 컬럼 리스트
        
        Returns:
            복합 키가 추가된 DataFrame
        """
        key_cols = [col(k.lower()) for k in keys]
        df = df.withColumn("_composite_key", concat_ws("||", *key_cols))
        return df
    
    @staticmethod
    def create_row_hash(df: DataFrame, exclude_keys: List[str] = None) -> DataFrame:
        """
        레코드 전체를 hash 값으로 변환
        
        Args:
            df: DataFrame
            exclude_keys: hash 생성에서 제외할 키 컬럼 리스트 (기본값: None)
        
        Returns:
            hash 컬럼이 추가된 DataFrame
        """
        # Primary Key는 hash에서 제외 (키는 별도로 관리)
        exclude_set = set()
        if exclude_keys:
            exclude_set = {k.lower() for k in exclude_keys}
        exclude_set.add("_composite_key")  # 복합 키도 제외
        
        # hash에 포함할 컬럼 선택 (Primary Key 제외, 정렬하여 순서 보장)
        hash_cols = sorted([c for c in df.columns if c.lower() not in exclude_set])
        
        if not hash_cols:
            # hash에 포함할 컬럼이 없으면 빈 문자열로 hash 생성
            df = df.withColumn("_row_hash", sha2(lit(""), 256))
        else:
            # 모든 컬럼 값을 하나의 문자열로 결합 후 hash 생성
            # NULL 값은 빈 문자열로 처리, 모든 값을 문자열로 변환
            hash_string_cols = [
                when(col(c).isNull(), lit("")).otherwise(col(c).cast("string"))
                for c in hash_cols
            ]
            
            # 모든 값을 구분자로 결합하여 hash 생성
            combined_string = concat_ws("||", *hash_string_cols)
            df = df.withColumn("_row_hash", sha2(combined_string, 256))
        
        return df

