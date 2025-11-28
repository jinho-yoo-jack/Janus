"""
데이터 비교 모듈
"""

import logging
from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .data_normalizer import DataNormalizer

logger = logging.getLogger(__name__)


class DataComparator:
    """데이터 비교 클래스"""
    
    def __init__(self, primary_keys: List[str]):
        self.primary_keys = primary_keys
        self.normalizer = DataNormalizer()
    
    def compare(self, oracle_df: DataFrame, hive_df: DataFrame) -> Dict[str, any]:
        """
        두 DataFrame을 키 기반으로 상세 비교
        
        Args:
            oracle_df: Oracle DataFrame
            hive_df: Hive DataFrame
        
        Returns:
            비교 결과 딕셔너리
        """
        logger.info("데이터 비교 시작")
        
        # 정규화
        oracle_df = self.normalizer.normalize(oracle_df, "Oracle")
        hive_df = self.normalizer.normalize(hive_df, "Hive")
        
        # 공통 컬럼 확인
        oracle_cols = set([c.lower() for c in oracle_df.columns])
        hive_cols = set([c.lower() for c in hive_df.columns])
        common_cols = oracle_cols.intersection(hive_cols)
        oracle_only_cols = oracle_cols - hive_cols
        hive_only_cols = hive_cols - oracle_cols
        
        logger.info(f"공통 컬럼 수: {len(common_cols)}")
        logger.info(f"Oracle 전용 컬럼: {oracle_only_cols}")
        logger.info(f"Hive 전용 컬럼: {hive_only_cols}")
        
        # 복합 키 생성
        oracle_df = self.normalizer.create_composite_key(oracle_df, self.primary_keys)
        hive_df = self.normalizer.create_composite_key(hive_df, self.primary_keys)
        
        # 레코드 hash 생성 (Primary Key 제외)
        logger.info("레코드 hash 생성 중...")
        oracle_df = self.normalizer.create_row_hash(oracle_df, self.primary_keys)
        hive_df = self.normalizer.create_row_hash(hive_df, self.primary_keys)
        
        # 키 기반 조인
        oracle_df_alias = oracle_df.alias("oracle")
        hive_df_alias = hive_df.alias("hive")
        
        # Full Outer Join으로 모든 차이점 확인
        joined_df = oracle_df_alias.join(
            hive_df_alias,
            oracle_df_alias["_composite_key"] == hive_df_alias["_composite_key"],
            "full_outer"
        )
        
        # 비교 결과 분류
        oracle_only = joined_df.filter(
            col("hive._composite_key").isNull()
        ).select("oracle.*")
        
        hive_only = joined_df.filter(
            col("oracle._composite_key").isNull()
        ).select("hive.*")
        
        both_exist = joined_df.filter(
            col("oracle._composite_key").isNotNull() & 
            col("hive._composite_key").isNotNull()
        )
        
        both_exist_count = both_exist.count()
        logger.info(f"양쪽 모두 존재하는 레코드 수: {both_exist_count}")
        
        # Hash 기반 비교: hash가 다른 레코드만 상세 비교
        diff_records = []
        diff_records_df = None
        
        if len(common_cols) > 0 and both_exist_count > 0:
            # Hash가 다른 레코드 찾기
            hash_diff_condition = col("oracle._row_hash") != col("hive._row_hash")
            hash_diff_records = both_exist.filter(hash_diff_condition)
            hash_diff_count = hash_diff_records.count()
            
            logger.info(f"Hash가 다른 레코드 수: {hash_diff_count}")
            
            if hash_diff_count > 0:
                # Hash가 다른 레코드에 대해서만 컬럼별 상세 비교
                diff_conditions = []
                for col_name in common_cols:
                    if col_name not in [k.lower() for k in self.primary_keys] and col_name not in ["_composite_key", "_row_hash"]:
                        # NULL 안전 비교
                        oracle_col = col(f"oracle.{col_name}")
                        hive_col = col(f"hive.{col_name}")
                        
                        # NULL이 아닌 경우에만 비교
                        diff_condition = (
                            (oracle_col.isNotNull() & hive_col.isNotNull() & (oracle_col != hive_col)) |
                            (oracle_col.isNull() & hive_col.isNotNull()) |
                            (oracle_col.isNotNull() & hive_col.isNull())
                        )
                        diff_conditions.append(diff_condition)
                        
                        # 컬럼별 차이 개수 계산 (hash가 다른 레코드 중에서만)
                        diff_count = hash_diff_records.filter(diff_condition).count()
                        if diff_count > 0:
                            diff_records.append({
                                "column": col_name,
                                "count": diff_count
                            })
                
                # 전체 차이 레코드 추출 (hash가 다른 레코드)
                diff_records_df = hash_diff_records
        
        # 통계 수집
        oracle_only_count = oracle_only.count()
        hive_only_count = hive_only.count()
        total_diff_count = len(diff_records)
        
        result = {
            "oracle_row_count": oracle_df.count(),
            "hive_row_count": hive_df.count(),
            "oracle_only_count": oracle_only_count,
            "hive_only_count": hive_only_count,
            "both_exist_count": both_exist_count,
            "common_columns": list(common_cols),
            "oracle_only_columns": list(oracle_only_cols),
            "hive_only_columns": list(hive_only_cols),
            "column_differences": diff_records,
            "total_column_differences": total_diff_count,
            "oracle_only_records": oracle_only,
            "hive_only_records": hive_only,
            "different_records": diff_records_df if diff_records_df is not None else None
        }
        
        logger.info(f"비교 완료 - Oracle: {result['oracle_row_count']}, Hive: {result['hive_row_count']}")
        logger.info(f"차이점 - Oracle 전용: {oracle_only_count}, Hive 전용: {hive_only_count}, 컬럼 차이: {total_diff_count}")
        
        return result

