"""
차이점 레코드 저장 모듈
"""

import logging
from typing import Dict
from config import OracleConfig

logger = logging.getLogger(__name__)


class DifferenceSaver:
    """차이점 레코드 저장 클래스"""
    
    def __init__(self, oracle_config: OracleConfig):
        self.oracle_config = oracle_config
    
    def save(self, comparison_result: Dict[str, any], target_date: str, output_path: str):
        """
        차이점 레코드를 파일로 저장
        
        Args:
            comparison_result: 비교 결과 딕셔너리
            target_date: 대상 날짜
            output_path: 출력 경로 (HDFS 경로)
        """
        table_name = self.oracle_config.table_name
        table_output_path = f"{output_path}/{table_name}"
        
        if comparison_result['oracle_only_count'] > 0:
            oracle_only_path = f"{table_output_path}/oracle_only/dt={target_date}"
            logger.info(f"Oracle 전용 레코드 저장: {oracle_only_path}")
            comparison_result['oracle_only_records'].write.mode("overwrite").parquet(oracle_only_path)
        
        if comparison_result['hive_only_count'] > 0:
            hive_only_path = f"{table_output_path}/hive_only/dt={target_date}"
            logger.info(f"Hive 전용 레코드 저장: {hive_only_path}")
            comparison_result['hive_only_records'].write.mode("overwrite").parquet(hive_only_path)
        
        if comparison_result['different_records'] is not None:
            diff_path = f"{table_output_path}/column_differences/dt={target_date}"
            logger.info(f"컬럼 차이 레코드 저장: {diff_path}")
            comparison_result['different_records'].write.mode("overwrite").parquet(diff_path)

