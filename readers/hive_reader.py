"""
Hive 데이터 읽기 모듈
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from config import HiveConfig

logger = logging.getLogger(__name__)


class HiveReader:
    """Hive 테이블 데이터 읽기 클래스"""
    
    def __init__(self, spark: SparkSession, config: HiveConfig):
        self.spark = spark
        self.config = config
    
    def read(self, target_date: str) -> DataFrame:
        """
        Hive 테이블에서 데이터 읽기
        
        Args:
            target_date: 대상 날짜 (YYYY-MM-DD 형식)
        
        Returns:
            DataFrame
        """
        logger.info(f"Hive 테이블에서 데이터 읽기 시작: {self.config.database}.{self.config.table_name}")
        logger.info(f"대상 날짜: {target_date}")
        
        try:
            df = self._read_from_hive(target_date)
            
            row_count = df.count()
            logger.info(f"Hive 테이블에서 읽은 레코드 수: {row_count:,}")
            
            return df
        except Exception as e:
            logger.error(f"Hive 테이블 읽기 실패: {str(e)}")
            raise
    
    def _read_from_hive(self, target_date: str) -> DataFrame:
        """PySpark를 사용하여 Hive 테이블 읽기"""
        # 날짜 형식에 따라 target_date 변환
        hive_date_value = self._convert_date_format(target_date)
        next_date_value = self._get_next_date(target_date)
        
        full_table_name = f"{self.config.database}.{self.config.table_name}" if self.config.database else self.config.table_name
        
        # SELECT 절 구성 (제외 컬럼 처리)
        select_clause = self._build_select_clause()
        
        # Hive 테이블 읽기 (범위 조건: date_column >= target_date AND date_column < next_date)
        df = self.spark.sql(f"""
            SELECT {select_clause}
            FROM {full_table_name}
            WHERE {self.config.date_column} >= '{hive_date_value}'
            AND {self.config.date_column} < '{next_date_value}'
        """)
        
        # 제외할 컬럼이 있으면 DataFrame에서 제외
        if self.config.exclude_columns:
            exclude_cols_lower = [col.lower() for col in self.config.exclude_columns]
            df_columns = [col for col in df.columns if col.lower() not in exclude_cols_lower]
            if df_columns:
                df = df.select(*df_columns)
            else:
                raise ValueError("모든 컬럼이 제외되어 선택할 컬럼이 없습니다.")
        
        return df
    
    def _build_select_clause(self) -> str:
        """
        SELECT 절 구성 (제외 컬럼 처리)
        
        Returns:
            SELECT 절 문자열
        """
        if not self.config.exclude_columns:
            return "*"
        
        # 제외할 컬럼이 있는 경우, 모든 컬럼을 가져온 후 제외
        # Hive에서는 동적으로 컬럼을 제외하기 어려우므로
        # 먼저 모든 컬럼을 읽고 DataFrame에서 제외하는 방식 사용
        # 여기서는 "*"로 읽고 나중에 DataFrame에서 제외
        return "*"
    
    def _convert_date_format(self, target_date: str) -> str:
        """날짜 형식에 따라 target_date 변환"""
        date_column_type = self.config.date_column_type
        
        if date_column_type == "yyyy-mm-dd":
            # 이미 YYYY-MM-DD 형식
            return target_date
        elif date_column_type == "yyyymmdd":
            # YYYY-MM-DD를 YYYYMMDD로 변환
            return target_date.replace("-", "")
        elif date_column_type == "yyyymmdd HH24:mm:ss":
            # YYYY-MM-DD를 YYYYMMDD HH24:MI:SS로 변환 (시간은 00:00:00)
            return target_date.replace("-", "") + " 00:00:00"
        elif date_column_type == "yyyy-mm-dd HH24:mm:ss":
            # YYYY-MM-DD를 YYYY-MM-DD HH24:MI:SS로 변환 (시간은 00:00:00)
            return target_date + " 00:00:00"
        else:
            raise ValueError(f"지원하지 않는 날짜 형식: {date_column_type}")
    
    def _get_next_date(self, target_date: str) -> str:
        """
        다음 날짜 계산 (target_date + 1일)
        
        Args:
            target_date: 대상 날짜 (YYYY-MM-DD 형식)
        
        Returns:
            다음 날짜 문자열 (날짜 컬럼 타입에 맞는 형식)
        """
        from datetime import datetime, timedelta
        
        # target_date를 datetime 객체로 변환
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        # 다음 날 계산
        next_date_obj = date_obj + timedelta(days=1)
        
        # 날짜 형식에 따라 변환
        date_column_type = self.config.date_column_type
        if date_column_type == "yyyy-mm-dd":
            return next_date_obj.strftime("%Y-%m-%d")
        elif date_column_type == "yyyymmdd":
            return next_date_obj.strftime("%Y%m%d")
        elif date_column_type == "yyyymmdd HH24:mm:ss":
            return next_date_obj.strftime("%Y%m%d") + " 00:00:00"
        elif date_column_type == "yyyy-mm-dd HH24:mm:ss":
            return next_date_obj.strftime("%Y-%m-%d") + " 00:00:00"
        else:
            raise ValueError(f"지원하지 않는 날짜 형식: {date_column_type}")

