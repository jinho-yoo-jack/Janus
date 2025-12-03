"""
Oracle 데이터 읽기 모듈
"""

import logging
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from config import OracleConfig

logger = logging.getLogger(__name__)


class OracleReader:
    """Oracle 테이블 데이터 읽기 클래스"""

    def __init__(self, spark: SparkSession, config: OracleConfig):
        self.spark = spark
        self.config = config

    def read(self, target_date: str) -> DataFrame:
        """
        Oracle 테이블에서 데이터 읽기
        
        Args:
            target_date: 대상 날짜 (YYYY-MM-DD 형식)
        
        Returns:
            DataFrame
        """
        logger.info(f"Oracle 테이블에서 데이터 읽기 시작: {self.config.schema}.{self.config.table_name}")
        logger.info(f"대상 날짜: {target_date}")

        try:
            df = self._read_from_oracle(target_date)

            row_count = df.count()
            logger.info(f"Oracle 테이블에서 읽은 레코드 수: {row_count:,}")

            return df
        except Exception as e:
            logger.error(f"Oracle 테이블 읽기 실패: {str(e)}")
            raise

    def _read_from_oracle(self, target_date: str) -> DataFrame:
        """PySpark를 사용하여 Oracle 테이블 읽기"""
        # 전체 테이블명 구성
        full_table_name = f"{self.config.schema}.{self.config.table_name}" if self.config.schema else self.config.table_name

        # 컬럼 선택 구성 (제외 컬럼 처리)
        select_columns = self._build_select_columns()

        # 날짜 형식에 따라 target_date 변환 및 Oracle WHERE 절 생성
        date_where_clause = self._build_where_clause(target_date)
        
        # 추가 WHERE 조건문이 있으면 AND로 결합
        if self.config.where_clause:
            where_clause = f"({date_where_clause} AND {self.config.where_clause})"
        else:
            where_clause = date_where_clause

        # 서브쿼리로 날짜 필터링 (서버 측에서 필터링하여 효율적)
        query = f"""
            (SELECT {select_columns}
             FROM {full_table_name}
             WHERE {where_clause}) oracle_data
        """

        # PySpark JDBC 읽기 옵션 설정
        read_options = {
            "url": self.config.jdbc_url,
            "dbtable": query,
            "user": self.config.user,
            "password": self.config.password,
            "driver": "oracle.jdbc.driver.OracleDriver",
            "fetchsize": "10000",  # 배치 크기 설정 (성능 향상)
            "numPartitions": "10",  # 병렬 읽기 파티션 수
        }

        # PySpark DataFrame API를 사용하여 Oracle 테이블 읽기
        df = self.spark.read \
            .format("jdbc") \
            .options(**read_options) \
            .load()

        # 제외할 컬럼이 있고 "*"로 읽은 경우, DataFrame에서 제외
        if self.config.exclude_columns and self.config.columns == ["*"]:
            exclude_cols_lower = [col.lower() for col in self.config.exclude_columns]
            df_columns = [col for col in df.columns if col.lower() not in exclude_cols_lower]
            if df_columns:
                df = df.select(*df_columns)
            else:
                raise ValueError("모든 컬럼이 제외되어 선택할 컬럼이 없습니다.")

        return df

    def _build_where_clause(self, target_date: str) -> str:
        """
        날짜 형식에 따라 WHERE 절 생성 (범위 조건)
        
        Args:
            target_date: 조회 시점의 날짜 문자열 (YYYY-MM-DD 형식)
        
        Returns:
            WHERE 절 문자열 (date_column >= target_date AND date_column < target_date + 1일)

        Example:
            yyyy-mm-dd: >= '2024-01-15' AND < '2024-01-16'
            yyyymmdd: >= '20240115' AND < '20240116'
            yyyymmdd HH24:mm:ss: >= '20240115 00:00:00' AND < '20240116 00:00:00'
            yyyy-mm-dd HH24:mm:ss: >= '2024-01-15 00:00:00' AND < '2024-01-16 00:00:00'
        """
        date_column = self.config.date_column
        date_column_type = self.config.date_column_type

        if date_column_type == "yyyy-mm-dd":
            oracle_date_format = "YYYY-MM-DD"
            oracle_date_value = target_date
            # 다음 날 계산
            next_date_value = self._get_next_date(target_date, "yyyy-mm-dd")
            # 범위 조건: date_column >= target_date AND date_column < next_date
            where_clause = f"""(
                {date_column} >= TO_DATE('{oracle_date_value}', '{oracle_date_format}')
                AND {date_column} < TO_DATE('{next_date_value}', '{oracle_date_format}')
            )"""
        elif date_column_type == "yyyymmdd":
            oracle_date_format = "YYYYMMDD"
            oracle_date_value = target_date.replace("-", "")
            # 다음 날 계산
            next_date_value = self._get_next_date(target_date, "yyyymmdd")
            # 범위 조건: date_column >= target_date AND date_column < next_date
            where_clause = f"""(
                TO_DATE(TO_CHAR({date_column}, '{oracle_date_format}'), '{oracle_date_format}') >= TO_DATE('{oracle_date_value}', '{oracle_date_format}')
                AND TO_DATE(TO_CHAR({date_column}, '{oracle_date_format}'), '{oracle_date_format}') < TO_DATE('{next_date_value}', '{oracle_date_format}')
            )"""
        elif date_column_type == "yyyymmdd HH24:mm:ss":
            oracle_date_format = "YYYYMMDD HH24:MI:SS"
            oracle_date_value = target_date.replace("-", "") + " 00:00:00"
            # 다음 날 계산
            next_date_value = self._get_next_date(target_date, "yyyymmdd HH24:mm:ss")
            # 범위 조건: date_column >= target_date AND date_column < next_date
            where_clause = f"""(
                TO_DATE(TO_CHAR({date_column}, '{oracle_date_format}'), '{oracle_date_format}') >= TO_DATE('{oracle_date_value}', '{oracle_date_format}')
                AND TO_DATE(TO_CHAR({date_column}, '{oracle_date_format}'), '{oracle_date_format}') < TO_DATE('{next_date_value}', '{oracle_date_format}')
            )"""
        elif date_column_type == "yyyy-mm-dd HH24:mm:ss":
            oracle_date_format = "YYYY-MM-DD HH24:MI:SS"
            oracle_date_value = target_date + " 00:00:00"
            # 다음 날 계산
            next_date_value = self._get_next_date(target_date, "yyyy-mm-dd HH24:mm:ss")
            # 범위 조건: date_column >= target_date AND date_column < next_date
            where_clause = f"""(
                {date_column} >= TO_DATE('{oracle_date_value}', '{oracle_date_format}')
                AND {date_column} < TO_DATE('{next_date_value}', '{oracle_date_format}')
            )"""
        else:
            raise ValueError(f"지원하지 않는 날짜 형식: {date_column_type}")

        return where_clause
    
    def _get_next_date(self, target_date: str, date_column_type: str) -> str:
        """
        다음 날짜 계산 (target_date + 1일)
        
        Args:
            target_date: 대상 날짜 (YYYY-MM-DD 형식)
            date_column_type: 날짜 컬럼 타입
        
        Returns:
            다음 날짜 문자열
        """
        from datetime import datetime, timedelta
        
        # target_date를 datetime 객체로 변환
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        # 다음 날 계산
        next_date_obj = date_obj + timedelta(days=1)
        
        # 날짜 형식에 따라 변환
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

    def _build_select_columns(self) -> str:
        """
        SELECT 절 구성 (제외 컬럼 처리)
        
        Returns:
            SELECT 절 문자열
        """
        if self.config.columns == ["*"]:
            # 모든 컬럼을 선택하되, 제외할 컬럼이 있으면 제외
            # if self.cfg.exclude_columns:
            # 제외할 컬럼 목록을 생성하기 위해 먼저 모든 컬럼을 읽어야 함
            # 하지만 Oracle에서는 이를 직접 할 수 없으므로,
            # 제외할 컬럼을 명시적으로 빼는 방식으로 처리
            # 실제로는 DataFrame에서 제외하는 것이 더 효율적
            # 여기서는 일단 "*"로 읽고 나중에 DataFrame에서 제외
            #    return "*"
            # else:
            return "*"
        else:
            # 특정 컬럼만 선택하는 경우, 제외 컬럼을 필터링
            selected_columns = [
                col for col in self.config.columns
                if col not in self.config.exclude_columns
            ]
            if not selected_columns:
                raise ValueError("모든 컬럼이 제외되어 선택할 컬럼이 없습니다.")
            return ", ".join(selected_columns)
