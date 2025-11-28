"""
Janus - Datawarehouse와 RDB 간 데이터 정합성 검증

이 모듈은 Oracle 테이블과 Hive 테이블의 데이터를 키 기반으로 상세 비교하여
차이점을 확인하고 리포트를 생성하는 Janus 검증 클래스를 제공합니다.
"""

from datetime import datetime, timedelta
from pyspark.sql import SparkSession

from utils import setup_logging
from readers import OracleReader, HiveReader
from comparison import DataComparator
from reporting import ReportGenerator, DifferenceSaver
from config import OracleConfig, HiveConfig, ValidationConfig

# 로깅 설정
logger = setup_logging()


class JanusValidator:
    """Janus - Datawarehouse와 RDB 간 데이터 정합성 검증 클래스"""
    
    def __init__(
        self,
        spark: SparkSession,
        oracle_config: OracleConfig,
        hive_config: HiveConfig,
        validation_config: ValidationConfig
    ):
        """
        JanusValidator 초기화
        
        Args:
            spark: SparkSession
            oracle_config: Oracle 설정
            hive_config: Hive 설정
            validation_config: 검증 설정
        """
        self.spark = spark
        self.oracle_config = oracle_config
        self.hive_config = hive_config
        self.validation_config = validation_config
        
        # 각 모듈 초기화
        self.oracle_reader = OracleReader(spark, oracle_config)
        self.hive_reader = HiveReader(spark, hive_config)
        self.comparator = DataComparator(oracle_config.primary_keys)
        self.report_generator = ReportGenerator(oracle_config, hive_config)
        self.difference_saver = DifferenceSaver(oracle_config)
    
    def get_target_date(self) -> str:
        """
        검증 대상 날짜 계산
        
        Returns:
            검증 대상 날짜 문자열 (YYYY-MM-DD 형식)
        """
        offset_days = self.validation_config.date_offset_days
        target_date = datetime.now() - timedelta(days=offset_days)
        return target_date.strftime("%Y-%m-%d")
    
    def run(self) -> bool:
        """
        Janus 검증 실행
        
        Returns:
            True: 모든 데이터 일치, False: 데이터 불일치 발견
        """
        try:
            logger.info("=" * 80)
            logger.info("Janus - 데이터 정합성 검증 시작")
            logger.info("=" * 80)
            
            # 대상 날짜 계산
            target_date = self.get_target_date()
            logger.info(f"검증 대상 날짜: {target_date}")
            
            # Oracle 데이터 읽기
            oracle_df = self.oracle_reader.read(target_date)
            
            # Hive 데이터 읽기
            hive_df = self.hive_reader.read(target_date)
            
            # 데이터 비교
            comparison_result = self.comparator.compare(oracle_df, hive_df)
            
            # 리포트 생성 (상세 내용 경로 포함)
            output_path = self.validation_config.output_path if self.validation_config.output_path else None
            report = self.report_generator.generate(comparison_result, target_date, output_path)
            logger.info("\n" + report)
            
            # 리포트 파일로 저장
            if self.validation_config.report_path:
                self.report_generator.save(report, target_date, self.validation_config.report_path)
            
            # 차이점 레코드 저장
            if self.validation_config.output_path:
                self.difference_saver.save(comparison_result, target_date, self.validation_config.output_path)
            
            # 검증 결과 반환
            has_differences = (
                comparison_result['oracle_only_count'] > 0 or
                comparison_result['hive_only_count'] > 0 or
                comparison_result['total_column_differences'] > 0
            )
            
            if has_differences:
                logger.warning("⚠️  데이터 불일치가 발견되었습니다.")
                return False
            else:
                logger.info("✅ 모든 데이터가 일치합니다.")
                return True
                
        except Exception as e:
            logger.error(f"Janus 검증 실행 중 오류 발생: {str(e)}", exc_info=True)
            raise

