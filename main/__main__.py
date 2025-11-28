"""
Janus 실행 진입점

이 모듈은 명령줄에서 Janus를 실행하기 위한 main 함수를 제공합니다.
`python -m main` 또는 `python main/__main__.py`로 실행 가능합니다.
"""

import sys
import argparse
from pyspark.sql import SparkSession

from config.etl_config_loader import ETLConfigLoader
from utils import setup_logging, create_spark_session
from janus import JanusValidator

# 로깅 설정
logger = setup_logging()


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='Janus - Datawarehouse와 RDB 간 데이터 정합성 검증')
    parser.add_argument('--oracle-db-name', type=str, required=True, help='Oracle 데이터베이스명 (예: pg_db)')
    parser.add_argument('--table-name', type=str, required=True, help='테이블명 (예: tp_cp_master)')
    parser.add_argument('--config-dir', type=str, default='config', help='설정 파일 디렉토리 경로 (기본값: config)')
    parser.add_argument('--common-config', type=str, default='config/application.yml', help='공통 설정 파일 경로 (기본값: config/application.yml)')
    parser.add_argument('--env', type=str, default='dev', choices=['dev', 'prod'], help='환경 (dev 또는 prod, 기본값: dev)')
    args = parser.parse_args()
    
    spark = None
    try:
        # Spark 세션 생성
        spark = create_spark_session(f"Janus_{args.oracle_db_name}_{args.table_name}")
        
        # 설정 로드
        oracle_config, hive_config, validation_config = ETLConfigLoader.load_configs(
            args.config_dir,
            args.oracle_db_name,
            args.table_name,
            args.common_config,
            args.env
        )
        
        # Janus 검증 실행
        validator = JanusValidator(spark, oracle_config, hive_config, validation_config)
        success = validator.run()
        
        # 종료 코드
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Janus 실행 실패: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

