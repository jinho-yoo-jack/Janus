"""
Janus 검증 모듈 - Livy에서 사용할 검증 로직

이 모듈은 Livy 세션에 pyFiles로 업로드되어 사용됩니다.
ZIP 파일로 업로드된 경우, 패키지 구조가 유지되어 import가 가능합니다.

사용 방법:
1. 함수 호출 방식: from janus_validation_module import run_validation
2. 직접 실행 방식: 모듈을 직접 실행 (if __name__ == "__main__")
"""

import sys
import traceback


def run_validation(
    spark,
    config_dir: str,
    oracle_db_name: str,
    table_name: str,
    common_config: str,
    environment: str
) -> int:
    """
    Janus 검증 실행
    
    Args:
        spark: SparkSession 객체 (Livy에서 자동 제공)
        config_dir: 설정 파일 디렉토리 경로
        oracle_db_name: Oracle 데이터베이스명
        table_name: 테이블명
        common_config: 공통 설정 파일 경로
        environment: 환경 (dev 또는 prod)
    
    Returns:
        0: 검증 성공, 1: 검증 실패
    """
    try:
        # pyFiles로 업로드된 모듈들을 import
        # ZIP 파일로 업로드된 경우, 패키지 구조가 유지되어 있음
        from cfg.etl_config_loader import ETLConfigLoader
        from utils import setup_logging
        from janus import JanusValidator
        
        # 로깅 설정
        logger = setup_logging()
        
        # 설정 로드
        oracle_config, hive_config, validation_config = ETLConfigLoader.load_configs(
            config_dir,
            oracle_db_name,
            table_name,
            common_config,
            environment
        )
        
        # Janus 검증 실행
        validator = JanusValidator(spark, oracle_config, hive_config, validation_config)
        success = validator.run()
        
        if success:
            print("✅ 검증 성공: 모든 데이터가 일치합니다.")
            return 0
        else:
            print("❌ 검증 실패: 데이터 불일치가 발견되었습니다.")
            return 1
    
    except Exception as e:
        error_msg = f"검증 실행 중 오류 발생: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        # logger가 설정되지 않았을 수 있으므로 print 사용
        raise


# 모듈을 직접 실행할 수 있도록 main 블록 추가
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Janus 검증 실행')
    parser.add_argument('--config-dir', required=True, help='설정 파일 디렉토리 경로')
    parser.add_argument('--oracle-db-name', required=True, help='Oracle 데이터베이스명')
    parser.add_argument('--table-name', required=True, help='테이블명')
    parser.add_argument('--common-config', required=True, help='공통 설정 파일 경로')
    parser.add_argument('--environment', default='dev', help='환경 (dev 또는 prod)')
    
    args = parser.parse_args()
    
    # Livy 세션에서는 spark 변수가 자동으로 제공됨
    # 별도로 SparkSession을 생성할 필요 없음
    try:
        result = run_validation(
            spark=spark,  # Livy에서 자동 제공
            config_dir=args.config_dir,
            oracle_db_name=args.oracle_db_name,
            table_name=args.table_name,
            common_config=args.common_config,
            environment=args.environment
        )
        sys.exit(result)
    except Exception as e:
        print(f"검증 실행 실패: {str(e)}")
        sys.exit(1)

