"""
Livy를 사용한 검증 Task 실행 함수
"""

import sys
import os
import logging
from typing import Dict, Any
from airflow.models import Variable
from utils.livy_session import LivySessionManager

logger = logging.getLogger(__name__)


def execute_validation_with_livy(
    oracle_db_name: str,
    table_name: str,
    **context
) -> Dict[str, Any]:
    """
    Livy를 사용하여 검증 작업 실행
    
    Args:
        oracle_db_name: Oracle 데이터베이스명
        table_name: 테이블명
        **context: Airflow context
    
    Returns:
        실행 결과
    """
    # 설정 파일 경로
    config_dir = Variable.get('config_dir', default_var='/opt/airflow/dags/config')
    environment = Variable.get('environment', default_var='dev')
    common_config = Variable.get('common_config_path', default_var=f'{config_dir}/application.yml')
    oracle_jdbc_jar = Variable.get('oracle_jdbc_jar_path', default_var='/opt/spark/jars/ojdbc8.jar')
    livy_url = Variable.get('livy_url', default_var='http://localhost:18998')
    
    # Livy 세션 관리자 초기화
    livy_manager = LivySessionManager(livy_url=livy_url)
    
    try:
        # Livy 세션 생성
        session_id = livy_manager.create_session(
            kind="pyspark",
            jars=[oracle_jdbc_jar] if os.path.exists(oracle_jdbc_jar) else None,
            driver_memory="2g",
            driver_cores=1,
            executor_memory="4g",
            executor_cores=2,
            num_executors=2,
            conf={
                "spark.sql.warehouse.dir": "/user/hive/warehouse",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            }
        )
        
        logger.info(f"Livy 세션 생성 완료: session_id={session_id}")
        
        # 검증 코드 준비
        # sys.path에 프로젝트 경로 추가
        project_path = Variable.get('project_path', default_var='/opt/airflow/dags')
        
        validation_code = f"""
import sys
import os

# 프로젝트 경로 추가
sys.path.insert(0, '{project_path}')

# Janus 검증 실행
from config.etl_config_loader import ETLConfigLoader
from utils import setup_logging
from janus import JanusValidator

# 로깅 설정
logger = setup_logging()

try:
    # Livy 세션에서는 spark 변수가 자동으로 제공됨
    # 별도로 SparkSession을 생성할 필요 없음
    
    # 설정 로드
    oracle_config, hive_config, validation_config = ETLConfigLoader.load_configs(
        '{config_dir}',
        '{oracle_db_name}',
        '{table_name}',
        '{common_config}',
        '{environment}'
    )
    
    # Janus 검증 실행
    validator = JanusValidator(spark, oracle_config, hive_config, validation_config)
    success = validator.run()
    
    if success:
        print("✅ 검증 성공: 모든 데이터가 일치합니다.")
        result = 0
    else:
        print("❌ 검증 실패: 데이터 불일치가 발견되었습니다.")
        result = 1
    
    result
except Exception as e:
    import traceback
    error_msg = f"검증 실행 중 오류 발생: {{str(e)}}\\n{{traceback.format_exc()}}"
    print(error_msg)
    logger.error(error_msg)
    raise
"""
        
        # 코드 실행
        statement_id = livy_manager.submit_code(validation_code)
        
        # 실행 완료 대기
        statement_result = livy_manager.wait_for_statement(statement_id)
        
        # 결과 확인
        output = statement_result.get("output", {})
        
        if output.get("status") == "ok":
            result_value = output.get("data", {}).get("text/plain", "")
            logger.info(f"검증 완료: {result_value}")
            
            # 결과가 0이면 성공, 1이면 실패
            if "검증 성공" in result_value or result_value.strip() == "0":
                return {"status": "success", "message": "검증 성공"}
            else:
                return {"status": "failure", "message": "데이터 불일치 발견"}
        else:
            error_message = output.get("evalue", "Unknown error")
            logger.error(f"검증 실패: {error_message}")
            raise RuntimeError(f"검증 실행 실패: {error_message}")
    
    except Exception as e:
        logger.error(f"Livy 검증 실행 중 오류: {str(e)}", exc_info=True)
        raise
    
    finally:
        # 세션 삭제
        try:
            livy_manager.delete_session()
        except Exception as e:
            logger.warning(f"Livy 세션 삭제 실패: {str(e)}")

