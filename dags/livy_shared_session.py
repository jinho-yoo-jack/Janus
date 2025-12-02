"""
Livy 공유 세션을 사용한 검증 Task 실행 함수
"""

import sys
import os
import logging
from typing import Dict, Any, Optional
from airflow.models import Variable
from utils.livy_session import LivySessionManager

logger = logging.getLogger(__name__)


def execute_validation_with_shared_livy_session(
    oracle_db_name: str,
    table_name: str,
    livy_session_id: Optional[int] = None,
    **context
) -> Dict[str, Any]:
    """
    공유 Livy 세션을 사용하여 검증 작업 실행
    
    Args:
        oracle_db_name: Oracle 데이터베이스명
        table_name: 테이블명
        livy_session_id: 공유 Livy 세션 ID (XCom에서 가져옴)
        **context: Airflow context
    
    Returns:
        실행 결과
    """
    # 설정 파일 경로
    config_dir = Variable.get('config_dir', default_var='/opt/airflow/dags/config')
    environment = Variable.get('environment', default_var='dev')
    common_config = Variable.get('common_config_path', default_var=f'{config_dir}/application.yml')
    project_path = Variable.get('project_path', default_var='/opt/airflow/dags')
    livy_url = Variable.get('livy_url', default_var='http://localhost:18998')
    
    # Livy 세션 관리자 초기화
    livy_manager = LivySessionManager(livy_url=livy_url)
    
    # 공유 세션 ID 사용 (XCom에서 가져오거나 파라미터로 받음)
    if livy_session_id is None:
        # XCom에서 세션 ID 가져오기
        ti = context['ti']
        dag_run = context['dag_run']
        create_session_task = dag_run.get_task_instance('create_livy_session')
        if create_session_task:
            livy_session_id = create_session_task.xcom_pull(key='livy_session_id')
    
    if livy_session_id is None:
        raise ValueError("Livy 세션 ID를 찾을 수 없습니다. create_livy_session Task가 먼저 실행되어야 합니다.")
    
    livy_manager.session_id = livy_session_id
    logger.info(f"공유 Livy 세션 사용: session_id={livy_session_id}")
    
    try:
        # 검증 코드 준비
        # 방법 1: 함수 호출 방식 (간단)
        validation_code = f"""
            from janus_validation_module import run_validation
            run_validation(spark, '{config_dir}', '{oracle_db_name}', '{table_name}', '{common_config}', '{environment}')
        """
        print(validation_code)
        
        # 방법 2: 모듈 직접 실행 방식 (더 간단하지만 argparse 필요)
        # validation_code = f"""
        # import sys
        # sys.argv = ['janus_validation_module.py', 
        #             '--config-dir', '{config_dir}',
        #             '--oracle-db-name', '{oracle_db_name}',
        #             '--table-name', '{table_name}',
        #             '--common-config', '{common_config}',
        #             '--environment', '{environment}']
        # exec(open('janus_validation_module.py').read())
        # """
        
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
                return {
                    "status": "success",
                    "message": "검증 성공",
                    "oracle_db_name": oracle_db_name,
                    "table_name": table_name
                }
            else:
                return {
                    "status": "failure",
                    "message": "데이터 불일치 발견",
                    "oracle_db_name": oracle_db_name,
                    "table_name": table_name
                }
        else:
            error_message = output.get("evalue", "Unknown error")
            logger.error(f"검증 실패: {error_message}")
            raise RuntimeError(f"검증 실행 실패: {error_message}")
    
    except Exception as e:
        logger.error(f"Livy 검증 실행 중 오류: {str(e)}", exc_info=True)
        raise

