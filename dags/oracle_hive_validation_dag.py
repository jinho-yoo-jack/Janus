"""
Janus - Datawarehouse와 RDB 간 데이터 정합성 검증 Airflow DAG

10개 테이블을 병렬로 검증하는 DAG입니다.
매일 00:00에 실행되며, 현재 날짜 - 2일 데이터를 검증합니다.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging
import os

# 로깅 설정
logger = logging.getLogger(__name__)

# 기본 인자
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# 검증할 테이블 목록: (oracle_db_name, table_name) 튜플 리스트
# 설정 파일 경로는 자동으로 config/{oracle_db_name}/{table_name}.yml로 구성됨
TABLE_LIST = [
    ('pg_db', 'tp_cp_master'),
    # 추가 테이블을 여기에 추가: (oracle_db_name, table_name)
]

# Spark 설정
SPARK_CONFIG = {
    'spark.master': 'yarn',
    'spark.submit.deployMode': 'client',
    'spark.executor.memory': '4g',
    'spark.executor.cores': '2',
    'spark.driver.memory': '2g',
    'spark.driver.maxResultSize': '2g',
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
}

# JDBC Driver 경로 (환경에 맞게 수정 필요)
ORACLE_JDBC_JAR = Variable.get(
    'oracle_jdbc_jar_path',
    default_var='/opt/spark/jars/ojdbc8.jar'
)

# 검증 스크립트 경로
VALIDATION_SCRIPT = Variable.get(
    'validation_script_path',
    default_var='/opt/airflow/dags/main.py'
)

# 공통 설정 파일 경로 (사용하지 않음, application_args에서 직접 지정)


def create_validation_task(oracle_db_name: str, table_name: str, dag) -> SparkSubmitOperator:
    """
    테이블별 검증 Task 생성
    
    Args:
        oracle_db_name: Oracle 데이터베이스명 (예: pg_db)
        table_name: 테이블명 (예: tp_cp_master)
        dag: DAG 객체
    
    Returns:
        SparkSubmitOperator
    """
    # 설정 파일 디렉토리 경로 (Airflow Variables에서 가져오거나 기본값 사용)
    config_dir = Variable.get('config_dir', default_var='/opt/airflow/dags/config')
    # 환경 설정 (Airflow Variables에서 가져오거나 기본값 'dev' 사용)
    environment = Variable.get('environment', default_var='dev')
    
    return SparkSubmitOperator(
        task_id=f'validate_{oracle_db_name}_{table_name}',
        application=VALIDATION_SCRIPT,
        name=f'Janus_{oracle_db_name}_{table_name}',
        conf=SPARK_CONFIG,
        jars=ORACLE_JDBC_JAR,
        driver_class_path=ORACLE_JDBC_JAR,
        application_args=[
            '--oracle-db-name', oracle_db_name,
            '--table-name', table_name,
            '--config-dir', config_dir,
            '--common-config', Variable.get('common_config_path', default_var=f'{config_dir}/application.yml'),
            '--env', environment,
        ],
        dag=dag,
        pool='validation_pool',  # 동시 실행 수 제어를 위한 pool 사용
        pool_slots=1,
    )


def create_summary_task(dag) -> PythonOperator:
    """
    검증 결과 요약 Task 생성
    
    Returns:
        PythonOperator
    """
    def summarize_validation_results(**context):
        """검증 결과 요약"""
        from airflow.models import DagRun
        from airflow.utils.state import TaskInstanceState
        
        logger.info("=" * 80)
        logger.info("검증 결과 요약")
        logger.info("=" * 80)
        
        success_count = 0
        failure_count = 0
        failed_tables = []
        
        # 현재 DagRun 가져오기
        dag_run = context['dag_run']
        
        # 각 테이블별 검증 결과 확인
        for oracle_db_name, table_name in TABLE_LIST:
            task_id = f'validate_{oracle_db_name}_{table_name}'
            try:
                # TaskInstance 상태 확인
                ti = dag_run.get_task_instance(task_id)
                
                if ti is None:
                    logger.warning(f"⚠️ {oracle_db_name}/{table_name}: TaskInstance를 찾을 수 없습니다")
                    failure_count += 1
                    failed_tables.append(f"{oracle_db_name}/{table_name}")
                elif ti.state == TaskInstanceState.SUCCESS:
                    success_count += 1
                    logger.info(f"✅ {oracle_db_name}/{table_name}: 검증 성공")
                elif ti.state == TaskInstanceState.FAILED:
                    failure_count += 1
                    failed_tables.append(f"{oracle_db_name}/{table_name}")
                    logger.warning(f"❌ {oracle_db_name}/{table_name}: 검증 실패")
                else:
                    logger.info(f"⏳ {oracle_db_name}/{table_name}: 상태 - {ti.state}")
            except Exception as e:
                logger.error(f"⚠️ {oracle_db_name}/{table_name}: 결과 확인 실패 - {str(e)}")
                failure_count += 1
                failed_tables.append(f"{oracle_db_name}/{table_name}")
        
        logger.info("=" * 80)
        logger.info(f"총 테이블 수: {len(TABLE_LIST)}")
        logger.info(f"성공: {success_count}")
        logger.info(f"실패: {failure_count}")
        
        if failed_tables:
            logger.warning(f"실패한 테이블: {', '.join(failed_tables)}")
        
        logger.info("=" * 80)
        
        # 결과를 XCom에 저장
        return {
            'total_tables': len(TABLE_LIST),
            'success_count': success_count,
            'failure_count': failure_count,
            'failed_tables': failed_tables,
        }
    
    return PythonOperator(
        task_id='summarize_results',
        python_callable=summarize_validation_results,
        dag=dag,
    )


def create_notification_task(dag) -> BashOperator:
    """
    검증 완료 알림 Task 생성 (선택사항)
    
    Returns:
        BashOperator
    """
    return BashOperator(
        task_id='send_notification',
        bash_command='echo "검증 작업이 완료되었습니다."',
        dag=dag,
    )


# DAG 정의
with DAG(
    'janus_validation',
    default_args=default_args,
    description='Oracle vs Hive 테이블 데이터 검증 DAG',
    schedule_interval='0 0 * * *',  # 매일 00:00 실행
    catchup=False,
    max_active_runs=1,
    tags=['validation', 'oracle', 'hive', 'data-quality'],
) as dag:
    
    # 시작 Task
    start_task = BashOperator(
        task_id='start',
        bash_command='echo "검증 작업 시작: {{ ds }}"',
        dag=dag,
    )
    
    # 테이블별 검증 Task Group (병렬 실행)
    with TaskGroup('table_validation_group') as validation_group:
        validation_tasks = []
        for oracle_db_name, table_name in TABLE_LIST:
            task = create_validation_task(oracle_db_name, table_name, dag)
            validation_tasks.append(task)
    
    # 결과 요약 Task
    summary_task = create_summary_task(dag)
    
    # 완료 Task
    end_task = BashOperator(
        task_id='end',
        bash_command='echo "검증 작업 완료: {{ ds }}"',
        dag=dag,
    )
    
    # Task 의존성 설정
    start_task >> validation_group >> summary_task >> end_task

