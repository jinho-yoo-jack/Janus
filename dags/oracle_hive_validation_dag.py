"""
Janus - Datawarehouse와 RDB 간 데이터 정합성 검증 Airflow DAG

Apache Livy를 사용하여 공유 Spark 세션으로 여러 테이블을 병렬로 검증합니다.
매일 00:00에 실행되며, 현재 날짜 - 2일 데이터를 검증합니다.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from dags.livy_shared_session import execute_validation_with_shared_livy_session
from utils.livy_session import LivySessionManager
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

# Livy 서버 URL
LIVY_URL = Variable.get(
    'livy_url',
    default_var='http://localhost:18998'
)


def create_livy_session(**context):
    """
    Livy 세션 생성 (전역 세션)
    
    Returns:
        세션 ID
    """
    import zipfile
    import tempfile
    
    oracle_jdbc_jar = Variable.get('oracle_jdbc_jar_path', default_var='/opt/spark/jars/ojdbc8.jar')
    livy_url = Variable.get('livy_url', default_var='http://localhost:18998')
    project_path = Variable.get('project_path', default_var='/opt/airflow/dags')
    config_dir = Variable.get('config_dir', default_var='/opt/airflow/dags/cfg')
    
    livy_manager = LivySessionManager(livy_url=livy_url)
    
    # pyFiles에 업로드할 파일 목록
    py_files = []
    
    # 방법 1: 검증 모듈만 추가 (간단하지만 프로젝트 경로가 Livy에서 접근 가능해야 함)
    validation_module_path = os.path.join(project_path, 'dags', 'janus_validation_module.py')
    if os.path.exists(validation_module_path):
        py_files.append(validation_module_path)
    
    # 방법 2: 프로젝트 전체를 ZIP으로 압축하여 HDFS에 업로드 (권장)
    # HDFS 경로를 사용하면 Livy 서버가 접근 가능합니다
    hdfs_zip_path = Variable.get('hdfs_zip_path', default_var=None)
    
    if hdfs_zip_path:
        # HDFS 경로가 설정되어 있으면 사용
        py_files.append(hdfs_zip_path)
        logger.info(f"HDFS ZIP 경로 사용: {hdfs_zip_path}")
    else:
        # HDFS 경로가 없으면 로컬에서 ZIP 생성 후 HDFS에 업로드
        try:
            import subprocess
            
            # 임시 ZIP 파일 생성
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_zip:
                zip_path = tmp_zip.name
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # 프로젝트의 주요 모듈들 추가
                modules_to_include = [
                    'janus.py',
                    'config.py',
                    'readers/__init__.py',
                    'readers/oracle_reader.py',
                    'readers/hive_reader.py',
                    'comparison/__init__.py',
                    'comparison/data_comparator.py',
                    'comparison/data_normalizer.py',
                    'reporting/__init__.py',
                    'reporting/report_generator.py',
                    'reporting/difference_saver.py',
                    'utils/__init__.py',
                    'utils/logger.py',
                    'utils/spark_session.py',
                    'config/etl_config_loader.py',
                    'dags/janus_validation_module.py',
                ]
                
                for module in modules_to_include:
                    module_path = os.path.join(project_path, module)
                    if os.path.exists(module_path):
                        # ZIP 내부 경로는 프로젝트 루트 기준으로 설정
                        arcname = module
                        zipf.write(module_path, arcname)
                        logger.info(f"ZIP에 추가: {module}")
                
                # 디렉토리 구조를 유지하기 위해 __init__.py 파일들도 추가
                init_files = [
                    'readers/__init__.py',
                    'comparison/__init__.py',
                    'reporting/__init__.py',
                    'utils/__init__.py',
                ]
                
                for init_file in init_files:
                    init_path = os.path.join(project_path, init_file)
                    if os.path.exists(init_path):
                        zipf.write(init_path, init_file)
            
            # HDFS에 업로드
            hdfs_upload_path = Variable.get(
                'hdfs_upload_path', 
                default_var='hdfs:///user/livy/janus_modules.zip'
            )
            
            # HDFS에 파일 업로드
            upload_cmd = ['hdfs', 'dfs', '-put', '-f', zip_path, hdfs_upload_path]
            result = subprocess.run(upload_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                py_files.append(hdfs_upload_path)
                logger.info(f"ZIP 파일을 HDFS에 업로드 완료: {hdfs_upload_path}")
            else:
                logger.warning(f"HDFS 업로드 실패: {result.stderr}")
                # 업로드 실패 시 로컬 경로 사용 (Livy 서버가 접근 가능한 경우)
                py_files.append(zip_path)
                logger.info(f"로컬 ZIP 경로 사용: {zip_path}")
            
            # 임시 파일 정리 (선택사항)
            # os.unlink(zip_path)
        
        except Exception as e:
            logger.warning(f"ZIP 파일 생성/업로드 실패, 개별 파일 방식 사용: {str(e)}")
            # ZIP 생성 실패 시 개별 파일 방식으로 폴백
            if 'zip_path' in locals() and os.path.exists(zip_path):
                try:
                    os.unlink(zip_path)
                except:
                    pass
    
    try:
        # Livy 세션 생성
        session_id = livy_manager.create_session(
            kind="pyspark",
            jars=[oracle_jdbc_jar] if os.path.exists(oracle_jdbc_jar) else None,
            py_files=py_files if py_files else None,
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
        
        # XCom에 세션 ID 저장
        return {"livy_session_id": session_id}
    
    except Exception as e:
        logger.error(f"Livy 세션 생성 실패: {str(e)}", exc_info=True)
        raise


def delete_livy_session(**context):
    """
    Livy 세션 삭제
    """
    livy_url = Variable.get('livy_url', default_var='http://localhost:18998')
    livy_manager = LivySessionManager(livy_url=livy_url)
    
    # XCom에서 세션 ID 가져오기
    ti = context['ti']
    dag_run = context['dag_run']
    create_session_task = dag_run.get_task_instance('create_livy_session')
    
    if create_session_task:
        session_info = create_session_task.xcom_pull(key='return_value')
        if session_info and 'livy_session_id' in session_info:
            livy_manager.session_id = session_info['livy_session_id']
            try:
                livy_manager.delete_session()
                logger.info(f"Livy 세션 삭제 완료: session_id={livy_manager.session_id}")
            except Exception as e:
                logger.warning(f"Livy 세션 삭제 실패: {str(e)}")
        else:
            logger.warning("세션 ID를 찾을 수 없습니다.")
    else:
        logger.warning("create_livy_session Task를 찾을 수 없습니다.")


def create_validation_task(oracle_db_name: str, table_name: str, dag) -> PythonOperator:
    """
    테이블별 검증 Task 생성 (공유 Livy 세션 사용)
    
    Args:
        oracle_db_name: Oracle 데이터베이스명 (예: pg_db)
        table_name: 테이블명 (예: tp_cp_master)
        dag: DAG 객체
    
    Returns:
        PythonOperator
    """
    def validation_wrapper(**context):
        """검증 작업 래퍼 함수"""
        # XCom에서 세션 ID 가져오기
        dag_run = context['dag_run']
        create_session_task = dag_run.get_task_instance('create_livy_session')
        
        if create_session_task:
            session_info = create_session_task.xcom_pull(key='return_value')
            livy_session_id = session_info.get('livy_session_id') if session_info else None
        else:
            livy_session_id = None
        
        return execute_validation_with_shared_livy_session(
            oracle_db_name,
            table_name,
            livy_session_id,
            **context
        )
    
    return PythonOperator(
        task_id=f'validate_{oracle_db_name}_{table_name}',
        python_callable=validation_wrapper,
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
        success_tables = []
        
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
                    # XCom에서 결과 가져오기
                    result = ti.xcom_pull(key='return_value')
                    if result and result.get('status') == 'success':
                        success_count += 1
                        success_tables.append(f"{oracle_db_name}/{table_name}")
                        logger.info(f"✅ {oracle_db_name}/{table_name}: 검증 성공")
                    else:
                        failure_count += 1
                        failed_tables.append(f"{oracle_db_name}/{table_name}")
                        logger.warning(f"❌ {oracle_db_name}/{table_name}: 검증 실패")
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
        
        if success_tables:
            logger.info(f"성공한 테이블: {', '.join(success_tables)}")
        if failed_tables:
            logger.warning(f"실패한 테이블: {', '.join(failed_tables)}")
        
        logger.info("=" * 80)
        
        # 결과를 XCom에 저장
        return {
            'total_tables': len(TABLE_LIST),
            'success_count': success_count,
            'failure_count': failure_count,
            'success_tables': success_tables,
            'failed_tables': failed_tables,
        }
    
    return PythonOperator(
        task_id='summarize_results',
        python_callable=summarize_validation_results,
        dag=dag,
    )


def generate_final_report(**context):
    """
    최종 리포트 생성
    
    검증 결과를 종합하여 최종 리포트를 생성합니다.
    """
    from airflow.models import DagRun
    
    logger.info("=" * 80)
    logger.info("최종 리포트 생성")
    logger.info("=" * 80)
    
    # 결과 요약 가져오기
    dag_run = context['dag_run']
    summary_task = dag_run.get_task_instance('summarize_results')
    
    if summary_task:
        summary_result = summary_task.xcom_pull(key='return_value')
        
        if summary_result:
            total = summary_result.get('total_tables', 0)
            success = summary_result.get('success_count', 0)
            failure = summary_result.get('failure_count', 0)
            success_tables = summary_result.get('success_tables', [])
            failed_tables = summary_result.get('failed_tables', [])
            
            logger.info(f"총 테이블 수: {total}")
            logger.info(f"성공: {success} ({success/total*100:.1f}%)" if total > 0 else "성공: {success}")
            logger.info(f"실패: {failure} ({failure/total*100:.1f}%)" if total > 0 else "실패: {failure}")
            
            if success_tables:
                logger.info(f"\n✅ 성공한 테이블 ({len(success_tables)}개):")
                for table in success_tables:
                    logger.info(f"  - {table}")
            
            if failed_tables:
                logger.warning(f"\n❌ 실패한 테이블 ({len(failed_tables)}개):")
                for table in failed_tables:
                    logger.warning(f"  - {table}")
            
            # 리포트 파일 저장 (선택사항)
            report_path = Variable.get('report_path', default_var='/opt/airflow/reports')
            if report_path:
                from datetime import datetime
                report_file = f"{report_path}/summary_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                
                os.makedirs(os.path.dirname(report_file), exist_ok=True)
                
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write("=" * 80 + "\n")
                    f.write("Janus 검증 최종 리포트\n")
                    f.write("=" * 80 + "\n")
                    f.write(f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"총 테이블 수: {total}\n")
                    f.write(f"성공: {success}\n")
                    f.write(f"실패: {failure}\n")
                    if success_tables:
                        f.write(f"\n성공한 테이블:\n")
                        for table in success_tables:
                            f.write(f"  - {table}\n")
                    if failed_tables:
                        f.write(f"\n실패한 테이블:\n")
                        for table in failed_tables:
                            f.write(f"  - {table}\n")
                    f.write("=" * 80 + "\n")
                
                logger.info(f"최종 리포트 저장: {report_file}")
    
    logger.info("=" * 80)


# DAG 정의
with DAG(
    'janus_validation',
    default_args=default_args,
    description='Oracle vs Hive 테이블 데이터 검증 DAG (Livy 공유 세션 사용)',
    schedule_interval='0 0 * * *',  # 매일 00:00 실행
    catchup=False,
    max_active_runs=1,
    tags=['validation', 'oracle', 'hive', 'data-quality', 'livy'],
) as dag:
    
    # 시작 Task
    start_task = BashOperator(
        task_id='start',
        bash_command='echo "검증 작업 시작: {{ ds }}"',
        dag=dag,
    )
    
    # Livy 세션 생성 Task
    create_session_task = PythonOperator(
        task_id='create_livy_session',
        python_callable=create_livy_session,
        dag=dag,
    )
    
    # 테이블별 검증 Task Group (병렬 실행, 공유 세션 사용)
    with TaskGroup('table_validation_group') as validation_group:
        validation_tasks = []
        for oracle_db_name, table_name in TABLE_LIST:
            task = create_validation_task(oracle_db_name, table_name, dag)
            validation_tasks.append(task)
    
    # 결과 요약 Task
    summary_task = create_summary_task(dag)
    
    # 최종 리포트 생성 Task
    generate_report_task = PythonOperator(
        task_id='generate_final_report',
        python_callable=generate_final_report,
        dag=dag,
    )
    
    # Livy 세션 삭제 Task
    delete_session_task = PythonOperator(
        task_id='delete_livy_session',
        python_callable=delete_livy_session,
        dag=dag,
        trigger_rule='all_done',  # 성공/실패 관계없이 실행
    )
    
    # 완료 Task
    end_task = BashOperator(
        task_id='end',
        bash_command='echo "검증 작업 완료: {{ ds }}"',
        dag=dag,
    )
    
    # Task 의존성 설정
    start_task >> create_session_task >> validation_group >> summary_task >> generate_report_task >> delete_session_task >> end_task
