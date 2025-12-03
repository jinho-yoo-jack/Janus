# Janus 검증 Airflow DAG 사용 가이드

## 개요

이 DAG는 Apache Livy를 사용하여 여러 테이블을 병렬로 검증하여 Oracle과 Hive 테이블 간의 데이터 일치 여부를 확인합니다.

**중요**: 이 DAG는 Apache Livy를 통해 Spark 세션을 관리합니다. Livy 서버가 실행 중이어야 합니다.

## DAG 구조

이 DAG는 Apache Livy를 사용하여 **공유 Spark 세션**으로 여러 테이블을 병렬 검증합니다:

```
start
  ↓
create_livy_session (Livy 세션 생성)
  ↓
table_validation_group (병렬 실행, 공유 세션 사용)
  ├── validate_pg_db_tp_cp_master
  ├── validate_table_2
  ├── validate_table_3
  └── ... (추가 테이블)
  ↓
summarize_results (결과 요약)
  ↓
generate_final_report (최종 리포트 생성)
  ↓
delete_livy_session (Livy 세션 삭제)
  ↓
end
```

### 실행 흐름

1. **Livy 세션 생성**: 하나의 Spark 세션을 생성하여 모든 검증 작업에서 공유
2. **병렬 검증**: 여러 테이블을 동시에 검증 (같은 Livy 세션 사용)
3. **결과 요약**: 모든 검증 결과를 종합하여 요약
4. **리포팅**: 최종 리포트 생성 및 저장
5. **세션 삭제**: 작업 완료 후 Livy 세션 정리

## 스케줄 설정

- **실행 주기**: 매일 00:00 (자정)
- **검증 대상 날짜**: 실행일 - 2일

## 설정 방법

### 1. Apache Livy 서버 확인

Livy 서버가 실행 중인지 확인하세요:

```bash
# Livy 서버 상태 확인
curl http://localhost:18998/sessions
```

### 2. Airflow Variables 설정

Airflow UI에서 다음 Variables를 설정하거나 `airflow variables set` 명령어를 사용하세요:

```bash
# Livy 서버 URL (필수)
airflow variables set livy_url http://localhost:18998

# Oracle JDBC Driver 경로
airflow variables set oracle_jdbc_jar_path /opt/spark/jars/ojdbc8.jar

# 프로젝트 경로 (Janus 코드가 있는 경로)
airflow variables set project_path /opt/airflow/dags

# 설정 파일 디렉토리 경로
airflow variables set config_dir /opt/airflow/dags/cfg

# 공통 설정 파일 경로
airflow variables set common_config_path /opt/airflow/dags/cfg/application.yml

# 환경 설정 (dev 또는 prod)
airflow variables set environment dev
```

### 3. 설정 파일 준비

`cfg/application.yml.example`과 `cfg/{db_name}/tables/*.yml.example`을 참고하여 설정 파일을 생성하세요:

```bash
# 공통 설정 파일 생성
cp cfg/application.yml.example cfg/application.yml
# cfg/application.yml 편집

# 데이터베이스별 디렉토리 및 설정 파일 생성
# 예: pg_db 데이터베이스의 경우
mkdir -p cfg/pg_db/tables
cp cfg/pg_db/database.yml.example cfg/pg_db/database.yml
# cfg/pg_db/database.yml 편집

cp cfg/pg_db/tables/tp_cp_master.yml.example cfg/pg_db/tables/tp_cp_master.yml
# cfg/pg_db/tables/tp_cp_master.yml 편집
```

각 테이블마다 하나의 YAML 파일이 필요하며, 다음 필드를 포함합니다:
- `source_db_name`: 소스 데이터베이스명 (database.yml에 정의된 DB 이름)
- `target_db_name`: 타겟 데이터베이스명 (database.yml에 정의된 DB 이름)
- `table_name`: 테이블명
- `date_column`: 날짜 컬럼명
- `date_column_type`: 날짜 컬럼 데이터 타입
- `primary_keys`: Primary Key 리스트
- `exclude_columns`: 비교 대상에서 제외할 컬럼 리스트 (선택사항)
- `oracle_where_clause`: Oracle 추가 WHERE 조건문 (선택사항)
- `hive_where_clause`: Hive 추가 WHERE 조건문 (선택사항)

### 4. 테이블 목록 수정

`dags/oracle_hive_validation_dag.py` 파일의 `TABLE_LIST` 변수를 수정하여 검증할 테이블 목록을 변경할 수 있습니다:

```python
TABLE_LIST = [
    ('pg_db', 'tp_cp_master'),  # (oracle_db_name, table_name) 튜플
    ('momopg_db', 'tp_cp_master'),
    # ... 추가 테이블
]
```

**참고**: 각 튜플은 `(oracle_db_name, table_name)` 형식입니다. 설정 파일 경로는 자동으로 `config/{oracle_db_name}/tables/{table_name}.yml`로 구성됩니다.

### 5. Spark Pool 설정 (선택사항)

동시 실행되는 검증 작업 수를 제어하려면 Airflow Pool을 생성하세요:

```bash
airflow pools set validation_pool 5  # 최대 5개 작업 동시 실행
```

## 실행 방법

### 수동 실행

Airflow UI에서 DAG를 활성화하고 "Trigger DAG" 버튼을 클릭합니다.

### CLI 실행

```bash
airflow dags trigger janus_validation
```

## 모니터링

### Airflow UI

1. **Graph View**: 전체 DAG 구조와 각 Task의 상태 확인
2. **Tree View**: 실행 이력 및 상태 확인
3. **Logs**: 각 Task의 실행 로그 확인

### 검증 결과 확인

#### 리포트 파일

각 테이블별 검증 리포트가 다음 경로에 저장됩니다:

```
{report_path}/validation_report_{table_name}_{date}.txt
```

예: `/opt/airflow/reports/validation_report_TABLE_1_2024-01-15.txt`

#### 차이점 레코드 (Parquet)

차이점이 발견된 경우, 다음 HDFS 경로에 저장됩니다:

```
{output_path}/{table_name}/oracle_only/dt={date}/
{output_path}/{table_name}/hive_only/dt={date}/
{output_path}/{table_name}/column_differences/dt={date}/
```

### 결과 요약

`summarize_results` Task의 로그에서 전체 검증 결과 요약을 확인할 수 있습니다:

```
================================================================================
검증 결과 요약
================================================================================
총 테이블 수: 10
성공: 8
실패: 2
실패한 테이블: table_3, table_7
================================================================================
```

## 문제 해결

### Task 실패 시

1. **Livy 서버 확인**: Livy 서버가 실행 중인지 확인
   ```bash
   curl http://localhost:18998/sessions
   ```

2. **로그 확인**: Airflow UI에서 실패한 Task의 로그 확인
   - Livy 세션 생성 실패 여부
   - 코드 실행 오류 메시지

3. **설정 확인**: 설정 파일의 해당 테이블 설정 확인
   - `cfg/{oracle_db_name}/tables/{table_name}.yml`
   - `cfg/{oracle_db_name}/database.yml`
   - `cfg/application.yml`

4. **연결 확인**: Oracle 및 Hive 연결 정보 확인

5. **재시도**: Airflow UI에서 Task를 수동으로 재시도

### 성능 최적화

- **Pool 크기 조정**: 동시 실행 작업 수 조정
- **Livy 세션 설정 조정**: `livy_validation_task.py`의 `create_session()` 파라미터 조정
  - `executor_memory`, `executor_cores`, `num_executors` 등
- **테이블별 분할 실행**: 큰 테이블은 별도 DAG로 분리

### 알림 설정

검증 실패 시 알림을 받으려면 `default_args`의 `email_on_failure`를 `True`로 설정하고 이메일 주소를 추가하세요:

```python
default_args = {
    'email': ['admin@example.com'],
    'email_on_failure': True,
    # ...
}
```

## 확장 방법

### 테이블 추가

1. `cfg/{oracle_db_name}/tables/{table_name}.yml` 설정 파일 생성
2. `TABLE_LIST`에 `(oracle_db_name, table_name)` 튜플 추가
3. DAG 자동으로 새 테이블 검증 Task 생성

### 커스텀 알림

`send_notification` Task를 수정하여 Slack, PagerDuty 등으로 알림을 보낼 수 있습니다.

### 다른 스케줄 설정

DAG의 `schedule_interval`을 수정:

```python
schedule_interval='0 */6 * * *',  # 6시간마다
schedule_interval='0 0 * * 1',     # 매주 월요일
```

