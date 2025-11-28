# Oracle vs Hive 검증 Airflow DAG 사용 가이드

## 개요

이 DAG는 10개의 테이블을 병렬로 검증하여 Oracle과 Hive 테이블 간의 데이터 일치 여부를 확인합니다.

## DAG 구조

```
start
  ↓
table_validation_group (병렬 실행)
  ├── validate_table_1
  ├── validate_table_2
  ├── validate_table_3
  ├── validate_table_4
  ├── validate_table_5
  ├── validate_table_6
  ├── validate_table_7
  ├── validate_table_8
  ├── validate_table_9
  └── validate_table_10
  ↓
summarize_results
  ↓
end
```

## 스케줄 설정

- **실행 주기**: 매일 00:00 (자정)
- **검증 대상 날짜**: 실행일 - 2일

## 설정 방법

### 1. Airflow Variables 설정

Airflow UI에서 다음 Variables를 설정하거나 `airflow variables set` 명령어를 사용하세요:

```bash
# Oracle JDBC Driver 경로
airflow variables set oracle_jdbc_jar_path /opt/spark/jars/ojdbc8.jar

# 검증 스크립트 경로
airflow variables set validation_script_path /opt/airflow/dags/etl_runner.py

# 공통 설정 파일 경로
airflow variables set common_config_path /opt/airflow/dags/config/common.yml

# 데이터베이스 접속 정보 파일 경로
airflow variables set database_config_path /opt/airflow/dags/config/database.yml

# 테이블 설정 파일 디렉토리
airflow variables set table_config_dir /opt/airflow/dags/config/tables
```

### 2. 설정 파일 준비

`config/common.yml.example`과 `config/tables/*.yml.example`을 참고하여 설정 파일을 생성하세요:

```bash
# 공통 설정 파일 생성
cp config/common.yml.example config/common.yml
# config/common.yml 편집

# 데이터베이스 접속 정보 파일 생성
cp config/database.yml.example config/database.yml
# config/database.yml 편집

# 테이블별 설정 파일 생성
cp config/tables/table_1.yml.example config/tables/table_1.yml
# config/tables/table_1.yml 편집
```

각 테이블마다 하나의 YAML 파일이 필요하며, 다음 필드를 포함합니다:
- `source_db_name`: 소스 데이터베이스명 (database.yml에 정의된 DB 이름)
- `target_db_name`: 타겟 데이터베이스명 (database.yml에 정의된 DB 이름)
- `table_name`: 테이블명
- `date_column`: 날짜 컬럼명
- `date_column_type`: 날짜 컬럼 데이터 타입
- `primary_keys`: Primary Key 리스트
- `exclude_columns`: 비교 대상에서 제외할 컬럼 리스트 (선택사항)

### 3. 테이블 목록 수정

`dags/oracle_hive_validation_dag.py` 파일의 `TABLE_LIST` 변수를 수정하여 검증할 테이블 목록을 변경할 수 있습니다:

```python
TABLE_LIST = [
    'table_1',
    'table_2',
    # ... 추가 테이블
]
```

### 4. Spark Pool 설정 (선택사항)

동시 실행되는 검증 작업 수를 제어하려면 Airflow Pool을 생성하세요:

```bash
airflow pools set validation_pool 5  # 최대 5개 작업 동시 실행
```

## 실행 방법

### 수동 실행

Airflow UI에서 DAG를 활성화하고 "Trigger DAG" 버튼을 클릭합니다.

### CLI 실행

```bash
airflow dags trigger oracle_hive_validation
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

1. **로그 확인**: Airflow UI에서 실패한 Task의 로그 확인
2. **설정 확인**: `config.ini` 파일의 해당 테이블 설정 확인
3. **연결 확인**: Oracle 및 Hive 연결 정보 확인
4. **재시도**: Airflow UI에서 Task를 수동으로 재시도

### 성능 최적화

- **Pool 크기 조정**: 동시 실행 작업 수 조정
- **Spark 설정 조정**: `SPARK_CONFIG`에서 메모리 및 코어 수 조정
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

1. `config.ini`에 새 테이블 설정 추가
2. `TABLE_LIST`에 테이블명 추가
3. DAG 자동으로 새 테이블 검증 Task 생성

### 커스텀 알림

`send_notification` Task를 수정하여 Slack, PagerDuty 등으로 알림을 보낼 수 있습니다.

### 다른 스케줄 설정

DAG의 `schedule_interval`을 수정:

```python
schedule_interval='0 */6 * * *',  # 6시간마다
schedule_interval='0 0 * * 1',     # 매주 월요일
```

