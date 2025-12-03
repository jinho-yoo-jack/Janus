# Janus

Janus는 Datawarehouse와 RDB 간의 데이터 정합성을 검증하는 애플리케이션입니다. 로마 신화에서 두 얼굴로 양방향을 바라보는 문의 신 야누스처럼, 두 시스템의 데이터를 동시에 감시하여 일관성을 보장합니다.

## 프로젝트 소개

Janus는 Oracle 테이블과 Hive 테이블의 데이터를 키 기반으로 상세 비교하여 차이점을 확인하는 검증 ETL입니다.

## 프로젝트 구조

이 프로젝트는 관심사별로 모듈을 분리하여 구성되어 있습니다:

- **readers/**: 데이터 읽기 모듈 (Oracle, Hive)
- **comparison/**: 데이터 비교 모듈 (정규화, 비교)
- **reporting/**: 리포트 생성 모듈 (리포트 생성, 차이점 저장)
- **utils/**: 유틸리티 모듈 (로깅, Spark 세션)
- **config.py**: 설정 관리
- **config/etl_config_loader.py**: ETL 설정 로드
- **janus.py**: Janus 검증 클래스 (검증 로직)
- **main/**: ETL 실행 진입점 패키지 (main 함수만 포함)

자세한 구조는 [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)를 참고하세요.

## 기능

- Oracle 테이블과 Hive 테이블 데이터 비교
- 키 기반 상세 비교 (복합 키 지원)
- 차이점 리포트 생성
- 차이점 레코드 저장 (Parquet 형식)
- 컬럼별 차이점 분석

## 요구사항

- Python 3.10
- PySpark 3.2.4
- Apache Spark 3.2.4
- Apache Hive 3.2.4
- Oracle JDBC Driver

## 설치

1. 의존성 패키지 설치:

```bash
pip install -r requirements.txt
```

2. 테스트 실행 (선택사항):

```bash
pytest
```

3. Oracle JDBC Driver 다운로드 및 Spark에 추가:

```bash
# ojdbc8.jar를 Spark jars 디렉토리에 복사하거나
# spark-submit 시 --jars 옵션으로 추가
```

4. 설정 파일 생성:

```bash
# 공통 설정 파일 생성
cp config/application.yml.example config/application.yml
# config/application.yml 편집

# 데이터베이스별 디렉토리 및 설정 파일 생성
# 예: pg_db 데이터베이스의 경우
mkdir -p config/pg_db/tables
cp config/pg_db/database.yml.example config/pg_db/database.yml
# config/pg_db/database.yml 편집

cp config/pg_db/tables/tp_cp_master.yml.example config/pg_db/tables/tp_cp_master.yml
# config/pg_db/tables/tp_cp_master.yml 편집
```

## 설정 파일 (YAML 형식)

### 공통 설정 (cfg/application.yml)

dev와 prod 환경별로 설정을 구분할 수 있습니다:

```yaml
dev:
  validation:
    report_path: /path/to/reports/dev
    output_path: hdfs:///validation/differences/dev
    date_offset_days: 2

prod:
  validation:
    report_path: /path/to/reports/prod
    output_path: hdfs:///validation/differences/prod
    date_offset_days: 2
```

**참고**: 기존 구조(`validation:` 섹션만 있는 경우)도 하위 호환성을 위해 지원됩니다.

### 데이터베이스 접속 정보 (cfg/{db_name}/database.yml)

모든 데이터베이스 접속 정보를 한 곳에서 관리:

```yaml
oracle:
  host: oracle.example.com
  port: 1521
  service_name: ORCL
  user: oracle_user
  password: oracle_password
  default_schema: SCHEMA_NAME

hive:
  database: default
```

### 테이블별 설정 (cfg/{db_name}/tables/{table_name}.yml)

각 테이블마다 하나의 설정 파일을 생성합니다. 설정 파일은 데이터베이스별 디렉토리 구조를 따릅니다:

```
config/
├── pg_db/
│   ├── database.yml
│   └── tables/
│       └── tp_cp_master.yml
└── momopg_db/
    ├── database.yml
    └── tables/
        └── tp_cp_master.yml
```

테이블 설정 파일 예시:

```yaml
# 소스 데이터베이스명 (database.yml에 정의된 DB 접속 정보 참조)
source_db_name: oracle

# 타겟 데이터베이스명 (database.yml에 정의된 DB 접속 정보 참조)
target_db_name: hive

# 테이블명
table_name: TP_CP_MASTER

# 날짜 컬럼명
date_column: CREATED_DATE

# 날짜 컬럼 데이터 타입: yyyy-mm-dd, yyyymmdd, yyyymmdd HH24:mm:ss, yyyy-mm-dd HH24:mm:ss
date_column_type: yyyy-mm-dd

# Primary Key (복합 키 가능)
primary_keys: ["COLUMN1", "COLUMN2"]

# 비교 대상에서 제외할 컬럼 (선택사항)
exclude_columns: ["UPDATED_DATE", "VERSION"]

# 읽을 컬럼 (선택사항, 기본값: "*")
# columns: "*"  # 또는 ["COLUMN1", "COLUMN2", "COLUMN3", ...]

# Oracle 테이블 스키마 (선택사항, database.yml의 default_schema 사용 시 생략 가능)
# oracle_schema: SCHEMA_NAME

# Hive 테이블 데이터베이스 (선택사항, database.yml의 database 사용 시 생략 가능)
# hive_database: default

# Oracle WHERE 조건문 (선택사항, 날짜 조건과 AND로 결합됨)
# 예시: "STATUS = 'ACTIVE' AND REGION = 'KR'"
# oracle_where_clause: "STATUS = 'ACTIVE' AND REGION = 'KR'"

# Hive WHERE 조건문 (선택사항, 날짜 조건과 AND로 결합됨)
# 예시: "status = 'active' AND region = 'kr'"
# hive_where_clause: "status = 'active' AND region = 'kr'"
```

### 제외 컬럼 (exclude_columns)

비교 대상에서 제외할 컬럼을 지정할 수 있습니다:

- **사용 사례**: 타임스탬프, 버전 정보, 메타데이터 등 비교가 불필요한 컬럼
- **형식**: 쉼표로 구분된 컬럼명 리스트
- **대소문자**: 대소문자 구분 없이 매칭 (자동으로 소문자로 변환하여 비교)
- **예시**: `exclude_columns = UPDATED_DATE, VERSION, METADATA`

제외된 컬럼은 Oracle과 Hive에서 데이터를 읽을 때 자동으로 제외되며, 비교 로직에서도 제외됩니다.

### 날짜 컬럼 타입

`date_column_type`은 Oracle과 Hive 테이블의 날짜 컬럼 데이터 형식을 지정합니다:

- **yyyy-mm-dd**: 표준 날짜 형식 (예: `2024-01-15`)
- **yyyymmdd**: 숫자 형식 날짜 (예: `20240115`)
- **yyyymmdd HH24:mm:ss**: 날짜와 시간 포함, 숫자 형식 (예: `20240115 14:30:00`)
- **yyyy-mm-dd HH24:mm:ss**: 날짜와 시간 포함, 표준 형식 (예: `2024-01-15 14:30:00`)

각 테이블별로 Oracle과 Hive의 날짜 형식이 다를 수 있으므로, 테이블 설정 파일에 각각 설정합니다.

자세한 내용은 [config/README.md](config/README.md)를 참고하세요.

### 검증 설정

```ini
[validation]
report_path = /path/to/reports
output_path = hdfs:///validation/differences
date_offset_days = 2
```

## 사용 방법

### 명령줄 인자

ETL 실행 시 다음 인자를 사용할 수 있습니다:

| 인자 | 필수 | 기본값 | 설명 |
|------|------|--------|------|
| `--oracle-db-name` | ✅ | - | Oracle 데이터베이스명 (예: `pg_db`, `momopg_db`) |
| `--table-name` | ✅ | - | 검증할 테이블명 (예: `tp_cp_master`) |
| `--config-dir` | ❌ | `cfg` | 설정 파일 디렉토리 경로 |
| `--common-config` | ❌ | `cfg/application.yml` | 공통 설정 파일 경로 |
| `--env` | ❌ | `dev` | 환경 (dev 또는 prod) |

### 실행 방법

#### 방법 1: Python 모듈로 실행 (권장)

```bash
python -m main \
  --oracle-db-name pg_db \
  --table-name tp_cp_master \
  --config-dir cfg \
  --common-config cfg/application.yml \
  --env dev
```

#### 방법 2: Python 스크립트로 실행

```bash
python main.py \
  --oracle-db-name pg_db \
  --table-name tp_cp_master \
  --config-dir cfg \
  --common-config cfg/application.yml
```

#### 방법 3: Spark Submit으로 실행

```bash
spark-submit \
  --jars /path/to/ojdbc8.jar \
  --driver-class-path /path/to/ojdbc8.jar \
  main.py \
  --oracle-db-name pg_db \
  --table-name tp_cp_master \
  --config-dir cfg \
  --common-config cfg/application.yml
```

#### 방법 4: Spark Submit (모듈 실행)

```bash
spark-submit \
  --jars /path/to/ojdbc8.jar \
  --driver-class-path /path/to/ojdbc8.jar \
  --py-files config.py,janus.py \
  -m main \
  --oracle-db-name pg_db \
  --table-name tp_cp_master \
  --config-dir cfg \
  --common-config cfg/application.yml
```

### 설정 파일 구조

실행 시 다음 경로에서 설정 파일을 찾습니다:

```
{config-dir}/
├── application.yml               # 공통 설정
└── {oracle-db-name}/
    ├── database.yml              # 데이터베이스 접속 정보
    └── tables/
        └── {table-name}.yml      # 테이블별 설정 파일
```

**참고**: 설정 파일 경로는 `cfg/{oracle-db-name}/tables/{table-name}.yml` 형식을 따릅니다.

### 실행 예시

#### 예시 1: 기본 설정으로 실행 (dev 환경)

```bash
python -m main \
  --oracle-db-name pg_db \
  --table-name tp_cp_master
```

#### 예시 1-1: prod 환경으로 실행

```bash
python -m main \
  --oracle-db-name pg_db \
  --table-name tp_cp_master \
  --env prod
```

#### 예시 2: 커스텀 설정 경로로 실행

```bash
python -m main \
  --oracle-db-name pg_db \
  --table-name tp_cp_master \
  --config-dir /path/to/custom/cfg \
  --common-config /path/to/custom/cfg/application.yml
```

#### 예시 3: 여러 테이블 순차 실행

```bash
# 테이블 1
python -m main --oracle-db-name pg_db --table-name table_1

# 테이블 2
python -m main --oracle-db-name pg_db --table-name table_2

# 테이블 3
python -m main --oracle-db-name pg_db --table-name table_3
```

### Airflow DAG에서 사용

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

dag = DAG(
    'janus_validation',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily'
)

validation_task = SparkSubmitOperator(
    task_id='janus_validate',
    application='/path/to/main.py',
    jars='/path/to/ojdbc8.jar',
    driver_class_path='/path/to/ojdbc8.jar',
    dag=dag
)
```

## 검증 로직

1. **날짜 계산**: 현재 날짜로부터 2일 전 데이터를 검증 대상으로 설정
2. **Oracle 데이터 읽기**: 지정된 날짜의 Oracle 테이블 데이터를 읽음 (제외 컬럼 자동 제외)
3. **Hive 데이터 읽기**: 동일한 날짜의 Hive 테이블 데이터를 읽음 (제외 컬럼 자동 제외)
4. **키 기반 비교**: Primary Key를 기준으로 Full Outer Join 수행
5. **차이점 분석**:
   - Oracle에만 있는 레코드
   - Hive에만 있는 레코드
   - 양쪽에 모두 있지만 값이 다른 레코드 (컬럼별 차이)

**제외 컬럼 처리**: 설정 파일의 `exclude_columns`에 지정된 컬럼은 데이터 읽기 단계에서 자동으로 제외되어 비교 대상에서 제외됩니다.

## 출력 결과

### 리포트 파일

검증 결과 리포트가 텍스트 파일로 저장됩니다:

```
validation_report_2024-01-15.txt
```

### 차이점 레코드 (Parquet)

차이점이 발견된 경우, 다음 경로에 Parquet 파일로 저장됩니다:

- `{output_path}/oracle_only/dt={target_date}/` - Oracle에만 있는 레코드
- `{output_path}/hive_only/dt={target_date}/` - Hive에만 있는 레코드
- `{output_path}/column_differences/dt={target_date}/` - 컬럼 값이 다른 레코드

## 리포트 및 결과물

Janus는 검증 결과를 두 가지 형태로 제공합니다:

1. **텍스트 리포트 파일**: 검증 결과 요약 정보
2. **Parquet 파일**: 차이점이 발견된 실제 레코드 데이터

### 리포트 파일 (텍스트)

검증 결과 리포트는 텍스트 파일로 저장되며, 다음 정보를 포함합니다:

- **기본 정보**: 생성 시간, 대상 날짜, 테이블 정보
- **데이터 통계**: 레코드 수, 양쪽 존재 여부, 전용 레코드 수
- **컬럼 정보**: 공통 컬럼, 전용 컬럼 목록
- **컬럼별 차이점**: 각 컬럼별로 차이가 있는 레코드 수
- **검증 결과**: 최종 일치/불일치 상태

#### 리포트 예시

```
================================================================================
Oracle vs Hive 테이블 데이터 비교 리포트
================================================================================
생성 시간: 2024-01-17 10:30:00
대상 날짜: 2024-01-15

Oracle 테이블: SCHEMA.TP_CP_MASTER
Hive 테이블: default.tp_cp_master

--------------------------------------------------------------------------------
데이터 통계
--------------------------------------------------------------------------------
Oracle 레코드 수: 10,000
Hive 레코드 수: 9,950
양쪽 모두 존재: 9,900
Oracle에만 존재: 100
Hive에만 존재: 50

--------------------------------------------------------------------------------
컬럼 정보
--------------------------------------------------------------------------------
공통 컬럼 수: 15
Oracle 전용 컬럼: ['oracle_metadata']
Hive 전용 컬럼: ['hive_partition_date']

--------------------------------------------------------------------------------
컬럼별 차이점
--------------------------------------------------------------------------------
  customer_name: 25개 레코드 차이
  amount: 12개 레코드 차이
  status: 8개 레코드 차이

--------------------------------------------------------------------------------
검증 결과
--------------------------------------------------------------------------------
상태: ❌ 차이점 발견

⚠️  데이터 불일치가 발견되었습니다.

상세 내용 확인:

  • Oracle 전용 레코드: hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/oracle_only/dt=2024-01-15
  • Hive 전용 레코드: hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/hive_only/dt=2024-01-15
  • 컬럼 차이 레코드: hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/column_differences/dt=2024-01-15

  Parquet 파일 조회 방법:
    spark.read.parquet('{경로}').show()
    또는 HDFS에서 직접 확인: hdfs dfs -ls {경로}
================================================================================
```

**리포트 파일 경로**: `{report_path}/validation_report_{table_name}_{date}.txt`

예시: `/opt/airflow/reports/dev/validation_report_TP_CP_MASTER_2024-01-15.txt`

**중요**: 리포트 파일에는 요약 정보만 포함되며, **상세 레코드 데이터는 리포트 하단에 표시된 HDFS 경로의 Parquet 파일에서 확인**할 수 있습니다.

### 차이점 레코드 (Parquet 파일)

차이점이 발견된 경우, 상세 레코드 데이터가 Parquet 형식으로 HDFS에 저장됩니다:

#### 저장 경로 구조

```
{output_path}/{table_name}/
├── oracle_only/
│   └── dt=2024-01-15/
│       └── part-00000-xxx.parquet  # Oracle에만 있는 레코드
├── hive_only/
│   └── dt=2024-01-15/
│       └── part-00000-xxx.parquet  # Hive에만 있는 레코드
└── column_differences/
    └── dt=2024-01-15/
        └── part-00000-xxx.parquet  # 양쪽에 있지만 값이 다른 레코드
```

예시: `hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/oracle_only/dt=2024-01-15/`

#### Parquet 파일 내용

각 Parquet 파일에는 다음 정보가 포함됩니다:

1. **oracle_only**: Oracle에만 존재하는 레코드
   - 모든 컬럼 데이터 포함
   - Primary Key로 식별 가능

2. **hive_only**: Hive에만 존재하는 레코드
   - 모든 컬럼 데이터 포함
   - Primary Key로 식별 가능

3. **column_differences**: 양쪽에 존재하지만 값이 다른 레코드
   - Oracle과 Hive의 모든 컬럼 포함 (접두사: `oracle.`, `hive.`)
   - 어떤 컬럼에서 차이가 있는지 확인 가능

#### Parquet 파일 조회 예시

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadDifference").getOrCreate()

# Oracle 전용 레코드 읽기
oracle_only = spark.read.parquet(
    "hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/oracle_only/dt=2024-01-15"
)

# Hive 전용 레코드 읽기
hive_only = spark.read.parquet(
    "hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/hive_only/dt=2024-01-15"
)

# 컬럼 차이 레코드 읽기
column_diff = spark.read.parquet(
    "hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/column_differences/dt=2024-01-15"
)

# Oracle과 Hive 값 비교
column_diff.select(
    "oracle.customer_id",
    "oracle.customer_name as oracle_name",
    "hive.customer_name as hive_name",
    "oracle.amount as oracle_amount",
    "hive.amount as hive_amount"
).show()
```

#### 결과 해석 가이드

1. **oracle_only 레코드가 있는 경우**
   - Oracle에는 있지만 Hive로 전송되지 않은 데이터
   - ETL 프로세스 확인 필요

2. **hive_only 레코드가 있는 경우**
   - Hive에는 있지만 Oracle에 없는 데이터
   - 데이터 소스 또는 ETL 로직 확인 필요

3. **column_differences 레코드가 있는 경우**
   - 양쪽에 존재하지만 특정 컬럼 값이 다른 데이터
   - 데이터 변환 로직 또는 타임스탬프 차이 확인 필요

### 상세 내용 확인 방법

리포트에서 "⚠️  데이터 불일치가 발견되었습니다." 메시지가 표시되면, 리포트 하단에 **상세 레코드 저장 경로**가 자동으로 표시됩니다.

#### 1단계: 리포트 파일 확인 (요약 정보)

```bash
# 리포트 파일 확인
cat /opt/airflow/reports/dev/validation_report_TP_CP_MASTER_2024-01-15.txt
```

리포트 하단에 다음과 같은 경로가 표시됩니다:
```
상세 내용 확인:

  • Oracle 전용 레코드: hdfs://.../oracle_only/dt=2024-01-15
  • Hive 전용 레코드: hdfs://.../hive_only/dt=2024-01-15
  • 컬럼 차이 레코드: hdfs://.../column_differences/dt=2024-01-15
```

#### 2단계: 상세 레코드 확인 (Parquet 파일)

리포트에 표시된 경로의 Parquet 파일에서 실제 차이점 레코드를 확인할 수 있습니다.

**방법 1: Spark/PySpark로 조회**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckDifferences").getOrCreate()

# Oracle 전용 레코드 확인
oracle_only = spark.read.parquet(
    "hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/oracle_only/dt=2024-01-15"
)
oracle_only.show(100)  # 상위 100개 레코드 확인

# 컬럼 차이 레코드 확인 (Oracle과 Hive 값 비교)
column_diff = spark.read.parquet(
    "hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/column_differences/dt=2024-01-15"
)
# Oracle과 Hive 값 비교
column_diff.select(
    "oracle.customer_id",
    "oracle.customer_name as oracle_name",
    "hive.customer_name as hive_name",
    "oracle.amount as oracle_amount",
    "hive.amount as hive_amount"
).show(100)
```

**방법 2: Spark SQL로 조회**

```bash
spark-sql --master yarn <<EOF
-- Oracle 전용 레코드 확인
SELECT * FROM parquet.`hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/oracle_only/dt=2024-01-15`
LIMIT 100;

-- 컬럼 차이 레코드 확인
SELECT 
  oracle.customer_id,
  oracle.customer_name as oracle_name,
  hive.customer_name as hive_name,
  oracle.amount as oracle_amount,
  hive.amount as hive_amount
FROM parquet.`hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/column_differences/dt=2024-01-15`
LIMIT 100;
EOF
```

**방법 3: HDFS 명령어로 파일 확인**

```bash
# 파일 목록 확인
hdfs dfs -ls hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/oracle_only/dt=2024-01-15

# 파일 크기 확인
hdfs dfs -du -h hdfs://kcp-hadoop-cluster/validation/differences/dev/TP_CP_MASTER/oracle_only/dt=2024-01-15
```

#### 3단계: Airflow 로그 확인

Airflow UI에서 각 Task의 로그를 확인하여 실행 과정과 상세 경로를 확인할 수 있습니다:

1. Airflow UI → DAG → Task 선택
2. "Log" 버튼 클릭
3. 로그에서 리포트 내용과 저장 경로 확인

### 결과 확인 요약

| 확인 항목 | 위치 | 내용 |
|----------|------|------|
| **요약 정보** | 리포트 파일 (`{report_path}/validation_report_*.txt`) | 통계, 컬럼 정보, 차이점 개수 |
| **상세 레코드** | Parquet 파일 (HDFS 경로) | 실제 차이점이 있는 레코드 데이터 |
| **실행 로그** | Airflow UI 로그 | 실행 과정 및 상세 경로 |

## 종료 코드

- `0`: 검증 성공 (모든 데이터 일치)
- `1`: 검증 실패 (데이터 불일치 발견 또는 오류 발생)

## Airflow DAG 사용

### DAG 파일 위치

```
dags/oracle_hive_validation_dag.py
```

**참고**: DAG ID는 `janus_validation`입니다.

### Apache Livy 사용

이 DAG는 **Apache Livy**를 사용하여 Spark 세션을 관리합니다. Livy는 REST API를 통해 Spark 작업을 실행하는 서버입니다.

#### Livy 서버 요구사항

- Livy 서버가 실행 중이어야 합니다 (기본 포트: 18998)
- Livy 서버 URL: `http://localhost:18998`

#### Livy 세션 관리

이 DAG는 **공유 Livy 세션** 방식을 사용합니다:
- 하나의 Livy 세션을 생성하여 모든 검증 작업에서 공유
- 세션 ID는 XCom을 통해 각 Task에 전달
- 모든 검증 작업 완료 후 세션 삭제
- 세션 생성 및 관리는 `utils/livy_session.py`의 `LivySessionManager` 클래스가 담당합니다

### DAG 실행 흐름

DAG는 다음 순서로 실행됩니다:

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

#### 단계별 설명

1. **create_livy_session**
   - Livy 서버에 Spark 세션을 생성합니다
   - 세션 ID를 XCom에 저장하여 후속 Task에서 사용할 수 있도록 합니다
   - 세션이 준비될 때까지 대기합니다 (기본 타임아웃: 300초)

2. **table_validation_group** (병렬 실행)
   - 여러 테이블을 동시에 검증합니다
   - 각 검증 Task는 공유 Livy 세션을 사용합니다
   - XCom에서 세션 ID를 가져와서 사용합니다
   - `dags/livy_shared_session.py`의 `execute_validation_with_shared_livy_session` 함수가 실행됩니다

3. **summarize_results**
   - 모든 검증 결과를 종합하여 요약합니다
   - 성공/실패 테이블 수를 집계합니다
   - 결과를 XCom에 저장합니다

4. **generate_final_report**
   - 검증 결과를 종합하여 최종 리포트를 생성합니다
   - 리포트 파일을 지정된 경로에 저장합니다 (Airflow Variable `report_path` 설정 필요)

5. **delete_livy_session**
   - 모든 작업 완료 후 Livy 세션을 삭제합니다
   - `trigger_rule='all_done'`으로 설정되어 있어 성공/실패 관계없이 실행됩니다

#### 공유 세션의 장점

- **리소스 효율성**: 하나의 Spark 세션을 재사용하여 세션 생성 오버헤드 감소
- **세션 관리 명확화**: 세션 생성/삭제를 명확히 분리하여 관리 용이
- **결과 추적**: 각 검증 결과를 XCom으로 전달하여 리포팅에 활용

### 설정

1. **Apache Livy 서버 확인**:

```bash
# Livy 서버 상태 확인
curl http://localhost:18998/sessions
```

2. **Airflow Variables 설정**:

```bash
# Livy 서버 URL (필수)
airflow variables set livy_url http://localhost:18998

# Oracle JDBC Driver 경로
airflow variables set oracle_jdbc_jar_path /opt/spark/jars/ojdbc8.jar

# 프로젝트 경로 (Janus 코드가 있는 경로)
airflow variables set project_path /opt/airflow/dags

# 설정 파일 디렉토리 경로
airflow variables set config_dir /opt/airflow/dags/config

# 공통 설정 파일 경로
airflow variables set common_config_path /opt/airflow/dags/config/application.yml

# 환경 설정 (dev 또는 prod)
airflow variables set environment dev

# 리포트 저장 경로 (선택사항, generate_final_report Task에서 사용)
airflow variables set report_path /opt/airflow/reports
```

#### Airflow Variables 설명

| Variable | 필수 | 기본값 | 설명 |
|----------|------|--------|------|
| `livy_url` | ✅ | `http://localhost:18998` | Livy 서버 URL |
| `oracle_jdbc_jar_path` | ❌ | `/opt/spark/jars/ojdbc8.jar` | Oracle JDBC Driver JAR 파일 경로 |
| `project_path` | ❌ | `/opt/airflow/dags` | Janus 프로젝트 코드 경로 |
| `config_dir` | ❌ | `/opt/airflow/dags/cfg` | 설정 파일 디렉토리 경로 |
| `common_config_path` | ❌ | `/opt/airflow/dags/cfg/application.yml` | 공통 설정 파일 경로 |
| `environment` | ❌ | `dev` | 환경 설정 (dev 또는 prod) |
| `report_path` | ❌ | `/opt/airflow/reports` | 최종 리포트 저장 경로 |

2. **설정 파일 준비**:

```bash
# 공통 설정 파일 생성
cp config/application.yml.example config/application.yml
# config/application.yml 편집

# 데이터베이스별 디렉토리 및 설정 파일 생성
# 예: pg_db 데이터베이스의 경우
mkdir -p config/pg_db/tables
cp config/pg_db/database.yml.example config/pg_db/database.yml
# config/pg_db/database.yml 편집

cp config/pg_db/tables/tp_cp_master.yml.example config/pg_db/tables/tp_cp_master.yml
# config/pg_db/tables/tp_cp_master.yml 편집
```

3. **테이블 목록 수정**:

`dags/oracle_hive_validation_dag.py`의 `TABLE_LIST`를 수정:

```python
TABLE_LIST = [
    ('pg_db', 'tp_cp_master'),  # (oracle_db_name, table_name) 튜플
    ('pg_db', 'table_2'),
    ('momopg_db', 'table_3'),
    # ... 추가 테이블
]
```

**참고**: 각 튜플은 `(oracle_db_name, table_name)` 형식입니다. 설정 파일 경로는 자동으로 `cfg/{oracle_db_name}/tables/{table_name}.yml`로 구성됩니다.

### 실행

- **스케줄**: 매일 00:00 자동 실행 (`schedule_interval='0 0 * * *'`)
- **수동 실행**: Airflow UI에서 "Trigger DAG" 클릭
- **최대 동시 실행**: 1개 (`max_active_runs=1`)

### 실행 모니터링

#### Airflow UI에서 확인

1. **DAG 실행 상태**: Airflow UI → DAGs → `janus_validation` → 실행 상태 확인
2. **Task별 로그**: 각 Task를 클릭하여 상세 로그 확인
3. **XCom 데이터**: Task 간 전달되는 데이터 (세션 ID, 검증 결과 등) 확인

#### 주요 확인 포인트

- **create_livy_session**: 세션 ID가 정상적으로 생성되었는지 확인
- **table_validation_group**: 각 테이블 검증이 성공적으로 완료되었는지 확인
- **summarize_results**: 검증 결과 요약이 정상적으로 생성되었는지 확인
- **generate_final_report**: 최종 리포트 파일이 생성되었는지 확인
- **delete_livy_session**: 세션이 정상적으로 삭제되었는지 확인

#### 로그에서 확인할 수 있는 정보

- Livy 세션 생성/삭제 상태
- 각 테이블 검증 결과 (성공/실패)
- 검증 결과 요약 (성공/실패 테이블 수)
- 최종 리포트 저장 경로

자세한 내용은 `dags/README_DAG.md`를 참고하세요.

## Oracle 데이터 읽기 방식

이 프로젝트는 PySpark의 DataFrame API를 사용하여 Oracle 데이터를 읽습니다:

- **JDBC 데이터 소스**: PySpark의 `spark.read.format("jdbc")` 사용
- **서버 측 필터링**: Oracle 서버에서 날짜 필터링을 수행하여 네트워크 트래픽 최소화
- **병렬 처리**: `numPartitions` 옵션으로 병렬 읽기 지원
- **배치 읽기**: `fetchsize` 옵션으로 배치 크기 최적화

### 성능 최적화 옵션

대용량 테이블의 경우 `config.py`의 `read_oracle_table` 함수에 다음 옵션을 추가할 수 있습니다:

- `num_partitions`: 병렬 읽기 파티션 수 (기본값: 10)
- `partition_column`: 파티션에 사용할 컬럼 (예: ID 컬럼)
- `lower_bound`: 파티션 하한값
- `upper_bound`: 파티션 상한값

## 주의사항

1. Oracle JDBC Driver가 Spark 클래스패스에 포함되어 있어야 합니다.
2. Hive Metastore에 접근할 수 있어야 합니다.
3. 대상 날짜의 데이터가 양쪽 테이블에 모두 존재해야 정확한 비교가 가능합니다.
4. Primary Key가 올바르게 설정되어 있어야 합니다.
5. 날짜 컬럼의 형식이 일치해야 합니다 (Oracle: DATE 형식, Hive: 문자열 형식 'YYYY-MM-DD').
6. 대용량 테이블의 경우 파티션 옵션을 설정하여 성능을 향상시킬 수 있습니다.

## 문제 해결

### Oracle 연결 오류

- JDBC URL이 올바른지 확인
- 방화벽 및 네트워크 설정 확인
- 사용자 권한 확인

### Hive 테이블 읽기 오류

- Hive Metastore URI 확인
- 테이블 존재 여부 확인
- 파티션 컬럼명 확인

### 메모리 부족

- Spark executor 메모리 증가
- 파티션 수 조정

### Airflow DAG 오류

- Airflow Variables 설정 확인
- 설정 파일 경로 확인
- Spark Submit Operator 설정 확인

## 테스트

프로젝트에는 각 모듈별로 테스트 코드가 포함되어 있습니다.

### 테스트 실행

```bash
# 모든 테스트 실행
pytest

# 특정 테스트 파일 실행
pytest tests/test_data_comparator.py

# 커버리지 포함 실행
pytest --cov=. --cov-report=html
```

### 테스트 구조

- `tests/test_data_normalizer.py`: 데이터 정규화 테스트
- `tests/test_data_comparator.py`: 데이터 비교 테스트
- `tests/test_oracle_reader.py`: Oracle Reader 테스트
- `tests/test_hive_reader.py`: Hive Reader 테스트
- `tests/test_report_generator.py`: 리포트 생성 테스트
- `tests/test_difference_saver.py`: 차이점 저장 테스트
- `tests/test_janus.py`: 통합 테스트
- `tests/test_config.py`: 설정 모듈 테스트

자세한 내용은 [tests/README.md](tests/README.md)를 참고하세요.

