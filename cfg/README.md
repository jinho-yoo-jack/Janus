# 설정 파일 가이드

이 디렉토리에는 Janus 검증에 필요한 모든 설정 파일이 포함됩니다.

## 디렉토리 구조

```
cfg/
├── application.yml              # 공통 검증 설정 (dev/prod 환경별)
├── {db_name}/                   # 데이터베이스별 디렉토리
│   ├── database.yml             # 데이터베이스 접속 정보
│   └── tables/                  # 테이블별 설정 파일
│       ├── {table_name}.yml
│       └── ...
└── ...
```

## 파일 구조

### 공통 설정 파일 (application.yml)

환경별(dev/prod) 검증 설정을 포함합니다.

### 데이터베이스별 설정 파일

각 데이터베이스별로 디렉토리를 생성하고, 그 안에 `database.yml`과 `tables/` 디렉토리를 만듭니다:

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

## 파일명 규칙

- 테이블 설정 파일: `{table_name}.yml`
- 예: `tp_cp_master.yml`, `table_1.yml`

## 설정 파일 구조

### 데이터베이스 접속 정보 (database.yml)

각 데이터베이스별 디렉토리에 `database.yml` 파일을 생성합니다:

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

### 테이블 설정 파일 ({table_name}.yml)

각 테이블마다 `tables/{table_name}.yml` 파일을 생성합니다:

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

## 필수 필드

- `source_db_name`: 소스 데이터베이스명 (database.yml에 정의된 DB 이름)
- `target_db_name`: 타겟 데이터베이스명 (database.yml에 정의된 DB 이름)
- `table_name`: 테이블명
- `date_column`: 날짜 컬럼명
- `date_column_type`: 날짜 컬럼 데이터 타입
- `primary_keys`: Primary Key 리스트

## 선택 필드

- `exclude_columns`: 비교 대상에서 제외할 컬럼 리스트 (기본값: `[]`)
- `columns`: 읽을 컬럼 (기본값: `"*"`)
- `oracle_schema`: Oracle 테이블 스키마 (database.yml의 default_schema 사용 시 생략 가능)
- `hive_database`: Hive 테이블 데이터베이스 (database.yml의 database 사용 시 생략 가능)
- `oracle_where_clause`: Oracle 추가 WHERE 조건문 (날짜 조건과 AND로 결합)
- `hive_where_clause`: Hive 추가 WHERE 조건문 (날짜 조건과 AND로 결합)

## 날짜 컬럼 타입

`date_column_type`은 다음 중 하나를 선택할 수 있습니다:

- `yyyy-mm-dd`: 표준 날짜 형식 (예: `2024-01-15`)
- `yyyymmdd`: 숫자 형식 날짜 (예: `20240115`)
- `yyyymmdd HH24:mm:ss`: 날짜와 시간 포함, 숫자 형식 (예: `20240115 14:30:00`)
- `yyyy-mm-dd HH24:mm:ss`: 날짜와 시간 포함, 표준 형식 (예: `2024-01-15 14:30:00`)

## 제외 컬럼 (exclude_columns)

`exclude_columns`는 비교 대상에서 제외할 컬럼을 지정합니다:

- YAML 리스트 형식으로 여러 컬럼 지정 가능
- 대소문자 구분 없이 매칭 (Oracle과 Hive 모두 소문자로 변환하여 비교)
- 빈 리스트(`[]`)이거나 설정하지 않으면 모든 컬럼을 비교
- 예: `exclude_columns: ["UPDATED_DATE", "VERSION", "METADATA"]`

**사용 사례:**
- 타임스탬프 컬럼 (업데이트 시간 등)
- 버전 정보 컬럼
- 메타데이터 컬럼
- 시스템 생성 컬럼

## WHERE 조건문 (oracle_where_clause / hive_where_clause)

추가 WHERE 조건문을 지정할 수 있습니다. 날짜 조건과 AND로 자동 결합됩니다.

**사용 사례:**
- 특정 상태의 레코드만 검증
- 특정 지역/부서의 데이터만 검증
- 삭제되지 않은 레코드만 검증

**예시:**
```yaml
# Oracle WHERE 조건문
oracle_where_clause: "STATUS = 'ACTIVE' AND REGION = 'KR'"

# Hive WHERE 조건문
hive_where_clause: "status = 'active' AND region = 'kr'"
```

**주의사항:**
- 날짜 조건은 자동으로 추가되므로, WHERE 조건문에는 날짜 관련 조건을 포함하지 마세요
- Oracle과 Hive의 SQL 문법 차이를 고려하여 각각 작성하세요
- 컬럼명의 대소문자를 주의하세요 (Oracle은 대문자, Hive는 소문자일 수 있음)

## 새 테이블 추가

새 테이블을 추가하려면:

1. 해당 데이터베이스 디렉토리 확인/생성: `config/{db_name}/`
2. `config/{db_name}/tables/{table_name}.yml` 파일 생성
3. 필수 필드 작성
4. Airflow DAG의 `TABLE_LIST`에 `(db_name, table_name)` 튜플 추가

**예시:**
```python
TABLE_LIST = [
    ('pg_db', 'tp_cp_master'),
    ('momopg_db', 'tp_cp_master'),
    # ...
]
```
