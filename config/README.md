# 테이블 설정 파일

이 디렉토리에는 각 테이블별 설정 파일이 포함됩니다.

## 파일 구조

각 테이블마다 하나의 `.yml` 파일을 생성합니다:

```
config/tables/
├── table_1.yml
├── table_2.yml
├── table_3.yml
└── ...
```

## 파일명 규칙

- 파일명은 `{table_name}.yml` 형식을 따릅니다.
- 예: `table_1.yml`, `table_2.yml`, `my_table.yml`

## 설정 파일 구조

각 테이블 설정 파일은 다음 필드를 포함합니다:

```yaml
# 소스 데이터베이스명 (database.yml에 정의된 DB 접속 정보 참조)
source_db_name: oracle

# 타겟 데이터베이스명 (database.yml에 정의된 DB 접속 정보 참조)
target_db_name: hive

# 테이블명
table_name: TABLE_1

# 날짜 컬럼명
date_column: CREATED_DATE

# 날짜 컬럼 데이터 타입: yyyy-mm-dd, yyyymmdd, yyyymmdd HH24:mm:ss, yyyy-mm-dd HH24:mm:ss
date_column_type: yyyy-mm-dd

# 비교 대상에서 제외할 컬럼 (선택사항)
exclude_columns: ["UPDATED_DATE", "VERSION"]

# Primary Key (복합 키 가능)
primary_keys: ["COLUMN1", "COLUMN2"]

# 읽을 컬럼 (선택사항, 기본값: "*")
# columns: "*"  # 또는 ["COLUMN1", "COLUMN2", "COLUMN3", ...]

# Oracle 테이블 스키마 (선택사항, database.yml의 default_schema 사용 시 생략 가능)
# oracle_schema: SCHEMA_NAME

# Hive 테이블 데이터베이스 (선택사항, database.yml의 database 사용 시 생략 가능)
# hive_database: default
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

## 새 테이블 추가

새 테이블을 추가하려면:

1. `config/tables/{table_name}.yml` 파일 생성
2. 필수 필드 작성
3. Airflow DAG의 `TABLE_LIST`에 테이블명 추가

## 예시 파일

- `table_1.yml.example`: 기본 설정 예시 (yyyy-mm-dd)
- `table_2.yml.example`: yyyymmdd 형식 예시
- `table_3.yml.example`: yyyymmdd HH24:mm:ss 형식 예시
- `table_4.yml.example`: yyyy-mm-dd HH24:mm:ss 형식 예시
