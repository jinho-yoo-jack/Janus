# 프로젝트 구조

이 프로젝트는 관심사별로 모듈을 분리하여 구성되어 있습니다.

## 디렉토리 구조

```
pyspark-for-perfect/
├── config.py                 # 설정 관리 모듈
├── janus.py                   # Janus 검증 클래스
├── main.py                    # ETL 실행 진입점 (별칭)
├── config.ini.example        # 설정 파일 예시
├── requirements.txt          # 의존성 패키지
├── README.md                 # 프로젝트 문서
│
├── readers/                  # 데이터 읽기 모듈
│   ├── __init__.py
│   ├── oracle_reader.py     # Oracle 데이터 읽기
│   └── hive_reader.py        # Hive 데이터 읽기
│
├── comparison/               # 데이터 비교 모듈
│   ├── __init__.py
│   ├── data_normalizer.py   # 데이터 정규화
│   └── data_comparator.py    # 데이터 비교
│
├── reporting/                # 리포트 생성 모듈
│   ├── __init__.py
│   ├── report_generator.py  # 리포트 생성
│   └── difference_saver.py  # 차이점 저장
│
├── utils/                    # 유틸리티 모듈
│   ├── __init__.py
│   ├── logger.py            # 로깅 유틸리티
│   └── spark_session.py     # Spark 세션 유틸리티
│
├── cfg/                   # 설정 관련 모듈
│   ├── etl_config_loader.py # ETL 설정 로드
│   └── ...
│
├── main/                     # ETL 실행 진입점 패키지
│   ├── __init__.py
│   └── __main__.py           # main 함수
│
└── dags/                     # Airflow DAG
    ├── oracle_hive_validation_dag.py  # Janus 검증 DAG
    └── README_DAG.md
```

## 모듈 설명

### 1. readers/ - 데이터 읽기 모듈

**역할**: 다양한 데이터 소스에서 데이터를 읽는 책임

- `oracle_reader.py`: Oracle 데이터베이스에서 데이터를 읽는 클래스
  - PySpark JDBC를 사용하여 Oracle 테이블 읽기
  - 날짜 필터링 및 서버 측 최적화
  
- `hive_reader.py`: Hive 테이블에서 데이터를 읽는 클래스
  - Spark SQL을 사용하여 Hive 테이블 읽기
  - 파티션 기반 필터링

### 2. comparison/ - 데이터 비교 모듈

**역할**: 데이터 정규화 및 비교 로직 담당

- `data_normalizer.py`: DataFrame 정규화
  - 컬럼명 정규화 (소문자 변환)
  - NULL 값 처리
  - 복합 키 생성

- `data_comparator.py`: 데이터 비교 수행
  - 키 기반 Full Outer Join
  - 차이점 분석 (Oracle 전용, Hive 전용, 컬럼 차이)
  - 통계 수집

### 3. reporting/ - 리포트 생성 모듈

**역할**: 검증 결과 리포트 생성 및 저장

- `report_generator.py`: 리포트 생성 및 저장
  - 텍스트 형식 리포트 생성
  - 파일 시스템에 저장

- `difference_saver.py`: 차이점 레코드 저장
  - Parquet 형식으로 차이점 저장
  - HDFS 경로에 저장

### 4. utils/ - 유틸리티 모듈

**역할**: 공통 유틸리티 함수 제공

- `logger.py`: 로깅 설정
- `spark_session.py`: Spark 세션 생성

### 5. config.py - 설정 관리

**역할**: 설정 파일 읽기 및 설정 객체 생성

- `OracleConfig`: Oracle 데이터베이스 설정
- `HiveConfig`: Hive 테이블 설정
- `ValidationConfig`: 검증 설정

### 6. janus.py - Janus 검증 클래스

**역할**: Janus 검증 로직 구현

- `JanusValidator` 클래스: 데이터 정합성 검증 프로세스 실행
- 각 모듈을 조합하여 검증 프로세스 실행
- 에러 처리 및 결과 반환

### 7. main/ - ETL 실행 진입점 패키지

**역할**: ETL 실행을 위한 main 함수 제공

- `__main__.py`: main 함수 (명령줄 인자 파싱, Spark 세션 생성, ETL 실행)
- `python -m main` 또는 `python main.py`로 실행 가능

### 8. cfg/etl_config_loader.py - ETL 설정 로드

**역할**: ETL 실행에 필요한 설정 파일 로드

- `ETLConfigLoader` 클래스: 설정 파일 로드 로직
- 통합 설정 파일 및 분리된 설정 파일 구조 지원

## 설계 원칙

1. **단일 책임 원칙 (SRP)**: 각 모듈은 하나의 책임만 가짐
2. **관심사 분리**: 데이터 읽기, 비교, 리포트 생성이 분리됨
3. **의존성 역전**: 인터페이스를 통한 느슨한 결합
4. **재사용성**: 각 모듈은 독립적으로 사용 가능

## 모듈 간 의존성

```
main/__main__.py
    ├── cfg/etl_config_loader.py (ETLConfigLoader)
    │   └── config.py
    └── janus.py (JanusValidator)
        ├── utils/
        ├── readers/
        ├── comparison/
        └── reporting/
```

## 확장 방법

### 새로운 데이터 소스 추가

1. `readers/` 디렉토리에 새 리더 클래스 추가
2. 공통 인터페이스 구현 (선택사항)
3. `janus.py`의 `JanusValidator` 클래스에서 사용

### 새로운 비교 로직 추가

1. `comparison/` 디렉토리에 새 비교 클래스 추가
2. `DataComparator`를 확장하거나 새 클래스 생성
3. `janus.py`의 `JanusValidator` 클래스에서 사용

### 새로운 리포트 형식 추가

1. `reporting/` 디렉토리에 새 리포트 생성기 추가
2. `ReportGenerator`를 확장하거나 새 클래스 생성
3. `janus.py`의 `JanusValidator` 클래스에서 사용

