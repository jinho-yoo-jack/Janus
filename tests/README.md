# 테스트 가이드

이 디렉토리에는 프로젝트의 모든 테스트 코드가 포함되어 있습니다.

## 테스트 구조

```
tests/
├── __init__.py
├── conftest.py              # 공통 픽스처
├── test_config.py          # 설정 모듈 테스트
├── test_data_normalizer.py # 데이터 정규화 테스트
├── test_data_comparator.py # 데이터 비교 테스트
├── test_oracle_reader.py   # Oracle Reader 테스트
├── test_hive_reader.py     # Hive Reader 테스트
├── test_report_generator.py # 리포트 생성 테스트
├── test_difference_saver.py # 차이점 저장 테스트
└── test_janus.py  # Janus 통합 테스트
```

## 테스트 실행

### 모든 테스트 실행

```bash
pytest
```

### 특정 테스트 파일 실행

```bash
pytest tests/test_data_comparator.py
```

### 특정 테스트 함수 실행

```bash
pytest tests/test_data_comparator.py::test_compare_identical_dataframes
```

### 커버리지 포함 실행

```bash
pytest --cov=. --cov-report=html
```

### 마커를 사용한 테스트 실행

```bash
# 단위 테스트만 실행
pytest -m unit

# 통합 테스트만 실행
pytest -m integration
```

## 테스트 작성 가이드

### 1. 테스트 함수 네이밍

- 테스트 함수는 `test_`로 시작해야 합니다.
- 함수명은 테스트하는 기능을 명확히 설명해야 합니다.

```python
def test_normalize_column_names(spark_session):
    """컬럼명 정규화 테스트"""
    pass
```

### 2. 픽스처 사용

`conftest.py`에 정의된 픽스처를 사용하세요:

- `spark_session`: Spark 세션
- `sample_oracle_config`: 샘플 Oracle 설정
- `sample_hive_config`: 샘플 Hive 설정
- `sample_validation_config`: 샘플 검증 설정
- `sample_dataframe`: 샘플 DataFrame

### 3. Mock 사용

외부 의존성(데이터베이스, 파일 시스템 등)은 Mock을 사용하여 테스트합니다.

```python
from unittest.mock import Mock, patch

@patch('readers.oracle_reader.SparkSession')
def test_read_from_oracle(mock_spark_session):
    # Mock 설정
    pass
```

### 4. Assertion 작성

명확한 assertion을 작성하세요:

```python
assert result["oracle_row_count"] == 100
assert "oracle_only" in str(mock_parquet.call_args)
```

## 테스트 커버리지

현재 테스트 커버리지 목표: **80% 이상**

커버리지 리포트 확인:

```bash
pytest --cov=. --cov-report=html
# htmlcov/index.html 파일을 브라우저에서 열기
```

## CI/CD 통합

GitHub Actions 등 CI/CD 파이프라인에서 테스트를 실행할 수 있습니다:

```yaml
- name: Run tests
  run: |
    pytest --cov=. --cov-report=xml
```

## 문제 해결

### Spark 세션 생성 오류

테스트 환경에서 Spark가 설치되지 않은 경우:

```bash
# PySpark 설치 확인
pip install pyspark==3.2.4
```

### Import 오류

프로젝트 루트에서 테스트를 실행하세요:

```bash
cd /path/to/pyspark-for-perfect
pytest
```

### Mock 관련 오류

Mock 패치 경로를 확인하세요. 실제 import 경로와 일치해야 합니다.

