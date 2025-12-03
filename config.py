"""
설정 파일 관리 모듈
"""

import yaml
from dataclasses import dataclass
from typing import List, Optional, Dict
import os


@dataclass
class DatabaseConfig:
    """데이터베이스 접속 정보"""
    jdbc_url: str
    user: str
    password: str
    default_schema: Optional[str] = None
    default_database: Optional[str] = None  # Hive용
    
    @classmethod
    def from_yaml(cls, config_path: str, db_name: str) -> "DatabaseConfig":
        """YAML 파일에서 DB 접속 정보 로드"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if db_name not in config:
            raise ValueError(f"설정 파일 {config_path}에 '{db_name}' DB 접속 정보가 없습니다.")
        
        db_info = config[db_name]
        
        # JDBC URL 구성 (Oracle의 경우)
        if db_name == "oracle":
            host = db_info.get("host", "")
            port = db_info.get("port", "1521")
            service_name = db_info.get("service_name", "")
            
            if host and service_name:
                jdbc_url = f"jdbc:oracle:thin:@{host}:{port}/{service_name}"
            else:
                jdbc_url = db_info.get("jdbc_url", "")
                if not jdbc_url:
                    raise ValueError("JDBC URL이 설정되지 않았습니다.")
            
            return cls(
                jdbc_url=jdbc_url,
                user=db_info.get("user", ""),
                password=db_info.get("password", ""),
                default_schema=db_info.get("default_schema", None)
            )
        elif db_name == "hive":
            # Hive는 Spark SQL을 통해 접근하므로 JDBC URL 불필요
            return cls(
                jdbc_url="",  # Hive는 사용하지 않음
                user="",      # Hive는 사용하지 않음
                password="",  # Hive는 사용하지 않음
                default_database=db_info.get("database", "default")
            )
        else:
            raise ValueError(f"지원하지 않는 데이터베이스: {db_name}")


@dataclass
class TableConfig:
    """테이블별 검증 설정"""
    source_db_name: str
    target_db_name: str
    table_name: str  # Oracle 테이블명
    hive_table_name: str  # Hive 테이블명 (선택사항, 없으면 table_name 사용)
    date_column: str
    date_column_type: str
    exclude_columns: List[str]
    primary_keys: List[str]
    columns: List[str] = None  # 선택사항, 기본값: ["*"]
    oracle_schema: Optional[str] = None  # 선택사항
    hive_database: Optional[str] = None  # 선택사항
    oracle_where_clause: Optional[str] = None  # Oracle WHERE 조건문 (선택사항, 날짜 조건과 AND로 결합)
    hive_where_clause: Optional[str] = None  # Hive WHERE 조건문 (선택사항, 날짜 조건과 AND로 결합)
    
    @classmethod
    def from_yaml(cls, config_path: str) -> "TableConfig":
        """YAML 파일에서 테이블 설정 로드 (통합 설정 파일 지원)"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 통합 설정 파일 구조 확인 (oracle_table, hive_table 섹션이 있는 경우)
        if "oracle_table" in config and "hive_table" in config:
            # 기존 구조: oracle_table, hive_table 섹션으로 구성
            oracle_table = config["oracle_table"]
            hive_table = config["hive_table"]
            
            # Primary Key 파싱
            primary_keys_value = oracle_table.get("table_primary_key", [])
            if isinstance(primary_keys_value, str):
                primary_keys = [k.strip() for k in primary_keys_value.split(",") if k.strip()]
            elif isinstance(primary_keys_value, list):
                primary_keys = primary_keys_value
            else:
                primary_keys = []
            
            if not primary_keys:
                raise ValueError("table_primary_key가 설정되지 않았습니다.")
            
            # 컬럼 파싱
            columns_value = oracle_table.get("table_columns", "*")
            if isinstance(columns_value, str):
                if columns_value == "*":
                    columns = ["*"]
                else:
                    columns = [c.strip() for c in columns_value.split(",")]
            elif isinstance(columns_value, list):
                columns = columns_value
            else:
                columns = ["*"]
            
            # 제외할 컬럼 파싱
            exclude_columns_value = oracle_table.get("exclude_columns", [])
            if isinstance(exclude_columns_value, str):
                exclude_columns = [c.strip() for c in exclude_columns_value.split(",") if c.strip()]
            elif isinstance(exclude_columns_value, list):
                exclude_columns = exclude_columns_value
            else:
                exclude_columns = []
            
            # 날짜 컬럼 타입 검증
            date_column_type = oracle_table.get("date_column_type", "yyyy-mm-dd")
            valid_date_types = ["yyyy-mm-dd", "yyyymmdd", "yyyymmdd HH24:mm:ss", "yyyy-mm-dd HH24:mm:ss"]
            if date_column_type not in valid_date_types:
                raise ValueError(
                    f"date_column_type은 다음 중 하나여야 합니다: {valid_date_types}. "
                    f"현재 값: {date_column_type}"
                )
            
            return cls(
                source_db_name="oracle",
                target_db_name="hive",
                table_name=oracle_table.get("table_name", ""),
                hive_table_name=hive_table.get("table_name", ""),  # Hive 테이블명
                date_column=oracle_table.get("date_column", "CREATED_DATE"),
                date_column_type=date_column_type,
                exclude_columns=exclude_columns,
                primary_keys=primary_keys,
                columns=columns,
                oracle_schema=None,  # database.yml에서 가져옴
                hive_database=hive_table.get("database", None),
                oracle_where_clause=oracle_table.get("where_clause", None),
                hive_where_clause=hive_table.get("where_clause", None)
            )
        else:
            # 새로운 구조: source_db_name, target_db_name 등으로 구성
            required_fields = ["source_db_name", "target_db_name", "table_name", "date_column", "date_column_type", "primary_keys"]
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"설정 파일 {config_path}에 필수 필드 '{field}'가 없습니다.")
            
            # 날짜 컬럼 타입 검증
            date_column_type = config["date_column_type"]
            valid_date_types = ["yyyy-mm-dd", "yyyymmdd", "yyyymmdd HH24:mm:ss", "yyyy-mm-dd HH24:mm:ss"]
            if date_column_type not in valid_date_types:
                raise ValueError(
                    f"date_column_type은 다음 중 하나여야 합니다: {valid_date_types}. "
                    f"현재 값: {date_column_type}"
                )
            
            # Primary Key 파싱
            primary_keys_value = config["primary_keys"]
            if isinstance(primary_keys_value, str):
                primary_keys = [k.strip() for k in primary_keys_value.split(",") if k.strip()]
            elif isinstance(primary_keys_value, list):
                primary_keys = primary_keys_value
            else:
                primary_keys = []
            
            if not primary_keys:
                raise ValueError("primary_keys가 설정되지 않았습니다.")
            
            # 제외할 컬럼 파싱
            exclude_columns_value = config.get("exclude_columns", [])
            if isinstance(exclude_columns_value, str):
                exclude_columns = [c.strip() for c in exclude_columns_value.split(",") if c.strip()]
            elif isinstance(exclude_columns_value, list):
                exclude_columns = exclude_columns_value
            else:
                exclude_columns = []
            
            # 컬럼 파싱 (선택사항)
            columns_value = config.get("columns", "*")
            if isinstance(columns_value, str):
                if columns_value == "*":
                    columns = ["*"]
                else:
                    columns = [c.strip() for c in columns_value.split(",")]
            elif isinstance(columns_value, list):
                columns = columns_value
            else:
                columns = ["*"]
            
            return cls(
                source_db_name=config["source_db_name"],
                target_db_name=config["target_db_name"],
                table_name=config["table_name"],
                date_column=config["date_column"],
                date_column_type=date_column_type,
                exclude_columns=exclude_columns,
                primary_keys=primary_keys,
                columns=columns,
                oracle_schema=config.get("oracle_schema", None),
                hive_database=config.get("hive_database", None),
                oracle_where_clause=config.get("oracle_where_clause", None),
                hive_where_clause=config.get("hive_where_clause", None)
            )
    
    @classmethod
    def from_unified_yaml(cls, config_path: str) -> tuple["TableConfig", "DatabaseConfig", "DatabaseConfig"]:
        """통합 설정 파일에서 테이블 설정과 DB 접속 정보를 모두 로드"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # DB 접속 정보 추출
        oracle_db_config = None
        hive_db_config = None
        
        if "oracle" in config:
            oracle_db_info = config["oracle"]
            host = oracle_db_info.get("host", "")
            port = oracle_db_info.get("port", "1521")
            service_name = oracle_db_info.get("service_name", "")
            
            if host and service_name:
                jdbc_url = f"jdbc:oracle:thin:@{host}:{port}/{service_name}"
            else:
                jdbc_url = oracle_db_info.get("jdbc_url", "")
                if not jdbc_url:
                    raise ValueError("Oracle JDBC URL이 설정되지 않았습니다.")
            
            oracle_db_config = DatabaseConfig(
                jdbc_url=jdbc_url,
                user=oracle_db_info.get("user", ""),
                password=oracle_db_info.get("password", ""),
                default_schema=oracle_db_info.get("default_schema", None)
            )
        
        if "hive" in config:
            hive_db_info = config["hive"]
            hive_db_config = DatabaseConfig(
                jdbc_url="",
                user="",
                password="",
                default_database=hive_db_info.get("database", "default")
            )
        
        # 테이블 설정 추출
        table_config = cls.from_yaml(config_path)
        
        return table_config, oracle_db_config, hive_db_config


@dataclass
class OracleConfig:
    """Oracle 데이터베이스 설정 (내부 사용)"""
    jdbc_url: str
    user: str
    password: str
    schema: str
    table_name: str
    columns: List[str]
    primary_keys: List[str]
    date_column: str
    date_column_type: str
    exclude_columns: List[str]
    where_clause: Optional[str] = None
    
    @classmethod
    def from_table_and_database_config(
        cls, 
        table_config: TableConfig, 
        database_config: DatabaseConfig
    ) -> "OracleConfig":
        """TableConfig와 DatabaseConfig로부터 OracleConfig 생성"""
        return cls(
            jdbc_url=database_config.jdbc_url,
            user=database_config.user,
            password=database_config.password,
            schema=table_config.oracle_schema or database_config.default_schema or "",
            table_name=table_config.table_name,
            columns=table_config.columns or ["*"],
            primary_keys=table_config.primary_keys,
            date_column=table_config.date_column,
            date_column_type=table_config.date_column_type,
            exclude_columns=table_config.exclude_columns,
            where_clause=table_config.oracle_where_clause
        )


@dataclass
class HiveConfig:
    """Hive 테이블 설정 (내부 사용)"""
    database: str
    table_name: str
    date_column: str
    date_column_type: str
    exclude_columns: List[str]
    where_clause: Optional[str] = None
    
    @classmethod
    def from_table_and_database_config(
        cls, 
        table_config: TableConfig, 
        database_config: DatabaseConfig
    ) -> "HiveConfig":
        """TableConfig와 DatabaseConfig로부터 HiveConfig 생성"""
        # Hive 테이블명: hive_table_name이 있으면 사용, 없으면 table_name 사용
        hive_table_name = table_config.hive_table_name or table_config.table_name
        
        return cls(
            database=table_config.hive_database or database_config.default_database or "default",
            table_name=hive_table_name,
            date_column=table_config.date_column,
            date_column_type=table_config.date_column_type,
            exclude_columns=table_config.exclude_columns,
            where_clause=table_config.hive_where_clause
        )


@dataclass
class ValidationConfig:
    """검증 설정"""
    report_path: Optional[str] = None  # 리포트 저장 경로
    output_path: Optional[str] = None  # 차이점 레코드 저장 경로 (HDFS 경로)
    date_offset_days: int = 2  # 현재 날짜로부터 몇 일 전 데이터를 검증할지
    
    @classmethod
    def from_yaml(cls, config_path: str, section: str = "validation", environment: str = "dev") -> "ValidationConfig":
        """
        YAML 파일에서 설정 로드
        
        Args:
            config_path: 설정 파일 경로
            section: 설정 섹션명 (기본값: "validation")
            environment: 환경명 (dev 또는 prod, 기본값: "dev")
        
        Returns:
            ValidationConfig 인스턴스
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        report_path = None
        output_path = None
        date_offset_days = 2
        
        # 환경별 설정 구조: {environment: {validation: {...}}}
        if environment in config:
            env_config = config[environment]
            if section in env_config:
                section_data = env_config[section]
                report_path = section_data.get("report_path", None)
                output_path = section_data.get("output_path", None)
                date_offset_days = section_data.get("date_offset_days", 2)
        # 하위 호환성: 기존 구조 {validation: {...}}도 지원
        elif section in config:
            section_data = config[section]
            report_path = section_data.get("report_path", None)
            output_path = section_data.get("output_path", None)
            date_offset_days = section_data.get("date_offset_days", 2)
        
        return cls(
            report_path=report_path,
            output_path=output_path,
            date_offset_days=date_offset_days
        )
    
    @classmethod
    def from_ini(cls, config_path: str, section: str = "validation") -> "ValidationConfig":
        """INI 파일에서 설정 로드 (하위 호환성)"""
        return cls.from_yaml(config_path, section)
