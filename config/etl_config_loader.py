"""
ETL 설정 로드 모듈

이 모듈은 ETL 실행에 필요한 설정 파일을 로드하는 기능을 제공합니다.
"""

import os
import logging
from config import TableConfig, DatabaseConfig, OracleConfig, HiveConfig, ValidationConfig

logger = logging.getLogger(__name__)


class ETLConfigLoader:
    """ETL 설정 로드 클래스"""
    
    @staticmethod
    def load_configs(
        config_dir: str,
        oracle_db_name: str,
        table_name: str,
        common_config_path: str,
        environment: str = "dev"
    ) -> tuple[OracleConfig, HiveConfig, ValidationConfig]:
        """
        설정 파일 로드
        
        Args:
            config_dir: 설정 파일 디렉토리 경로
            oracle_db_name: Oracle 데이터베이스명
            table_name: 테이블명
            common_config_path: 공통 설정 파일 경로
            environment: 환경명 (dev 또는 prod, 기본값: "dev")
        
        Returns:
            (OracleConfig, HiveConfig, ValidationConfig) 튜플
        """
        # 설정 파일 경로 구성
        table_config_path = os.path.join(config_dir, oracle_db_name, f"{table_name}.yml")
        database_config_path = os.path.join(config_dir, oracle_db_name, "database.yml")
        
        # 통합 설정 파일이 있는지 확인 (하나의 파일에 모든 정보)
        if os.path.exists(table_config_path):
            logger.info(f"통합 설정 파일 경로: {table_config_path}")
            # 통합 설정 파일에서 테이블 설정과 DB 접속 정보 로드
            table_config, oracle_db_config, hive_db_config = TableConfig.from_unified_yaml(table_config_path)
            
            if oracle_db_config is None:
                raise ValueError(f"설정 파일 {table_config_path}에 Oracle 접속 정보가 없습니다.")
            if hive_db_config is None:
                raise ValueError(f"설정 파일 {table_config_path}에 Hive 접속 정보가 없습니다.")
        else:
            # 분리된 설정 파일 구조 (database.yml + tables/{table_name}.yml)
            tables_dir = os.path.join(config_dir, oracle_db_name, "tables")
            separated_table_config_path = os.path.join(tables_dir, f"{table_name}.yml")
            
            if not os.path.exists(separated_table_config_path):
                raise FileNotFoundError(
                    f"설정 파일을 찾을 수 없습니다.\n"
                    f"통합 파일: {table_config_path}\n"
                    f"또는 분리된 파일: {separated_table_config_path}\n"
                    f"설정 파일 경로를 확인하세요."
                )
            
            if not os.path.exists(database_config_path):
                raise FileNotFoundError(
                    f"데이터베이스 접속 정보 파일을 찾을 수 없습니다: {database_config_path}"
                )
            
            logger.info(f"테이블 설정 파일 경로: {separated_table_config_path}")
            logger.info(f"데이터베이스 접속 정보 파일 경로: {database_config_path}")
            
            # 분리된 파일에서 로드
            table_config = TableConfig.from_yaml(separated_table_config_path)
            oracle_db_config = DatabaseConfig.from_yaml(database_config_path, "oracle")
            hive_db_config = DatabaseConfig.from_yaml(database_config_path, "hive")
        
        # OracleConfig와 HiveConfig 생성
        oracle_config = OracleConfig.from_table_and_database_config(table_config, oracle_db_config)
        hive_config = HiveConfig.from_table_and_database_config(table_config, hive_db_config)
        
        # ValidationConfig 로드 (환경별 설정)
        validation_config = ValidationConfig.from_yaml(common_config_path, environment=environment)
        
        return oracle_config, hive_config, validation_config

