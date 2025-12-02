"""
Apache Livy 세션 관리 유틸리티 모듈
"""

import requests
import time
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class LivySessionManager:
    """Apache Livy 세션 관리 클래스"""
    
    def __init__(self, livy_url: str = "http://localhost:18998"):
        """
        LivySessionManager 초기화
        
        Args:
            livy_url: Livy 서버 URL (기본값: http://localhost:18998)
        """
        self.livy_url = livy_url.rstrip('/')
        self.session_id: Optional[int] = None
    
    def create_session(
        self,
        kind: str = "pyspark",
        proxy_user: Optional[str] = None,
        jars: Optional[list] = None,
        py_files: Optional[list] = None,
        files: Optional[list] = None,
        driver_memory: str = "2g",
        driver_cores: int = 1,
        executor_memory: str = "4g",
        executor_cores: int = 2,
        num_executors: int = 2,
        conf: Optional[Dict[str, str]] = None
    ) -> int:
        """
        Livy 세션 생성
        
        Args:
            kind: 세션 종류 (pyspark, spark, pyspark3 등)
            proxy_user: 프록시 사용자
            jars: JAR 파일 리스트
            py_files: Python 파일 리스트
            files: 파일 리스트
            driver_memory: Driver 메모리
            driver_cores: Driver 코어 수
            executor_memory: Executor 메모리
            executor_cores: Executor 코어 수
            num_executors: Executor 수
            conf: Spark 설정 딕셔너리
        
        Returns:
            세션 ID
        """
        session_data = {
            "kind": kind,
            "driverMemory": driver_memory,
            "driverCores": driver_cores,
            "executorMemory": executor_memory,
            "executorCores": executor_cores,
            "numExecutors": num_executors,
        }
        
        if proxy_user:
            session_data["proxyUser"] = proxy_user
        
        if jars:
            session_data["jars"] = jars
        
        if py_files:
            session_data["pyFiles"] = py_files
        
        if files:
            session_data["files"] = files
        
        if conf:
            session_data["conf"] = conf
        
        logger.info(f"Livy 세션 생성 요청: {self.livy_url}/sessions")
        response = requests.post(
            f"{self.livy_url}/sessions",
            json=session_data,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        
        session_info = response.json()
        self.session_id = session_info["id"]
        logger.info(f"Livy 세션 생성됨: session_id={self.session_id}")
        
        # 세션이 준비될 때까지 대기
        self._wait_for_session_ready()
        
        return self.session_id
    
    def _wait_for_session_ready(self, timeout: int = 300, check_interval: int = 5):
        """
        세션이 준비될 때까지 대기
        
        Args:
            timeout: 타임아웃 (초)
            check_interval: 체크 간격 (초)
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self.get_session_status()
            
            if status == "idle":
                logger.info(f"Livy 세션 준비 완료: session_id={self.session_id}")
                return
            elif status in ["error", "dead"]:
                raise RuntimeError(f"Livy 세션 생성 실패: status={status}")
            
            logger.info(f"Livy 세션 대기 중: status={status}")
            time.sleep(check_interval)
        
        raise TimeoutError(f"Livy 세션 준비 타임아웃: session_id={self.session_id}")
    
    def get_session_status(self) -> str:
        """
        세션 상태 조회
        
        Returns:
            세션 상태 (starting, idle, busy, error, dead 등)
        """
        if self.session_id is None:
            raise ValueError("세션이 생성되지 않았습니다.")
        
        response = requests.get(f"{self.livy_url}/sessions/{self.session_id}")
        response.raise_for_status()
        
        session_info = response.json()
        return session_info.get("state", "unknown")
    
    def submit_code(self, code: str) -> int:
        """
        코드 실행
        
        Args:
            code: 실행할 코드 (Python 코드)
        
        Returns:
            Statement ID
        """
        if self.session_id is None:
            raise ValueError("세션이 생성되지 않았습니다.")
        
        statement_data = {
            "code": code
        }
        
        logger.info(f"코드 실행 요청: session_id={self.session_id}")
        response = requests.post(
            f"{self.livy_url}/sessions/{self.session_id}/statements",
            json=statement_data,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        
        statement_info = response.json()
        statement_id = statement_info["id"]
        logger.info(f"코드 실행 시작: statement_id={statement_id}")
        
        return statement_id
    
    def get_statement_status(self, statement_id: int) -> Dict[str, Any]:
        """
        Statement 상태 조회
        
        Args:
            statement_id: Statement ID
        
        Returns:
            Statement 상태 정보
        """
        if self.session_id is None:
            raise ValueError("세션이 생성되지 않았습니다.")
        
        response = requests.get(
            f"{self.livy_url}/sessions/{self.session_id}/statements/{statement_id}"
        )
        response.raise_for_status()
        
        return response.json()
    
    def wait_for_statement(
        self,
        statement_id: int,
        timeout: int = 3600,
        check_interval: int = 5
    ) -> Dict[str, Any]:
        """
        Statement 완료 대기
        
        Args:
            statement_id: Statement ID
            timeout: 타임아웃 (초)
            check_interval: 체크 간격 (초)
        
        Returns:
            Statement 결과
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status_info = self.get_statement_status(statement_id)
            state = status_info.get("state", "unknown")
            
            if state == "available":
                logger.info(f"Statement 완료: statement_id={statement_id}")
                return status_info
            elif state in ["error", "cancelled"]:
                output = status_info.get("output", {})
                error_message = output.get("evalue", "Unknown error")
                raise RuntimeError(f"Statement 실행 실패: {error_message}")
            
            logger.debug(f"Statement 실행 중: statement_id={statement_id}, state={state}")
            time.sleep(check_interval)
        
        raise TimeoutError(f"Statement 실행 타임아웃: statement_id={statement_id}")
    
    def delete_session(self):
        """세션 삭제"""
        if self.session_id is None:
            return
        
        logger.info(f"Livy 세션 삭제: session_id={self.session_id}")
        response = requests.delete(f"{self.livy_url}/sessions/{self.session_id}")
        
        if response.status_code == 404:
            logger.warning(f"세션이 이미 삭제되었습니다: session_id={self.session_id}")
        else:
            response.raise_for_status()
        
        self.session_id = None

