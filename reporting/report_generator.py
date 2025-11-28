"""
리포트 생성 모듈
"""

import logging
import os
from datetime import datetime
from typing import Dict
from config import OracleConfig, HiveConfig

logger = logging.getLogger(__name__)


class ReportGenerator:
    """검증 결과 리포트 생성 클래스"""
    
    def __init__(self, oracle_config: OracleConfig, hive_config: HiveConfig):
        self.oracle_config = oracle_config
        self.hive_config = hive_config
    
    def generate(self, comparison_result: Dict[str, any], target_date: str) -> str:
        """
        비교 결과 리포트 생성
        
        Args:
            comparison_result: 비교 결과 딕셔너리
            target_date: 대상 날짜
        
        Returns:
            리포트 문자열
        """
        report_lines = [
            "=" * 80,
            "Oracle vs Hive 테이블 데이터 비교 리포트",
            "=" * 80,
            f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"대상 날짜: {target_date}",
            "",
            f"Oracle 테이블: {self.oracle_config.schema}.{self.oracle_config.table_name}",
            f"Hive 테이블: {self.hive_config.database}.{self.hive_config.table_name}",
            "",
            "-" * 80,
            "데이터 통계",
            "-" * 80,
            f"Oracle 레코드 수: {comparison_result['oracle_row_count']:,}",
            f"Hive 레코드 수: {comparison_result['hive_row_count']:,}",
            f"양쪽 모두 존재: {comparison_result['both_exist_count']:,}",
            f"Oracle에만 존재: {comparison_result['oracle_only_count']:,}",
            f"Hive에만 존재: {comparison_result['hive_only_count']:,}",
            "",
            "-" * 80,
            "컬럼 정보",
            "-" * 80,
            f"공통 컬럼 수: {len(comparison_result['common_columns'])}",
            f"Oracle 전용 컬럼: {comparison_result['oracle_only_columns']}",
            f"Hive 전용 컬럼: {comparison_result['hive_only_columns']}",
            "",
        ]
        
        if comparison_result['column_differences']:
            report_lines.extend([
                "-" * 80,
                "컬럼별 차이점",
                "-" * 80,
            ])
            for diff in comparison_result['column_differences']:
                report_lines.append(f"  {diff['column']}: {diff['count']:,}개 레코드 차이")
            report_lines.append("")
        
        # 검증 결과
        has_differences = (
            comparison_result['oracle_only_count'] > 0 or
            comparison_result['hive_only_count'] > 0 or
            comparison_result['total_column_differences'] > 0
        )
        
        report_lines.extend([
            "-" * 80,
            "검증 결과",
            "-" * 80,
            f"상태: {'❌ 차이점 발견' if has_differences else '✅ 일치'}",
            "",
        ])
        
        if has_differences:
            report_lines.append("⚠️  데이터 불일치가 발견되었습니다. 상세 내용을 확인하세요.")
        else:
            report_lines.append("✅ 모든 데이터가 일치합니다.")
        
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)
    
    def save(self, report: str, target_date: str, report_path: str):
        """
        리포트를 파일로 저장
        
        Args:
            report: 리포트 문자열
            target_date: 대상 날짜
            report_path: 리포트 저장 경로
        """
        table_name = self.oracle_config.table_name
        report_file = f"{report_path}/validation_report_{table_name}_{target_date}.txt"
        logger.info(f"리포트 저장: {report_file}")
        
        # 디렉토리 생성
        os.makedirs(os.path.dirname(report_file), exist_ok=True)
        
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report)

