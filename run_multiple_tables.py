"""
여러 테이블을 한번에 실행하는 스크립트

사용 방법:
    python run_multiple_tables.py --config-dir cfg --common-config cfg/application.yml --env dev
"""

import sys
import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_validation(
    oracle_db_name: str,
    table_name: str,
    config_dir: str,
    common_config: str,
    environment: str
) -> Tuple[str, str, bool, str]:
    """
    단일 테이블 검증 실행
    
    Args:
        oracle_db_name: Oracle 데이터베이스명
        table_name: 테이블명
        config_dir: 설정 파일 디렉토리 경로
        common_config: 공통 설정 파일 경로
        environment: 환경 (dev 또는 prod)
    
    Returns:
        (oracle_db_name, table_name, success, error_message) 튜플
    """
    try:
        cmd = [
            sys.executable, '-m', 'main',
            '--oracle-db-name', oracle_db_name,
            '--table-name', table_name,
            '--config-dir', config_dir,
            '--common-config', common_config,
            '--env', environment
        ]
        
        logger.info(f"실행 중: {oracle_db_name}/{table_name}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1시간 타임아웃
        )
        
        if result.returncode == 0:
            logger.info(f"✅ 성공: {oracle_db_name}/{table_name}")
            return (oracle_db_name, table_name, True, "")
        else:
            error_msg = result.stderr or result.stdout
            logger.error(f"❌ 실패: {oracle_db_name}/{table_name} - {error_msg[:200]}")
            return (oracle_db_name, table_name, False, error_msg)
    
    except subprocess.TimeoutExpired:
        error_msg = "타임아웃 (1시간 초과)"
        logger.error(f"⏱️ 타임아웃: {oracle_db_name}/{table_name}")
        return (oracle_db_name, table_name, False, error_msg)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ 오류: {oracle_db_name}/{table_name} - {error_msg}")
        return (oracle_db_name, table_name, False, error_msg)


def run_sequential(
    table_list: List[Tuple[str, str]],
    config_dir: str,
    common_config: str,
    environment: str
) -> dict:
    """
    여러 테이블을 순차적으로 실행
    
    Args:
        table_list: (oracle_db_name, table_name) 튜플 리스트
        config_dir: 설정 파일 디렉토리 경로
        common_config: 공통 설정 파일 경로
        environment: 환경 (dev 또는 prod)
    
    Returns:
        실행 결과 딕셔너리
    """
    logger.info(f"순차 실행 시작: {len(table_list)}개 테이블")
    
    results = []
    for oracle_db_name, table_name in table_list:
        result = run_validation(oracle_db_name, table_name, config_dir, common_config, environment)
        results.append(result)
    
    return {
        'total': len(table_list),
        'success': sum(1 for r in results if r[2]),
        'failure': sum(1 for r in results if not r[2]),
        'results': results
    }


def run_parallel(
    table_list: List[Tuple[str, str]],
    config_dir: str,
    common_config: str,
    environment: str,
    max_workers: int = 3
) -> dict:
    """
    여러 테이블을 병렬로 실행
    
    Args:
        table_list: (oracle_db_name, table_name) 튜플 리스트
        config_dir: 설정 파일 디렉토리 경로
        common_config: 공통 설정 파일 경로
        environment: 환경 (dev 또는 prod)
        max_workers: 최대 동시 실행 수 (기본값: 3)
    
    Returns:
        실행 결과 딕셔너리
    """
    logger.info(f"병렬 실행 시작: {len(table_list)}개 테이블 (최대 {max_workers}개 동시 실행)")
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 모든 작업 제출
        future_to_table = {
            executor.submit(
                run_validation,
                oracle_db_name,
                table_name,
                config_dir,
                common_config,
                environment
            ): (oracle_db_name, table_name)
            for oracle_db_name, table_name in table_list
        }
        
        # 결과 수집
        for future in as_completed(future_to_table):
            oracle_db_name, table_name = future_to_table[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"❌ 예외 발생: {oracle_db_name}/{table_name} - {str(e)}")
                results.append((oracle_db_name, table_name, False, str(e)))
    
    return {
        'total': len(table_list),
        'success': sum(1 for r in results if r[2]),
        'failure': sum(1 for r in results if not r[2]),
        'results': results
    }


def print_summary(results: dict):
    """실행 결과 요약 출력"""
    print("=" * 80)
    print("실행 결과 요약")
    print("=" * 80)
    print(f"총 테이블 수: {results['total']}")
    print(f"성공: {results['success']}")
    print(f"실패: {results['failure']}")
    print()
    
    if results['success'] > 0:
        print("✅ 성공한 테이블:")
        for oracle_db_name, table_name, success, _ in results['results']:
            if success:
                print(f"  - {oracle_db_name}/{table_name}")
    
    if results['failure'] > 0:
        print("\n❌ 실패한 테이블:")
        for oracle_db_name, table_name, success, error_msg in results['results']:
            if not success:
                print(f"  - {oracle_db_name}/{table_name}")
                if error_msg:
                    print(f"    오류: {error_msg[:100]}...")
    
    print("=" * 80)


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='Janus - 여러 테이블을 한번에 검증',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  # 순차 실행
  python run_multiple_tables.py --tables pg_db:tp_cp_master,pg_db:table_2 --mode sequential
  
  # 병렬 실행 (최대 3개 동시)
  python run_multiple_tables.py --tables pg_db:tp_cp_master,pg_db:table_2 --mode parallel --max-workers 3
  
  # 파일에서 테이블 목록 읽기
  python run_multiple_tables.py --table-list-file tables.txt --mode parallel
        """
    )
    
    parser.add_argument(
        '--tables',
        type=str,
        help='테이블 목록 (형식: db1:table1,db2:table2 또는 db1:table1 db2:table2)'
    )
    parser.add_argument(
        '--table-list-file',
        type=str,
        help='테이블 목록 파일 경로 (한 줄에 하나씩, 형식: db_name:table_name)'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['sequential', 'parallel'],
        default='sequential',
        help='실행 모드: sequential (순차) 또는 parallel (병렬, 기본값: sequential)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=3,
        help='병렬 실행 시 최대 동시 실행 수 (기본값: 3)'
    )
    parser.add_argument(
        '--config-dir',
        type=str,
        default='cfg',
        help='설정 파일 디렉토리 경로 (기본값: cfg)'
    )
    parser.add_argument(
        '--common-config',
        type=str,
        default='cfg/application.yml',
        help='공통 설정 파일 경로 (기본값: cfg/application.yml)'
    )
    parser.add_argument(
        '--env',
        type=str,
        default='dev',
        choices=['dev', 'prod'],
        help='환경 (dev 또는 prod, 기본값: dev)'
    )
    
    args = parser.parse_args()
    
    # 테이블 목록 파싱
    table_list = []
    
    if args.table_list_file:
        # 파일에서 읽기
        try:
            with open(args.table_list_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split(':')
                        if len(parts) == 2:
                            table_list.append((parts[0].strip(), parts[1].strip()))
                        else:
                            logger.warning(f"잘못된 형식 무시: {line}")
        except FileNotFoundError:
            logger.error(f"파일을 찾을 수 없습니다: {args.table_list_file}")
            sys.exit(1)
    elif args.tables:
        # 명령줄 인자에서 읽기
        # 쉼표 또는 공백으로 구분
        if ',' in args.tables:
            table_strs = args.tables.split(',')
        else:
            table_strs = args.tables.split()
        
        for table_str in table_strs:
            table_str = table_str.strip()
            if ':' in table_str:
                parts = table_str.split(':')
                if len(parts) == 2:
                    table_list.append((parts[0].strip(), parts[1].strip()))
                else:
                    logger.warning(f"잘못된 형식 무시: {table_str}")
            else:
                logger.warning(f"잘못된 형식 무시: {table_str} (형식: db_name:table_name)")
    else:
        logger.error("테이블 목록을 지정해야 합니다. --tables 또는 --table-list-file 옵션을 사용하세요.")
        parser.print_help()
        sys.exit(1)
    
    if not table_list:
        logger.error("유효한 테이블이 없습니다.")
        sys.exit(1)
    
    logger.info(f"검증할 테이블 목록 ({len(table_list)}개):")
    for oracle_db_name, table_name in table_list:
        logger.info(f"  - {oracle_db_name}/{table_name}")
    
    # 실행
    if args.mode == 'parallel':
        results = run_parallel(table_list, args.config_dir, args.common_config, args.env, args.max_workers)
    else:
        results = run_sequential(table_list, args.config_dir, args.common_config, args.env)
    
    # 결과 출력
    print_summary(results)
    
    # 종료 코드
    sys.exit(0 if results['failure'] == 0 else 1)


if __name__ == "__main__":
    main()

