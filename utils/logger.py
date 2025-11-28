"""
로깅 유틸리티 모듈
"""

import logging


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """
    로깅 설정
    
    Args:
        level: 로그 레벨
    
    Returns:
        Logger 인스턴스
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

