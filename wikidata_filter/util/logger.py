import logging
import logging.handlers
import os
from typing import Optional, Dict, Any


class ProductionLogger:
    """日志记录器"""

    def __init__(
            self,
            name: str,
            log_level: int = logging.INFO,
            log_file: Optional[str] = None,
            max_bytes: int = 10 * 1024 * 1024,  # 10MB
            backup_count: int = 5,
            extra_fields: Optional[Dict[str, Any]] = None
    ):
        """
        初始化生产环境日志记录器

        参数:
            name: 日志记录器名称
            log_level: 日志级别 (logging.DEBUG, logging.INFO, etc.)
            log_file: 日志文件路径，如果为None则只输出到控制台
            max_bytes: 单个日志文件最大大小(字节)
            backup_count: 保留的备份日志文件数量
            json_format: 是否使用JSON格式输出日志
            extra_fields: 要包含在每条日志中的额外字段
        """
        self._logger = logging.getLogger(name)
        self._extra_fields = extra_fields or {}

        # 避免重复添加handler
        if not self._logger.handlers:
            self._setup_logger(log_level, log_file, max_bytes, backup_count)

    def _setup_logger(
            self,
            log_level: int,
            log_file: Optional[str],
            max_bytes: int,
            backup_count: int
    ) -> None:
        """配置日志处理器和格式化器"""
        self._logger.setLevel(log_level)

        # 创建处理器
        handlers = []
        handlers.append(self._create_console_handler())

        if log_file:
            handlers.append(self._create_file_handler(log_file, max_bytes, backup_count))

        # 添加处理器到logger
        for handler in handlers:
            self._logger.addHandler(handler)

    def _create_console_handler(self) -> logging.Handler:
        """创建控制台处理器"""
        handler = logging.StreamHandler()
        handler.setFormatter(self._create_formatter())
        return handler

    def _create_file_handler(
            self,
            log_file: str,
            max_bytes: int,
            backup_count: int
    ) -> logging.Handler:
        """创建文件处理器"""
        # 确保日志目录存在
        dirname = os.path.dirname(log_file)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        handler.setFormatter(self._create_formatter())
        return handler

    def _create_formatter(self) -> logging.Formatter:
        """创建日志格式化器"""
        return self._create_text_formatter()

    def _create_text_formatter(self) -> logging.Formatter:
        """创建文本格式的日志格式化器"""
        return logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s '
            '[%(filename)s:%(lineno)d]'
        )

    def _add_extra_fields(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """合并额外字段"""
        merged = self._extra_fields.copy()
        if extra:
            merged.update(extra)
        return merged or {}

    def debug(self, msg: str, extra: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """记录DEBUG级别日志"""
        self._logger.debug(msg, extra=self._add_extra_fields(extra), **kwargs)

    def info(self, msg: str, extra: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """记录INFO级别日志"""
        self._logger.info(msg, extra=self._add_extra_fields(extra), **kwargs)

    def warning(self, msg: str, extra: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """记录WARNING级别日志"""
        self._logger.warning(msg, extra=self._add_extra_fields(extra), **kwargs)

    def error(self, msg: str, extra: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """记录ERROR级别日志"""
        self._logger.error(msg, extra=self._add_extra_fields(extra), **kwargs)

    def critical(self, msg: str, extra: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """记录CRITICAL级别日志"""
        self._logger.critical(msg, extra=self._add_extra_fields(extra), **kwargs)

    def exception(self, msg: str, extra: Optional[Dict[str, Any]] = None, **kwargs) -> None:
        """记录异常日志（自动包含堆栈跟踪）"""
        self._logger.exception(msg, extra=self._add_extra_fields(extra), **kwargs)
