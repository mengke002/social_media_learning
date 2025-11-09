"""
配置管理模块
支持环境变量 > config.ini > 默认值的优先级机制
"""
import os
import configparser
import logging
from typing import Dict, Any, List
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class Config:
    """配置管理类，支持环境变量优先级的配置加载"""

    def __init__(self, config_path: str = 'config.ini'):
        # 本地开发时可加载 .env
        try:
            load_dotenv()
        except Exception:
            pass

        self.config_parser = configparser.ConfigParser()

        # 兼容多种位置查找 config.ini
        possible_paths = [
            config_path,
            os.path.join(os.getcwd(), config_path),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), config_path),
        ]

        self.config_file = None
        for p in possible_paths:
            if os.path.exists(p):
                self.config_file = p
                break

        if self.config_file:
            try:
                self.config_parser.read(self.config_file, encoding='utf-8')
                logger.info(f"已加载配置文件: {self.config_file}")
            except (configparser.Error, UnicodeDecodeError):
                logger.warning("读取配置文件失败,跳过。")
        else:
            logger.info("未发现配置文件,将仅使用环境变量与默认值。")

    def _get_config_value(self, section: str, key: str, env_var: str, default_value: Any, value_type=str) -> Any:
        """按优先级获取配置值：环境变量 > config.ini > 默认值"""
        env_val = os.getenv(env_var)
        if env_val is not None:
            try:
                return value_type(env_val)
            except (ValueError, TypeError):
                return default_value

        try:
            if self.config_parser.has_section(section) and self.config_parser.has_option(section, key):
                cfg_val = self.config_parser.get(section, key)
                try:
                    return value_type(cfg_val)
                except (ValueError, TypeError):
                    return default_value
        except (configparser.Error, UnicodeDecodeError):
            pass

        return default_value

    def _parse_model_list(self, raw_value: str) -> List[str]:
        """将逗号分隔的模型字符串解析为有序且去重的列表"""
        if not raw_value:
            return []

        models: List[str] = []
        for item in raw_value.split(','):
            candidate = item.strip()
            if candidate and candidate not in models:
                models.append(candidate)
        return models

    def get_database_config(self) -> Dict[str, Any]:
        """获取学习数据库配置（环境变量 > config.ini > 默认值）"""
        password = os.getenv('LEARNING_DB_PASSWORD')
        if password is None:
            try:
                if self.config_parser.has_section('database') and self.config_parser.has_option('database', 'password'):
                    password = self.config_parser.get('database', 'password')
            except (configparser.Error, UnicodeDecodeError):
                pass

        config = {
            'host': self._get_config_value('database', 'host', 'LEARNING_DB_HOST', None),
            'user': self._get_config_value('database', 'user', 'LEARNING_DB_USER', None),
            'database': self._get_config_value('database', 'database', 'LEARNING_DB_NAME', None),
            'port': self._get_config_value('database', 'port', 'LEARNING_DB_PORT', 3306, int),
            'password': password,
            'charset': 'utf8mb4',
            'autocommit': True,
        }

        # SSL 配置
        ssl_mode = self._get_config_value('database', 'ssl_mode', 'LEARNING_DB_SSL_MODE', 'disabled')
        if isinstance(ssl_mode, str) and ssl_mode.upper() == 'REQUIRED':
            config['ssl'] = {'mode': 'REQUIRED'}

        # 校验必填
        required = ['host', 'user', 'database', 'password']
        missing = [k for k in required if not config.get(k)]
        if missing:
            raise ValueError(f"学习数据库核心配置缺失: {', '.join(missing)}。请在 GitHub Secrets 或 config.ini 中设置。")

        return config

    def get_source_x_config(self) -> Dict[str, Any]:
        """获取X数据源数据库配置"""
        password = os.getenv('SOURCE_X_DB_PASSWORD')
        if password is None:
            try:
                if self.config_parser.has_section('source_x') and self.config_parser.has_option('source_x', 'password'):
                    password = self.config_parser.get('source_x', 'password')
            except (configparser.Error, UnicodeDecodeError):
                pass

        config = {
            'host': self._get_config_value('source_x', 'host', 'SOURCE_X_DB_HOST', None),
            'user': self._get_config_value('source_x', 'user', 'SOURCE_X_DB_USER', None),
            'database': self._get_config_value('source_x', 'database', 'SOURCE_X_DB_NAME', None),
            'port': self._get_config_value('source_x', 'port', 'SOURCE_X_DB_PORT', 3306, int),
            'password': password,
            'charset': 'utf8mb4',
            'autocommit': True,
        }

        # SSL 配置
        ssl_mode = self._get_config_value('source_x', 'ssl_mode', 'SOURCE_X_DB_SSL_MODE', 'disabled')
        if isinstance(ssl_mode, str) and ssl_mode.upper() == 'REQUIRED':
            config['ssl'] = {'mode': 'REQUIRED'}

        required = ['host', 'user', 'database', 'password']
        missing = [k for k in required if not config.get(k)]
        if missing:
            raise ValueError(f"X数据源数据库核心配置缺失: {', '.join(missing)}")

        return config

    def get_source_jike_config(self) -> Dict[str, Any]:
        """获取即刻数据源数据库配置"""
        password = os.getenv('SOURCE_JIKE_DB_PASSWORD')
        if password is None:
            try:
                if self.config_parser.has_section('source_jike') and self.config_parser.has_option('source_jike', 'password'):
                    password = self.config_parser.get('source_jike', 'password')
            except (configparser.Error, UnicodeDecodeError):
                pass

        config = {
            'host': self._get_config_value('source_jike', 'host', 'SOURCE_JIKE_DB_HOST', None),
            'user': self._get_config_value('source_jike', 'user', 'SOURCE_JIKE_DB_USER', None),
            'database': self._get_config_value('source_jike', 'database', 'SOURCE_JIKE_DB_NAME', None),
            'port': self._get_config_value('source_jike', 'port', 'SOURCE_JIKE_DB_PORT', 3306, int),
            'password': password,
            'charset': 'utf8mb4',
            'autocommit': True,
        }

        # SSL 配置
        ssl_mode = self._get_config_value('source_jike', 'ssl_mode', 'SOURCE_JIKE_DB_SSL_MODE', 'disabled')
        if isinstance(ssl_mode, str) and ssl_mode.upper() == 'REQUIRED':
            config['ssl'] = {'mode': 'REQUIRED'}

        required = ['host', 'user', 'database', 'password']
        missing = [k for k in required if not config.get(k)]
        if missing:
            raise ValueError(f"即刻数据源数据库核心配置缺失: {', '.join(missing)}")

        return config

    def get_llm_config(self) -> Dict[str, Any]:
        """获取LLM配置,优先级:环境变量 > config.ini > 默认值"""
        openai_api_key = self._get_config_value('llm', 'openai_api_key', 'OPENAI_API_KEY', None)
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY 未设置。请在环境变量或config.ini中设置LLM功能需要API密钥。")

        # 解析smart models列表
        models_raw = self._get_config_value('llm', 'smart_models', 'LLM_SMART_MODELS', '', str)
        smart_models = self._parse_model_list(models_raw)

        return {
            'fast_model_name': self._get_config_value('llm', 'fast_model_name', 'LLM_FAST_MODEL_NAME', 'gpt-3.5-turbo-16k'),
            'smart_models': smart_models,
            'openai_api_key': openai_api_key,
            'openai_base_url': self._get_config_value('llm', 'openai_base_url', 'OPENAI_BASE_URL', 'https://api.openai.com/v1'),
            'max_content_length': self._get_config_value('llm', 'max_content_length', 'LLM_MAX_CONTENT_LENGTH', 380000, int),
            'max_tokens': self._get_config_value('llm', 'max_tokens', 'LLM_MAX_TOKENS', 20000, int),
        }

    def get_notion_config(self) -> Dict[str, Any]:
        """获取Notion集成配置"""
        return {
            'integration_token': self._get_config_value('notion', 'integration_token', 'NOTION_INTEGRATION_TOKEN', None),
            'parent_page_id': self._get_config_value('notion', 'parent_page_id', 'NOTION_PARENT_PAGE_ID', None)
        }

    def get_processing_config(self) -> Dict[str, int]:
        """获取处理任务配置"""
        return {
            'days_back': self._get_config_value('processing', 'days_back', 'PROCESSING_DAYS_BACK', 1, int),
            'priority_threshold': self._get_config_value('processing', 'priority_threshold', 'PROCESSING_PRIORITY_THRESHOLD', 40, int),
            'top_n_posts': self._get_config_value('processing', 'top_n_posts', 'PROCESSING_TOP_N_POSTS', 50, int),
            'fast_llm_workers': self._get_config_value('processing', 'fast_llm_workers', 'PROCESSING_FAST_LLM_WORKERS', 10, int),
            'fast_llm_delay': self._get_config_value('processing', 'fast_llm_delay', 'PROCESSING_FAST_LLM_DELAY', 0.5, float),
            'smart_model_workers': self._get_config_value('processing', 'smart_model_workers', 'PROCESSING_SMART_MODEL_WORKERS', 2, int),
            'smart_model_delay': self._get_config_value('processing', 'smart_model_delay', 'PROCESSING_SMART_MODEL_DELAY', 2.0, float),
            'smart_model_retry_delay': self._get_config_value('processing', 'smart_model_retry_delay', 'PROCESSING_SMART_MODEL_RETRY_DELAY', 10.0, float),
        }

    def get_logging_config(self) -> Dict[str, Any]:
        """获取日志配置"""
        return {
            'log_file': self._get_config_value('logging', 'log_file', 'LOG_FILE', 'logs/learning_pipeline.log'),
            'log_level': self._get_config_value('logging', 'log_level', 'LOG_LEVEL', 'INFO'),
            'max_bytes': self._get_config_value('logging', 'max_bytes', 'LOG_MAX_BYTES', 10485760, int),
            'backup_count': self._get_config_value('logging', 'backup_count', 'LOG_BACKUP_COUNT', 5, int),
        }


# 全局配置实例
config = Config()
