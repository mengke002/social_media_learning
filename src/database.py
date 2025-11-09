"""
MySQL 数据库管理器
用于管理学习数据库的processed_posts表
"""
import logging
import pymysql
from contextlib import contextmanager
from typing import Any, Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class DatabaseManager:
    """学习数据库管理器"""

    def __init__(self, config=None, auto_init=True):
        """初始化数据库管理器

        Args:
            config: 配置对象
            auto_init: 是否自动初始化数据库表
        """
        if config is None:
            from .config import config as default_config
            config = default_config

        self.config = config
        self.db_config = config.get_database_config()

        if auto_init:
            self.init_database()

    @contextmanager
    def get_connection(self):
        """获取数据库连接上下文管理器"""
        conn = None
        try:
            conn = pymysql.connect(**self.db_config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def init_database(self):
        """初始化数据库表结构"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 创建 processed_posts 表
                cursor.execute(self._get_processed_posts_table_sql())
                logger.info("已创建或确认 processed_posts 表")

                conn.commit()
                logger.info("数据库表初始化完成")

        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise

    def _get_processed_posts_table_sql(self) -> str:
        """获取创建 processed_posts 表的SQL"""
        return """
        CREATE TABLE IF NOT EXISTS `processed_posts` (
          `id` INT AUTO_INCREMENT PRIMARY KEY,
          `source_post_id` VARCHAR(255) NOT NULL COMMENT '源帖子ID',
          `source_platform` ENUM('X', 'Jike') NOT NULL COMMENT '来源平台',
          `original_content` TEXT COMMENT '原始帖子内容',
          `original_url` VARCHAR(512) COMMENT '原始帖子URL',
          `author_name` VARCHAR(255) COMMENT '作者名称',

          -- 优先级评估结果
          `priority_analysis` JSON DEFAULT NULL COMMENT '来自Fast LLM的分类、图片和属性判断JSON',
          `final_priority_score` INT DEFAULT 0 COMMENT '混合规则计算出的最终优先级分数',
          `is_worth_processing` BOOLEAN DEFAULT FALSE COMMENT '是否值得深度处理',

          -- 深度分析结果
          `analysis_report` JSON DEFAULT NULL COMMENT '来自Smart Model的完整JSON分析报告',
          `model_used` VARCHAR(255) COMMENT '使用的分析模型名称',

          -- 状态管理
          `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
          `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
          `pushed_to_notion` BOOLEAN DEFAULT FALSE COMMENT '是否已推送到Notion',
          `notion_page_url` VARCHAR(512) DEFAULT NULL COMMENT 'Notion页面URL',

          UNIQUE KEY `uniq_source` (`source_platform`, `source_post_id`),
          KEY `idx_created_at` (`created_at`),
          KEY `idx_is_worth_processing` (`is_worth_processing`),
          KEY `idx_pushed_to_notion` (`pushed_to_notion`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='已处理的社交媒体帖子';
        """

    def check_if_processed(self, source_platform: str, source_post_id: str) -> bool:
        """检查某个源帖子是否已被处理

        Args:
            source_platform: 来源平台 ('X' 或 'Jike')
            source_post_id: 源帖子ID

        Returns:
            是否已处理
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                sql = """
                SELECT COUNT(*) FROM processed_posts
                WHERE source_platform = %s AND source_post_id = %s
                """

                cursor.execute(sql, (source_platform, source_post_id))
                count = cursor.fetchone()[0]

                return count > 0

        except Exception as e:
            logger.error(f"检查帖子是否已处理失败: {e}")
            return False

    def save_priority_analysis(self, post_data: Dict[str, Any]) -> bool:
        """保存第一阶段Fast LLM的优先级分析结果

        Args:
            post_data: 帖子数据,包含:
                - source_platform: 来源平台
                - source_post_id: 源帖子ID
                - original_content: 原始内容
                - original_url: 原始URL (可选)
                - author_name: 作者名称 (可选)
                - priority_analysis: 优先级分析JSON
                - final_priority_score: 最终优先级分数
                - is_worth_processing: 是否值得处理

        Returns:
            是否保存成功
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                import json
                sql = """
                INSERT INTO processed_posts
                (source_platform, source_post_id, original_content, original_url, author_name,
                 priority_analysis, final_priority_score, is_worth_processing)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    priority_analysis = VALUES(priority_analysis),
                    final_priority_score = VALUES(final_priority_score),
                    is_worth_processing = VALUES(is_worth_processing),
                    updated_at = NOW()
                """

                cursor.execute(sql, (
                    post_data['source_platform'],
                    post_data['source_post_id'],
                    post_data.get('original_content'),
                    post_data.get('original_url'),
                    post_data.get('author_name'),
                    json.dumps(post_data.get('priority_analysis', {}), ensure_ascii=False),
                    post_data.get('final_priority_score', 0),
                    post_data.get('is_worth_processing', False)
                ))

                conn.commit()
                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"保存优先级分析结果失败: {e}")
            return False

    def update_with_depth_analysis(self, source_platform: str, source_post_id: str,
                                   analysis_report: Dict[str, Any], model_used: str) -> bool:
        """更新第二阶段Smart Model的深度分析结果

        Args:
            source_platform: 来源平台
            source_post_id: 源帖子ID
            analysis_report: 深度分析报告JSON
            model_used: 使用的模型名称

        Returns:
            是否更新成功
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                import json
                sql = """
                UPDATE processed_posts
                SET analysis_report = %s,
                    model_used = %s,
                    updated_at = NOW()
                WHERE source_platform = %s AND source_post_id = %s
                """

                cursor.execute(sql, (
                    json.dumps(analysis_report, ensure_ascii=False),
                    model_used,
                    source_platform,
                    source_post_id
                ))

                conn.commit()
                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"更新深度分析结果失败: {e}")
            return False

    def get_posts_for_depth_analysis(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取需要进行深度分析的帖子列表

        Args:
            limit: 最大返回数量

        Returns:
            帖子信息列表
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                sql = """
                SELECT id, source_platform, source_post_id, original_content,
                       original_url, author_name, final_priority_score
                FROM processed_posts
                WHERE is_worth_processing = TRUE
                  AND analysis_report IS NULL
                ORDER BY final_priority_score DESC, created_at DESC
                LIMIT %s
                """

                cursor.execute(sql, (limit,))
                posts = cursor.fetchall()

                logger.info(f"获取到 {len(posts)} 个待深度分析的帖子")
                return posts

        except Exception as e:
            logger.error(f"获取待深度分析帖子失败: {e}")
            return []

    def get_reports_for_notion_push(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取已完成深度分析且尚未推送到Notion的报告

        Args:
            limit: 最大返回数量

        Returns:
            报告列表
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                sql = """
                SELECT id, source_platform, source_post_id, original_content,
                       original_url, author_name, analysis_report, model_used,
                       created_at
                FROM processed_posts
                WHERE analysis_report IS NOT NULL
                  AND pushed_to_notion = FALSE
                ORDER BY final_priority_score DESC, created_at DESC
                LIMIT %s
                """

                cursor.execute(sql, (limit,))
                reports = cursor.fetchall()

                logger.info(f"获取到 {len(reports)} 个待推送到Notion的报告")
                return reports

        except Exception as e:
            logger.error(f"获取待推送报告失败: {e}")
            return []

    def mark_as_pushed(self, source_platform: str, source_post_id: str, notion_page_url: str) -> bool:
        """标记报告已推送到Notion

        Args:
            source_platform: 来源平台
            source_post_id: 源帖子ID
            notion_page_url: Notion页面URL

        Returns:
            是否更新成功
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                sql = """
                UPDATE processed_posts
                SET pushed_to_notion = TRUE,
                    notion_page_url = %s,
                    updated_at = NOW()
                WHERE source_platform = %s AND source_post_id = %s
                """

                cursor.execute(sql, (notion_page_url, source_platform, source_post_id))
                conn.commit()

                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"标记为已推送失败: {e}")
            return False

    def get_statistics(self) -> Dict[str, int]:
        """获取处理统计信息

        Returns:
            统计信息字典
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                stats = {}

                # 总处理数
                cursor.execute("SELECT COUNT(*) FROM processed_posts")
                stats['total_processed'] = cursor.fetchone()[0]

                # 值得处理的数量
                cursor.execute("SELECT COUNT(*) FROM processed_posts WHERE is_worth_processing = TRUE")
                stats['worth_processing'] = cursor.fetchone()[0]

                # 已完成深度分析的数量
                cursor.execute("SELECT COUNT(*) FROM processed_posts WHERE analysis_report IS NOT NULL")
                stats['depth_analyzed'] = cursor.fetchone()[0]

                # 已推送到Notion的数量
                cursor.execute("SELECT COUNT(*) FROM processed_posts WHERE pushed_to_notion = TRUE")
                stats['pushed_to_notion'] = cursor.fetchone()[0]

                # 今日处理数
                cursor.execute("""
                    SELECT COUNT(*) FROM processed_posts
                    WHERE DATE(created_at) = CURDATE()
                """)
                stats['today_processed'] = cursor.fetchone()[0]

                return stats

        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}
