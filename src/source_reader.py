"""
源数据读取模块
从X和即刻数据库中读取未处理的帖子
"""
import logging
import pymysql
from contextlib import contextmanager
from typing import Dict, Any, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class SourceReader:
    """源数据读取器,从X和即刻数据库读取未处理的帖子"""

    def __init__(self, config, learning_db_manager):
        """初始化源数据读取器

        Args:
            config: 配置对象
            learning_db_manager: 学习数据库管理器实例
        """
        self.config = config
        self.learning_db = learning_db_manager

        # 延迟初始化数据库配置,只在需要时才获取
        self._x_config = None
        self._jike_config = None

    def _ensure_x_config(self):
        """确保X数据库配置已加载"""
        if self._x_config is None:
            self._x_config = self.config.get_source_x_config()
        return self._x_config

    def _ensure_jike_config(self):
        """确保即刻数据库配置已加载"""
        if self._jike_config is None:
            self._jike_config = self.config.get_source_jike_config()
        return self._jike_config

    @property
    def x_config(self):
        """获取X数据库配置"""
        return self._ensure_x_config()

    @property
    def jike_config(self):
        """获取即刻数据库配置"""
        return self._ensure_jike_config()

    @contextmanager
    def _get_x_connection(self):
        """获取X数据库连接"""
        conn = None
        try:
            conn = pymysql.connect(**self.x_config)
            yield conn
        except Exception as e:
            logger.error(f"X数据库连接失败: {e}")
            raise
        finally:
            if conn:
                conn.close()

    @contextmanager
    def _get_jike_connection(self):
        """获取即刻数据库连接"""
        conn = None
        try:
            conn = pymysql.connect(**self.jike_config)
            yield conn
        except Exception as e:
            logger.error(f"即刻数据库连接失败: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def get_unprocessed_x_posts(self, days_back: int = 1) -> List[Dict[str, Any]]:
        """从X数据库获取未处理的帖子

        Args:
            days_back: 回溯天数

        Returns:
            帖子列表
        """
        try:
            logger.info(f"开始从X数据库查询最近 {days_back} 天的帖子...")

            with self._get_x_connection() as conn:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                # 查询 twitter_posts 和 post_insights 表
                # 只获取有完整分析的帖子
                sql = """
                SELECT
                    p.id,
                    p.post_url,
                    p.post_content,
                    p.published_at,
                    p.media_urls,
                    u.user_id,
                    pi.summary,
                    pi.tag,
                    pi.content_type,
                    pi.interpretation
                FROM twitter_posts p
                JOIN twitter_users u ON p.user_table_id = u.id
                LEFT JOIN post_insights pi ON p.id = pi.post_id AND pi.status = 'completed'
                WHERE p.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY p.published_at DESC
                LIMIT 1000
                """

                cursor.execute(sql, (days_back,))
                posts = cursor.fetchall()
                logger.info(f"从X数据库查询到 {len(posts)} 个帖子")

            # 批量检查已处理的帖子ID
            if not posts:
                return []

            post_ids = [str(post['id']) for post in posts]
            logger.info(f"批量检查 {len(post_ids)} 个帖子的处理状态...")
            processed_ids = self.learning_db.get_processed_post_ids('X', post_ids)
            logger.info(f"其中 {len(processed_ids)} 个已处理")

            # 过滤掉已处理的帖子
            unprocessed = []
            for post in posts:
                post_id = str(post['id'])
                if post_id not in processed_ids:
                    unprocessed.append({
                        'source_post_id': post_id,
                        'source_platform': 'X',
                        'original_content': post['post_content'] or '',
                        'original_url': post['post_url'],
                        'author_name': post['user_id'],
                        'published_at': post['published_at'],
                        'media_urls': post.get('media_urls'),
                        'summary': post.get('summary'),
                        'tag': post.get('tag'),
                        'content_type': post.get('content_type'),
                        'interpretation': post.get('interpretation')
                    })

            logger.info(f"从X数据库获取到 {len(unprocessed)} 个未处理的帖子")
            return unprocessed

        except Exception as e:
            logger.error(f"从X数据库获取帖子失败: {e}", exc_info=True)
            return []

    def get_unprocessed_jike_posts(self, days_back: int = 1) -> List[Dict[str, Any]]:
        """从即刻数据库获取未处理的帖子

        Args:
            days_back: 回溯天数

        Returns:
            帖子列表
        """
        try:
            logger.info(f"开始从即刻数据库查询最近 {days_back} 天的帖子...")

            with self._get_jike_connection() as conn:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                # 查询 jk_posts 和 postprocessing 表
                sql = """
                SELECT
                    p.id,
                    p.link,
                    p.title,
                    p.summary,
                    p.published_at,
                    prof.nickname,
                    prof.jike_user_id,
                    pp.interpretation_text
                FROM jk_posts p
                JOIN jk_profiles prof ON p.profile_id = prof.id
                LEFT JOIN postprocessing pp ON p.id = pp.post_id AND pp.status = 'success'
                WHERE p.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY p.published_at DESC
                LIMIT 1000
                """

                cursor.execute(sql, (days_back,))
                posts = cursor.fetchall()
                logger.info(f"从即刻数据库查询到 {len(posts)} 个帖子")

            # 批量检查已处理的帖子ID
            if not posts:
                return []

            post_ids = [str(post['id']) for post in posts]
            logger.info(f"批量检查 {len(post_ids)} 个帖子的处理状态...")
            processed_ids = self.learning_db.get_processed_post_ids('Jike', post_ids)
            logger.info(f"其中 {len(processed_ids)} 个已处理")

            # 过滤掉已处理的帖子
            unprocessed = []
            for post in posts:
                post_id = str(post['id'])
                if post_id not in processed_ids:
                    # 组合标题和摘要作为内容
                    content_parts = []
                    if post.get('title'):
                        content_parts.append(post['title'])
                    if post.get('summary'):
                        content_parts.append(post['summary'])
                    original_content = '\n\n'.join(content_parts)

                    unprocessed.append({
                        'source_post_id': post_id,
                        'source_platform': 'Jike',
                        'original_content': original_content,
                        'original_url': post['link'],
                        'author_name': post['nickname'] or post['jike_user_id'],
                        'published_at': post['published_at'],
                        'interpretation': post.get('interpretation_text')
                    })

            logger.info(f"从即刻数据库获取到 {len(unprocessed)} 个未处理的帖子")
            return unprocessed

        except Exception as e:
            logger.error(f"从即刻数据库获取帖子失败: {e}", exc_info=True)
            return []

    def get_all_unprocessed_posts(self, days_back: int = 1) -> List[Dict[str, Any]]:
        """获取所有未处理的帖子(X + 即刻)

        Args:
            days_back: 回溯天数

        Returns:
            所有未处理帖子的列表
        """
        all_posts = []

        # 获取X帖子
        x_posts = self.get_unprocessed_x_posts(days_back)
        all_posts.extend(x_posts)
        logger.info(f"从X获取 {len(x_posts)} 个未处理帖子")

        # 获取即刻帖子
        jike_posts = self.get_unprocessed_jike_posts(days_back)
        all_posts.extend(jike_posts)
        logger.info(f"从即刻获取 {len(jike_posts)} 个未处理帖子")

        logger.info(f"总共获取到 {len(all_posts)} 个未处理的帖子")
        return all_posts

    def get_interpretation_by_post_ids(self, platform: str, post_ids: List[str]) -> Dict[str, str]:
        """批量获取帖子的VLM图片解读

        Args:
            platform: 平台名称 ('X' 或 'Jike')
            post_ids: 帖子ID列表

        Returns:
            字典: {post_id: interpretation}
        """
        if not post_ids:
            return {}

        try:
            if platform == 'X':
                with self._get_x_connection() as conn:
                    cursor = conn.cursor(pymysql.cursors.DictCursor)

                    placeholders = ','.join(['%s'] * len(post_ids))
                    sql = f"""
                    SELECT pi.post_id, pi.interpretation
                    FROM post_insights pi
                    WHERE pi.post_id IN ({placeholders})
                      AND pi.status = 'completed'
                      AND pi.interpretation IS NOT NULL
                    """

                    cursor.execute(sql, post_ids)
                    results = cursor.fetchall()

                    return {str(row['post_id']): row['interpretation'] for row in results}

            elif platform == 'Jike':
                with self._get_jike_connection() as conn:
                    cursor = conn.cursor(pymysql.cursors.DictCursor)

                    placeholders = ','.join(['%s'] * len(post_ids))
                    sql = f"""
                    SELECT pp.post_id, pp.interpretation_text
                    FROM postprocessing pp
                    WHERE pp.post_id IN ({placeholders})
                      AND pp.status = 'success'
                      AND pp.interpretation_text IS NOT NULL
                    """

                    cursor.execute(sql, post_ids)
                    results = cursor.fetchall()

                    return {str(row['post_id']): row['interpretation_text'] for row in results}

            else:
                logger.warning(f"不支持的平台: {platform}")
                return {}

        except Exception as e:
            logger.error(f"批量获取{platform}图片解读失败: {e}", exc_info=True)
            return {}
