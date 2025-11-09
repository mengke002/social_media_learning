"""
社交媒体学习流水线主程序
实现每日自动化处理:优先级评估 -> 深度分析 -> Notion推送
"""
import argparse
import logging
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

from src.config import config
from src.database import DatabaseManager
from src.source_reader import SourceReader
from src.llm_processor import LLMProcessor
from src.notion_client import NotionClient


def setup_logging():
    """设置日志系统"""
    log_config = config.get_logging_config()

    # 创建logs目录
    import os
    log_dir = os.path.dirname(log_config['log_file'])
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 配置日志格式
    logging.basicConfig(
        level=getattr(logging, log_config['log_level']),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_config['log_file'], encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("社交媒体学习流水线启动")
    logger.info("=" * 60)
    return logger


def process_priority_analysis_batch(posts: List[Dict[str, Any]], llm_processor: LLMProcessor,
                                    db_manager: DatabaseManager, processing_config: Dict[str, int]) -> List[Dict[str, Any]]:
    """第一阶段:批量进行优先级分析

    Args:
        posts: 帖子列表
        llm_processor: LLM处理器
        db_manager: 数据库管理器
        processing_config: 处理配置

    Returns:
        高价值帖子列表
    """
    logger = logging.getLogger(__name__)
    logger.info(f"=" * 60)
    logger.info(f"第一阶段:优先级评估 (共 {len(posts)} 个帖子)")
    logger.info(f"=" * 60)

    high_value_posts = []
    threshold = processing_config['priority_threshold']
    fast_workers = processing_config['fast_llm_workers']
    fast_delay = processing_config['fast_llm_delay']

    def analyze_single_post(post):
        """分析单个帖子"""
        try:
            post_id = post['source_post_id']
            platform = post['source_platform']

            logger.info(f"分析帖子: {platform}/{post_id}")

            # 运行Fast LLM优先级分析
            priority_result = llm_processor.run_priority_analysis(post['original_content'])

            if not priority_result.get('success'):
                logger.error(f"优先级分析失败: {priority_result.get('error')}")
                return None

            # 计算最终分数 - 使用去除图片markdown后的内容长度
            import re
            original_content = post.get('original_content', '')
            cleaned_content = re.sub(r'!\[.*?\]\(.*?\)', '', original_content).strip()
            content_length = len(cleaned_content)

            final_score = llm_processor.calculate_priority_score(priority_result, content_length)

            # 判断是否值得处理
            is_worth = final_score >= threshold

            logger.info(f"帖子 {platform}/{post_id} - 分数: {final_score}, 值得处理: {is_worth}")

            # 保存到数据库
            post_data = {
                'source_platform': platform,
                'source_post_id': post_id,
                'original_content': post.get('original_content'),
                'original_url': post.get('original_url'),
                'author_name': post.get('author_name'),
                'priority_analysis': priority_result,
                'final_priority_score': final_score,
                'is_worth_processing': is_worth
            }

            db_manager.save_priority_analysis(post_data)

            if is_worth:
                return {
                    'source_platform': platform,
                    'source_post_id': post_id,
                    'original_content': post.get('original_content'),
                    'original_url': post.get('original_url'),
                    'author_name': post.get('author_name'),
                    'final_priority_score': final_score,
                    'has_image': priority_result.get('has_image', False),
                    'interpretation': post.get('interpretation')  # VLM解读内容
                }

            return None

        except Exception as e:
            logger.error(f"处理帖子时出错: {e}")
            return None

    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=fast_workers) as executor:
        futures = [executor.submit(analyze_single_post, post) for post in posts]

        for future in as_completed(futures):
            result = future.result()
            if result:
                high_value_posts.append(result)

            # 添加延迟避免API限速
            time.sleep(fast_delay)

    logger.info(f"第一阶段完成: {len(high_value_posts)}/{len(posts)} 个帖子值得深度处理")
    return high_value_posts


def process_depth_analysis_batch(posts: List[Dict[str, Any]], llm_processor: LLMProcessor,
                                 db_manager: DatabaseManager, processing_config: Dict[str, int]) -> List[Dict[str, Any]]:
    """第二阶段:批量进行深度分析

    Args:
        posts: 高价值帖子列表
        llm_processor: LLM处理器
        db_manager: 数据库管理器
        processing_config: 处理配置

    Returns:
        完成深度分析的报告列表
    """
    logger = logging.getLogger(__name__)
    logger.info(f"=" * 60)
    logger.info(f"第二阶段:深度分析 (共 {len(posts)} 个帖子)")
    logger.info(f"=" * 60)

    analyzed_reports = []
    smart_workers = processing_config['smart_model_workers']
    smart_delay = processing_config['smart_model_delay']
    retry_delay = processing_config['smart_model_retry_delay']

    def analyze_single_post(post):
        """深度分析单个帖子"""
        try:
            post_id = post['source_post_id']
            platform = post['source_platform']

            logger.info(f"深度分析帖子: {platform}/{post_id}")

            # 构建分析内容:如果有图片且有VLM解读,则组合原文+解读
            has_image = post.get('has_image', False)
            interpretation = post.get('interpretation')
            original_content = post['original_content']

            # 去除原文中的图片markdown标记,避免干扰模型
            import re
            cleaned_content = re.sub(r'!\[.*?\]\(.*?\)', '', original_content)
            cleaned_content = cleaned_content.strip()

            if has_image and interpretation:
                # 有图片:组合清理后的原文和VLM解读
                analysis_content = f"""[原始帖子内容]
{cleaned_content}

[图片视觉解读]
{interpretation}
"""
                logger.info(f"帖子包含图片,已附加VLM解读 (解读长度: {len(interpretation)} 字符)")
            else:
                # 无图片:只用清理后的原文
                analysis_content = cleaned_content
                logger.info("帖子不含图片,仅使用原文内容")

            # 运行Smart Model深度分析
            depth_result = llm_processor.run_depth_analysis(analysis_content, retry_delay=retry_delay)

            if not depth_result.get('success'):
                logger.error(f"深度分析失败: {depth_result.get('error')}")
                return None

            # 更新数据库
            db_manager.update_with_depth_analysis(
                platform,
                post_id,
                depth_result['report'],
                depth_result['model']
            )

            logger.info(f"帖子 {platform}/{post_id} 深度分析完成,使用模型: {depth_result['model']}")

            return {
                'source_platform': platform,
                'source_post_id': post_id,
                'original_content': post.get('original_content'),
                'original_url': post.get('original_url'),
                'author_name': post.get('author_name'),
                'analysis_report': depth_result['report'],
                'model_used': depth_result['model']
            }

        except Exception as e:
            logger.error(f"深度分析帖子时出错: {e}")
            return None

    # 使用线程池并发处理(数量较少以避免限速)
    with ThreadPoolExecutor(max_workers=smart_workers) as executor:
        futures = [executor.submit(analyze_single_post, post) for post in posts]

        for future in as_completed(futures):
            result = future.result()
            if result:
                analyzed_reports.append(result)

            # 添加延迟避免API限速
            time.sleep(smart_delay)

    logger.info(f"第二阶段完成: {len(analyzed_reports)}/{len(posts)} 个帖子深度分析成功")
    return analyzed_reports


def push_to_notion_batch(reports: List[Dict[str, Any]], notion_client: NotionClient,
                        db_manager: DatabaseManager) -> int:
    """第三阶段:批量推送到Notion

    Args:
        reports: 报告列表
        notion_client: Notion客户端
        db_manager: 数据库管理器

    Returns:
        成功推送数量
    """
    logger = logging.getLogger(__name__)
    logger.info(f"=" * 60)
    logger.info(f"第三阶段:推送到Notion (共 {len(reports)} 个报告)")
    logger.info(f"=" * 60)

    # 创建或获取当天的学习页面
    today = datetime.now()
    daily_page_id = notion_client.create_daily_learning_page(today)

    if not daily_page_id:
        logger.error("无法创建每日学习页面,推送终止")
        return 0

    success_count = 0

    for i, report in enumerate(reports):
        try:
            logger.info(f"推送报告 {i+1}/{len(reports)}: {report['source_platform']}/{report['source_post_id']}")

            # 格式化并推送报告
            push_result = notion_client.format_and_push_report(report, daily_page_id)

            if push_result.get('success'):
                # 标记为已推送
                db_manager.mark_as_pushed(
                    report['source_platform'],
                    report['source_post_id'],
                    push_result['page_url']
                )
                success_count += 1
                logger.info(f"报告推送成功: {push_result['page_url']}")
            else:
                logger.error(f"报告推送失败: {push_result.get('error')}")

            # 添加延迟避免API限速
            time.sleep(0.5)

        except Exception as e:
            logger.error(f"推送报告时出错: {e}")

    logger.info(f"第三阶段完成: {success_count}/{len(reports)} 个报告推送成功")
    return success_count


def task_daily_learning(args):
    """执行每日学习任务"""
    logger = logging.getLogger(__name__)

    try:
        # 初始化所有组件
        logger.info("初始化组件...")
        db_manager = DatabaseManager(config)
        source_reader = SourceReader(config, db_manager)
        llm_processor = LLMProcessor(config)
        notion_client = NotionClient(config)
        processing_config = config.get_processing_config()

        # 获取配置
        days_back = processing_config['days_back']
        top_n = processing_config['top_n_posts']

        # 第一阶段:获取未处理的帖子并进行优先级评估
        logger.info(f"获取最近 {days_back} 天的未处理帖子...")
        all_posts = source_reader.get_all_unprocessed_posts(days_back)

        if not all_posts:
            logger.info("没有找到未处理的帖子")
            return

        # 进行优先级分析
        high_value_posts = process_priority_analysis_batch(all_posts, llm_processor, db_manager, processing_config)

        if not high_value_posts:
            logger.info("没有找到高价值帖子")
            return

        # 按分数排序,取Top N
        high_value_posts.sort(key=lambda x: x.get('final_priority_score', 0), reverse=True)
        top_posts = high_value_posts[:top_n]

        logger.info(f"选取Top {len(top_posts)} 个帖子进行深度分析")

        # 第二阶段:深度分析
        analyzed_reports = process_depth_analysis_batch(top_posts, llm_processor, db_manager, processing_config)

        if not analyzed_reports:
            logger.info("没有成功完成深度分析的报告")
            return

        # 第三阶段:推送到Notion
        push_to_notion_batch(analyzed_reports, notion_client, db_manager)

        # 打印统计信息
        stats = db_manager.get_statistics()
        logger.info("=" * 60)
        logger.info("任务完成 - 统计信息:")
        logger.info(f"  总处理数: {stats.get('total_processed', 0)}")
        logger.info(f"  值得处理: {stats.get('worth_processing', 0)}")
        logger.info(f"  深度分析: {stats.get('depth_analyzed', 0)}")
        logger.info(f"  已推送: {stats.get('pushed_to_notion', 0)}")
        logger.info(f"  今日处理: {stats.get('today_processed', 0)}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"执行每日学习任务时出错: {e}", exc_info=True)
        raise


def task_fast_llm_analysis(args):
    """任务1: Fast LLM优先级评估 (每2小时运行)

    处理所有未分析的帖子,进行快速优先级评估并保存结果到数据库
    """
    logger = logging.getLogger(__name__)

    try:
        # 初始化组件
        logger.info("初始化组件...")
        db_manager = DatabaseManager(config)
        source_reader = SourceReader(config, db_manager)
        llm_processor = LLMProcessor(config)
        processing_config = config.get_processing_config()

        # 获取未处理的帖子
        days_back = processing_config['days_back']
        logger.info(f"获取最近 {days_back} 天的未处理帖子...")
        all_posts = source_reader.get_all_unprocessed_posts(days_back)

        if not all_posts:
            logger.info("没有找到未处理的帖子")
            return

        # 进行优先级分析
        logger.info(f"开始Fast LLM优先级评估,共 {len(all_posts)} 个帖子")
        high_value_posts = process_priority_analysis_batch(all_posts, llm_processor, db_manager, processing_config)

        # 打印统计
        logger.info("=" * 60)
        logger.info("Fast LLM分析完成 - 统计信息:")
        logger.info(f"  总处理数: {len(all_posts)}")
        logger.info(f"  高价值帖子: {len(high_value_posts)}")
        logger.info(f"  阈值: {processing_config['priority_threshold']}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"执行Fast LLM分析任务时出错: {e}", exc_info=True)
        raise


def task_smart_model_analysis(args):
    """任务2: Smart Model深度分析 + Notion推送 (每4小时运行)

    从数据库中获取高分帖子,进行深度分析并推送到Notion
    """
    logger = logging.getLogger(__name__)

    try:
        # 初始化组件
        logger.info("初始化组件...")
        db_manager = DatabaseManager(config)
        source_reader = SourceReader(config, db_manager)
        llm_processor = LLMProcessor(config)
        notion_client = NotionClient(config)
        processing_config = config.get_processing_config()

        top_n = processing_config['top_n_posts']

        # 从数据库获取待深度分析的帖子
        logger.info(f"从数据库获取Top {top_n} 待深度分析的帖子...")
        top_posts_data = db_manager.get_posts_for_depth_analysis(limit=top_n)

        if not top_posts_data:
            logger.info("没有找到待深度分析的帖子")
            return

        # 按平台分组,批量获取VLM图片解读
        logger.info("批量获取VLM图片解读...")
        x_post_ids = [p['source_post_id'] for p in top_posts_data if p['source_platform'] == 'X']
        jike_post_ids = [p['source_post_id'] for p in top_posts_data if p['source_platform'] == 'Jike']

        x_interpretations = source_reader.get_interpretation_by_post_ids('X', x_post_ids) if x_post_ids else {}
        jike_interpretations = source_reader.get_interpretation_by_post_ids('Jike', jike_post_ids) if jike_post_ids else {}

        logger.info(f"获取到 {len(x_interpretations)} 个X图片解读, {len(jike_interpretations)} 个即刻图片解读")

        # 转换为处理格式,并补充interpretation
        top_posts = []
        for post in top_posts_data:
            platform = post['source_platform']
            post_id = post['source_post_id']

            # 获取interpretation
            interpretation = None
            if platform == 'X':
                interpretation = x_interpretations.get(post_id)
            elif platform == 'Jike':
                interpretation = jike_interpretations.get(post_id)

            # 判断是否有图片 (有interpretation就认为有图片)
            has_image = bool(interpretation)

            top_posts.append({
                'source_platform': platform,
                'source_post_id': post_id,
                'original_content': post['original_content'],
                'original_url': post.get('original_url'),
                'author_name': post.get('author_name'),
                'final_priority_score': post.get('final_priority_score', 0),
                'has_image': has_image,
                'interpretation': interpretation
            })

        logger.info(f"选取Top {len(top_posts)} 个帖子进行深度分析 (其中 {sum(1 for p in top_posts if p['has_image'])} 个包含图片)")

        # 第二阶段:深度分析
        analyzed_reports = process_depth_analysis_batch(top_posts, llm_processor, db_manager, processing_config)

        if not analyzed_reports:
            logger.info("没有成功完成深度分析的报告")
            return

        # 第三阶段:推送到Notion
        push_to_notion_batch(analyzed_reports, notion_client, db_manager)

        # 打印统计信息
        stats = db_manager.get_statistics()
        logger.info("=" * 60)
        logger.info("Smart Model分析完成 - 统计信息:")
        logger.info(f"  深度分析: {stats.get('depth_analyzed', 0)}")
        logger.info(f"  已推送: {stats.get('pushed_to_notion', 0)}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"执行Smart Model分析任务时出错: {e}", exc_info=True)
        raise


def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description='社交媒体学习流水线')
    parser.add_argument('--task', type=str,
                       choices=['daily_learning', 'fast_llm', 'smart_model'],
                       required=True,
                       help='要执行的任务: daily_learning(完整流程), fast_llm(优先级评估), smart_model(深度分析+推送)')

    args = parser.parse_args()

    # 设置日志
    logger = setup_logging()

    try:
        if args.task == 'daily_learning':
            # 完整流程(向后兼容)
            task_daily_learning(args)
        elif args.task == 'fast_llm':
            # Fast LLM优先级评估
            task_fast_llm_analysis(args)
        elif args.task == 'smart_model':
            # Smart Model深度分析 + Notion推送
            task_smart_model_analysis(args)

        logger.info("程序执行完成")

    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
