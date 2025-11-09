"""
Notion API 客户端
用于将学习报告推送到Notion页面,支持年/月/日层级结构
"""
import logging
import requests
import json
import re
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class NotionClient:
    """Notion API 客户端"""

    def __init__(self, config):
        """初始化Notion客户端

        Args:
            config: 配置对象
        """
        self.base_url = "https://api.notion.com/v1"
        self.version = "2022-06-28"

        notion_config = config.get_notion_config()
        self.integration_token = notion_config.get('integration_token')
        self.parent_page_id = notion_config.get('parent_page_id')

        if not self.integration_token:
            logger.warning("Notion集成token未配置")
        if not self.parent_page_id:
            logger.warning("Notion父页面ID未配置")

        logger.info("Notion客户端初始化成功")

    def _get_headers(self) -> Dict[str, str]:
        """获取API请求头"""
        return {
            "Authorization": f"Bearer {self.integration_token}",
            "Content-Type": "application/json",
            "Notion-Version": self.version
        }

    def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict[str, Any]:
        """发送API请求"""
        url = f"{self.base_url}/{endpoint}"
        headers = self._get_headers()

        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, timeout=30)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, json=data, timeout=30)
            elif method.upper() == "PATCH":
                response = requests.patch(url, headers=headers, json=data, timeout=30)
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")

            response.raise_for_status()
            return {"success": True, "data": response.json()}

        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            try:
                if hasattr(e, 'response') and e.response is not None:
                    error_detail = e.response.json()
                    if 'message' in error_detail:
                        error_msg = f"{e}: {error_detail['message']}"
            except:
                pass

            logger.error(f"Notion API请求失败: {error_msg}")
            return {"success": False, "error": error_msg}

    def get_page_children(self, page_id: str) -> Dict[str, Any]:
        """获取页面的子页面"""
        return self._make_request("GET", f"blocks/{page_id}/children")

    def create_page(self, parent_id: str, title: str, content_blocks: List[Dict] = None) -> Dict[str, Any]:
        """创建新页面"""
        data = {
            "parent": {"page_id": parent_id},
            "properties": {
                "title": {
                    "title": [{"text": {"content": title}}]
                }
            }
        }

        if content_blocks:
            # Notion限制:单次最多100个块
            data["children"] = content_blocks[:100]

        return self._make_request("POST", "pages", data)

    def _extract_page_title(self, page_data: Dict) -> str:
        """从页面数据中提取标题"""
        try:
            if page_data.get("type") == "child_page":
                title_data = page_data.get("child_page", {}).get("title", "")
                return title_data
            return ""
        except Exception:
            return ""

    def find_or_create_year_page(self, year: str) -> Optional[str]:
        """查找或创建年份页面"""
        try:
            # 获取父页面的子页面
            children_result = self.get_page_children(self.parent_page_id)
            if not children_result.get("success"):
                logger.error(f"获取父页面子页面失败: {children_result.get('error')}")
                return None

            # 查找年份页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == year:
                        logger.info(f"找到现有年份页面: {year}")
                        return child["id"]

            # 创建年份页面
            logger.info(f"创建年份页面: {year}")
            create_result = self.create_page(self.parent_page_id, year)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                logger.error(f"创建年份页面失败: {create_result.get('error')}")
                return None

        except Exception as e:
            logger.error(f"查找或创建年份页面时出错: {e}")
            return None

    def find_or_create_month_page(self, year_page_id: str, month: str) -> Optional[str]:
        """查找或创建月份页面"""
        try:
            # 获取年份页面的子页面
            children_result = self.get_page_children(year_page_id)
            if not children_result.get("success"):
                logger.error(f"获取年份页面子页面失败: {children_result.get('error')}")
                return None

            # 查找月份页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == month:
                        logger.info(f"找到现有月份页面: {month}")
                        return child["id"]

            # 创建月份页面
            logger.info(f"创建月份页面: {month}")
            create_result = self.create_page(year_page_id, month)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                logger.error(f"创建月份页面失败: {create_result.get('error')}")
                return None

        except Exception as e:
            logger.error(f"查找或创建月份页面时出错: {e}")
            return None

    def find_or_create_day_page(self, month_page_id: str, day: str) -> Optional[str]:
        """查找或创建日期页面"""
        try:
            # 获取月份页面的子页面
            children_result = self.get_page_children(month_page_id)
            if not children_result.get("success"):
                logger.error(f"获取月份页面子页面失败: {children_result.get('error')}")
                return None

            # 查找日期页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == day:
                        logger.info(f"找到现有日期页面: {day}")
                        return child["id"]

            # 创建日期页面
            logger.info(f"创建日期页面: {day}")
            create_result = self.create_page(month_page_id, day)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                logger.error(f"创建日期页面失败: {create_result.get('error')}")
                return None

        except Exception as e:
            logger.error(f"查找或创建日期页面时出错: {e}")
            return None

    def create_daily_learning_page(self, report_date: datetime) -> Optional[str]:
        """创建每日学习页面,使用年/月/日层级结构

        Args:
            report_date: 报告日期

        Returns:
            日期页面ID,失败返回None
        """
        try:
            year = str(report_date.year)
            month = f"{report_date.month:02d}月"
            day = f"{report_date.day:02d}日"

            logger.info(f"创建/查找每日学习页面: {year}/{month}/{day}")

            # 1. 查找或创建年份页面
            year_page_id = self.find_or_create_year_page(year)
            if not year_page_id:
                return None

            # 2. 查找或创建月份页面
            month_page_id = self.find_or_create_month_page(year_page_id, month)
            if not month_page_id:
                return None

            # 3. 查找或创建日期页面
            day_page_id = self.find_or_create_day_page(month_page_id, day)
            if not day_page_id:
                return None

            logger.info(f"每日学习页面ID: {day_page_id}")
            return day_page_id

        except Exception as e:
            logger.error(f"创建每日学习页面时出错: {e}")
            return None

    def _parse_rich_text(self, text: str) -> List[Dict]:
        """解析文本中的链接和Markdown格式,支持链接和加粗"""
        if not text:
            return [{"type": "text", "text": {"content": ""}}]

        rich_text = []
        # 匹配Markdown链接: [文本](URL)
        # 匹配Markdown加粗: **文本**
        # 组合模式,按顺序处理
        combined_pattern = r'(\[([^\]]+)\]\((https?://[^)]+)\))|(\*\*([^*]+)\*\*)'
        last_end = 0

        for match in re.finditer(combined_pattern, text):
            # 添加匹配前的普通文本
            if match.start() > last_end:
                before_text = text[last_end:match.start()]
                if before_text:
                    rich_text.append({
                        "type": "text",
                        "text": {"content": before_text}
                    })

            # 判断是链接还是加粗
            if match.group(1):  # 链接
                link_text = match.group(2)
                link_url = match.group(3)
                rich_text.append({
                    "type": "text",
                    "text": {
                        "content": link_text,
                        "link": {"url": link_url}
                    }
                })
            elif match.group(4):  # 加粗
                bold_text = match.group(5)
                rich_text.append({
                    "type": "text",
                    "text": {"content": bold_text},
                    "annotations": {"bold": True}
                })

            last_end = match.end()

        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.append({
                    "type": "text",
                    "text": {"content": remaining_text}
                })

        # 如果没有找到任何内容,返回普通文本
        if not rich_text:
            rich_text = [{"type": "text", "text": {"content": text}}]

        return rich_text

    def format_and_push_report(self, report_data: Dict[str, Any], parent_page_id: str) -> Dict[str, Any]:
        """格式化并推送报告到Notion,使用优美的排版

        Args:
            report_data: 报告数据,包含:
                - original_content: 原文内容
                - original_url: 原文URL
                - author_name: 作者名称
                - source_platform: 来源平台
                - analysis_report: 分析报告JSON
            parent_page_id: 父页面ID(日期页面)

        Returns:
            推送结果
        """
        try:
            analysis = report_data.get('analysis_report', {})
            if isinstance(analysis, str):
                analysis = json.loads(analysis)

            # 提取核心信息
            deconstruction = analysis.get('deconstruction', {})
            internalization = analysis.get('internalization_and_expression_techniques', {})
            reconstruction = analysis.get('reconstruction_showcase', [])

            # 生成页面标题 - 优先使用LLM生成的title,否则使用core_thesis
            page_title = analysis.get('page_title')
            if not page_title:
                core_thesis = deconstruction.get('core_thesis', '学习笔记')
                author = report_data.get('author_name', '未知')
                page_title = f"📝 {core_thesis[:40]} - {author}"
            else:
                # 如果有LLM生成的title,在前面加个图标
                page_title = f"📝 {page_title}"

            # 提取core_thesis用于显示
            core_thesis = deconstruction.get('core_thesis', '')

            # 构建Notion blocks
            blocks = []

            # 1. 原文引用(Callout块,带链接)
            original_text = report_data.get('original_content', '')[:1900]
            original_url = report_data.get('original_url', '')

            # 如果有URL,在原文末尾添加链接
            if original_url:
                original_text += f"\n\n[查看原文]({original_url})"

            blocks.append({
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": self._parse_rich_text(original_text),
                    "icon": {"emoji": "📌"},
                    "color": "gray_background"
                }
            })

            # 2. 核心论点(Quote块)
            if core_thesis:
                blocks.append({
                    "object": "block",
                    "type": "quote",
                    "quote": {
                        "rich_text": [{"type": "text", "text": {"content": core_thesis}}],
                        "color": "blue_background"
                    }
                })

            # 3. 解构分析(Toggle块)
            deconstruction_children = []

            post_type = deconstruction.get('post_type', '')
            if post_type:
                deconstruction_children.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {"type": "text", "text": {"content": "类型: "}, "annotations": {"bold": True}},
                            {"type": "text", "text": {"content": post_type}}
                        ]
                    }
                })

            underlying = deconstruction.get('underlying_assumption', '')
            if underlying:
                deconstruction_children.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {"type": "text", "text": {"content": "潜在假设: "}, "annotations": {"bold": True}},
                            {"type": "text", "text": {"content": underlying}}
                        ]
                    }
                })

            if deconstruction_children:
                blocks.append({
                    "object": "block",
                    "type": "toggle",
                    "toggle": {
                        "rich_text": [{"type": "text", "text": {"content": "🔍 解构分析"}, "annotations": {"bold": True}}],
                        "children": deconstruction_children,
                        "color": "default"
                    }
                })

            # 4. 核心洞察(Callout块)
            primary_insight = internalization.get('primary_insight', '')
            if primary_insight:
                blocks.append({
                    "object": "block",
                    "type": "callout",
                    "callout": {
                        "rich_text": [{"type": "text", "text": {"content": primary_insight}}],
                        "icon": {"emoji": "💡"},
                        "color": "yellow_background"
                    }
                })

            # 5. 表达技巧(Toggle块)
            technique_analysis = internalization.get('technique_analysis', [])
            if technique_analysis:
                technique_children = []

                for tech in technique_analysis:
                    tech_name = tech.get('technique_name', '')
                    tech_suggestion = tech.get('application_suggestion', '')

                    if tech_name and tech_suggestion:
                        # 技巧名称(加粗蓝色)
                        technique_children.append({
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [
                                    {"type": "text", "text": {"content": tech_name}, "annotations": {"bold": True, "color": "blue"}}
                                ]
                            }
                        })

                        # 技巧建议(支持markdown格式)
                        technique_children.append({
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": self._parse_rich_text(tech_suggestion[:1500])
                            }
                        })

                        # 添加分隔
                        if tech != technique_analysis[-1]:
                            technique_children.append({
                                "object": "block",
                                "type": "divider",
                                "divider": {}
                            })

                if technique_children:
                    blocks.append({
                        "object": "block",
                        "type": "toggle",
                        "toggle": {
                            "rich_text": [{"type": "text", "text": {"content": "✨ 表达技巧"}, "annotations": {"bold": True}}],
                            "children": technique_children,
                            "color": "default"
                        }
                    })

            # 6. 重构作品(Toggle块)
            if reconstruction:
                reconstruction_children = []

                for recon in reconstruction:
                    style = recon.get('style', '')
                    content = recon.get('content', '')
                    rationale = recon.get('rationale', '')

                    if style and content:
                        # 风格标题
                        reconstruction_children.append({
                            "object": "block",
                            "type": "heading_3",
                            "heading_3": {
                                "rich_text": [{"type": "text", "text": {"content": style}}],
                                "color": "green"
                            }
                        })

                        # 重构内容 - 使用callout块,有背景色且支持自动换行和markdown格式
                        reconstruction_children.append({
                            "object": "block",
                            "type": "callout",
                            "callout": {
                                "rich_text": self._parse_rich_text(content[:1900]),
                                "icon": {"emoji": "✍️"},
                                "color": "gray_background"
                            }
                        })

                        # 思路说明
                        if rationale:
                            reconstruction_children.append({
                                "object": "block",
                                "type": "paragraph",
                                "paragraph": {
                                    "rich_text": [
                                        {"type": "text", "text": {"content": "思路: "}, "annotations": {"italic": True, "color": "gray"}},
                                        {"type": "text", "text": {"content": rationale[:900]}, "annotations": {"italic": True}}
                                    ]
                                }
                            })

                        # 添加分隔(如果不是最后一个)
                        if recon != reconstruction[-1]:
                            reconstruction_children.append({
                                "object": "block",
                                "type": "divider",
                                "divider": {}
                            })

                if reconstruction_children:
                    blocks.append({
                        "object": "block",
                        "type": "toggle",
                        "toggle": {
                            "rich_text": [{"type": "text", "text": {"content": "✍️ 重构作品"}, "annotations": {"bold": True}}],
                            "children": reconstruction_children,
                            "color": "default"
                        }
                    })

            # 7. 元信息(分隔线 + 灰色文本)
            blocks.append({
                "object": "block",
                "type": "divider",
                "divider": {}
            })

            platform = report_data.get('source_platform', '未知')
            meta_info = f"📱 {platform} | 👤 {author}"

            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": meta_info}, "annotations": {"color": "gray"}}]
                }
            })

            # 创建页面(限制为100个块,但使用了Toggle所以一般不会超)
            create_result = self.create_page(parent_page_id, page_title, blocks[:100])

            if create_result.get("success"):
                page_id = create_result["data"]["id"]
                page_url = f"https://www.notion.so/{page_id.replace('-', '')}"

                logger.info(f"报告页面创建成功: {page_url}")
                return {
                    "success": True,
                    "page_id": page_id,
                    "page_url": page_url
                }
            else:
                logger.error(f"创建报告页面失败: {create_result.get('error')}")
                return create_result

        except Exception as e:
            logger.error(f"格式化并推送报告时出错: {e}", exc_info=True)
            return {"success": False, "error": str(e)}
