"""
LLM处理模块
实现优先级评估(Fast LLM)和深度分析(Smart Model)两阶段处理
"""
import logging
import json
import time
import re
from typing import Dict, Any, Optional
from openai import OpenAI

logger = logging.getLogger(__name__)


class LLMProcessor:
    """LLM处理器,支持优先级评估和深度分析"""

    def __init__(self, config):
        """初始化LLM处理器

        Args:
            config: 配置对象
        """
        self.config = config
        llm_config = config.get_llm_config()

        self.api_key = llm_config.get('openai_api_key')
        self.base_url = llm_config.get('openai_base_url')
        self.fast_model = llm_config.get('fast_model_name')
        self.smart_models = llm_config.get('smart_models', [])
        self.max_tokens = llm_config.get('max_tokens', 20000)

        if not self.api_key:
            raise ValueError("未找到OPENAI_API_KEY配置")

        self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)

        logger.info(f"LLM处理器初始化成功")
        logger.info(f"Fast Model: {self.fast_model}")
        logger.info(f"Smart Models: {self.smart_models}")

    def _make_request(self, prompt: str, model_name: str, temperature: float = 0.3, max_retries: int = 3) -> Dict[str, Any]:
        """执行LLM请求,支持streaming和重试机制

        Args:
            prompt: 提示词
            model_name: 模型名称
            temperature: 生成温度
            max_retries: 最大重试次数

        Returns:
            响应结果字典
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"调用LLM: {model_name} (尝试 {attempt + 1}/{max_retries})")

                # 创建streaming请求
                response = self.client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {'role': 'system', 'content': '你是一个专业的内容分析师,擅长总结和提取关键信息。'},
                        {'role': 'user', 'content': prompt}
                    ],
                    temperature=temperature,
                    max_tokens=self.max_tokens,
                    stream=True
                )

                # 收集streaming响应
                full_content = ""
                chunk_count = 0

                for chunk in response:
                    chunk_count += 1
                    try:
                        if not hasattr(chunk, 'choices') or not chunk.choices:
                            continue

                        if len(chunk.choices) == 0:
                            continue

                        delta = chunk.choices[0].delta
                        content_chunk = getattr(delta, 'content', None)

                        if content_chunk:
                            full_content += content_chunk

                    except Exception as chunk_error:
                        logger.warning(f"Chunk {chunk_count} 处理异常: {chunk_error}")
                        continue

                logger.info(f"LLM调用完成 - 响应内容长度: {len(full_content)} 字符")

                if not full_content.strip():
                    raise ValueError("LLM返回空响应")

                return {
                    'success': True,
                    'content': full_content.strip(),
                    'model': model_name,
                    'attempt': attempt + 1
                }

            except Exception as e:
                error_msg = f"LLM调用失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}"
                logger.error(error_msg)

                if attempt == max_retries - 1:
                    return {
                        'success': False,
                        'error': error_msg,
                        'model': model_name,
                        'total_attempts': max_retries
                    }
                else:
                    wait_time = (attempt + 1) * 2
                    logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)

    def run_priority_analysis(self, post_content: str) -> Dict[str, Any]:
        """运行优先级分析(Fast LLM)

        Args:
            post_content: 帖子内容

        Returns:
            分析结果,包含:
                - success: 是否成功
                - post_category: 帖子分类
                - has_image: 是否包含图片
                - attributes: 各项属性判断
                - error: 错误信息(如果失败)
        """
        # 构建提示词
        prompt = f"""# 角色
你是一名高效、精准的内容预处理器。你的任务是分析一篇社交媒体帖子,并以严格的JSON格式输出其元数据和属性。

# 核心任务
我将提供一篇社交媒体帖子。请完成以下三项分析:
1. **分类**: 从给定列表中为帖子选择最合适的类别。
2. **图片检测**: 判断帖子内容中是否包含图片链接(格式如 `![](URL)` 或 `![alt](URL)`)。
3. **属性判断**: 判断帖子是否具备某些关键特质。

# 约束条件
* 对于所有的布尔类型判断,请使用 `1` 代表 `true`,使用 `0` 代表 `false`。

# 输出格式 (严格遵循此JSON结构,不要有任何额外解释)
{{
  "post_category": "<从'技术洞察', '行业观察', '产品评论', '个人感悟', '新闻速递', '生活分享', '教程指南', '其他'中选择一个>",
  "has_image": <1 或 0>,
  "attributes": {{
    "has_unique_insight": <1 或 0>,
    "is_inspirational": <1 或 0>,
    "is_well_written": <1 或 0>,
    "is_debatable": <1 或 0>
  }}
}}

[原文Post]如下:
```
{post_content}
```
"""

        # 调用Fast Model
        result = self._make_request(prompt, self.fast_model, temperature=0.1)

        if not result.get('success'):
            return result

        # 解析JSON响应
        try:
            content = result['content']

            # 尝试提取JSON部分(可能包含在```json```代码块中)
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # 尝试直接解析
                json_str = content

            analysis = json.loads(json_str)

            return {
                'success': True,
                'post_category': analysis.get('post_category', '其他'),
                'has_image': bool(analysis.get('has_image', 0)),
                'attributes': analysis.get('attributes', {}),
                'model': result['model']
            }

        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}")
            logger.error(f"原始响应: {result['content']}")
            return {
                'success': False,
                'error': f"JSON解析失败: {str(e)}",
                'raw_content': result['content']
            }

    def calculate_priority_score(self, priority_analysis: Dict[str, Any], content_length: int) -> int:
        """计算最终优先级分数

        Args:
            priority_analysis: 优先级分析结果
            content_length: 内容字数

        Returns:
            最终分数 (0-100)
        """
        score = 0

        # LLM属性分 (总计70分)
        attributes = priority_analysis.get('attributes', {})
        if attributes.get('has_unique_insight'):
            score += 35
        if attributes.get('is_inspirational'):
            score += 20
        if attributes.get('is_debatable'):
            score += 10
        if attributes.get('is_well_written'):
            score += 5

        # 内容类型分 (总计15分)
        category = priority_analysis.get('post_category', '')
        if category in ['技术洞察', '行业观察', '产品评论']:
            score += 15
        elif category in ['个人感悟', '教程指南']:
            score += 10
        elif category in ['新闻速递', '生活分享']:
            score += 5

        # 内容丰富度分 (总计15分)
        if content_length > 200:
            score += 10
        if priority_analysis.get('has_image'):
            score += 5

        logger.info(f"计算得分: {score} (属性分 + 类型分 + 丰富度分)")
        return score

    def _extract_json_from_response(self, content: str) -> Optional[Dict]:
        """从LLM响应中提取JSON,支持多种格式

        Args:
            content: LLM响应内容

        Returns:
            解析后的JSON对象,失败返回None
        """
        if not content or not content.strip():
            logger.warning("LLM响应内容为空")
            return None

        content = content.strip()

        # 方法1: 尝试直接解析
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        # 方法2: 提取 ```json ... ``` 代码块 (贪婪模式)
        json_block_pattern = r'```(?:json)?\s*(\{.*\})\s*```'
        json_match = re.search(json_block_pattern, content, re.DOTALL)
        if json_match:
            try:
                json_str = json_match.group(1).strip()
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                logger.warning(f"代码块内JSON解析失败: {e}")

        # 方法3: 去除markdown代码块标记后解析
        if content.startswith('```'):
            lines = content.split('\n')
            # 去除第一行(```json)
            if lines[0].startswith('```'):
                lines = lines[1:]
            # 去除最后一行(```)
            if lines and lines[-1].strip() == '```':
                lines = lines[:-1]
            cleaned = '\n'.join(lines).strip()
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError as e:
                logger.warning(f"去除代码块标记后解析失败: {e}")

        # 方法4: 提取第一个 { 到最后一个 } 之间的内容
        first_brace = content.find('{')
        last_brace = content.rfind('}')
        if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
            extracted = content[first_brace:last_brace+1]
            try:
                return json.loads(extracted)
            except json.JSONDecodeError as e:
                logger.warning(f"提取大括号内容后解析失败: {e}")
                # 记录解析失败的位置
                error_pos = getattr(e, 'pos', None)
                if error_pos and error_pos < len(extracted):
                    logger.debug(f"错误位置附近: {extracted[max(0, error_pos-30):error_pos+30]}")

        # 方法5: 尝试找到所有可能的JSON对象 (使用非贪婪和贪婪两种模式)
        # 某些LLM可能在JSON前后添加额外的文本
        all_brace_patterns = [
            r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',  # 非贪婪,匹配嵌套
            r'\{.*\}',  # 贪婪模式
        ]

        for pattern in all_brace_patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            # 按长度从长到短排序,优先尝试最长的匹配
            matches.sort(key=len, reverse=True)

            for match in matches:
                try:
                    parsed = json.loads(match)
                    # 验证是否包含必要的顶层键
                    if isinstance(parsed, dict) and len(parsed) > 0:
                        logger.info(f"使用正则模式 {pattern} 成功提取JSON")
                        return parsed
                except json.JSONDecodeError:
                    continue

        # 方法6: 尝试修复常见的JSON格式问题
        # 6.1: 去除JSON字符串中的控制字符
        try:
            # 移除不可见字符
            cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', content)
            # 提取大括号内容
            first_brace = cleaned.find('{')
            last_brace = cleaned.rfind('}')
            if first_brace != -1 and last_brace != -1:
                extracted = cleaned[first_brace:last_brace+1]
                return json.loads(extracted)
        except json.JSONDecodeError:
            pass

        # 方法7: 最后的托底 - 如果响应看起来像是描述性文本,尝试基于关键字提取
        # 检查是否包含JSON的关键结构词
        if 'deconstruction' in content and 'reconstruction_showcase' in content:
            logger.warning("响应包含预期的关键字但无法解析为JSON,可能是格式错误")
            logger.debug(f"完整响应内容:\n{content}")

        logger.error(f"所有JSON提取方法均失败")
        logger.debug(f"响应前200字符: {content[:200]}")
        logger.debug(f"响应后200字符: {content[-200:]}")

        return None

    def run_depth_analysis(self, post_content: str, retry_delay: float = 2.0) -> Dict[str, Any]:
        """运行深度分析(Smart Model)

        Args:
            post_content: 帖子内容(可能包含原文+VLM图片解读)
            retry_delay: 模型间重试延迟

        Returns:
            分析结果,包含完整的JSON报告
        """
        # 构建深度分析提示词(根据规划文档)
        prompt = f"""# 1. 角色 (Role)

你是一位顶级的演讲教练与内容策略顾问,擅长将复杂或零散的信息,通过深刻的洞察和精妙的语言技巧,重塑为具有强大影响力和传播力的内容。你的核心能力是"点石成金",而非简单复述。

# 2. 核心任务 (Task)

我将提供一个`[原文Post]`,它来自社交媒体。Post内容可能包含两部分:
1. **原始帖子内容**: 用户发布的文字
2. **图片视觉解读** (可选): 如果帖子包含图片,我会提供VLM(视觉语言模型)对图片的解读

你的任务是深度分析这篇Post(综合文字和图片信息),并输出一份结构化的《内容内化与再创作报告》,旨在帮助我提升语言组织、逻辑和表达能力。

# 3. 约束条件 (Constraints)

* **深度与增量**: 你的分析和再创作必须提供超越原文的价值,严禁简单的同义替换或总结。
* **教学导向**: 你的报告不是为了直接发布,而是为了"教会"我。因此,过程、方法和技巧的拆解至关重要。
* **结构化输出**: 必须严格遵循下面定义的JSON输出格式,不得有任何遗漏。
* **图文融合**: 如果提供了图片解读,请在分析中充分融合视觉信息与文字信息,提炼出更立体的洞察。
* **CRITICAL**: 你必须只返回JSON格式的数据,不要添加任何markdown格式、标题、说明文字或其他内容。输出必须是可以直接被json.loads()解析的纯JSON对象。

# 4. 工作流与输出格式

请严格按照以下JSON结构,完成你的分析与创作报告:

{{
  "deconstruction": {{
    "post_type": "分析原文属于哪种类型。候选:'技术洞察', '行业观察', '产品评论', '个人感悟', '新闻速递', '生活分享', '教程指南'。",
    "core_thesis": "用一句话精准提炼原文的核心论点或情感核心。如果有图片,请融合视觉信息。",
    "underlying_assumption": "分析原文背后未明说的假设、价值观或情绪动机。"
  }},
  "internalization_and_expression_techniques": {{
    "primary_insight": "这个信息最重要的价值点或最触动人心的洞察是什么?(So What?) 如果有图片,图片传递了什么关键信息?",
    "technique_analysis": [
      {{
        "technique_name": "类比/比喻 (Analogy/Metaphor)",
        "application_suggestion": "针对原文内容(含图片信息),提出一个绝妙的、能让外行秒懂的类比。如果原文不适合此类比,请说明原因。"
      }},
      {{
        "technique_name": "故事化叙事 (Storytelling)",
        "application_suggestion": "如何将原文的观点或信息,包装成一个带有角色、冲突和解决方案的微型故事?请构思一个简短的故事框架。"
      }},
      {{
        "technique_name": "数据/案例支撑 (Data/Case Support)",
        "application_suggestion": "如果原文是观点型,可以引用哪些数据或具体案例来增强其说服力?如果原文是事实型,如何提炼其关键数据使其更具冲击力?"
      }},
      {{
        "technique_name": "挑战常规/逆向思考 (Contrarian Thinking)",
        "application_suggestion": "原文的观点是否存在可以挑战的盲区?提出一个与原文相反或更高维度的看问题的角度。"
      }}
    ]
  }},
  "reconstruction_showcase": [
    {{
      "style": "锐利断言式 (适合X/Twitter)",
      "content": "创作一条140字以内的、以强有力断言开头的Post,结尾附带一个引发思考的开放式问题。",
      "rationale": "解释为什么这种风格适合这个主题,以及它如何抓住注意力。"
    }},
    {{
      "style": "温和分享式 (适合即刻/朋友圈)",
      "content": "创作一条带有呼吸感、分段清晰、使用1-2个Emoji来营造氛围的Post,侧重于分享个人化的感受和启发。",
      "rationale": "解释这种风格如何建立情感连接和亲和力。"
    }},
    {{
      "style": "深度分析式 (适合作为演讲或播客素材)",
      "content": "将原文内容扩展成一段300字左右的短评,结构为:引入背景 -> 阐述核心观点 -> 引用类比或案例 -> 总结拔高。",
      "rationale": "解释这种结构如何清晰地传递深度信息,并展示逻辑层次。"
    }}
  ]
}}

**重要提示**:
1. 你的响应必须是上面JSON结构的精确实现,不要有任何额外的文字、标题、前言或说明
2. 不要使用markdown代码块包裹(不要使用 ```json ... ```)
3. 直接输出原始JSON对象,确保可以被标准JSON解析器直接解析
4. 所有字段都必须填写完整,不能遗漏
5. 禁止输出任何非JSON格式的内容,包括markdown标题、分隔线、解释性文字等

[原文Post]如下:
```
{post_content}
```
"""

        # 尝试多个Smart Model
        last_result = None

        for model_name in self.smart_models:
            logger.info(f"尝试使用Smart Model: {model_name}")

            result = self._make_request(prompt, model_name, temperature=0.5, max_retries=2)

            if result.get('success'):
                # 使用改进的JSON提取方法
                content = result['content']
                analysis_report = self._extract_json_from_response(content)

                if analysis_report:
                    # 验证必要的字段
                    required_fields = ['deconstruction', 'internalization_and_expression_techniques', 'reconstruction_showcase']
                    missing_fields = [f for f in required_fields if f not in analysis_report]

                    if missing_fields:
                        logger.warning(f"JSON缺少必要字段: {missing_fields}")
                        logger.warning(f"原始响应(前500字符): {content[:500]}")
                        last_result = {
                            'success': False,
                            'error': f"JSON缺少必要字段: {missing_fields}",
                            'model': model_name,
                            'raw_content': content
                        }
                    else:
                        logger.info(f"成功解析JSON报告,包含所有必要字段")
                        return {
                            'success': True,
                            'report': analysis_report,
                            'model': result['model']
                        }
                else:
                    logger.error(f"无法从响应中提取有效JSON (模型: {model_name})")
                    logger.error(f"完整原始响应:\n{content}")
                    last_result = {
                        'success': False,
                        'error': '无法从响应中提取有效JSON',
                        'model': model_name,
                        'raw_content': content
                    }

            else:
                last_result = result

            # 如果不是最后一个模型,等待后重试下一个
            if model_name != self.smart_models[-1]:
                logger.info(f"模型 {model_name} 失败,等待 {retry_delay} 秒后尝试下一个模型...")
                time.sleep(retry_delay)

        # 所有模型都失败
        return last_result or {
            'success': False,
            'error': '所有Smart Model调用失败'
        }
