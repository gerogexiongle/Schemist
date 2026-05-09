#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DeepAgent 基础框架 — 轻量级 Agent + Skills 调度兼容 Python 3.7+, 使用 openai SDK 调用 OpenAI 兼容网关（URL/Key 见环境变量 `.env.example`）。
"""
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from openai import OpenAI

from config.settings import LLM_API_KEY, LLM_API_URL, LLM_MAX_TOKENS, LLM_MODEL, LLM_TEMPERATURE, LLM_TIMEOUT

logger = logging.getLogger("deep_agent")


def _openai_compatible_base_url(api_url: str) -> str:
    """ Strip optional /chat/completions suffix from LLM_API_URL for OpenAI client base_url. """
    u = (api_url or "").strip()
    if not u:
        raise RuntimeError(
            "LLM_API_URL is not set. Configure it in `.env` (see `ai_service_2/.env.example`), "
            "e.g. https://api.openai.com/v1/chat/completions for OpenAI-compatible providers."
        )
    base = u.rsplit("/chat/completions", 1)[0].rstrip("/")
    return base or u


class Skill:
    """可注册到 Agent 的单个技能/工具"""

    def __init__(self, name: str, description: str, func: Callable, parameters: Optional[Dict] = None):
        self.name = name
        self.description = description
        self.func = func
        self.parameters = parameters or {}

    def run(self, **kwargs) -> Any:
        return self.func(**kwargs)

    def to_tool_schema(self) -> Dict:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class DeepAgent:
    """
    DeepAgent: 多步推理 Agent, 支持 function calling 循环
    1. 将用户问题 + system prompt 发送给 LLM
    2. 如果 LLM 返回 tool_calls, 执行对应 Skill 并把结果喂回
    3. 循环直到 LLM 输出最终文本
    """

    def __init__(
        self,
        name: str = "DeepAgent",
        system_prompt: str = "",
        skills: Optional[List[Skill]] = None,
        model: str = LLM_MODEL,
        temperature: float = LLM_TEMPERATURE,
        max_tokens: int = LLM_MAX_TOKENS,
        max_iterations: int = 6,
    ):
        self.name = name
        self.system_prompt = system_prompt
        self.skills = {s.name: s for s in (skills or [])}
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_iterations = max_iterations

        self.client = OpenAI(
            api_key=LLM_API_KEY or "",
            base_url=_openai_compatible_base_url(LLM_API_URL),
            timeout=LLM_TIMEOUT,
        )

    def register_skill(self, skill: Skill):
        self.skills[skill.name] = skill

    def _build_tools(self) -> Optional[List[Dict]]:
        if not self.skills:
            return None
        return [s.to_tool_schema() for s in self.skills.values()]

    def run(self, user_message: str, history: Optional[List[Dict]] = None,
            extra_system: str = "", temperature: Optional[float] = None,
            max_tokens: Optional[int] = None) -> str:
        """
        执行 Agent 推理循环, 返回最终文本
        """
        messages = []
        combined_system = self.system_prompt
        if extra_system:
            combined_system = combined_system + "\n\n" + extra_system
        if combined_system:
            messages.append({"role": "system", "content": combined_system})

        if history:
            for msg in history:
                messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})

        messages.append({"role": "user", "content": user_message})

        tools = self._build_tools()
        temp = temperature if temperature is not None else self.temperature
        mtk = max_tokens if max_tokens is not None else self.max_tokens

        for iteration in range(self.max_iterations):
            start = time.time()
            try:
                kwargs = dict(
                    model=self.model,
                    messages=messages,
                    temperature=temp,
                    max_tokens=mtk,
                    stream=False,
                )
                if tools:
                    kwargs["tools"] = tools
                    kwargs["tool_choice"] = "auto"

                response = self.client.chat.completions.create(**kwargs)
            except Exception as e:
                logger.error("[%s] LLM 调用异常 (iter=%d): %s", self.name, iteration, e)
                raise

            choice = response.choices[0]
            assistant_msg = choice.message
            elapsed = time.time() - start
            logger.info("[%s] iter=%d, finish=%s, elapsed=%.1fs", self.name, iteration, choice.finish_reason, elapsed)

            if choice.finish_reason == "tool_calls" or getattr(assistant_msg, "tool_calls", None):
                messages.append(assistant_msg.model_dump())
                for tc in assistant_msg.tool_calls:
                    fn_name = tc.function.name
                    try:
                        fn_args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                    except json.JSONDecodeError:
                        fn_args = {}

                    skill = self.skills.get(fn_name)
                    if skill is None:
                        result_str = json.dumps({"error": "unknown skill: " + fn_name})
                    else:
                        try:
                            result = skill.run(**fn_args)
                            result_str = json.dumps(result, ensure_ascii=False, default=str)
                        except Exception as e:
                            logger.warning("[%s] skill '%s' error: %s", self.name, fn_name, e)
                            result_str = json.dumps({"error": str(e)})

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result_str,
                    })
                continue

            # 无 tool_calls：正常文本结束
            content = assistant_msg.content or ""
            if not content.strip() and tools and iteration < self.max_iterations - 1:
                logger.warning(
                    "[%s] empty assistant text at iter=%d (finish=%s), nudging",
                    self.name,
                    iteration,
                    choice.finish_reason,
                )
                try:
                    dump = assistant_msg.model_dump()
                except Exception:
                    dump = {"role": "assistant", "content": content}
                dump.pop("audio", None)
                if not dump.get("tool_calls"):
                    dump.pop("tool_calls", None)
                messages.append(dump)
                messages.append({
                    "role": "user",
                    "content": (
                        "上一步回复为空。请根据工具返回的表结构，立即给出最终答案，禁止再留空。\n"
                        "二选一：（1）仅输出一个 JSON 对象，含 sql、explanation、tables_used、execution_plan；"
                        "（2）或直接输出 ```sql 代码块。\n"
                        "sql 必须可执行且字段与工具返回一致。"
                    ),
                })
                continue

            return content

        logger.warning("[%s] 达到最大迭代次数 %d", self.name, self.max_iterations)
        return messages[-1].get("content", "") if messages else ""

    def simple_chat(self, user_message: str, system_prompt: str = "",
                    temperature: Optional[float] = None, max_tokens: Optional[int] = None) -> str:
        """不使用 skills 的简单对话"""
        messages = []
        sp = system_prompt or self.system_prompt
        if sp:
            messages.append({"role": "system", "content": sp})
        messages.append({"role": "user", "content": user_message})

        temp = temperature if temperature is not None else self.temperature
        mtk = max_tokens if max_tokens is not None else self.max_tokens

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temp,
                max_tokens=mtk,
                stream=False,
            )
            return response.choices[0].message.content or ""
        except Exception as e:
            logger.error("[%s] simple_chat error: %s", self.name, e)
            raise
