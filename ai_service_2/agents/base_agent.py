#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DeepAgent 基础框架 — 轻量级 Agent + Skills 调度引擎
兼容 Python 3.7+, 使用 openai SDK 调用 OpenAI 兼容网关（URL/Key 见 `.env` / `.env.example`）。
"""
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from openai import OpenAI

from config.settings import LLM_API_KEY, LLM_API_URL, LLM_MAX_TOKENS, LLM_MODEL, LLM_TEMPERATURE, LLM_TIMEOUT

logger = logging.getLogger("deep_agent")


def _openai_compatible_base_url(api_url: str) -> str:
    u = (api_url or "").strip()
    if not u:
        raise RuntimeError(
            "LLM_API_URL 未设置。请在 `ai_service_2/.env` 中配置（见 `.env.example`），"
            "例如 https://api.openai.com/v1/chat/completions 或自建网关完整 URL。"
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
        force_finalize_on_limit: bool = False,
        finalization_prompt: str = "",
        skill_call_limits: Optional[Dict[str, int]] = None,
        max_total_skill_calls: Optional[int] = None,
        block_duplicate_skill_calls: bool = False,
        request_timeout: Optional[float] = None,
        max_retries: Optional[int] = None,
    ):
        self.name = name
        self.system_prompt = system_prompt
        self.skills = {s.name: s for s in (skills or [])}
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_iterations = max_iterations
        self.force_finalize_on_limit = force_finalize_on_limit
        self.finalization_prompt = finalization_prompt
        self.skill_call_limits = dict(skill_call_limits or {})
        self.max_total_skill_calls = max_total_skill_calls
        self.block_duplicate_skill_calls = block_duplicate_skill_calls
        self.request_timeout = request_timeout
        self.max_retries = max_retries
        self.last_messages = []
        self.last_run_info = {}

        client_options = dict(
            api_key=LLM_API_KEY or "",
            base_url=_openai_compatible_base_url(LLM_API_URL),
            timeout=request_timeout if request_timeout is not None else LLM_TIMEOUT,
        )
        if max_retries is not None:
            client_options["max_retries"] = max(0, int(max_retries))
        self.client = OpenAI(**client_options)

    def register_skill(self, skill: Skill):
        self.skills[skill.name] = skill

    def _build_tools(self) -> Optional[List[Dict]]:
        if not self.skills:
            return None
        return [s.to_tool_schema() for s in self.skills.values()]

    @staticmethod
    def _message_dump(message) -> Dict:
        try:
            dump = message.model_dump()
        except Exception:
            dump = {
                "role": "assistant",
                "content": getattr(message, "content", "") or "",
            }
        dump.pop("audio", None)
        if not dump.get("tool_calls"):
            dump.pop("tool_calls", None)
        return dump

    @staticmethod
    def _tool_call_key(name: str, arguments: Dict) -> str:
        return "{}:{}".format(
            name,
            json.dumps(arguments or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        )

    def _finalize_messages(self, messages: List[Dict], prompt: str, model: Optional[str] = None,
                           temperature: float = 0.1, max_tokens: Optional[int] = None,
                           request_timeout: Optional[float] = None,
                           max_retries: Optional[int] = None) -> str:
        """Run exactly one completion without tools, forcing the model to return final text."""
        final_messages = list(messages or [])
        final_messages.append({"role": "user", "content": prompt})
        selected_model = model or self.model
        completion_client = self.client
        client_options = {}
        if request_timeout is not None:
            client_options["timeout"] = request_timeout
        if max_retries is not None:
            client_options["max_retries"] = max(0, int(max_retries))
        if client_options:
            completion_client = self.client.with_options(**client_options)
        response = completion_client.chat.completions.create(
            model=selected_model,
            messages=final_messages,
            temperature=temperature,
            max_tokens=max_tokens if max_tokens is not None else self.max_tokens,
            stream=False,
        )
        assistant_msg = response.choices[0].message
        final_messages.append(self._message_dump(assistant_msg))
        self.last_messages = final_messages
        return assistant_msg.content or ""

    def finalize_last_run(self, prompt: str, model: Optional[str] = None,
                          temperature: float = 0.1, max_tokens: Optional[int] = None,
                          request_timeout: Optional[float] = None,
                          max_retries: Optional[int] = None) -> str:
        """Review/finalize the previous run once with tools disabled."""
        if not self.last_messages:
            raise RuntimeError("Agent has no previous run to finalize")
        return self._finalize_messages(
            self.last_messages,
            prompt=prompt,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            request_timeout=request_timeout,
            max_retries=max_retries,
        )

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
        seen_tool_calls = set()
        skill_call_counts = {}
        total_skill_calls = 0
        duplicate_hits = 0
        limit_hits = 0

        self.last_messages = list(messages)
        self.last_run_info = {
            "forced_finalization": False,
            "tool_rounds": 0,
            "skill_calls": {},
            "duplicate_hits": 0,
            "limit_hits": 0,
        }

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
                messages.append(self._message_dump(assistant_msg))
                self.last_run_info["tool_rounds"] += 1
                for tc in assistant_msg.tool_calls:
                    fn_name = tc.function.name
                    try:
                        fn_args = json.loads(tc.function.arguments) if tc.function.arguments else {}
                    except json.JSONDecodeError:
                        fn_args = {}

                    skill = self.skills.get(fn_name)
                    call_key = self._tool_call_key(fn_name, fn_args)
                    per_skill_limit = self.skill_call_limits.get(fn_name)
                    current_count = skill_call_counts.get(fn_name, 0)

                    if self.block_duplicate_skill_calls and call_key in seen_tool_calls:
                        duplicate_hits += 1
                        result_str = json.dumps({
                            "error": "duplicate_tool_call",
                            "tool": fn_name,
                            "message": "相同工具和参数已经执行过，请使用已有结果并输出最终答案。",
                        }, ensure_ascii=False)
                        logger.warning("[%s] blocked duplicate skill '%s'", self.name, fn_name)
                    elif per_skill_limit is not None and current_count >= max(0, int(per_skill_limit)):
                        limit_hits += 1
                        result_str = json.dumps({
                            "error": "skill_call_limit_reached",
                            "tool": fn_name,
                            "limit": per_skill_limit,
                            "message": "该工具已达到调用上限，请根据已有结果输出最终答案。",
                        }, ensure_ascii=False)
                        logger.warning("[%s] skill '%s' reached limit=%s", self.name, fn_name, per_skill_limit)
                    elif self.max_total_skill_calls is not None and total_skill_calls >= max(0, int(self.max_total_skill_calls)):
                        limit_hits += 1
                        result_str = json.dumps({
                            "error": "total_skill_call_limit_reached",
                            "limit": self.max_total_skill_calls,
                            "message": "工具总调用次数已达到上限，请根据已有结果输出最终答案。",
                        }, ensure_ascii=False)
                        logger.warning("[%s] total skill calls reached limit=%s", self.name, self.max_total_skill_calls)
                    elif skill is None:
                        result_str = json.dumps({"error": "unknown skill: " + fn_name})
                    else:
                        seen_tool_calls.add(call_key)
                        skill_call_counts[fn_name] = current_count + 1
                        total_skill_calls += 1
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
                self.last_messages = list(messages)
                self.last_run_info["skill_calls"] = dict(skill_call_counts)
                self.last_run_info["duplicate_hits"] = duplicate_hits
                self.last_run_info["limit_hits"] = limit_hits
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
                dump = self._message_dump(assistant_msg)
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
                self.last_messages = list(messages)
                continue

            messages.append(self._message_dump(assistant_msg))
            self.last_messages = list(messages)
            return content

        logger.warning("[%s] 达到最大迭代次数 %d", self.name, self.max_iterations)
        self.last_messages = list(messages)
        if self.force_finalize_on_limit:
            prompt = self.finalization_prompt or (
                "工具调用阶段已经结束。禁止再调用任何工具；请使用已有工具结果，立即输出最终答案。"
            )
            logger.info("[%s] forced_finalize.start model=%s", self.name, self.model)
            try:
                content = self._finalize_messages(
                    messages,
                    prompt=prompt,
                    model=self.model,
                    temperature=min(temp, 0.1),
                    max_tokens=mtk,
                )
                self.last_run_info["forced_finalization"] = True
                logger.info("[%s] forced_finalize.done chars=%d", self.name, len(content or ""))
                return content
            except Exception as e:
                logger.error("[%s] forced_finalize.failed: %s", self.name, e)
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
