# -*- coding: utf-8 -*-
"""Skills pack: 提示词模板能力包（现代 Agent Skill 风格）。

每个技能是 skills_pack/<id>/SKILL.md 形式的 Markdown：顶部 YAML frontmatter 描述元数据，
正文是固化的 prompt 模板，支持 {{user_query}} / {{engine}} / {{today}} 变量占位。

真正的加载逻辑在 registry.py。"""

from .registry import (  # noqa: F401
    SkillsRegistry,
    get_registry,
    SkillNotFound,
    SkillValidationError,
)
