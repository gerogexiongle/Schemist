#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Skills pack registry.

文件布局：
  skills_pack/
    <skill_id>/
      SKILL.md          # YAML frontmatter + Markdown 模板正文

frontmatter 支持字段：
  id:           技能 id（字符串，需与文件夹名一致；建议小写字母、数字、连字符）
  name:         显示名
  description:  一句话说明
  icon:         可选 emoji / icon 文本
  placeholder:  选中技能后输入框 placeholder 提示
  enabled:      bool，默认 true；false 时不在下拉中显示
  order:        int，排序权重（小的靠前）
  engine_hint:  可选，"spark" / "trino"（仅作提示，不强制）
  llm_model_hint: 可选，建议使用的大模型 id
  tags:         list[str]，标签

正文支持变量占位：
  {{user_query}}  必含；若缺失，注册表会自动在末尾追加「{{user_query}}」以保证可用。
  {{engine}}      当前 SQL 引擎
  {{today}}       YYYY-MM-DD
"""

from __future__ import annotations

import logging
import os
import re
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger("skills_pack")

_SKILLS_DIR = os.path.dirname(os.path.abspath(__file__))
_SKILL_FILE = "SKILL.md"
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{0,63}$")
_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n(.*)$", re.DOTALL)


class SkillNotFound(Exception):
    pass


class SkillValidationError(Exception):
    pass


def _is_valid_id(skill_id: str) -> bool:
    return isinstance(skill_id, str) and bool(_ID_RE.match(skill_id))


def _parse_skill_file(path: str, fallback_id: str) -> Dict[str, Any]:
    """读取 SKILL.md，返回 {meta: dict, body: str}。"""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    meta: Dict[str, Any] = {}
    body = text
    m = _FRONTMATTER_RE.match(text.lstrip("\ufeff"))
    if m:
        raw_meta, body = m.group(1), m.group(2)
        try:
            parsed = yaml.safe_load(raw_meta) or {}
            if isinstance(parsed, dict):
                meta = parsed
        except yaml.YAMLError as e:
            logger.warning("skill %s frontmatter 解析失败: %s", fallback_id, e)

    meta.setdefault("id", fallback_id)
    meta.setdefault("name", meta.get("id", fallback_id))
    meta.setdefault("description", "")
    meta.setdefault("icon", "")
    meta.setdefault("placeholder", "")
    meta.setdefault("enabled", True)
    meta.setdefault("order", 100)
    meta.setdefault("engine_hint", "")
    meta.setdefault("llm_model_hint", "")
    meta.setdefault("tags", [])

    return {"meta": meta, "body": body.strip("\n")}


def _serialize_skill_file(meta: Dict[str, Any], body: str) -> str:
    """把 meta + body 拼成 SKILL.md 文本（frontmatter + body）。"""
    meta_out = {
        "id": meta.get("id"),
        "name": meta.get("name"),
        "description": meta.get("description", ""),
        "icon": meta.get("icon", ""),
        "placeholder": meta.get("placeholder", ""),
        "enabled": bool(meta.get("enabled", True)),
        "order": int(meta.get("order", 100)),
    }
    if meta.get("engine_hint"):
        meta_out["engine_hint"] = meta["engine_hint"]
    if meta.get("llm_model_hint"):
        meta_out["llm_model_hint"] = meta["llm_model_hint"]
    if meta.get("tags"):
        meta_out["tags"] = list(meta["tags"])

    fm = yaml.safe_dump(meta_out, allow_unicode=True, sort_keys=False).strip()
    body_clean = (body or "").rstrip() + "\n"
    return "---\n{}\n---\n\n{}".format(fm, body_clean)


def render_template(body: str, variables: Dict[str, Any]) -> str:
    """把 {{var}} 占位替换成真实值；未提供的变量保持原样。"""
    def _sub(match: "re.Match") -> str:
        key = match.group(1).strip()
        if key in variables and variables[key] is not None:
            return str(variables[key])
        return match.group(0)

    return re.sub(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}", _sub, body or "")


class SkillsRegistry:
    """基于文件系统的技能注册表，mtime 热加载，进程内线程安全。"""

    def __init__(self, base_dir: str = _SKILLS_DIR):
        self.base_dir = base_dir
        self._lock = threading.RLock()
        self._cache: Dict[str, Dict[str, Any]] = {}  # id -> {meta, body, mtime}
        self._dir_mtime_cache: Dict[str, float] = {}
        os.makedirs(self.base_dir, exist_ok=True)

    def _skill_dir(self, skill_id: str) -> str:
        return os.path.join(self.base_dir, skill_id)

    def _skill_file(self, skill_id: str) -> str:
        return os.path.join(self._skill_dir(skill_id), _SKILL_FILE)

    def _iter_skill_ids_on_disk(self) -> List[str]:
        ids: List[str] = []
        if not os.path.isdir(self.base_dir):
            return ids
        for entry in os.listdir(self.base_dir):
            full = os.path.join(self.base_dir, entry)
            if not os.path.isdir(full):
                continue
            if entry.startswith(".") or entry.startswith("_"):
                continue
            if not _is_valid_id(entry):
                continue
            if os.path.isfile(os.path.join(full, _SKILL_FILE)):
                ids.append(entry)
        return ids

    def _load_one(self, skill_id: str) -> Optional[Dict[str, Any]]:
        path = self._skill_file(skill_id)
        if not os.path.isfile(path):
            return None
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            return None
        cached = self._cache.get(skill_id)
        if cached and cached.get("mtime") == mtime:
            return cached
        try:
            parsed = _parse_skill_file(path, skill_id)
        except Exception as e:
            logger.warning("加载技能 %s 失败: %s", skill_id, e)
            return None
        parsed["mtime"] = mtime
        self._cache[skill_id] = parsed
        return parsed

    def refresh(self) -> None:
        """按目录 mtime 判断是否需要重扫。"""
        with self._lock:
            try:
                cur = os.path.getmtime(self.base_dir)
            except OSError:
                cur = 0
            last = self._dir_mtime_cache.get(self.base_dir, -1)
            if cur != last:
                self._dir_mtime_cache[self.base_dir] = cur
                current_ids = set(self._iter_skill_ids_on_disk())
                for gone in list(self._cache.keys()):
                    if gone not in current_ids:
                        self._cache.pop(gone, None)
            for sid in self._iter_skill_ids_on_disk():
                self._load_one(sid)

    def list(self, include_disabled: bool = False, include_body: bool = False) -> List[Dict[str, Any]]:
        self.refresh()
        out: List[Dict[str, Any]] = []
        with self._lock:
            for sid, rec in self._cache.items():
                meta = dict(rec.get("meta") or {})
                if not include_disabled and not meta.get("enabled", True):
                    continue
                item = dict(meta)
                item["id"] = sid
                item["updated_at"] = datetime.fromtimestamp(rec.get("mtime", 0)).strftime("%Y-%m-%d %H:%M:%S")
                if include_body:
                    item["body"] = rec.get("body", "")
                out.append(item)
        out.sort(key=lambda x: (int(x.get("order", 100)), str(x.get("name", ""))))
        return out

    def get(self, skill_id: str) -> Dict[str, Any]:
        if not _is_valid_id(skill_id):
            raise SkillValidationError("非法 skill id: {}".format(skill_id))
        self.refresh()
        with self._lock:
            rec = self._cache.get(skill_id) or self._load_one(skill_id)
            if not rec:
                raise SkillNotFound("skill 不存在: {}".format(skill_id))
            meta = dict(rec.get("meta") or {})
            meta["id"] = skill_id
            meta["body"] = rec.get("body", "")
            meta["updated_at"] = datetime.fromtimestamp(rec.get("mtime", 0)).strftime("%Y-%m-%d %H:%M:%S")
            return meta

    def _validate_meta(self, meta: Dict[str, Any]) -> None:
        if not isinstance(meta, dict):
            raise SkillValidationError("meta 必须是对象")
        sid = meta.get("id")
        if not _is_valid_id(sid or ""):
            raise SkillValidationError(
                "id 非法：需为小写字母/数字/下划线/连字符，首字符为字母或数字，长度<=64"
            )
        if not str(meta.get("name", "")).strip():
            raise SkillValidationError("name 不能为空")

    def save(self, meta: Dict[str, Any], body: str, *, create: bool = False) -> Dict[str, Any]:
        self._validate_meta(meta)
        sid = meta["id"]
        d = self._skill_dir(sid)
        path = self._skill_file(sid)
        with self._lock:
            exists = os.path.isfile(path)
            if create and exists:
                raise SkillValidationError("id 已存在: {}".format(sid))
            if not create and not exists:
                raise SkillNotFound("skill 不存在: {}".format(sid))
            os.makedirs(d, exist_ok=True)
            if "{{user_query}}" not in (body or ""):
                body = (body or "").rstrip() + "\n\n【用户问题】\n{{user_query}}\n"
            text = _serialize_skill_file(meta, body)
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(tmp, path)
            self._cache.pop(sid, None)
            self._dir_mtime_cache.pop(self.base_dir, None)
        return self.get(sid)

    def delete(self, skill_id: str) -> None:
        if not _is_valid_id(skill_id):
            raise SkillValidationError("非法 skill id: {}".format(skill_id))
        path = self._skill_file(skill_id)
        with self._lock:
            if not os.path.isfile(path):
                raise SkillNotFound("skill 不存在: {}".format(skill_id))
            try:
                os.remove(path)
            except OSError as e:
                raise SkillValidationError("删除失败: {}".format(e))
            d = self._skill_dir(skill_id)
            try:
                if os.path.isdir(d) and not os.listdir(d):
                    os.rmdir(d)
            except OSError:
                pass
            self._cache.pop(skill_id, None)
            self._dir_mtime_cache.pop(self.base_dir, None)

    def render(self, skill_id: str, variables: Dict[str, Any]) -> str:
        skill = self.get(skill_id)
        return render_template(skill.get("body") or "", variables or {})


_registry_singleton: Optional[SkillsRegistry] = None
_registry_lock = threading.Lock()


def get_registry() -> SkillsRegistry:
    global _registry_singleton
    if _registry_singleton is None:
        with _registry_lock:
            if _registry_singleton is None:
                _registry_singleton = SkillsRegistry()
    return _registry_singleton
