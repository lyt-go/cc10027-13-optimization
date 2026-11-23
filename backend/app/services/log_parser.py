"""日志解析：按规则解析原始日志行，提取结构化字段。"""
from __future__ import annotations

import json
import re
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ParserRuleWriteError(RuntimeError):
    """默认解析规则落盘失败：批量接入用它模拟某源规则写入故障。"""


class LogParserService(BaseModuleService):
    MODULE = "log_parser"
    ENTITY = "解析规则"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["规则编号", "关联源", "正则表达式", "字段提取", "时间格式", "日志级别", "多行合并", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "关联源", "正则表达式"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # 故障注入：集合内源编号在写入默认解析规则时抛错，模拟规则落盘失败
    _fail_write_keys: set[str] = set()

    @classmethod
    def set_write_failure(cls, source_key: str) -> None:
        """打开指定源的规则落盘故障注入。"""
        cls._fail_write_keys.add(source_key)

    @classmethod
    def clear_write_failure(cls, source_key: str | None = None) -> None:
        """关闭规则落盘故障注入；不传 source_key 时全部清除。"""
        if source_key is None:
            cls._fail_write_keys.clear()
        else:
            cls._fail_write_keys.discard(source_key)

    def write_default_rule(self, source_key: str, *, origin: str = "auto") -> dict[str, Any]:
        """为日志源写入默认解析规则并标记生效。

        - origin=auto 表示批量接入自动配套；origin=manual 表示人工配套。
        - 已存在同编号规则时直接返回既有身份，不覆盖人工/既有配置。
        - 命中故障注入时抛 ParserRuleWriteError，且仓库中不留半成品规则。
        """
        if source_key in self._fail_write_keys:
            raise ParserRuleWriteError(
                f"日志源 {source_key} 默认解析规则落盘失败：规则目录不可写"
            )

        rule_id = f"PARSER-{source_key}"
        existing = self._find_rule_by_id(rule_id)
        if existing is not None:
            return existing

        rows = store.rows(self.MODULE)
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "规则编号": rule_id,
            "关联源": source_key,
            "正则表达式": r"(?P<timestamp>\S+)\s+\[(?P<source>[^\]]+)\]\s+(?P<level>\w+)\s+(?P<body>.*)",
            "字段提取": "auto_extract",
            "时间格式": "auto",
            "日志级别": "auto",
            "多行合并": "否",
            "规则版本": "v1.0",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "配置来源": origin,
        }
        rows.append(entry)
        # 规则落盘回执：把生效规则身份持久化，便于事后核对
        self._write_jsonl(
            self._data_dir("onboarding") / "parser_rules.jsonl",
            [{
                "rule_id": rule_id,
                "source_key": source_key,
                "origin": origin,
                "status": "已生效",
                "at": time.time(),
            }],
        )
        return entry

    def _find_rule_by_id(self, rule_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("规则编号") == rule_id:
                return row
        return None

    def get_effective_rule_for_source(
        self, source_key: str, *, allow_legacy: bool = True
    ) -> dict[str, Any] | None:
        """返回某源当前真正生效（已生效 + 正则可编译）的解析规则，没有则 None。

        优先精确匹配该源自身的规则；对批量接入体系建立前的遗留源（源行无 ready
        标记、规则无配置来源标记），保留历史全局生效规则兜底；批量接入管理的源
        规则未就绪时不允许借用遗留规则，避免给不完整的源制造可解析假象。
        """
        for row in store.rows(self.MODULE):
            if row.get("关联源") != source_key or row.get("status") != "已生效":
                continue
            pattern = row.get("正则表达式", "")
            try:
                re.compile(pattern)
            except re.error:
                continue
            return row

        if allow_legacy:
            source = next(
                (s for s in store.rows("log_source") if s.get("源编号") == source_key),
                None,
            )
            # 批量接入登记过的源（含待补源）不做遗留兜底
            if source is not None and "ready" in source:
                return None
            for row in store.rows(self.MODULE):
                if row.get("status") != "已生效" or row.get("配置来源"):
                    continue
                try:
                    re.compile(row.get("正则表达式", ""))
                except re.error:
                    continue
                legacy = dict(row)
                legacy["规则编号"] = row.get("规则编号")
                legacy["配置来源"] = "legacy"
                return legacy
        return None

    def source_is_parseable(self, source_key: str) -> bool:
        """核对某源是否具备实际可解析能力：存在生效且正则可编译的规则。"""
        return self.get_effective_rule_for_source(source_key) is not None

    def parse_log_file(self, source_key: str) -> dict[str, Any]:
        """解析指定日志源已采集的日志文件。"""
        # 先核对规则就绪：规则未就绪的源不进入解析，避免产出误导成品
        own_rule = self.get_effective_rule_for_source(source_key)
        if own_rule is None:
            return {
                "source_key": source_key,
                "skipped": True,
                "reason": "规则未就绪：无生效且可编译的解析规则，跳过解析避免产出误导成品",
                "parsed": 0,
                "errors": 0,
            }
        parsers = [own_rule]

        log_file = self._data_dir("logs") / f"{source_key}.jsonl"
        if not log_file.exists():
            return {"error": f"日志源 {source_key} 无已采集数据", "parsed": 0}

        records = self._read_jsonl(log_file)

        parsed = []
        errors = []
        for rec in records:
            parsed_entry = self._apply_parsers(rec, parsers)
            if parsed_entry.get("parse_error"):
                errors.append(parsed_entry)
            else:
                parsed.append(parsed_entry)

        output_file = self._data_dir("logs") / f"{source_key}_parsed.jsonl"
        written = self._write_jsonl(output_file, parsed)

        if errors:
            self._write_jsonl(
                self._data_dir("logs") / f"{source_key}_parse_errors.jsonl",
                errors
            )

        self._write_checkpoint(f"{source_key}_parse", {
            "total": len(records),
            "parsed": written,
            "errors": len(errors),
            "completed_at": time.time(),
        })

        return {
            "source_key": source_key,
            "total": len(records),
            "parsed": written,
            "errors": len(errors),
            "output": str(output_file),
        }

    def _apply_parsers(self, rec: dict[str, Any], parsers: list[dict[str, Any]]) -> dict[str, Any]:
        raw = rec.get("raw", "")
        result = dict(rec)
        result["fields"] = {}
        result["parse_error"] = None

        if not parsers:
            # 无规则时使用默认解析
            m = re.match(r"(\S+) (\S+) \[(\w+)\] (\S+) (\S+) (.+)", raw)
            if m:
                result["fields"] = {
                    "timestamp": m.group(1),
                    "host": m.group(2),
                    "source": m.group(3),
                    "level": m.group(4),
                    "module": m.group(5),
                    "message": m.group(6),
                }
            else:
                result["parse_error"] = "默认正则未匹配"
            return result

        for parser in parsers:
            try:
                pattern = parser.get("正则表达式", "")
                if pattern and pattern not in ("", "日志解析样例1"):
                    m = re.search(pattern, raw)
                    if m:
                        result["fields"] = m.groupdict() if m.groupdict() else {str(i): g for i, g in enumerate(m.groups())}
                        result["parser_rule"] = parser["规则编号"]
                        return result
            except re.error:
                continue

        # 默认解析兜底
        result["fields"]["raw_message"] = raw
        result["fields"]["collected_at"] = rec.get("collected_at")
        return result

    def match_source_to_parser(self, source_id: int) -> list[dict[str, Any]]:
        """查找匹配指定日志源的解析规则。"""
        source_code = f"LOG_-{source_id:04d}"
        matches = []
        for p in store.rows(self.MODULE):
            if p.get("status") == "已生效" and (p.get("关联源") == source_code or p.get("关联源") == str(source_id)):
                matches.append(p)
        return matches