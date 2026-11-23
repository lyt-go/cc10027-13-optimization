"""日志源管理：接入日志源、模拟日志采集、管理源状态与生命周期。"""
from __future__ import annotations

import threading
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.log_parser import LogParserService, ParserRuleWriteError
from app.store import store


class LogSourceService(BaseModuleService):
    MODULE = "log_source"
    ENTITY = "日志源"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["源编号", "源名称", "日志类型", "接入方式", "路径模版", "字符编码", "轮转规则", "源状态"]
    REQUIRED_FIELDS = ["源编号", "源名称", "日志类型"]
    STATUS_ORDER = ["草稿", "待校验", "已接入", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认接入": "已接入", "停用源": "已停用"}
    NEGATIVE_ACTIONS = ["停用源"]

    # 批量接入串行化：同一进程内并发提交同一来源时，后到者只核对，不重写
    _batch_lock = threading.RLock()
    # 按源编号细粒度加锁，保证两个操作者同时提交相同来源时只有一个执行写入
    _source_locks: dict[str, threading.Lock] = {}
    _source_locks_guard = threading.Lock()

    def onboard_source(self, values: dict[str, Any]) -> dict[str, Any]:
        """接入新日志源并初始化采集位点。"""
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"缺少必填字段: {missing}")
        # 初始化采集位点
        source_key = entry["源编号"]
        self._write_checkpoint(source_key, {
            "source_key": source_key,
            "offset": 0,
            "line_number": 0,
            "status": "active",
            "created_at": time.time(),
        })
        # 写入接入日志
        self._write_jsonl(
            self._data_dir("logs") / "onboard.jsonl",
            [{"action": "onboard", "source_key": source_key, "name": entry["源名称"], "at": time.time()}]
        )
        return entry

    # -- 批量接入与规则就绪回执 --

    @classmethod
    def _lock_for_source(cls, source_key: str) -> threading.Lock:
        with cls._source_locks_guard:
            lock = cls._source_locks.get(source_key)
            if lock is None:
                lock = threading.Lock()
                cls._source_locks[source_key] = lock
            return lock

    def _receipt_path(self):
        return self._data_dir("onboarding") / "batch_receipts.jsonl"

    def _append_receipt(self, receipt: dict[str, Any]) -> None:
        self._write_jsonl(self._receipt_path(), [receipt])

    def list_batch_receipts(self) -> list[dict[str, Any]]:
        """读取全部批量接入回执。"""
        return self._read_jsonl(self._receipt_path())

    def get_batch_receipt(self, batch_id: str) -> dict[str, Any] | None:
        """按批次号取回执。"""
        for receipt in reversed(self.list_batch_receipts()):
            if receipt.get("batch_id") == batch_id:
                return receipt
        return None

    def _find_source_by_key(self, source_key: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("源编号") == source_key:
                return row
        return None

    def _ensure_buffer_config(self, source_key: str) -> dict[str, Any]:
        """为源写入配套缓冲配置；已有同编号配置（含人工配套）时不重写。"""
        config_id = f"BUFFER-{source_key}"
        for row in store.rows("stream_buffer"):
            if row.get("配置编号") == config_id:
                return {"entry": row, "created": False, "protected": row.get("配置来源") == "manual"}
        entry = self._auto_create_seed("stream_buffer", {
            "配置编号": config_id,
            "关联源": source_key,
            "缓冲区大小": "1024",
            "批量大小": "100",
            "刷写间隔": "30",
            "内存上限": "512",
            "溢出策略": "block",
            "配置版本": "v1.0",
            "配置来源": "auto",
        })
        return {"entry": entry, "created": True, "protected": False}

    def _onboard_one_source(
        self,
        spec: dict[str, Any],
        *,
        operator: str,
        batch_id: str,
        attempt: int,
    ) -> dict[str, Any]:
        """处理单个来源的接入；按源加锁，并发/复跑同一来源只确认一份有效配置。"""
        source_key = str(spec["源编号"]).strip()
        lock = self._lock_for_source(source_key)
        with lock:
            parser_svc = LogParserService()
            source_entry = self._find_source_by_key(source_key)
            preexisting = source_entry is not None
            duplicate = False
            manual_parser_protected = False

            # 1) 源登记：已存在则不重复建源
            if source_entry is None:
                values = {
                    "源编号": source_key,
                    "源名称": spec.get("源名称") or f"批量接入源{source_key}",
                    "日志类型": spec.get("日志类型") or "app",
                    "接入方式": spec.get("接入方式") or "api",
                    "路径模版": spec.get("路径模版") or f"/var/log/{source_key}",
                    "字符编码": spec.get("字符编码") or "UTF-8",
                    "轮转规则": spec.get("轮转规则") or "daily",
                }
                source_entry, missing = self.create_entry(values)
                if source_entry is None:
                    return {
                        "source_key": source_key,
                        "ready": False,
                        "status": "failed",
                        "reason": f"缺少必填字段: {missing}",
                        "attempt": attempt,
                    }
                self._write_checkpoint(source_key, {
                    "source_key": source_key,
                    "offset": 0,
                    "line_number": 0,
                    "status": "active",
                    "created_at": time.time(),
                })
            else:
                duplicate = True

            # 批量接入体系登记的源立即打标记：规则未就绪时禁止借用遗留解析规则
            source_entry.setdefault("ready", False)

            # 2) 默认解析规则落盘（与实际可解析状态直接关联）
            existing_parser = parser_svc.get_effective_rule_for_source(source_key)
            parser_identity: str | None = None
            parser_origin = None
            if existing_parser is not None:
                # 已有人工/历史生效规则：并发提交和复跑都不能重写
                parser_identity = existing_parser["规则编号"]
                parser_origin = existing_parser.get("配置来源", "legacy")
                manual_parser_protected = parser_origin == "manual"
                parser_write = {"status": "kept", "rule_id": parser_identity, "origin": parser_origin}
            else:
                try:
                    parser_entry = parser_svc.write_default_rule(source_key, origin="auto")
                    parser_identity = parser_entry["规则编号"]
                    parser_origin = parser_entry.get("配置来源", "auto")
                    parser_write = {"status": "created", "rule_id": parser_identity, "origin": parser_origin}
                except ParserRuleWriteError as exc:
                    parser_write = {"status": "failed", "reason": str(exc)}

            # 3) 缓冲配套只跟随解析就绪状态写入；规则失败的源不产生“看似完整”的配套
            buffer_write = None
            if parser_identity is not None:
                buf = self._ensure_buffer_config(source_key)
                buffer_write = {
                    "status": "created" if buf["created"] else "kept",
                    "config_id": buf["entry"]["配置编号"],
                    "protected": buf["protected"],
                }

            # 4) 就绪判定：规则真实可解析才置为已接入；否则保留为待补来源
            parseable = parser_svc.source_is_parseable(source_key)
            if parseable:
                source_entry["status"] = "已接入"
                source_entry["源状态"] = "已接入"
                source_entry["pending"] = False
                source_entry["abnormal"] = False
                source_entry["ready"] = True
                ready, status = True, "ready"
                reason = None
            else:
                source_entry["status"] = "待校验"
                source_entry["源状态"] = "待补配套"
                source_entry["pending"] = True
                source_entry["abnormal"] = True
                source_entry["ready"] = False
                ready, status = False, "pending_supplement"
                reason = parser_write.get("reason") or "解析规则未生效，源保留为待补"

            result = {
                "batch_id": batch_id,
                "operator": operator,
                "source_key": source_key,
                "source_id": source_entry["id"],
                "ready": ready,
                "status": status,
                "duplicate": duplicate,
                "preexisting_source": preexisting,
                "manual_parser_protected": manual_parser_protected,
                "parseable": parseable,
                "parser_rule_id": parser_identity,
                "parser_origin": parser_origin,
                "parser_write": parser_write,
                "buffer_write": buffer_write,
                "reason": reason,
                "attempt": attempt,
                "at": time.time(),
            }
            return result

    def batch_onboard_sources(
        self,
        specs: list[dict[str, Any]],
        *,
        operator: str = "operator-a",
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """一次提交多源批量接入，返回逐源规则就绪回执。

        - 整组内逐源处理，单个源规则落盘失败不影响其他源；
        - 失败源进入 pending_sources 待补清单并保留原因；
        - 并发/复跑同一来源只确认同一份有效配置身份，不重写人工配套。
        """
        if not specs:
            raise ValueError("批量接入至少需要一个来源")
        batch_id = batch_id or f"BATCH-{time.strftime('%Y%m%d%H%M%S')}-{len(self.list_batch_receipts()) + 1:03d}"
        started = time.time()

        # 同批次内去重：两个操作者提交相同来源时，同键只处理一次，第二次记为重复提交
        seen: set[str] = set()
        results: list[dict[str, Any]] = []
        with self._batch_lock:
            for index, spec in enumerate(specs, start=1):
                source_key = str(spec.get("源编号", "")).strip()
                if not source_key:
                    results.append({
                        "batch_id": batch_id,
                        "operator": operator,
                        "source_key": "",
                        "ready": False,
                        "status": "failed",
                        "reason": "缺少源编号",
                        "attempt": index,
                        "at": time.time(),
                    })
                    continue
                attempt = 1
                if source_key in seen:
                    attempt = 2
                seen.add(source_key)
                results.append(self._onboard_one_source(
                    spec, operator=operator, batch_id=batch_id, attempt=attempt
                ))

        ready_sources = [r["source_key"] for r in results if r.get("ready")]
        pending_sources = [
            {"source_key": r["source_key"], "reason": r.get("reason"), "attempt": r.get("attempt")}
            for r in results if not r.get("ready")
        ]
        receipt = {
            "batch_id": batch_id,
            "operator": operator,
            "submitted": len(specs),
            "ready_count": len(ready_sources),
            "pending_count": len(pending_sources),
            "ready_sources": ready_sources,
            "pending_sources": pending_sources,
            "results": results,
            "started_at": started,
            "finished_at": time.time(),
        }
        self._append_receipt(receipt)
        return receipt

    def retry_pending_source(
        self,
        source_key: str,
        *,
        operator: str = "operator-a",
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """恢复后复跑单个待补来源：成功则确认同一配置身份，仍失败则继续保留待补原因。"""
        source = self._find_source_by_key(source_key)
        if source is None:
            return {
                "source_key": source_key,
                "ready": False,
                "status": "failed",
                "reason": "来源尚未登记，无法复跑",
            }
        spec = {
            "源编号": source_key,
            "源名称": source.get("源名称"),
            "日志类型": source.get("日志类型"),
        }
        return self.batch_onboard_sources(
            [spec], operator=operator, batch_id=batch_id or f"RETRY-{source_key}-{int(time.time())}"
        )

    def get_source_readiness(self, source_key: str) -> dict[str, Any]:
        """核对已接入源的就绪状态与实际生效解析配置身份。"""
        source = self._find_source_by_key(source_key)
        parser_svc = LogParserService()
        rule = parser_svc.get_effective_rule_for_source(source_key)
        buffer_config = next(
            (r for r in store.rows("stream_buffer")
             if r.get("关联源") == source_key and r.get("配置编号") == f"BUFFER-{source_key}"),
            None,
        )
        return {
            "source_key": source_key,
            "source_exists": source is not None,
            "source_status": source.get("status") if source else None,
            "ready": bool(source and source.get("ready")) and parser_svc.source_is_parseable(source_key),
            "parseable": parser_svc.source_is_parseable(source_key),
            "effective_parser_rule": (
                {"rule_id": rule["规则编号"], "origin": rule.get("配置来源", "legacy"), "status": rule["status"]}
                if rule else None
            ),
            "buffer_config": (
                {"config_id": buffer_config["配置编号"], "origin": buffer_config.get("配置来源", "legacy")}
                if buffer_config else None
            ),
        }

    def get_ready_sources_for_parsing(self) -> list[dict[str, Any]]:
        """日志解析按规则就绪回执取可用来源：只返回规则真实可解析的源。"""
        parser_svc = LogParserService()
        usable = []
        for source in store.rows(self.MODULE):
            key = source.get("源编号", "")
            if source.get("status") == "已接入" and parser_svc.source_is_parseable(key):
                usable.append({
                    "source_key": key,
                    "source_id": source.get("id"),
                    "parser_rule_id": parser_svc.get_effective_rule_for_source(key)["规则编号"],
                })
        return usable

    def activate_source(self, source_id: int) -> dict[str, Any]:
        """确认接入日志源，并自动创建配套的解析规则、缓冲配置和代理注册。"""
        entry, msg = self.run_action(source_id, "确认接入")
        if entry is None:
            return {"error": msg, "source_id": source_id}
        source_key = entry["源编号"]

        triggers = {}
        # 自动创建配套log_parser规则
        try:
            parser_svc = self._safe_import("app.services.log_parser", "LogParserService")()
            parser_entry = self._auto_create_seed("log_parser", {
                "规则编号": f"PARSER-{source_key}",
                "关联源": source_key,
                "正则表达式": "parser_auto",
                "字段提取": "auto_extract",
                "时间格式": "auto",
                "日志级别": "auto",
                "多行合并": "否",
                "规则版本": "v1.0",
            })
            if parser_entry:
                triggers["log_parser"] = {"created": parser_entry["规则编号"], "status": "已生效"}
        except Exception as e:
            triggers["log_parser"] = {"error": str(e)}

        # 自动创建配套stream_buffer配置
        try:
            buffer_svc = self._safe_import("app.services.stream_buffer", "StreamBufferService")()
            buffer_entry = self._auto_create_seed("stream_buffer", {
                "配置编号": f"BUFFER-{source_key}",
                "关联源": source_key,
                "缓冲区大小": "1024",
                "批量大小": "100",
                "刷写间隔": "30",
                "内存上限": "512",
                "溢出策略": "block",
                "配置版本": "v1.0",
            })
            if buffer_entry:
                triggers["stream_buffer"] = {"created": buffer_entry["配置编号"], "status": "已生效"}
        except Exception as e:
            triggers["stream_buffer"] = {"error": str(e)}

        # 自动注册采集代理
        try:
            agent_svc = self._safe_import("app.services.agent_heartbeat", "AgentHeartbeatService")()
            agent_entry = self._auto_create_seed("agent_heartbeat", {
                "代理编号": f"AGENT-{source_key}",
                "部署主机": f"host-{source_key}",
                "代理版本": "v2.1",
                "上次心跳": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
                "采集量": "0",
                "错误率": "0",
                "内存占用": "0",
                "代理状态": "运行中",
            })
            if agent_entry:
                triggers["agent_heartbeat"] = {"created": agent_entry["代理编号"], "status": "运行中"}
        except Exception as e:
            triggers["agent_heartbeat"] = {"error": str(e)}

        return {
            "source_entry": entry,
            "source_key": source_key,
            "activated_at": time.time(),
            "auto_triggers": triggers,
        }

    def collect_from_source(self, source_id: int, batch_size: int = 100) -> dict[str, Any]:
        """从指定日志源采集一批日志行到本地日志文件。"""
        source = store.find(self.MODULE, source_id)
        if source is None:
            return {"error": f"日志源 {source_id} 不存在"}
        if source.get("status") != "已接入":
            return {"error": f"日志源 {source_id} 未接入，当前状态: {source.get('status')}"}

        # 规则就绪回执把关：解析规则未就绪的源只登记原始采集，不进入解析，避免误导成品
        parser_svc = LogParserService()
        rule_ready = parser_svc.source_is_parseable(source["源编号"])

        cp = self._read_checkpoint(source["源编号"]) or {"offset": 0, "line_number": 0}
        lines = self._simulate_log_lines(source, count=batch_size)
        start_line = cp["line_number"]

        records = []
        for i, line in enumerate(lines):
            records.append({
                "source_key": source["源编号"],
                "source_name": source["源名称"],
                "line_no": start_line + i + 1,
                "offset": cp["offset"] + len(line.encode("utf-8")) + 1,
                "raw": line,
                "collected_at": time.time(),
            })

        output_file = self._data_dir("logs") / f"{source['源编号']}.jsonl"
        written = self._write_jsonl(output_file, records)

        new_line = start_line + written
        new_offset = cp["offset"] + sum(len(r["raw"].encode("utf-8")) + 1 for r in records)
        self._write_checkpoint(source["源编号"], {
            "source_key": source["源编号"],
            "offset": new_offset,
            "line_number": new_line,
            "status": "active",
            "last_collected_at": time.time(),
        })

        result = {
            "source_key": source["源编号"],
            "batch_size": batch_size,
            "written": written,
            "start_line": start_line,
            "end_line": new_line,
            "checkpoint_offset": new_offset,
        }

        # 采集后按各自规则触发解析；规则未就绪的源只保留原始日志，不产出误导成品
        if not rule_ready:
            result["parse_skipped"] = (
                f"日志源 {source['源编号']} 解析规则未就绪，跳过解析与后续成品生成"
            )
            return result

        parse_r = self._trigger_parse_for_source(source["源编号"])
        if "error" not in parse_r and not parse_r.get("skipped"):
            result["auto_parse"] = parse_r
            # 解析成功后自动触发映射
            map_r = self._trigger_mapping_for_source(source["源编号"])
            if "error" not in map_r:
                result["auto_mapping"] = map_r

        return result

    def get_active_sources(self) -> list[dict[str, Any]]:
        """获取所有已接入的活跃日志源。"""
        return self._collect_sources()