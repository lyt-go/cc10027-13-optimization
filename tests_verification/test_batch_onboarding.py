"""批量接入与规则就绪回执：端到端场景验证。

场景：
1. 一次提交三源，模拟第三源默认解析规则落盘失败；
2. 核对各源就绪状态、实际能否解析、生效解析配置身份；
3. 两个操作者并发提交相同来源，验证不重写已有人工配套；
4. 恢复后复跑同一源，确认只得到同一份有效配置身份；
5. 日志解析只取就绪来源，未就绪源不产生误导成品。

直接用后端虚拟环境运行：
  backend/.venv/bin/python -I tests_verification/test_batch_onboarding.py
"""
from __future__ import annotations

import json
import sys
import threading
import tempfile
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent / "backend"
sys.path.insert(0, str(BACKEND))

# 每个测试进程使用独立数据目录，避免与历史 data/ 产物相互污染
_TMP_DATA = tempfile.mkdtemp(prefix="batch_onboarding_data_")

import app.services.base as base_mod  # noqa: E402

base_mod.DATA_ROOT = Path(_TMP_DATA)

from app.services.log_parser import LogParserService, ParserRuleWriteError  # noqa: E402
from app.services.log_source import LogSourceService  # noqa: E402
from app.services.pipeline import CrossModulePipeline  # noqa: E402
from app.store import store  # noqa: E402

# 场景隔离：清空种子数据，只保留本脚本批量接入的源
for table in list(store.module_names()):
    store.rows(table).clear()

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: object = None) -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} -> {detail}")


def fresh_source_specs(prefix: str) -> list[dict]:
    return [
        {"源编号": f"SRC-{prefix}-A", "源名称": f"批量源A-{prefix}", "日志类型": "app"},
        {"源编号": f"SRC-{prefix}-B", "源名称": f"批量源B-{prefix}", "日志类型": "app"},
        {"源编号": f"SRC-{prefix}-C", "源名称": f"批量源C-{prefix}", "日志类型": "app"},
    ]


def main() -> int:
    svc = LogSourceService()
    parser_svc = LogParserService()

    print("== 场景1：一次提交三源，第三源规则落盘失败 ==")
    specs = fresh_source_specs("T1")
    fail_key = specs[2]["源编号"]
    LogParserService.set_write_failure(fail_key)

    # 故障注入下直接写规则应抛错，且不留半成品
    try:
        parser_svc.write_default_rule(fail_key)
        raise AssertionError("应当抛 ParserRuleWriteError")
    except ParserRuleWriteError:
        pass
    check("故障注入生效：规则写入抛 ParserRuleWriteError", True)
    check(
        "落盘失败不留半成品规则",
        parser_svc.get_effective_rule_for_source(fail_key) is None,
    )

    receipt = svc.batch_onboard_sources(specs, operator="operator-a", batch_id="BATCH-T1")
    print("  回执摘要:", json.dumps(
        {k: receipt[k] for k in ("submitted", "ready_count", "pending_count",
                                 "ready_sources", "pending_sources")},
        ensure_ascii=False))

    check("整组 3 源中恰好 2 源就绪", receipt["ready_count"] == 2, receipt["ready_count"])
    check("1 个源进入待补清单", receipt["pending_count"] == 1, receipt["pending_sources"])
    check(
        "待补清单保留失败来源与原因",
        receipt["pending_sources"][0]["source_key"] == fail_key
        and "落盘失败" in (receipt["pending_sources"][0]["reason"] or ""),
        receipt["pending_sources"],
    )

    by_key = {r["source_key"]: r for r in receipt["results"]}
    check("就绪源 A 生成解析规则身份", by_key[specs[0]["源编号"]]["parser_rule_id"] == f"PARSER-{specs[0]['源编号']}")
    check("就绪源 B 生成解析规则身份", by_key[specs[1]["源编号"]]["parser_rule_id"] == f"PARSER-{specs[1]['源编号']}")
    check("失败源 C 无解析规则身份", by_key[fail_key]["parser_rule_id"] is None)
    check("失败源 C 状态为待补配套", by_key[fail_key]["status"] == "pending_supplement")
    check(
        "写入配套与可解析状态关联：失败源不写缓冲配置",
        by_key[fail_key]["buffer_write"] is None,
        by_key[fail_key]["buffer_write"],
    )
    check("就绪源 A 缓冲配套已写入", by_key[specs[0]["源编号"]]["buffer_write"]["status"] == "created")
    check("就绪源 B 缓冲配套已写入", by_key[specs[1]["源编号"]]["buffer_write"]["status"] == "created")

    print("== 场景2：核对就绪状态、实际可解析性与配置身份 ==")
    for spec in specs[:2]:
        readiness = svc.get_source_readiness(spec["源编号"])
        check(f"源 {spec['源编号']} 就绪且可解析", readiness["ready"] and readiness["parseable"], readiness)
        check(
            f"源 {spec['源编号']} 生效配置身份可核对",
            readiness["effective_parser_rule"]["rule_id"] == f"PARSER-{spec['源编号']}",
            readiness,
        )
    fail_readiness = svc.get_source_readiness(fail_key)
    check("失败源未就绪、不可解析", not fail_readiness["ready"] and not fail_readiness["parseable"], fail_readiness)
    check("失败源无生效解析规则", fail_readiness["effective_parser_rule"] is None)
    check("失败源无缓冲配套", fail_readiness["buffer_config"] is None)

    parseable = svc.get_ready_sources_for_parsing()
    parseable_keys = {item["source_key"] for item in parseable}
    check("可解析来源清单只含 2 个就绪源", parseable_keys == {specs[0]["源编号"], specs[1]["源编号"]}, parseable_keys)
    check("失败源不在可解析清单中", fail_key not in parseable_keys)

    print("== 场景3：两操作者并发提交相同来源，不重写已有人工配套 ==")
    manual_key = "SRC-MANUAL-1"
    # 先准备一份人工配套（模拟人工已配置并生效的规则）
    svc.batch_onboard_sources(
        [{"源编号": manual_key, "源名称": "人工配套源", "日志类型": "app"}],
        operator="bootstrap", batch_id="BATCH-MANUAL-INIT",
    )
    manual_rows = [r for r in store.rows("log_parser") if r.get("关联源") == manual_key]
    check("人工配套准备：自动规则已存在", len(manual_rows) == 1)
    manual_rows[0]["配置来源"] = "manual"
    manual_rows[0]["规则编号"] = "PARSER-MANUAL-HUMAN"
    manual_identity_before = manual_rows[0]["规则编号"]

    duplicate_spec = {"源编号": manual_key, "源名称": "人工配套源", "日志类型": "app"}
    concurrent_results: list[dict] = []
    barrier = threading.Barrier(2)

    def submit(operator: str) -> None:
        barrier.wait()  # 尽量让两个线程同时进入
        r = svc.batch_onboard_sources(
            [dict(duplicate_spec)], operator=operator, batch_id=f"BATCH-CONC-{operator}"
        )
        concurrent_results.append(r)

    t1 = threading.Thread(target=submit, args=("operator-a",))
    t2 = threading.Thread(target=submit, args=("operator-b",))
    t1.start(); t2.start(); t1.join(); t2.join()

    check("两个并发提交都返回回执", len(concurrent_results) == 2)
    identities = set()
    for r in concurrent_results:
        row = r["results"][0]
        check(f"并发提交 {r['operator']} 识别为已存在来源", row["duplicate"] is True, row)
        check(f"并发提交 {r['operator']} 标记人工配套受保护", row["manual_parser_protected"] is True, row)
        check(
            f"并发提交 {r['operator']} 未新建规则而是保留既有身份",
            row["parser_write"]["status"] == "kept",
            row["parser_write"],
        )
        identities.add(row["parser_rule_id"])
    check("两个提交确认的是同一份配置身份", identities == {manual_identity_before}, identities)

    manual_rows_after = [r for r in store.rows("log_parser") if r.get("关联源") == manual_key]
    check("人工配套未被重写：规则仍只有一份", len(manual_rows_after) == 1, len(manual_rows_after))
    check("人工配套身份保持不变", manual_rows_after[0]["规则编号"] == manual_identity_before)
    check("人工配套来源标记保留", manual_rows_after[0].get("配置来源") == "manual")

    print("== 场景4：同批次重复键 + 恢复后复跑，配置身份保持唯一 ==")
    # 同一次提交里两个操作者提交相同来源（重复键）
    dup_batch = svc.batch_onboard_sources(
        [dict(duplicate_spec), dict(duplicate_spec)],
        operator="operator-a", batch_id="BATCH-DUP-IN-BATCH",
    )
    check("同批次重复提交只产生 2 条逐源记录（第二次为重复确认）", len(dup_batch["results"]) == 2)
    check("第二次提交标记 attempt=2", dup_batch["results"][1]["attempt"] == 2, dup_batch["results"][1])
    check(
        "重复确认不产生第二份规则",
        len([r for r in store.rows("log_parser") if r.get("关联源") == manual_key]) == 1,
    )

    # 恢复：解除第三源故障后复跑
    LogParserService.clear_write_failure(fail_key)
    retry_receipt = svc.retry_pending_source(fail_key, operator="operator-a", batch_id="BATCH-T1-RETRY")
    retry_row = retry_receipt["results"][0]
    check("恢复后复跑成功就绪", retry_receipt["ready_count"] == 1 and retry_row["ready"] is True, retry_row)
    check("复跑得到确定的规则身份", retry_row["parser_rule_id"] == f"PARSER-{fail_key}", retry_row)

    # 再次复跑：只确认同一份有效配置，不新建
    retry_receipt_2 = svc.retry_pending_source(fail_key, operator="operator-b", batch_id="BATCH-T1-RETRY-2")
    retry_row_2 = retry_receipt_2["results"][0]
    check("二次复跑保留既有规则身份", retry_row_2["parser_write"]["status"] == "kept", retry_row_2)
    check(
        "二次复跑身份与首次一致",
        retry_row_2["parser_rule_id"] == f"PARSER-{fail_key}",
        retry_row_2,
    )
    parser_count = len([r for r in store.rows("log_parser") if r.get("关联源") == fail_key])
    check("复跑后该源规则仍只有一份", parser_count == 1, parser_count)

    readiness_c = svc.get_source_readiness(fail_key)
    check("复跑后源状态为就绪可解析", readiness_c["ready"] and readiness_c["parseable"], readiness_c)

    print("== 场景5：采集上报按各自规则处理，未就绪过的源不产误导成品 ==")
    # 重新对一个新源制造失败，验证采集时规则未就绪会跳过解析
    specs2 = fresh_source_specs("T2")
    LogParserService.set_write_failure(specs2[1]["源编号"])
    receipt2 = svc.batch_onboard_sources(specs2, operator="operator-a", batch_id="BATCH-T2")
    check("第二批仍为 2 就绪 1 待补", receipt2["ready_count"] == 2 and receipt2["pending_count"] == 1)

    ready_source = svc._find_source_by_key(specs2[0]["源编号"])
    collect_ok = svc.collect_from_source(ready_source["id"], batch_size=5)
    check("就绪源采集成功并自动解析", collect_ok.get("written", 0) == 5 and "auto_parse" in collect_ok, collect_ok)
    check("就绪源解析无跳过", not collect_ok.get("parse_skipped"))

    # 待补源不允许采集进解析（状态非已接入，直接被拦截）
    pending_source = svc._find_source_by_key(specs2[1]["源编号"])
    collect_blocked = svc.collect_from_source(pending_source["id"], batch_size=5)
    check("待补源采集被拦截，不产生成品", "error" in collect_blocked, collect_blocked)

    # 解析服务直接对待补源调用：跳过且不写 parsed 文件
    skipped = parser_svc.parse_log_file(specs2[1]["源编号"])
    check("规则未就绪源解析被显式跳过", skipped.get("skipped") is True and skipped.get("parsed") == 0, skipped)
    parsed_file = base_mod.DATA_ROOT / "logs" / f"{specs2[1]['源编号']}_parsed.jsonl"
    check("未就绪源没有误导性解析成品文件", not parsed_file.exists())

    # 完整采集管道只处理规则就绪源
    pipeline = CrossModulePipeline()
    pipe_result = pipeline.full_collect_pipeline()
    pipe_keys = set(pipe_result.get("stages", {}).keys())
    check("采集管道只处理规则就绪源", specs2[1]["源编号"] not in pipe_keys, pipe_keys)
    check("采集管道包含就绪源", specs2[0]["源编号"] in pipe_keys, pipe_keys)

    print("== 场景6：回执持久化与查询 ==")
    all_receipts = svc.list_batch_receipts()
    check("回执已落盘可查询", len(all_receipts) >= 4, len(all_receipts))
    got = svc.get_batch_receipt("BATCH-T1")
    check("按批次号可取回首批回执", got is not None and got["batch_id"] == "BATCH-T1")
    check("首批回执保留待补原因", got["pending_sources"][0]["reason"] is not None)

    print()
    print(f"结果：通过 {PASS} 项，失败 {FAIL} 项；数据目录 {_TMP_DATA}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
