"""HTTP 层冒烟：通过 FastAPI TestClient 验证批量接入端到端接口。"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent / "backend"
sys.path.insert(0, str(BACKEND))

_TMP = tempfile.mkdtemp(prefix="batch_http_data_")
import app.services.base as base_mod  # noqa: E402

base_mod.DATA_ROOT = Path(_TMP)

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.services.log_parser import LogParserService  # noqa: E402
from app.store import store  # noqa: E402

for table in list(store.module_names()):
    store.rows(table).clear()

client = TestClient(app)
failures = []


def expect(cond: bool, msg: str) -> None:
    print(("  ✓ " if cond else "  ✗ ") + msg)
    if not cond:
        failures.append(msg)


# 健康检查
r = client.get("/api/health")
expect(r.status_code == 200 and r.json()["ok"], "健康检查 200")

# 故障注入：第三源规则落盘失败
sources = [
    {"源编号": "HTTP-A", "源名称": "HTTP源A", "日志类型": "app"},
    {"源编号": "HTTP-B", "源名称": "HTTP源B", "日志类型": "app"},
    {"源编号": "HTTP-C", "源名称": "HTTP源C", "日志类型": "app"},
]
r = client.post("/api/log-sources/simulate-parser-write-failure/HTTP-C")
expect(r.status_code == 200 and r.json()["write_failure_enabled"] is True, "打开规则落盘故障注入")

# 批量接入
r = client.post("/api/log-sources/batch-onboard", json={
    "sources": sources, "operator": "operator-a", "batch_id": "HTTP-BATCH-1",
})
expect(r.status_code == 200, f"批量接入 200（实际 {r.status_code}: {r.text[:200]}）")
receipt = r.json()
expect(receipt["ready_count"] == 2 and receipt["pending_count"] == 1, "2 就绪 1 待补")
expect(receipt["pending_sources"][0]["source_key"] == "HTTP-C", "待补源为 HTTP-C 且原因保留")

# 回执查询
r = client.get("/api/log-sources/batch-receipts/HTTP-BATCH-1")
expect(r.status_code == 200 and r.json()["batch_id"] == "HTTP-BATCH-1", "按批次号查回执")
r = client.get("/api/log-sources/batch-receipts/not-exist")
expect(r.status_code == 404, "未知批次回执 404")
r = client.get("/api/log-sources/batch-receipts")
expect(r.status_code == 200 and r.json()["total"] == 1, "回执清单")

# 就绪核对
r = client.get("/api/log-sources/HTTP-A/readiness")
body = r.json()
expect(body["ready"] and body["parseable"] and body["effective_parser_rule"]["rule_id"] == "PARSER-HTTP-A",
       "就绪源生效配置身份可核对")
r = client.get("/api/log-sources/HTTP-C/readiness")
body = r.json()
expect(not body["ready"] and not body["parseable"] and body["effective_parser_rule"] is None,
       "待补源未就绪不可解析")

# 可解析来源清单
r = client.get("/api/log-sources/ready/parseable")
keys = {i["source_key"] for i in r.json()["items"]}
expect(keys == {"HTTP-A", "HTTP-B"}, f"可解析清单仅含两源（{keys}）")

# 并发重复提交（HTTP 串行模拟两个操作者）
for op in ("operator-a", "operator-b"):
    r = client.post("/api/log-sources/batch-onboard", json={
        "sources": [{"源编号": "HTTP-A", "源名称": "HTTP源A", "日志类型": "app"}],
        "operator": op,
        "batch_id": f"HTTP-DUP-{op}",
    })
    row = r.json()["results"][0]
    expect(row["duplicate"] and row["parser_write"]["status"] == "kept",
           f"操作者 {op} 重复提交只确认既有配置")
parser_rows = [x for x in store.rows("log_parser") if x.get("关联源") == "HTTP-A"]
expect(len(parser_rows) == 1, "HTTP-A 规则仍只有一份")

# 恢复 + 复跑
client.post("/api/log-sources/simulate-parser-write-failure/HTTP-C", params={"enabled": False})
r = client.post("/api/log-sources/HTTP-C/retry", json={"operator": "operator-a", "batch_id": "HTTP-RETRY-1"})
row = r.json()["results"][0]
expect(row["ready"] and row["parser_rule_id"] == "PARSER-HTTP-C", "恢复后复跑就绪且身份确定")
r = client.post("/api/log-sources/HTTP-C/retry", json={"operator": "operator-b", "batch_id": "HTTP-RETRY-2"})
row = r.json()["results"][0]
expect(row["parser_write"]["status"] == "kept", "二次复跑只确认同一份配置")
parser_rows = [x for x in store.rows("log_parser") if x.get("关联源") == "HTTP-C"]
expect(len(parser_rows) == 1 and parser_rows[0]["规则编号"] == "PARSER-HTTP-C", "HTTP-C 规则唯一且身份不变")

# 采集：就绪源自动解析；待补源不存在于已接入状态被拦截
src_id = next(s["id"] for s in store.rows("log_source") if s["源编号"] == "HTTP-A")
r = client.post("/api/pipelines/collect")
stage_keys = set(r.json().get("stages", {}).keys())
expect("HTTP-A" in stage_keys and "HTTP-B" in stage_keys, f"采集管道只处理就绪源（{stage_keys}）")

# 空批次 400
r = client.post("/api/log-sources/batch-onboard", json={"sources": []})
expect(r.status_code == 400, "空批次返回 400")

print()
print(f"HTTP 冒烟：{'全部通过' if not failures else f'{len(failures)} 项失败'}")
sys.exit(1 if failures else 0)
