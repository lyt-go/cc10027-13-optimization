"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, BatchOnboardPayload, EntryPayload, PageResult
from app.services import service_for
from app.services.log_parser import LogParserService
from app.services.log_source import LogSourceService
from app.services.pipeline import pipeline

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


# -- 批量接入与规则就绪回执（固定路径，须在 /{module} 动态路由之前注册）--

@router.post("/log-sources/batch-onboard")
def batch_onboard_sources(payload: BatchOnboardPayload) -> dict[str, Any]:
    """一次提交多源批量接入，返回逐源规则就绪回执。"""
    if not payload.sources:
        raise HTTPException(status_code=400, detail="批量接入至少需要一个来源")
    return LogSourceService().batch_onboard_sources(
        payload.sources, operator=payload.operator, batch_id=payload.batch_id
    )


@router.get("/log-sources/batch-receipts")
def list_batch_receipts() -> dict[str, Any]:
    """批量接入回执清单。"""
    receipts = LogSourceService().list_batch_receipts()
    return {"items": receipts, "total": len(receipts)}


@router.get("/log-sources/batch-receipts/{batch_id}")
def get_batch_receipt(batch_id: str) -> dict[str, Any]:
    receipt = LogSourceService().get_batch_receipt(batch_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id} 回执不存在")
    return receipt


@router.get("/log-sources/{source_key}/readiness")
def get_source_readiness(source_key: str) -> dict[str, Any]:
    """核对某源就绪状态、实际可解析性与生效解析配置身份。"""
    return LogSourceService().get_source_readiness(source_key)


@router.post("/log-sources/{source_key}/retry")
def retry_pending_source(source_key: str, payload: BatchOnboardPayload | None = None) -> dict[str, Any]:
    """恢复后复跑待补来源。"""
    operator = payload.operator if payload and payload.operator else "operator-a"
    batch_id = payload.batch_id if payload else None
    return LogSourceService().retry_pending_source(source_key, operator=operator, batch_id=batch_id)


@router.get("/log-sources/ready/parseable")
def get_parseable_sources() -> dict[str, Any]:
    """日志解析按就绪回执取实际可解析来源。"""
    items = LogSourceService().get_ready_sources_for_parsing()
    return {"items": items, "total": len(items)}


@router.post("/log-sources/simulate-parser-write-failure/{source_key}")
def simulate_parser_write_failure(source_key: str, enabled: bool = Query(default=True)) -> dict[str, Any]:
    """打开/关闭某源默认解析规则落盘故障注入。"""
    if enabled:
        LogParserService.set_write_failure(source_key)
    else:
        LogParserService.clear_write_failure(source_key)
    return {"ok": True, "source_key": source_key, "write_failure_enabled": enabled}


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# -- 跨模块管道端点 --

@router.post("/pipelines/collect")
def run_collect_pipeline(source_id: int = Query(default=0)) -> dict[str, Any]:
    """执行完整采集管道：采集 -> 解析 -> 映射 -> 输出。"""
    return pipeline.full_collect_pipeline(source_id)


@router.post("/pipelines/event/{event_code}")
def run_event_pipeline(event_code: str) -> dict[str, Any]:
    """执行事件追踪管道：跟踪 -> 缓冲 -> 校验。"""
    return pipeline.event_tracking_pipeline([event_code])


@router.post("/pipelines/backfill/{source_key}")
def run_backfill_pipeline(source_key: str, time_range: str = Query(default="2026-09-01~2026-09-30")) -> dict[str, Any]:
    """执行回溯采集管道：创建任务 -> 分批执行 -> 位点更新。"""
    return pipeline.backfill_pipeline(source_key, time_range)


@router.post("/pipelines/quality-alert/{source_key}")
def run_quality_alert_pipeline(source_key: str) -> dict[str, Any]:
    """执行质量诊断+告警管道：诊断 -> 评估 -> 触发告警 -> 位点回滚。"""
    return pipeline.quality_alert_pipeline(source_key)


@router.post("/pipelines/enrich-aggregate/{source_key}")
def run_enrich_aggregate_pipeline(source_key: str) -> dict[str, Any]:
    """执行富化+聚合管道：构建维表 -> 富化 -> 聚合 -> 告警评估。"""
    return pipeline.enrichment_aggregation_pipeline(source_key)


@router.post("/pipelines/dedup-sample/{source_key}")
def run_dedup_sample_pipeline(source_key: str) -> dict[str, Any]:
    """执行去重+采样管道：窗口去重 -> 采样 -> 格式化输出。"""
    return pipeline.dedup_sample_pipeline(source_key)


@router.post("/pipelines/rotate-partition/{source_key}")
def run_rotate_partition_pipeline(source_key: str) -> dict[str, Any]:
    """执行轮转+分区管道：轮转 -> 分区路由 -> 归并。"""
    return pipeline.rotate_partition_pipeline(source_key)


@router.post("/pipelines/agent-checkpoint/{agent_code}")
def run_agent_checkpoint_pipeline(agent_code: str) -> dict[str, Any]:
    """执行代理心跳+位点管道：心跳上报 -> 位点同步 -> 离线检测。"""
    return pipeline.agent_checkpoint_pipeline(agent_code)


@router.post("/pipelines/run-all")
def run_all_pipelines() -> dict[str, Any]:
    """运行所有管道组合。"""
    return pipeline.run_all_pipelines()
