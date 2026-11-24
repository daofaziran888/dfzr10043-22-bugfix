"""步骤轨迹服务：执行步骤记录、快照对比与异常标记。

记录流程每一步的输入输出快照与耗时，为失败定位和数据回溯
提供逐步骤证据链。支持复核、异常标记和归档流转。

当前校验结果以「复核清单」挂到轨迹上：字段更正并校验通过后，复核清单
记录新的校验编号与实际取值依据，复核通过的异常轨迹会退出异常态，
避免重试策略页面沿过时失败记录误操作。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class StepTraceService(BaseModuleService):
    MODULE = "step_trace"
    ENTITY = "步骤轨迹"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "轨迹编号", "关联执行", "步骤序号", "步骤名称", "输入快照",
        "输出快照", "耗时毫秒", "退出码",
    ]
    REQUIRED_FIELDS = ["轨迹编号", "关联执行", "步骤序号"]
    STATUS_ORDER = ["已记录", "待复核", "异常", "已归档"]
    ACTION_RULES = {
        "提交复核": "待复核",
        "标记异常": "异常",
        "复核通过": "待复核",
        "归档轨迹": "已归档",
    }
    NEGATIVE_ACTIONS = []

    def mark_abnormal(self, trace_id: int, exit_code: int = 1) -> dict[str, Any]:
        """标记步骤异常并设置退出码。"""
        entry = self.get_entry(trace_id)
        if entry is None:
            return {"ok": False, "reason": "轨迹不存在"}
        entry["退出码"] = str(exit_code)
        self.run_action(trace_id, "标记异常")
        return {"ok": True, "trace_id": trace_id, "exit_code": exit_code}

    def add_review_item(self, trace_id: int, validation_record: dict[str, Any]) -> dict[str, Any]:
        """把当前校验结果加入步骤轨迹的复核清单，作为复核取值依据。"""
        entry = self.get_entry(trace_id)
        if entry is None:
            return {"ok": False, "reason": "轨迹不存在"}
        checklist = entry.setdefault("复核清单", [])
        # 同一校验编号只保留一条，迟到重放复用原记录时不重复入清单
        if any(item.get("校验编号") == validation_record.get("validation_id")
               for item in checklist):
            return {"ok": True, "trace_id": trace_id, "checklist": checklist,
                    "duplicated": True}
        checklist.append({
            "校验编号": validation_record.get("validation_id"),
            "字段编号": validation_record.get("field_code"),
            "取值依据": validation_record.get("basis"),
            "通过": validation_record.get("valid"),
            "原因": validation_record.get("reason", ""),
            "时间": validation_record.get("at"),
        })
        return {"ok": True, "trace_id": trace_id, "checklist": checklist}

    def review_abnormal_trace(self, trace_id: int, validation_id: str) -> dict[str, Any]:
        """依据复核清单中的通过结论复核异常轨迹，清除过时异常。

        只有复核清单里确实存在该校验编号的通过记录时才允许复核通过，
        防止无依据地把异常轨迹改回正常态。
        """
        entry = self.get_entry(trace_id)
        if entry is None:
            return {"ok": False, "reason": "轨迹不存在"}
        checklist = entry.get("复核清单", [])
        matched = next(
            (item for item in checklist
             if item.get("校验编号") == validation_id and item.get("通过")),
            None,
        )
        if matched is None:
            return {"ok": False, "reason": "复核清单中无该编号的通过校验，不能复核通过"}
        self.run_action(trace_id, "复核通过")
        entry["abnormal"] = False
        entry["复核结论"] = {
            "validation_id": validation_id,
            "basis": matched.get("取值依据"),
            "reviewed_at": self._now(),
        }
        return {"ok": True, "trace_id": trace_id, "status": entry["status"]}

    def get_traces_by_run(self, run_code: str) -> list[dict[str, Any]]:
        """按关联执行编号获取所有步骤轨迹。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("关联执行") == run_code]

    def archive_completed_traces(self) -> dict[str, Any]:
        """归档所有已完成步骤的轨迹。"""
        items, _ = self.list_entries(size=10000)
        archived = 0
        for it in items:
            if it.get("status") in ("已记录", "待复核"):
                self.run_action(it["id"], "归档轨迹")
                archived += 1
        return {"ok": True, "archived_count": archived}

    @staticmethod
    def _now() -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
