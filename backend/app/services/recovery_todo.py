"""恢复待办服务：字段缺取值来源失败后的补采任务、领用与取消联动。

字段在某次执行中因缺少取值来源校验失败时，按「原尝试」幂等生成一条补采待办，
完整保留原失败回报、领用记录、派发记录与取消原因。待办与当次校验结果联动：
字段更正并复核通过后，尚未实际派发的待办会被取消，已派发/已完成的不受影响，
且一次更正只作用于同一字段在同一执行上的待办，不波及其它执行。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService

# 补采机会终止前的状态：只有这些状态允许被校验复核取消
PRE_DISPATCH_STATUSES = ("待领用", "已领用")


class RecoveryTodoService(BaseModuleService):
    MODULE = "recovery_todo"
    ENTITY = "恢复待办"
    OPERATION_TERM = "补采"
    LIST_FIELDS = [
        "待办编号", "字段编号", "关联执行", "原尝试", "失败报告",
        "取值依据", "领用值班员", "取消原因",
    ]
    REQUIRED_FIELDS = ["待办编号", "字段编号", "关联执行", "原尝试"]
    STATUS_ORDER = ["待领用", "已领用", "补采中", "已完成", "已取消"]
    ACTION_RULES = {
        "领用待办": "已领用",
        "派发补采": "补采中",
        "完成补采": "已完成",
        "取消待办": "已取消",
    }
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """补采完成与取消都是终态，需同时清掉待处理标记。"""
        entry, message = super().run_action(entry_id, action)
        if entry is not None and entry.get("status") in ("已完成", "已取消"):
            entry["pending"] = False
        return entry, message

    # ── 待办开立：按原尝试幂等，重复失败回报不会多出补采机会 ──────────

    def get_by_attempt(self, attempt_id: str) -> dict[str, Any] | None:
        """按原尝试编号查找补采待办。"""
        for row in self.list_entries(size=10000)[0]:
            if row.get("原尝试") == attempt_id:
                return row
        return None

    def open_for_failure(
        self,
        *,
        attempt_id: str,
        field_code: str,
        run_code: str,
        basis: str,
        failure_report: dict[str, Any],
        trace_id: int | None = None,
        policy_id: int | None = None,
        operator: str = "system",
    ) -> tuple[dict[str, Any], bool]:
        """为一次失败尝试开立补采待办；同一原尝试重复提交只返回原待办。

        返回 (待办条目, 是否新开立)。
        """
        existing = self.get_by_attempt(attempt_id)
        if existing is not None:
            return existing, False

        values = {
            "待办编号": f"RC-{field_code}-{run_code}",
            "字段编号": field_code,
            "关联执行": run_code,
            "原尝试": attempt_id,
            "失败报告": failure_report.get("报告编号", attempt_id),
            "取值依据": basis,
            "领用值班员": "",
            "取消原因": "",
        }
        entry, missing = self.create_entry(values)
        if missing:  # pragma: no cover - 调用方保证必填齐全
            raise RuntimeError(f"恢复待办必填字段缺失：{missing}")

        entry["attempt_id"] = attempt_id
        entry["trace_id"] = trace_id
        entry["policy_id"] = policy_id
        entry["validation_id"] = None
        entry["原失败回报"] = dict(failure_report)
        entry["领取记录"] = None
        entry["派发记录"] = None
        entry["取消记录"] = None
        entry["结果关联"] = {"outcome": "opened", "validation_id": None, "audit_ids": []}

        audit_id = self._write_audit(
            entry, "补采待办开立",
            f"字段{field_code}在执行{run_code}缺取值来源，按原尝试{attempt_id}开立补采待办",
            operator,
        )
        entry["结果关联"]["audit_ids"].append(audit_id)
        return entry, True

    # ── 领用 / 派发 / 完成 / 取消 ────────────────────────────────────

    def claim_todo(self, todo_id: int, operator: str) -> dict[str, Any]:
        """值班员领用待办：仅待领用状态可领用，领用记录留痕。"""
        entry = self.get_entry(todo_id)
        if entry is None:
            return {"ok": False, "reason": "恢复待办不存在"}
        if entry.get("status") != "待领用":
            return {"ok": False, "reason": f"当前状态「{entry.get('status')}」不可领用"}
        self.run_action(todo_id, "领用待办")
        record = {"值班员": operator, "领用时间": self._now()}
        entry["领取记录"] = record
        entry["领用值班员"] = operator
        audit_id = self._write_audit(
            entry, "补采领用",
            f"值班员{operator}领用字段{entry.get('字段编号')}的补采待办",
            operator,
        )
        entry["结果关联"]["audit_ids"].append(audit_id)
        return {"ok": True, "todo_id": todo_id, "status": entry["status"], "claim": record}

    def dispatch_supplement(self, todo_id: int, operator: str = "system") -> dict[str, Any]:
        """派发补采：只有已领用且尚未读取的待办可派发，派发才递增重试次数。"""
        entry = self.get_entry(todo_id)
        if entry is None:
            return {"ok": False, "reason": "恢复待办不存在"}
        if entry.get("status") != "已领用":
            return {"ok": False, "reason": f"当前状态「{entry.get('status')}」不可派发补采"}
        self.run_action(todo_id, "派发补采")
        entry["派发记录"] = {"值班员": operator, "派发时间": self._now()}
        entry["结果关联"]["outcome"] = "dispatched"

        policy_id = entry.get("policy_id")
        if policy_id:
            # 局部导入避免服务注册阶段的循环依赖
            from app.services import service_for
            retry_svc = service_for("retry_policy")
            retry_svc.increment_retry(policy_id)

        audit_id = self._write_audit(
            entry, "补采派发",
            f"字段{entry.get('字段编号')}补采待办实际派发，关联重试策略{policy_id or '无'}",
            operator,
        )
        entry["结果关联"]["audit_ids"].append(audit_id)
        return {
            "ok": True, "todo_id": todo_id, "status": entry["status"],
            "policy_id": policy_id, "dispatched": True,
        }

    def complete_todo(self, todo_id: int, operator: str = "system") -> dict[str, Any]:
        """完成补采：仅补采中的待办可完成。"""
        entry = self.get_entry(todo_id)
        if entry is None:
            return {"ok": False, "reason": "恢复待办不存在"}
        if entry.get("status") != "补采中":
            return {"ok": False, "reason": f"当前状态「{entry.get('status')}」不可完成补采"}
        self.run_action(todo_id, "完成补采")
        entry["结果关联"]["outcome"] = "completed"
        audit_id = self._write_audit(
            entry, "补采完成",
            f"字段{entry.get('字段编号')}补采待办完成",
            operator,
        )
        entry["结果关联"]["audit_ids"].append(audit_id)
        return {"ok": True, "todo_id": todo_id, "status": entry["status"]}

    def cancel_todo(
        self,
        todo_id: int,
        reason: str,
        *,
        validation_id: str | None = None,
        operator: str = "system",
    ) -> dict[str, Any]:
        """取消待办：只允许取消尚未实际派发（待领用/已领用）的待办。"""
        entry = self.get_entry(todo_id)
        if entry is None:
            return {"ok": False, "reason": "恢复待办不存在"}
        if entry.get("status") not in PRE_DISPATCH_STATUSES:
            return {"ok": False, "reason": f"当前状态「{entry.get('status')}」已派发或终态，不可取消"}
        self.run_action(todo_id, "取消待办")
        record = {
            "原因": reason,
            "取消时间": self._now(),
            "校验编号": validation_id,
            "值班员": operator,
        }
        entry["取消记录"] = record
        entry["取消原因"] = reason
        entry["validation_id"] = validation_id
        entry["结果关联"]["outcome"] = "canceled"
        entry["结果关联"]["validation_id"] = validation_id
        audit_id = self._write_audit(entry, "补采取消", reason, operator)
        entry["结果关联"]["audit_ids"].append(audit_id)
        return {"ok": True, "todo_id": todo_id, "status": entry["status"], "cancel": record}

    # ── 与校验结果联动 ───────────────────────────────────────────────

    def reconcile_with_validation(
        self, field_code: str, run_code: str, validation_id: str
    ) -> list[int]:
        """用当前校验结果核对已领用范围：取消同字段同执行下尚未派发的待办。

        一次更正只影响同一字段在同一执行上的待办，其它执行或已派发/已完成的
        待办保持原状。返回实际取消的待办 id 列表，并把校验编号关联回原尝试。
        """
        canceled: list[int] = []
        for row in self.list_entries(size=10000)[0]:
            if row.get("字段编号") != field_code or row.get("关联执行") != run_code:
                continue
            if row.get("status") in PRE_DISPATCH_STATUSES:
                result = self.cancel_todo(
                    row["id"],
                    f"字段{field_code}更正后校验通过（{validation_id}），补采待办取消",
                    validation_id=validation_id,
                )
                if result.get("ok"):
                    canceled.append(row["id"])
            else:
                # 已派发/已完成/已取消：仅登记复核结论，不改变状态
                row["validation_id"] = row.get("validation_id") or validation_id
        return canceled

    # ── 查询 ─────────────────────────────────────────────────────────

    def list_by_field(self, field_code: str) -> list[dict[str, Any]]:
        """按字段编号列出全部补采待办（含其它执行）。"""
        return [
            row for row in self.list_entries(size=10000)[0]
            if row.get("字段编号") == field_code
        ]

    def list_claimed_by(self, operator: str) -> list[dict[str, Any]]:
        """查询某值班员领用范围内的待办。"""
        return [
            row for row in self.list_entries(size=10000)[0]
            if row.get("领用值班员") == operator
        ]

    # ── 内部工具 ─────────────────────────────────────────────────────

    def _write_audit(self, entry: dict[str, Any], action: str, summary: str,
                     operator: str) -> int:
        from app.services import service_for
        audit_svc = service_for("audit_trail")
        values = {
            "条目编号": f"AUDI-RC{entry['id']}-{self._now().replace(':', '').replace('-', '')}",
            "操作对象": f"恢复待办.{entry.get('待办编号', '')}",
            "操作类型": action,
            "操作者标识": operator,
            "操作时间": self._now(),
            "变更摘要": summary,
            "关联执行": entry.get("关联执行", ""),
            "追溯标记": f"recovery-{entry.get('attempt_id', entry['id'])}",
        }
        audit_entry, _ = audit_svc.create_entry(values)
        return audit_entry["id"]

    @staticmethod
    def _now() -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
