"""表单字段服务：取值绑定、校验规则和填充顺序管理。

维护流程中表单填充字段的取值来源、校验规则和填充顺序，
校验失败时阻断该步骤。支持重试校验和按模板筛选。

每次校验都会落一条校验记录，记录「本次实际参与采集的取值依据」而非字段
当前值：迟到的旧失败回报可凭校验编号回溯，字段后续更正不会改写历史依据。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


def is_usable_basis(source: Any, field_name: str = "") -> bool:
    """取值依据有效：非空、非纯空白，且不能用字段名称占位。"""
    text = "" if source is None else str(source).strip()
    if not text:
        return False
    if field_name and text == str(field_name).strip():
        return False
    return True


class FormFieldService(BaseModuleService):
    MODULE = "form_field"
    ENTITY = "填充字段"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "字段编号", "关联模板", "表单名称", "字段名称", "取值来源",
        "校验规则", "填充顺序", "失败提示",
    ]
    REQUIRED_FIELDS = ["字段编号", "关联模板", "字段名称"]
    STATUS_ORDER = ["草稿", "已绑定", "校验失败", "已停用"]
    ACTION_RULES = {"绑定取值": "已绑定", "重新校验": "校验失败", "停用字段": "已停用"}
    NEGATIVE_ACTIONS = ["停用字段"]

    def _now(self) -> str:
        return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")

    def validate_field(self, field_id: int, actual_value: str = "") -> dict[str, Any]:
        """校验表单字段：取值来源非空则通过，否则标记校验失败。"""
        entry = self.get_entry(field_id)
        if entry is None:
            return {"ok": False, "reason": "字段不存在"}
        source = entry.get("取值来源", "")
        if not is_usable_basis(source, entry.get("字段名称", "")):
            self.run_action(field_id, "重新校验")
            return {"ok": False, "reason": "取值来源无效，校验失败"}
        self.run_action(field_id, "绑定取值")
        return {"ok": True, "field_id": field_id, "valid": True}

    def record_validation(
        self,
        field_id: int,
        *,
        run_code: str,
        attempt_id: str,
        basis: str,
        valid: bool,
        reason: str = "",
    ) -> dict[str, Any]:
        """登记一条校验记录，快照本次实际参与采集的取值依据。

        校验编号按字段+执行+原尝试生成，同一原尝试重复上报不重复登记，
        保证迟到回报与原记录对得上，且后续更正不影响历史依据。
        """
        entry = self.get_entry(field_id)
        if entry is None:
            return {"ok": False, "reason": "字段不存在"}
        registry: list[dict[str, Any]] = entry.setdefault("校验记录", [])
        validation_id = f"VAL-{entry.get('字段编号', field_id)}-{run_code}-{attempt_id}"
        for record in registry:
            if record.get("validation_id") == validation_id:
                return {"ok": True, "record": record, "duplicated": True}
        record = {
            "validation_id": validation_id,
            "field_id": field_id,
            "field_code": entry.get("字段编号", ""),
            "run_code": run_code,
            "attempt_id": attempt_id,
            # 关键：固化本次实际使用的依据，字段后续更正不会回溯修改
            "basis": str(basis or ""),
            "valid": bool(valid),
            "reason": reason,
            "at": self._now(),
        }
        registry.append(record)
        return {"ok": True, "record": record, "duplicated": False}

    def get_validation_record(self, field_id: int, validation_id: str) -> dict[str, Any] | None:
        """按校验编号查询历史校验记录（含当时的取值依据）。"""
        entry = self.get_entry(field_id)
        if entry is None:
            return None
        for record in entry.get("校验记录", []):
            if record.get("validation_id") == validation_id:
                return record
        return None

    def get_fields_by_template(self, template_code: str) -> list[dict[str, Any]]:
        """按关联模板获取表单字段列表。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("关联模板") == template_code]
