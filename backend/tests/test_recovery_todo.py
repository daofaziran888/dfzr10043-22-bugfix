"""链路6 字段取值依据与恢复待办状态联动的确定性测试。

复现值班场景：A2、B2 两个字段在各自执行中都缺取值来源；领用 A2 的失败
待办后更正 A2，在尚未读取（未实际派发补采）时重放原失败。断言：
- 校验判断指向实际参与本次采集的取值依据，原失败回报与领用记录可查；
- 一次更正只影响同字段同执行的待办，不波及其它执行（B2 可继续补采）；
- 重复提交旧失败不会多出补采机会、不多出采集与重试次数；
- 当前校验结果进入步骤轨迹复核清单，过时异常可复核清除；
- 派发/取消结果关联回原尝试。
"""
from __future__ import annotations

from app.pipeline import (
    continue_supplement,
    replay_field_failure,
    validate_form_field,
)
from app.services import service_for


def _prepare_field(template_code: str, field_code: str, name: str) -> dict:
    """造一个缺取值来源的表单字段。"""
    field_svc = service_for("form_field")
    entry, _ = field_svc.create_entry({
        "字段编号": field_code,
        "关联模板": template_code,
        "表单名称": "复核表单",
        "字段名称": name,
        "取值来源": "",
        "校验规则": "required",
        "填充顺序": "1",
        "失败提示": "字段缺取值来源",
    })
    return entry


def _prepare_trace(run_code: str) -> dict:
    trace_svc = service_for("step_trace")
    entry, _ = trace_svc.create_entry({
        "轨迹编号": f"STEP-{run_code}",
        "关联执行": run_code,
        "步骤序号": "1",
        "步骤名称": "表单填充",
        "输入快照": "{}",
        "输出快照": "{}",
        "耗时毫秒": "300",
        "退出码": "0",
    })
    return entry


def _policy() -> dict:
    retry_svc = service_for("retry_policy")
    return next(
        p for p in retry_svc.list_entries(size=1000)[0]
        if p.get("关联规则") == "EXCE-0004"
    )


class TestRecoveryTodoLinkage:
    """A2/B2 场景全链路。"""

    def test_full_a2_b2_recovery_scenario(self):
        field_svc = service_for("form_field")
        todo_svc = service_for("recovery_todo")
        trace_svc = service_for("step_trace")
        policy = _policy()
        policy_id = policy["id"]
        assert policy["累计重试"] == "0"

        a2 = _prepare_field("TPL-A", "FORM-A2", "A2字段")
        b2 = _prepare_field("TPL-B", "FORM-B2", "B2字段")
        trace_a = _prepare_trace("RUN-A")
        trace_b = _prepare_trace("RUN-B")
        attempt_a = "ATT-A2-RUN-A-1"
        attempt_b = "ATT-B2-RUN-B-1"

        # 1) 两个字段都缺来源，各自产生且仅产生一条补采待办
        fail_a = validate_form_field(
            a2["id"], trace_a["id"], run_code="RUN-A", attempt_id=attempt_a)
        fail_b = validate_form_field(
            b2["id"], trace_b["id"], run_code="RUN-B", attempt_id=attempt_b)
        assert fail_a["status"] == "validation_failed"
        assert fail_a["todo_created"] is True
        assert fail_b["todo_created"] is True
        todo_a = todo_svc.get_entry(fail_a["todo_id"])
        todo_b = todo_svc.get_entry(fail_b["todo_id"])
        assert todo_a["status"] == "待领用"
        assert todo_b["status"] == "待领用"
        # 轨迹按本次失败标记异常，失败时快照的依据为空（实际参与采集的依据）
        assert trace_svc.get_entry(trace_a["id"])["status"] == "异常"
        assert todo_a["取值依据"] == ""
        assert todo_a["原失败回报"]["报告编号"] == f"FAIL-{fail_a['validation_id']}"

        # 2) 值班员领用 A2 的失败待办，但还没开始读取（未派发补采）
        claim = todo_svc.claim_todo(todo_a["id"], "duty_zhang")
        assert claim["ok"] is True
        assert todo_svc.get_entry(todo_a["id"])["status"] == "已领用"
        assert todo_a["领取记录"]["值班员"] == "duty_zhang"

        # 3) 更正 A2 的取值来源后重新校验通过
        passed = validate_form_field(
            a2["id"], trace_a["id"], run_code="RUN-A",
            attempt_id="ATT-A2-RUN-A-2", new_source="datasource_a2_fixed")
        assert passed["status"] == "validated"
        assert passed["is_valid"] is True
        assert passed["basis"] == "datasource_a2_fixed"

        # 4) 当前校验结果进入复核清单，过时异常被复核清除
        trace_after = trace_svc.get_entry(trace_a["id"])
        assert trace_after["status"] == "待复核"
        checklist = trace_after["复核清单"]
        assert any(item["校验编号"] == passed["validation_id"]
                   and item["通过"] is True
                   and item["取值依据"] == "datasource_a2_fixed"
                   for item in checklist)

        # 5) 恢复待办按已领用范围核对：A2 待办取消，取消结果关联回原尝试
        assert passed["canceled_todo_ids"] == [todo_a["id"]]
        todo_a = todo_svc.get_entry(todo_a["id"])
        assert todo_a["status"] == "已取消"
        assert "校验通过" in todo_a["取消原因"]
        assert todo_a["validation_id"] == passed["validation_id"]
        assert todo_a["结果关联"]["outcome"] == "canceled"

        # 6) 在仍未读取时重放 A2 的原失败（迟到回报），不新增补采机会
        replay = replay_field_failure(
            a2["id"], trace_a["id"], run_code="RUN-A", attempt_id=attempt_a,
            basis="", reason="先前的失败报告迟到")
        assert replay["status"] == "replay_rejected"
        assert replay["new_todo_created"] is False
        # 没有新的 A2 待办，原待办仍是已取消
        a2_todos = todo_svc.list_by_field("FORM-A2")
        assert len(a2_todos) == 1
        assert a2_todos[0]["status"] == "已取消"
        # 迟到回报仍可查
        assert todo_a["迟到回报"][0]["late_arrival"] is True
        assert todo_a["迟到回报"][0]["basis"] == ""

        # 7) 继续原领用的补采：已取消，不产生新采集、不增加重试次数
        retry_before = int(_policy()["累计重试"])
        cont = continue_supplement(todo_a["id"], operator="duty_zhang")
        assert cont["ok"] is False
        assert cont["dispatched"] is False
        assert int(_policy()["累计重试"]) == retry_before == 0

        # 8) 原依据、领取记录与取消原因仍可复查
        record = field_svc.get_validation_record(a2["id"], fail_a["validation_id"])
        assert record is not None
        assert record["valid"] is False
        assert record["basis"] == ""  # 历史失败依据不被后续更正改写
        passed_record = field_svc.get_validation_record(a2["id"], passed["validation_id"])
        assert passed_record["basis"] == "datasource_a2_fixed"
        assert todo_a["领取记录"]["值班员"] == "duty_zhang"
        assert todo_a["取消记录"]["校验编号"] == passed["validation_id"]

        # 9) 一次更正不影响 B2：B2 待办仍可领用并实际派发补采
        todo_b = todo_svc.get_entry(todo_b["id"])
        assert todo_b["status"] == "待领用"
        assert todo_svc.claim_todo(todo_b["id"], "duty_li")["ok"] is True
        dispatch_b = continue_supplement(todo_b["id"], operator="duty_li")
        assert dispatch_b["ok"] is True
        assert dispatch_b["dispatched"] is True
        assert todo_svc.get_entry(todo_b["id"])["status"] == "补采中"
        # 实际派发才递增一次重试
        assert int(_policy()["累计重试"]) == retry_before + 1
        # B2 完成补采后再次继续不会多出采集
        todo_svc.complete_todo(todo_b["id"])
        again_b = continue_supplement(todo_b["id"], operator="duty_li")
        assert again_b["dispatched"] is False
        assert int(_policy()["累计重试"]) == 1

    def test_duplicate_failure_reports_do_not_create_extra_todos(self):
        field_svc = service_for("form_field")
        todo_svc = service_for("recovery_todo")
        field = _prepare_field("TPL-C", "FORM-C2", "C2字段")
        trace = _prepare_trace("RUN-C")
        attempt = "ATT-C2-RUN-C-1"

        first = validate_form_field(
            field["id"], trace["id"], run_code="RUN-C", attempt_id=attempt)
        assert first["todo_created"] is True
        # 同一原尝试重复上报失败：直接去重，不新立待办
        second = replay_field_failure(
            field["id"], trace["id"], run_code="RUN-C", attempt_id=attempt,
            basis="", reason="重复提交旧失败")
        assert second["new_todo_created"] is False
        assert second["todo_id"] == first["todo_id"]
        assert len(todo_svc.list_by_field("FORM-C2")) == 1
        # 字段始终缺依据，重放登记为去重而非驳回
        assert second["status"] == "replay_deduplicated"

    def test_correction_on_one_run_does_not_touch_other_run_todos(self):
        todo_svc = service_for("recovery_todo")
        field = _prepare_field("TPL-D", "FORM-D2", "D2字段")
        trace_1 = _prepare_trace("RUN-D1")
        trace_2 = _prepare_trace("RUN-D2")

        r1 = validate_form_field(
            field["id"], trace_1["id"], run_code="RUN-D1", attempt_id="ATT-D1")
        r2 = validate_form_field(
            field["id"], trace_2["id"], run_code="RUN-D2", attempt_id="ATT-D2")
        todo_svc.claim_todo(r1["todo_id"], "duty_wang")
        # 更正只针对 RUN-D1
        result = validate_form_field(
            field["id"], trace_1["id"], run_code="RUN-D1",
            attempt_id="ATT-D1-FIX", new_source="datasource_d2_fixed")
        assert result["canceled_todo_ids"] == [r1["todo_id"]]
        assert todo_svc.get_entry(r1["todo_id"])["status"] == "已取消"
        # RUN-D2 的待办不受影响，仍可领用补采
        todo_2 = todo_svc.get_entry(r2["todo_id"])
        assert todo_2["status"] == "待领用"
        todo_svc.claim_todo(todo_2["id"], "duty_chen")
        dispatch = continue_supplement(todo_2["id"])
        assert dispatch["dispatched"] is True

    def test_dispatched_todo_is_not_canceled_by_later_correction(self):
        todo_svc = service_for("recovery_todo")
        field = _prepare_field("TPL-E", "FORM-E2", "E2字段")
        trace = _prepare_trace("RUN-E")
        attempt = "ATT-E2-RUN-E-1"
        failed = validate_form_field(
            field["id"], trace["id"], run_code="RUN-E", attempt_id=attempt)
        todo_id = failed["todo_id"]
        todo_svc.claim_todo(todo_id, "duty_zhao")
        # 已经实际派发补采后字段才更正通过
        dispatched = continue_supplement(todo_id, operator="duty_zhao")
        assert dispatched["dispatched"] is True
        passed = validate_form_field(
            field["id"], trace["id"], run_code="RUN-E",
            attempt_id="ATT-E2-RUN-E-2", new_source="datasource_e2_fixed")
        # 已派发的待办不在取消范围内，结果仅登记复核结论
        assert passed["canceled_todo_ids"] == []
        assert todo_svc.get_entry(todo_id)["status"] == "补采中"

    def test_review_pass_requires_passing_checklist_item(self):
        trace_svc = service_for("step_trace")
        trace = _prepare_trace("RUN-F")
        trace_svc.mark_abnormal(trace["id"])
        # 无复核清单通过依据时，不能直接把异常复核通过
        rejected = trace_svc.review_abnormal_trace(trace["id"], "VAL-NOT-EXIST")
        assert rejected["ok"] is False
        assert trace_svc.get_entry(trace["id"])["status"] == "异常"
