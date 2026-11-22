"""批量查询领域服务：管理批量API查询任务的排队、执行和结果汇总。

分项结果回填模型（修复"每项都直接记成功"的问题）：
- 每个子查询独立记录状态：success（已确认拿到完整结果）/ timeout（超时未拿到数据）
  / failed（明确失败），不再无条件记成功。
- 超时/失败子项进入「批量查询待补数据」（backlog），保留可定位的查询标识，可继续恢复。
- 迟到应答与重试通过去重账本（attempt ledger）归并：同一子项只有第一个被确认的
  结果计入有效行数，迟到重报/重试重复到达只记 duplicate，不会变成两次成功。
- 重跑只针对待补子项发起，已确认的结果直接复用，不重复执行。
- 汇总数量只统计已确认子项；缓存预热只使用确认过的结果。

跨模块数据流：
- 消费 api_registry 快照 → 批量查询API合法性校验
- 消费 query_optimize 快照 → 应用每个子查询的优化规则
- 消费 cache_layer 快照 → 批量查询结果缓存策略
- 批量结果 → 写入共享快照供下游（含 cache_layer 预热）消费
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 子项状态
STATUS_SUCCESS = "success"
STATUS_TIMEOUT = "timeout"
STATUS_FAILED = "failed"
# 待补（未确认）状态集合
PENDING_STATUSES = (STATUS_TIMEOUT, STATUS_FAILED)


class BatchQueryService(BaseModuleService):
    MODULE = "batch_query"
    ENTITY = "批量任务"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["任务编号", "关联API", "查询清单", "并发度", "失败策略", "结果汇总", "超时控制", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "关联API", "查询清单"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "部分完成"]
    ACTION_RULES = {"发起执行": "执行中", "确认完成": "已完成", "标记部分完成": "部分完成",
                    "重跑失败项": "部分完成"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        # 全局递增的应答序号，用于区分迟到应答与重试提交的先后（不依赖墙钟，测试可确定）
        self._seq = 0

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    @staticmethod
    def _parse_queries(entry: dict[str, Any]) -> list[str]:
        return [q.strip() for q in str(entry.get("查询清单", "")).split(",") if q.strip()]

    @staticmethod
    def _sub_results(entry: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """读取（必要时初始化）任务的分项结果账本。"""
        ledger = entry.get("sub_results")
        if not isinstance(ledger, dict):
            ledger = {}
            entry["sub_results"] = ledger
        return ledger

    def _new_sub(self, query: str, api_name: str, latency: float, optimized: bool) -> dict[str, Any]:
        return {
            "query": query,
            "api": api_name,
            "status": STATUS_TIMEOUT,
            "rows": 0,
            "latency_ms": round(latency, 2),
            "optimized": optimized,
            "confirmed": False,
            "result_ref": None,
            "attempts": [],          # 每次应答（含迟到/重试）都留痕
            "confirmed_attempt": None,
            "duplicate_count": 0,    # 确认之后再次到达的重复应答数
            "last_error": None,
        }

    @staticmethod
    def _counts(ledger: dict[str, dict[str, Any]]) -> dict[str, int]:
        confirmed = [s for s in ledger.values() if s.get("confirmed")]
        pending = [s for s in ledger.values()
                   if not s.get("confirmed") and s.get("status") in PENDING_STATUSES]
        # 重复应答只认"确认之后"再次到达的应答（apply_sub_result 显式记账）；
        # 确认之前的超时/失败尝试属于待补过程，不算重复成功
        duplicate = sum(s.get("duplicate_count", 0) for s in ledger.values())
        return {
            "query_count": len(ledger),
            "confirmed_count": len(confirmed),
            "backlog_count": len(pending),
            "duplicate_count": duplicate,
        }

    def get_backlog(self, batch_id: int) -> list[dict[str, Any]]:
        """返回批量查询待补数据：超时/失败、始终没有拿到数据的子项。"""
        entry = store.find(self.MODULE, batch_id)
        if entry is None:
            return []
        ledger = self._sub_results(entry)
        return [
            {"query": s["query"], "api": s.get("api"), "status": s.get("status"),
             "last_error": s.get("last_error"), "attempts": len(s.get("attempts", []))}
            for s in ledger.values()
            if not s.get("confirmed") and s.get("status") in PENDING_STATUSES
        ]

    def get_confirmed(self, batch_id: int) -> list[dict[str, Any]]:
        """返回已确认拿到完整结果的子项（重跑与预热复用这些结果）。"""
        entry = store.find(self.MODULE, batch_id)
        if entry is None:
            return []
        ledger = self._sub_results(entry)
        return [self._public_sub(s) for s in ledger.values() if s.get("confirmed")]

    @staticmethod
    def _public_sub(sub: dict[str, Any]) -> dict[str, Any]:
        return {
            "query": sub["query"],
            "api": sub.get("api"),
            "rows": sub.get("rows", 0),
            "latency_ms": sub.get("latency_ms"),
            "status": sub.get("status"),
            "optimized": sub.get("optimized"),
            "confirmed": sub.get("confirmed", False),
            "result_ref": sub.get("result_ref"),
            "confirmed_attempt": sub.get("confirmed_attempt"),
        }

    # ------------------------------------------------------------------
    # 分项结果回填
    # ------------------------------------------------------------------
    def apply_sub_result(
        self,
        batch_id: int,
        query: str,
        status: str,
        *,
        rows: int = 10,
        source: str = "late",
        error: str | None = None,
        result_ref: str | None = None,
    ) -> dict[str, Any]:
        """回填单个子查询的应答结果（迟到应答/重试结果统一走这里）。

        去重规则：
        - 子项已确认：后续任何应答（迟到重报、重试回报）一律记为 duplicate，
          有效行数与确认结果不变。
        - 子项未确认且本次为 success：确认为有效结果，计入有效行数并可被预热。
        - 子项未确认且本次仍 timeout/failed：保留在待补数据中，更新错误信息，
          值班人可继续定位恢复。
        """
        entry = store.find(self.MODULE, batch_id)
        if entry is None:
            return {"ok": False, "message": "批量任务不存在", "accepted": False, "duplicate": False}

        ledger = self._sub_results(entry)
        if query not in ledger:
            # 清单外的查询不属于本任务，拒绝回填，避免伪造成功
            return {"ok": False, "message": f"子查询 {query} 不在批量任务清单内",
                    "accepted": False, "duplicate": False}

        sub = ledger[query]
        attempt = {"seq": self._next_seq(), "source": source, "status": status, "rows": rows}
        sub.setdefault("attempts", []).append(attempt)

        if sub.get("confirmed"):
            # 迟到应答与重试不能都计入有效行数：归并为重复应答
            sub["duplicate_count"] = sub.get("duplicate_count", 0) + 1
            self._sync_entry_and_snapshot(entry)
            return {"ok": True, "accepted": False, "duplicate": True,
                    "message": "子项结果已确认，重复应答已忽略",
                    "query": query, "result_ref": sub.get("result_ref")}

        if status == STATUS_SUCCESS:
            sub["status"] = STATUS_SUCCESS
            sub["rows"] = rows
            sub["confirmed"] = True
            sub["confirmed_attempt"] = attempt["seq"]
            sub["result_ref"] = result_ref or f"{entry.get('任务编号')}::{query}::A{attempt['seq']}"
            sub["last_error"] = None
            accepted = True
        else:
            # 仍然没有拿到数据：继续挂在待补数据上
            sub["status"] = status if status in PENDING_STATUSES else STATUS_TIMEOUT
            sub["last_error"] = error or "子查询超时未返回数据"
            accepted = False

        self._sync_entry_and_snapshot(entry)
        return {"ok": True, "accepted": accepted, "duplicate": False,
                "message": "子项结果已确认" if accepted else "子项仍待补",
                "query": query, "result_ref": sub.get("result_ref")}

    # ------------------------------------------------------------------
    # 首次执行
    # ------------------------------------------------------------------
    def execute_batch(
        self,
        batch_id: int,
        *,
        timeout_queries: list[str] | None = None,
        fail_queries: list[str] | None = None,
        outcomes: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """执行批量查询任务，按子查询分别记录成功/超时/失败。

        参数（用于注入真实执行中会出现的分项结果）：
        - timeout_queries: 本次执行超时、拿不到数据的子查询
        - fail_queries: 明确失败的子查询
        - outcomes: {query: status} 显式指定每个子查询的结果，优先级最高
        其余子查询拿到完整结果，记为已确认成功。
        """
        entry = store.find(self.MODULE, batch_id)
        if entry is None:
            return {"ok": False, "message": "批量任务不存在"}

        from app.services.data_manager import write_module_snapshot
        api_name = entry.get("关联API", "")
        try:
            concurrency = int(entry.get("并发度", "1") or 1)
        except (ValueError, TypeError):
            concurrency = 1

        # 联动增强：调用 query_optimize 获取优化计划
        from app.services import service_for
        opt_svc = service_for("query_optimize")
        opt_plan = {}
        if opt_svc and hasattr(opt_svc, "get_optimization_plan"):
            opt_plan = opt_svc.get_optimization_plan(api_name)

        benefit = 0.0
        if opt_plan:
            try:
                benefit = float(str(opt_plan.get("estimated_benefit", "0%")).replace("%", "")) / 100.0
            except (ValueError, AttributeError):
                benefit = 0.0

        timeout_set = set(timeout_queries or [])
        fail_set = set(fail_queries or [])
        outcomes = outcomes or {}

        # 重新执行时重建账本（首次执行语义；待补恢复走 apply_sub_result/rerun_pending）
        ledger: dict[str, dict[str, Any]] = {}
        entry["sub_results"] = ledger

        for query in self._parse_queries(entry):
            latency = (45.0 + len(query) * 2) * (1.0 - benefit)
            sub = self._new_sub(query, api_name, latency, bool(opt_plan))
            status = outcomes.get(query)
            if status is None:
                if query in timeout_set:
                    status = STATUS_TIMEOUT
                elif query in fail_set:
                    status = STATUS_FAILED
                else:
                    status = STATUS_SUCCESS
            sub["attempts"].append({"seq": self._next_seq(), "source": "initial",
                                    "status": status, "rows": 10 if status == STATUS_SUCCESS else 0})
            if status == STATUS_SUCCESS:
                sub["status"] = STATUS_SUCCESS
                sub["confirmed"] = True
                sub["rows"] = 10
                sub["confirmed_attempt"] = sub["attempts"][-1]["seq"]
                sub["result_ref"] = f"{entry.get('任务编号')}::{query}::A{sub['confirmed_attempt']}"
            else:
                sub["status"] = status if status in PENDING_STATUSES else STATUS_TIMEOUT
                sub["last_error"] = "子查询超时未返回数据" if sub["status"] == STATUS_TIMEOUT else "子查询执行失败"
            ledger[query] = sub

        entry["concurrency_effective"] = concurrency
        self._sync_entry_and_snapshot(entry)

        # 联动增强：只用确认过的结果触发 cache_layer 预热
        cache_svc = service_for("cache_layer")
        warmup = {"ok": False}
        if cache_svc and hasattr(cache_svc, "warmup_confirmed"):
            warmup = cache_svc.warmup_confirmed(api_name, self.get_confirmed(batch_id))

        counts = self._counts(ledger)
        return {
            "ok": True,
            "batch_id": batch_id,
            "query_count": counts["query_count"],
            "confirmed_count": counts["confirmed_count"],
            "backlog_count": counts["backlog_count"],
            "duplicate_count": counts["duplicate_count"],
            "results": [self._public_sub(s) for s in ledger.values()],
            "backlog": self.get_backlog(batch_id),
            "warmup": warmup,
        }

    # ------------------------------------------------------------------
    # 按失败项重跑（复用已确认结果）
    # ------------------------------------------------------------------
    def rerun_pending(
        self,
        batch_id: int,
        *,
        outcomes: dict[str, str] | None = None,
        still_timeout: list[str] | None = None,
    ) -> dict[str, Any]:
        """值班人按待补（失败/超时）子项发起重跑。

        - 只重跑待补数据中的子项；已确认子项直接复用原结果，不重复执行、不重复计数。
        - outcomes 可指定重跑结果（success/timeout/failed），默认重跑成功。
        - 重跑期间迟到的旧应答走 apply_sub_result，由去重账本判定为 duplicate。
        """
        entry = store.find(self.MODULE, batch_id)
        if entry is None:
            return {"ok": False, "message": "批量任务不存在"}

        ledger = self._sub_results(entry)
        if not ledger:
            return {"ok": False, "message": "任务尚未执行，无待补子项"}

        outcomes = outcomes or {}
        still_timeout_set = set(still_timeout or [])
        reran: list[str] = []
        reused: list[str] = [q for q, s in ledger.items() if s.get("confirmed")]

        for query, sub in ledger.items():
            if sub.get("confirmed"):
                continue  # 复用已确认结果
            status = outcomes.get(query, STATUS_TIMEOUT if query in still_timeout_set else STATUS_SUCCESS)
            attempt = {"seq": self._next_seq(), "source": "rerun",
                      "status": status, "rows": 10 if status == STATUS_SUCCESS else 0}
            sub.setdefault("attempts", []).append(attempt)
            if status == STATUS_SUCCESS:
                sub["status"] = STATUS_SUCCESS
                sub["rows"] = 10
                sub["confirmed"] = True
                sub["confirmed_attempt"] = attempt["seq"]
                sub["result_ref"] = f"{entry.get('任务编号')}::{query}::A{attempt['seq']}"
                sub["last_error"] = None
            else:
                sub["status"] = status if status in PENDING_STATUSES else STATUS_TIMEOUT
                sub["last_error"] = "重跑后仍超时未返回数据" if sub["status"] == STATUS_TIMEOUT else "重跑仍失败"
            reran.append(query)

        self._sync_entry_and_snapshot(entry)

        # 重跑新确认的结果再预热；缓存层按 (api, query) 去重，已预热的不会重复写入
        from app.services import service_for
        cache_svc = service_for("cache_layer")
        warmup = {"ok": False}
        if cache_svc and hasattr(cache_svc, "warmup_confirmed"):
            warmup = cache_svc.warmup_confirmed(entry.get("关联API", ""),
                                                self.get_confirmed(batch_id))

        counts = self._counts(ledger)
        return {
            "ok": True,
            "batch_id": batch_id,
            "reran": reran,
            "reused_confirmed": reused,
            "confirmed_count": counts["confirmed_count"],
            "backlog_count": counts["backlog_count"],
            "duplicate_count": counts["duplicate_count"],
            "backlog": self.get_backlog(batch_id),
            "warmup": warmup,
        }

    # ------------------------------------------------------------------
    # 汇总与快照
    # ------------------------------------------------------------------
    def _sync_entry_and_snapshot(self, entry: dict[str, Any]) -> None:
        """把分项账本回写到条目、结果汇总字段和共享快照。"""
        from app.services.data_manager import write_module_snapshot

        ledger = self._sub_results(entry)
        counts = self._counts(ledger)
        api_name = entry.get("关联API", "")

        if counts["backlog_count"] == 0:
            entry["status"] = "已完成"
            entry["pending"] = False
        else:
            entry["status"] = "部分完成"
            entry["pending"] = True

        entry["结果汇总"] = (
            f"共{counts['query_count']}个查询，已确认{counts['confirmed_count']}个，"
            f"待补{counts['backlog_count']}个，重复应答{counts['duplicate_count']}次"
        )

        confirmed_rows = [self._public_sub(s) for s in ledger.values() if s.get("confirmed")]
        snapshot = {
            "batch_id": entry["id"],
            "task_no": entry.get("任务编号"),
            "api": api_name,
            "query_count": counts["query_count"],
            "completed_count": counts["confirmed_count"],   # 有效行数只认真正确认的
            "confirmed_count": counts["confirmed_count"],
            "backlog_count": counts["backlog_count"],
            "duplicate_count": counts["duplicate_count"],
            "results": confirmed_rows,
            "backlog": self.get_backlog(entry["id"]),
            "sub_results": {q: self._public_sub(s) for q, s in ledger.items()},
        }
        write_module_snapshot(self.MODULE, snapshot)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "发起执行":
            result = self.execute_batch(entry_id)
            if not result.get("ok"):
                return None, result.get("message", "批量查询失败")
            if result.get("backlog_count"):
                return entry, (f"批量查询部分完成：已确认{result['confirmed_count']}个，"
                               f"待补{result['backlog_count']}个")
            return entry, f"批量查询完成，共{result.get('query_count', 0)}个查询"

        if action == "重跑失败项":
            result = self.rerun_pending(entry_id)
            if not result.get("ok"):
                return None, result.get("message", "重跑失败")
            return entry, (f"重跑{len(result.get('reran', []))}个待补子项，"
                           f"复用已确认结果{len(result.get('reused_confirmed', []))}个，"
                           f"仍待补{result.get('backlog_count', 0)}个")

        result = super().run_action(entry_id, action)
        self._sync_entry_and_snapshot(entry)
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        batch_list = []
        total_confirmed = 0
        total_backlog = 0
        total_duplicate = 0
        for row in rows:
            ledger = row.get("sub_results")
            if isinstance(ledger, dict):
                counts = self._counts(ledger)
                confirmed = counts["confirmed_count"]
                backlog = counts["backlog_count"]
                duplicate = counts["duplicate_count"]
            else:
                confirmed = backlog = duplicate = 0
            total_confirmed += confirmed
            total_backlog += backlog
            total_duplicate += duplicate
            batch_list.append({
                "id": row.get("id"),
                "task_no": row.get("任务编号"),
                "api": row.get("关联API"),
                "queries": row.get("查询清单"),
                "concurrency": row.get("并发度"),
                "failure_policy": row.get("失败策略"),
                "results": row.get("结果汇总"),
                "timeout": row.get("超时控制"),
                "status": row.get("status"),
                "confirmed_count": confirmed,
                "backlog_count": backlog,
                "duplicate_count": duplicate,
                "backlog": self.get_backlog(row["id"]) if row.get("id") is not None else [],
            })
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(batch_list),
            "batches": batch_list,
            "completed_count": sum(1 for r in batch_list if r.get("status") == "已完成"),
            "confirmed_count": total_confirmed,
            "backlog_count": total_backlog,
            "duplicate_count": total_duplicate,
        }
