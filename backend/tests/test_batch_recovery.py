"""批量查询分项恢复测试：超时、迟到应答、按失败项重跑与缓存预热去重。

验证场景（对应需求时间线）：
1. 批量执行时一个子查询先拿到完整结果，另一个超时迟到 → 超时项进待补数据，不记成功。
2. 值班人恰好按失败项发起重跑；重跑复用已确认结果，不重复执行/计数。
3. 同一子项的成功结果在重试提交前后各迟到一回 → 两次都判重，有效行数只认一次。
4. 始终超时拿不到数据的子项持续保留在待补数据中，可继续定位恢复。
5. 汇总数量只统计已确认子项；缓存分层从待补数据中辨认真正可用子项，
   预热只按确认结果发起，不把超时和迟到重报当两次不同的成功。
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

temp_data = tempfile.mkdtemp(prefix='cc10026_batch_test_')
os.environ['DATA_ROOT'] = temp_data


class TestBatchSubitemRecovery:
    """批量查询分项结果回填与恢复。"""

    def setup_method(self):
        from app.seed import SEED_ROWS
        from app.store import store as s
        s._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
        import shutil
        # 清掉 data_manager 实际绑定的数据目录（DATA_ROOT 在模块导入时绑定，
        # 不能只清本文件声明的临时目录），避免缓存快照跨测试污染去重判断
        from app.services.data_manager import DATA_ROOT
        if DATA_ROOT.exists():
            shutil.rmtree(DATA_ROOT, ignore_errors=True)
        DATA_ROOT.mkdir(parents=True, exist_ok=True)

    def _make_batch(self, queries, api_name="缓存分层样例3"):
        from app.services.batch_query import BatchQueryService
        svc = BatchQueryService()
        entry, missing = svc.create_entry({
            "任务编号": "BATC-RECOVERY-001",
            "关联API": api_name,
            "查询清单": ",".join(queries),
            "并发度": "2",
            "失败策略": "失败重试",
            "超时控制": "500ms",
        })
        assert missing == []
        return svc, entry["id"]

    def test_first_execution_records_timeout_as_backlog_not_success(self):
        """首次执行：成功项确认，超时/失败项进入待补数据而非记成功。"""
        svc, batch_id = self._make_batch(["q1", "q2", "q3", "q4", "q5"])

        result = svc.execute_batch(
            batch_id,
            timeout_queries=["q4"],
            fail_queries=["q5"],
        )
        assert result["ok"]
        # 分项计数：3 个确认成功，2 个待补，尚无重复应答
        assert result["query_count"] == 5
        assert result["confirmed_count"] == 3
        assert result["backlog_count"] == 2
        assert result["duplicate_count"] == 0

        backlog = svc.get_backlog(batch_id)
        backlog_queries = {item["query"] for item in backlog}
        assert backlog_queries == {"q4", "q5"}
        # 待补项可继续定位：保留状态与错误原因
        for item in backlog:
            assert item["status"] in ("timeout", "failed")
            assert item["last_error"]

        entry = svc.get_entry(batch_id)
        assert entry["status"] == "部分完成"
        assert "已确认3个" in entry["结果汇总"] and "待补2个" in entry["结果汇总"]

    def test_late_answer_before_and_after_rerun_both_deduped(self):
        """同一成功结果在重试提交前后各迟到一回，均判重，有效行数只认一次。"""
        svc, batch_id = self._make_batch(["q1", "q2", "q3", "q4", "q5"])
        svc.execute_batch(batch_id, timeout_queries=["q4"], fail_queries=["q5"])

        # q1 已确认成功。第一回迟到：重跑提交之前到达
        late_before = svc.apply_sub_result(batch_id, "q1", "success", rows=10, source="late")
        assert late_before["duplicate"] is True
        assert late_before["accepted"] is False
        assert late_before["result_ref"].endswith("::q1::A1")  # 仍指向首次确认的结果

        # 值班人按失败项发起重跑：q4 这次成功，q5 始终超时
        rerun = svc.rerun_pending(batch_id, still_timeout=["q5"])
        assert set(rerun["reran"]) == {"q4", "q5"}
        # 已确认的 q1/q2/q3 被复用，不重跑
        assert set(rerun["reused_confirmed"]) == {"q1", "q2", "q3"}
        assert rerun["confirmed_count"] == 4
        assert rerun["backlog_count"] == 1

        # 第二回迟到：重跑提交之后到达，仍是同一子项的重复应答
        late_after = svc.apply_sub_result(batch_id, "q1", "success", rows=10, source="late-rerun")
        assert late_after["duplicate"] is True
        assert late_after["accepted"] is False

        # 有效行数始终只认一次：q1 只有一次确认，attempts 留痕 3 次（首次+两回迟到）
        sub = svc.get_entry(batch_id)["sub_results"]["q1"]
        assert sub["confirmed"] is True
        assert sub["confirmed_attempt"] == 1
        assert len(sub["attempts"]) == 3
        assert sub.get("duplicate_count") == 2

        # 汇总的重复应答只来自确认后的迟到（q1 两次）；q4/q5 确认前的超时尝试不算重复
        from app.services.data_manager import read_module_snapshot
        snap = read_module_snapshot("batch_query")
        assert snap["duplicate_count"] == 2
        assert snap["confirmed_count"] == 4
        assert snap["backlog_count"] == 1

    def test_rerun_reuses_confirmed_and_keeps_permanent_timeout_recoverable(self):
        """重跑复用已确认结果；始终超时的子项保留在待补数据可继续恢复。"""
        svc, batch_id = self._make_batch(["q1", "q2", "q3", "q4", "q5"])
        first = svc.execute_batch(batch_id, timeout_queries=["q4"], fail_queries=["q5"])
        assert first["backlog_count"] == 2

        # 重跑：q4 恢复成功，q5 依旧超时
        rerun = svc.rerun_pending(batch_id, outcomes={"q4": "success", "q5": "timeout"})
        assert rerun["backlog_count"] == 1
        backlog = svc.get_backlog(batch_id)
        assert [item["query"] for item in backlog] == ["q5"]
        assert backlog[0]["status"] == "timeout"
        # q5 历经首次失败 + 重跑超时，attempts 留痕，值班人可继续定位
        assert backlog[0]["attempts"] == 2

        # 再次重跑，q5 终于恢复
        second = svc.rerun_pending(batch_id)
        assert second["backlog_count"] == 0
        assert second["confirmed_count"] == 5
        assert svc.get_entry(batch_id)["status"] == "已完成"

        # 汇总有效行数：5 个子项各确认一次，没有任何子项被算成两次成功
        snapshot = svc.get_confirmed(batch_id)
        assert len(snapshot) == 5
        assert len({s["query"] for s in snapshot}) == 5

    def test_summary_counts_only_confirmed_and_snapshot_matches(self):
        """共享快照的汇总数量只认真正确认的子项，待补与重复应答不计入。"""
        from app.services.data_manager import read_module_snapshot
        svc, batch_id = self._make_batch(["q1", "q2", "q3", "q4"])
        svc.execute_batch(batch_id, timeout_queries=["q4"])

        # 迟到重报 q2 两次
        svc.apply_sub_result(batch_id, "q2", "success", source="late")
        svc.apply_sub_result(batch_id, "q2", "success", source="late")

        snap = read_module_snapshot("batch_query")
        assert snap["query_count"] == 4
        assert snap["confirmed_count"] == 3
        assert snap["completed_count"] == 3          # 汇总有效行数
        assert snap["backlog_count"] == 1
        assert snap["duplicate_count"] == 2
        # results 只含确认子项，超时项不在有效结果里
        assert {r["query"] for r in snap["results"]} == {"q1", "q2", "q3"}
        assert {b["query"] for b in snap["backlog"]} == {"q4"}

        # domain_snapshot 同样只统计确认/待补/重复
        domain = svc.domain_snapshot()
        target = next(b for b in domain["batches"] if b["id"] == batch_id)
        assert target["confirmed_count"] == 3
        assert target["backlog_count"] == 1
        assert target["duplicate_count"] == 2

    def test_unknown_query_cannot_inject_fake_success(self):
        """清单外的回填被拒绝，不能伪造成功。"""
        svc, batch_id = self._make_batch(["q1", "q2"])
        svc.execute_batch(batch_id, timeout_queries=["q2"])
        inject = svc.apply_sub_result(batch_id, "not-in-list", "success")
        assert inject["ok"] is False
        assert inject["duplicate"] is False

    def test_cache_layer_identifies_available_and_warms_confirmed_only(self):
        """缓存分层从待补数据辨认可用子项，预热只按确认结果且按 (api,query) 去重。"""
        from app.services.cache_layer import CacheLayerService
        from app.services.data_manager import read_module_snapshot

        svc, batch_id = self._make_batch(["q1", "q2", "q3", "q4", "q5"])
        api = "缓存分层样例3"  # seed 中唯一「已生效」的缓存策略
        first = svc.execute_batch(batch_id, timeout_queries=["q4"], fail_queries=["q5"])

        cache_svc = CacheLayerService()
        # 首次预热由 execute_batch 自动触发：只预热 3 个确认项
        assert first["warmup"]["ok"] is True
        assert first["warmup"]["entries"] == 3
        assert first["warmup"]["skipped_duplicates"] == 0

        # 缓存层从批量快照辨认当前真正可用的子项
        identified = cache_svc.identify_available_subs(batch_id)
        assert {s["query"] for s in identified["available"]} == {"q1", "q2", "q3"}
        assert {s["query"] for s in identified["backlog"]} == {"q4", "q5"}

        # 超时项的迟到失败应答不会产生缓存
        svc.apply_sub_result(batch_id, "q4", "timeout", source="late", error="still late")
        # 已确认项的迟到成功重报判重，不会再预热一次
        svc.apply_sub_result(batch_id, "q1", "success", source="late")

        # 重跑：q4 恢复，q5 始终超时
        rerun = svc.rerun_pending(batch_id, still_timeout=["q5"])
        # 只新增 q4 一个缓存条目；q1~q3 复用已确认结果，预热跳过重复
        assert rerun["warmup"]["entries"] == 1
        assert rerun["warmup"]["skipped_duplicates"] == 3

        cache_snap = read_module_snapshot("cache_layer")
        batch_entries = [c for c in cache_snap["caches"]
                         if c.get("api") == api and c.get("source") == "batch_confirmed"]
        # 4 个确认子项各一条缓存，迟到/重报没有制造"第二次成功"
        assert len(batch_entries) == 4
        keys = [c["cache_key"] for c in batch_entries]
        assert len(set(keys)) == 4
        assert f"{api}::q5" not in keys  # 始终超时的子项不预热
        assert f"{api}::q4" in keys      # 重跑确认后才预热

    def test_no_cache_policy_skips_warmup_without_duplicate_success(self):
        """无生效缓存策略时预热跳过，但分项确认与去重语义不受影响。"""
        svc, batch_id = self._make_batch(["q1", "q2"], api_name="没有缓存策略的API")
        result = svc.execute_batch(batch_id, timeout_queries=["q2"])
        assert result["warmup"]["ok"] is False
        assert result["confirmed_count"] == 1
        # 重跑成功后仍无法预热，但结果只确认一次
        rerun = svc.rerun_pending(batch_id)
        assert rerun["confirmed_count"] == 2
        assert rerun["warmup"]["ok"] is False

    def test_rerun_action_endpoint_reuses_confirmed_results(self):
        """通过 run_action 走「重跑失败项」动作的端到端路径。"""
        svc, batch_id = self._make_batch(["q1", "q2", "q3"])
        # 直接注入超时场景后通过动作重跑
        svc.execute_batch(batch_id, timeout_queries=["q2", "q3"])
        entry, message = svc.run_action(batch_id, "重跑失败项")
        assert entry is not None
        assert "重跑2个待补子项" in message
        assert "复用已确认结果1个" in message
        assert entry["status"] == "已完成"
        assert svc.get_backlog(batch_id) == []
