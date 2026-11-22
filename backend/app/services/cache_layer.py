"""缓存分层领域服务：管理数据API缓存策略、TTL和预热配置。

跨模块数据流：
- 消费 api_registry 快照 → 关联API获取缓存策略
- 被 query_route 消费 → 路由规则与缓存层级联动
- 被 query_optimize 消费 → 优化规则参考缓存命中率
- 缓存失效 → 触发过期淘汰和预热
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CacheLayerService(BaseModuleService):
    MODULE = "cache_layer"
    ENTITY = "缓存策略"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["策略编号", "关联API", "缓存层级", "TTL秒数", "最大条目", "淘汰算法", "预热策略", "缓存版本"]
    REQUIRED_FIELDS = ["策略编号", "关联API", "缓存层级"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def get_cache_config(self, api_name: str) -> dict[str, Any]:
        """获取指定API的缓存配置，供其他模块消费。"""
        rows = store.rows(self.MODULE)
        for row in rows:
            if row.get("关联API") == api_name and row.get("status") == "已生效":
                return {
                    "policy_no": row.get("策略编号"),
                    "tier": row.get("缓存层级"),
                    "ttl_seconds": self._safe_int(row.get("TTL秒数")),
                    "max_entries": self._safe_int(row.get("最大条目")),
                    "eviction_algorithm": row.get("淘汰算法"),
                    "warmup_policy": row.get("预热策略"),
                    "version": row.get("缓存版本"),
                }
        return {}

    def evict_expired(self, api_name: str) -> int:
        """淘汰指定API的过期缓存条目，返回淘汰数量。"""
        config = self.get_cache_config(api_name)
        if not config:
            return 0
        ttl = config.get("ttl_seconds", 0)
        if ttl <= 0:
            return 0
        # 读取当前缓存层快照
        from app.services.data_manager import read_module_snapshot
        snap = read_module_snapshot("cache_layer")
        cached = snap.get("caches", [])
        expired = [c for c in cached if c.get("api") == api_name and c.get("age", 0) > ttl]
        # 写入淘汰日志
        from app.services.data_manager import write_module_snapshot
        remaining = [c for c in cached if c not in expired]
        snap["caches"] = remaining
        snap["_evicted_at"] = self._now_iso()
        snap["_evicted_count"] = len(expired)
        write_module_snapshot(self.MODULE, snap)
        return len(expired)

    def warmup_cache(self, api_name: str, entry_count: int) -> dict[str, Any]:
        """预热缓存：批量查询完成后将热点数据写入缓存层。

        跨模块：供 batch_query 调用，在批量查询完成后自动预热。
        返回预热结果。
        """
        config = self.get_cache_config(api_name)
        if not config:
            return {"ok": False, "message": "无生效缓存策略"}

        from app.services.data_manager import read_module_snapshot, write_module_snapshot
        snap = read_module_snapshot("cache_layer")
        if not snap:
            snap = {"module": self.MODULE, "caches": []}

        warmup_entry = {
            "api": api_name,
            "tier": config.get("tier", "L1"),
            "ttl": config.get("ttl_seconds", 300),
            "entries": entry_count,
            "warmup_policy": config.get("warmup_policy", "按需加载"),
            "created_at": self._now_iso(),
            "age": 0,
        }
        snap["caches"].append(warmup_entry)
        snap["_warmup_at"] = self._now_iso()
        snap["_warmup_entries"] = entry_count
        write_module_snapshot(self.MODULE, snap)
        return {"ok": True, "api": api_name, "entries": entry_count}

    @staticmethod
    def _cache_key(api_name: str, query: str) -> str:
        return f"{api_name}::{query}"

    def identify_available_subs(self, batch_id: int) -> dict[str, list[dict[str, Any]]]:
        """从批量查询快照（含待补数据）辨认当前真正可用的子项。

        只有 confirmed=True 的子项可用于预热；超时未确认、迟到重报的子项排除在外。
        返回 {"available": [...], "backlog": [...]}，供预热与值班排查分别使用。
        """
        from app.services.data_manager import read_module_snapshot
        batch_snap = read_module_snapshot("batch_query")
        if not batch_snap or batch_snap.get("batch_id") != batch_id:
            return {"available": [], "backlog": []}
        sub_results = batch_snap.get("sub_results", {})
        available, backlog = [], []
        for sub in sub_results.values():
            if not isinstance(sub, dict):
                continue
            if sub.get("confirmed") and sub.get("status") == "success":
                available.append(sub)
            else:
                backlog.append(sub)
        return {"available": available, "backlog": backlog}

    def warmup_confirmed(self, api_name: str, confirmed_subs: list[dict[str, Any]]) -> dict[str, Any]:
        """按批量查询「已确认」的分项结果预热，超时/迟到重报不会被当成成功预热。

        - 只接受 confirmed 子项，按 (api, query) 去重：迟到应答与重试不会重复写入。
        - 已存在同键缓存条目的子项跳过，返回 skipped_duplicates。
        - 无生效缓存策略时返回 ok=False，不写任何条目。
        """
        config = self.get_cache_config(api_name)
        if not config:
            return {"ok": False, "message": "无生效缓存策略", "entries": 0,
                    "skipped_duplicates": 0, "keys": []}

        from app.services.data_manager import read_module_snapshot, write_module_snapshot
        snap = read_module_snapshot("cache_layer")
        if not snap:
            snap = {"module": self.MODULE, "caches": []}

        existing_keys = {
            c.get("cache_key") for c in snap.get("caches", [])
            if isinstance(c, dict) and c.get("cache_key")
        }

        warmed_keys: list[str] = []
        skipped = 0
        for sub in confirmed_subs:
            if not isinstance(sub, dict) or not sub.get("confirmed"):
                continue  # 超时/未确认/迟到重报一律不预热
            query = str(sub.get("query", ""))
            if not query:
                continue
            key = self._cache_key(api_name, query)
            if key in existing_keys:
                # 重跑复用已确认结果时，缓存只认一次
                skipped += 1
                continue
            existing_keys.add(key)
            snap["caches"].append({
                "api": api_name,
                "query": query,
                "cache_key": key,
                "result_ref": sub.get("result_ref"),
                "rows": sub.get("rows", 0),
                "tier": config.get("tier", "L1"),
                "ttl": config.get("ttl_seconds", 300),
                "warmup_policy": config.get("warmup_policy", "按需加载"),
                "source": "batch_confirmed",
                "created_at": self._now_iso(),
                "age": 0,
            })
            warmed_keys.append(key)

        snap["_warmup_at"] = self._now_iso()
        snap["_warmup_entries"] = len(warmed_keys)
        snap["_warmup_skipped_duplicates"] = skipped
        write_module_snapshot(self.MODULE, snap)
        return {"ok": True, "api": api_name, "entries": len(warmed_keys),
                "skipped_duplicates": skipped, "keys": warmed_keys}

    def _now_iso(self) -> str:
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone(timedelta(hours=8))).isoformat()

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        if action == "标记生效":
            # 预写缓存快照
            from app.services.data_manager import read_module_snapshot, write_module_snapshot
            snap = read_module_snapshot("cache_layer")
            if not snap:
                snap = {"module": self.MODULE, "caches": []}
            snap["caches"].append({
                "api": entry.get("关联API"),
                "tier": entry.get("缓存层级"),
                "ttl": entry.get("TTL秒数"),
                "created_at": self._now_iso(),
            })
            write_module_snapshot(self.MODULE, snap)

        result = super().run_action(entry_id, action)
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        cache_list = []
        for row in rows:
            cache_list.append({
                "id": row.get("id"),
                "policy_no": row.get("策略编号"),
                "api": row.get("关联API"),
                "tier": row.get("缓存层级"),
                "ttl": row.get("TTL秒数"),
                "max_entries": row.get("最大条目"),
                "eviction": row.get("淘汰算法"),
                "warmup": row.get("预热策略"),
                "version": row.get("缓存版本"),
                "status": row.get("status"),
            })
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(cache_list),
            "caches": cache_list,
            "active_count": sum(1 for r in cache_list if r.get("status") == "已生效"),
        }