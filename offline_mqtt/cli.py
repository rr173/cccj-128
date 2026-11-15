"""命令行入口：用于在独立进程中播种 / 检查数据库，验证进程重启持久性。

用法::

    python -m offline_mqtt.cli seed  DB_PATH [--sub FILTER ...]
    python -m offline_mqtt.cli dump  DB_PATH
    python -m offline_mqtt.cli poll  DB_PATH SUB_ID

新增的采集范围准入（受控）命令::

    python -m offline_mqtt.cli check-topic  POLICY PRINCIPAL TOPIC
    python -m offline_mqtt.cli check-scope  POLICY PRINCIPAL FILTER
    python -m offline_mqtt.cli csubscribe   DB_PATH POLICY PRINCIPAL -- FILTER ...
    python -m offline_mqtt.cli cpoll        DB_PATH SUB_ID

退出码约定：0=命令成功（check-* 中 safe=false / allowed=false 也是成功，结论在
JSON 里）；2=输入/策略错误；3=资源不足无法裁定。
"""

from __future__ import annotations

import argparse
import json
import sys

from .admission import AdmissionInputError, AdmissionResourceLimit
from .controlled import ControlledIngress
from .admission import AdmissionEngine
from .generator import (
    connect,
    publish,
    retained_clear,
    retained_publish,
    subscribe,
)
from .ingress import MqttIngress
from .policy import PolicyError, load_policy_file

# 退出码：输入/策略错误=2，资源不足=3。
EXIT_INPUT_ERROR = 2
EXIT_RESOURCE_LIMIT = 3


def _seed(db_path: str, filters: list[str]) -> int:
    with MqttIngress(db_path) as ing:
        ing.ingest(connect("dev-1"))
        # 同一主题两次保留发布：只有后者是当前版本。
        ing.ingest(retained_publish("dev-1", "sensors/temp", "v1",
                                    qos=1, message_id=1))
        ing.ingest(retained_publish("dev-1", "sensors/temp", "v2",
                                    qos=1, message_id=2))
        ing.ingest(retained_publish("dev-1", "sensors/hum", "h1",
                                    qos=1, message_id=3))
        # 普通实时消息（含重复 DUP）。
        ing.ingest(publish("dev-1", "sensors/temp", "live-1",
                           qos=1, message_id=4))
        ing.ingest(publish("dev-1", "sensors/temp", "live-1-dup",
                           qos=1, message_id=4, dup=True))
        # 空载荷保留发布清除另一主题。
        ing.ingest(retained_publish("dev-1", "sensors/obsolete", "old",
                                    qos=1, message_id=5))
        ing.ingest(retained_clear("dev-1", "sensors/obsolete", message_id=6))

        sub_id = None
        if filters:
            res = ing.ingest(subscribe("dev-1", filters, message_id=10))
            sub_id = res.subscription_id

        state = {
            "ledger_count": len(ing.list_events()),
            "retained": {r["topic"]: r["payload"].decode() for r in ing.list_retained()},
            "quarantine_count": len(ing.list_quarantine()),
            "subscription_id": sub_id,
        }
    print(json.dumps(state, ensure_ascii=False, indent=2))
    return 0


def _dump(db_path: str) -> int:
    with MqttIngress(db_path) as ing:
        out = {
            "events": [
                {"seq": e["seq"], "topic": e["topic"],
                 "payload": e["payload"].decode("utf-8", "replace"),
                 "retain": bool(e["retain"]), "dup": bool(e["dup"]),
                 "message_id": e["message_id"], "session_id": e["session_id"]}
                for e in ing.list_events()
            ],
            "retained": [
                {"topic": r["topic"], "payload": r["payload"].decode("utf-8", "replace"),
                 "version": r["version"], "last_event_seq": r["last_event_seq"]}
                for r in ing.list_retained()
            ],
            "quarantine": ing.list_quarantine(),
        }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def _poll(db_path: str, sub_id: int) -> int:
    with MqttIngress(db_path) as ing:
        result = ing.poll(sub_id)
        out = {
            "retained_exhausted": result.retained_exhausted,
            "next_cursor": result.next_cursor,
            "messages": [
                {"topic": m.topic, "payload": m.payload.decode("utf-8", "replace"),
                 "retain_delivery": m.retain, "event_seq": m.event_seq,
                 "retain_version": m.retain_version}
                for m in result.messages
            ],
        }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def _load_engine(policy_path: str) -> AdmissionEngine:
    return AdmissionEngine(load_policy_file(policy_path))


def _print_error(kind: str, errors) -> None:
    print(json.dumps({"status": "error", "error_kind": kind, "errors": errors},
                     ensure_ascii=False, indent=2))


def _check_topic(policy_path: str, principal: str, topic: str) -> int:
    try:
        engine = _load_engine(policy_path)
        decision = engine.decide_topic(principal, topic)
    except PolicyError as exc:
        _print_error("policy_input_error", exc.errors)
        return EXIT_INPUT_ERROR
    except AdmissionInputError as exc:
        _print_error("admission_input_error", exc.errors)
        return EXIT_INPUT_ERROR
    except AdmissionResourceLimit as exc:
        _print_error("resource_limit", [{"reason": "RESOURCE_LIMIT", "detail": str(exc)}])
        return EXIT_RESOURCE_LIMIT
    print(json.dumps(decision.to_dict(), ensure_ascii=False, indent=2))
    return 0


def _check_scope(policy_path: str, principal: str, topic_filter: str) -> int:
    try:
        engine = _load_engine(policy_path)
        decision = engine.decide_scope(principal, topic_filter)
    except PolicyError as exc:
        _print_error("policy_input_error", exc.errors)
        return EXIT_INPUT_ERROR
    except AdmissionInputError as exc:
        _print_error("admission_input_error", exc.errors)
        return EXIT_INPUT_ERROR
    except AdmissionResourceLimit as exc:
        _print_error("resource_limit", [{"reason": "RESOURCE_LIMIT", "detail": str(exc)}])
        return EXIT_RESOURCE_LIMIT
    print(json.dumps(decision.to_dict(), ensure_ascii=False, indent=2))
    return 0


def _csubscribe(db_path: str, policy_path: str, principal: str,
                filters: list, message_id) -> int:
    # 记录里的 session_id 只是帧载荷；身份由 --principal 显式传入，绝不从中推断。
    record = subscribe("untrusted-session-record", filters, message_id=message_id)
    try:
        with MqttIngress(db_path) as ing:
            engine = _load_engine(policy_path)
            controlled = ControlledIngress(ing, engine)
            result = controlled.controlled_subscribe(principal, record)
    except PolicyError as exc:
        _print_error("policy_input_error", exc.errors)
        return EXIT_INPUT_ERROR
    except AdmissionInputError as exc:
        _print_error("admission_input_error", exc.errors)
        return EXIT_INPUT_ERROR
    except AdmissionResourceLimit as exc:
        _print_error("resource_limit", [{"reason": "RESOURCE_LIMIT", "detail": str(exc)}])
        return EXIT_RESOURCE_LIMIT
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0


def _cpoll(db_path: str, sub_id: int) -> int:
    # 受控读取的过滤只依赖采集命名空间，不需要再加载策略。
    from .namespace import is_namespace_topic
    from .topic import topic_matches
    with MqttIngress(db_path) as ing:
        store = ing.store
        if store.get_subscription(sub_id) is None:
            _print_error("input_error", [{"location": "sub_id",
                         "reason": "UNKNOWN_SUBSCRIPTION",
                         "detail": f"sub_id={sub_id} 不存在"}])
            return EXIT_INPUT_ERROR
        cursor = store.get_cursor(sub_id)
        messages = []
        retained_exhausted = True
        pending = store.pending_retained(sub_id)
        if pending:
            retained_exhausted = False
            topics = [r["topic"] for r in pending]
            for r in pending:
                if is_namespace_topic(r["topic"]):
                    messages.append({
                        "topic": r["topic"],
                        "payload": bytes(r["payload"]).decode("utf-8", "replace"),
                        "retain_delivery": True,
                        "retain_version": r["retain_version"],
                    })
            with store.write() as conn:
                store.mark_retained_delivered(conn, sub_id, topics)
        else:
            rows = store.live_events_after(cursor, None)
            if rows:
                new_cursor = int(rows[-1]["seq"])
                with store.write() as conn:
                    store.advance_cursor(conn, sub_id, new_cursor)
                filters = store.get_filters(sub_id)
                for r in rows:
                    if any(topic_matches(f, r["topic"]) for f in filters) \
                            and is_namespace_topic(r["topic"]):
                        messages.append({
                            "topic": r["topic"],
                            "payload": bytes(r["payload"]).decode("utf-8", "replace"),
                            "retain_delivery": False, "event_seq": r["seq"],
                        })
                cursor = new_cursor
        out = {"retained_exhausted": retained_exhausted,
               "next_cursor": cursor, "messages": messages}
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="offline_mqtt")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_seed = sub.add_parser("seed", help="播种确定性场景")
    p_seed.add_argument("db_path")
    p_seed.add_argument("--sub", nargs="*", default=[])

    p_dump = sub.add_parser("dump", help="导出库内全部状态")
    p_dump.add_argument("db_path")

    p_poll = sub.add_parser("poll", help="按订阅拉取一批消息")
    p_poll.add_argument("db_path")
    p_poll.add_argument("sub_id", type=int)

    p_topic = sub.add_parser("check-topic", help="具体主题准入裁定")
    p_topic.add_argument("policy_path")
    p_topic.add_argument("principal_id")
    p_topic.add_argument("topic")

    p_scope = sub.add_parser("check-scope", help="过滤器范围准入裁定（非枚举）")
    p_scope.add_argument("policy_path")
    p_scope.add_argument("principal_id")
    p_scope.add_argument("topic_filter")

    p_csub = sub.add_parser("csubscribe", help="受控创建真实订阅")
    p_csub.add_argument("db_path")
    p_csub.add_argument("policy_path")
    p_csub.add_argument("principal_id")
    p_csub.add_argument("--message-id", type=int, default=None)
    p_csub.add_argument("filters", nargs="+", help="一个或多个请求过滤器")
    p_cpoll = sub.add_parser("cpoll", help="受控读取（只交付采集命名空间消息）")
    p_cpoll.add_argument("db_path")
    p_cpoll.add_argument("sub_id", type=int)

    args = parser.parse_args(argv)
    if args.cmd == "seed":
        return _seed(args.db_path, args.sub)
    if args.cmd == "dump":
        return _dump(args.db_path)
    if args.cmd == "poll":
        return _poll(args.db_path, args.sub_id)
    if args.cmd == "check-topic":
        return _check_topic(args.policy_path, args.principal_id, args.topic)
    if args.cmd == "check-scope":
        return _check_scope(args.policy_path, args.principal_id, args.topic_filter)
    if args.cmd == "csubscribe":
        return _csubscribe(args.db_path, args.policy_path, args.principal_id,
                           args.filters, args.message_id)
    return _cpoll(args.db_path, args.sub_id)


if __name__ == "__main__":
    sys.exit(main())
