"""命令行入口：用于在独立进程中播种 / 检查数据库，验证进程重启持久性。

用法::

    python -m offline_mqtt.cli seed  DB_PATH [--sub FILTER ...]
    python -m offline_mqtt.cli dump  DB_PATH
    python -m offline_mqtt.cli poll  DB_PATH SUB_ID
"""

from __future__ import annotations

import argparse
import json
import sys

from .generator import (
    connect,
    publish,
    retained_clear,
    retained_publish,
    subscribe,
)
from .ingress import MqttIngress


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

    args = parser.parse_args(argv)
    if args.cmd == "seed":
        return _seed(args.db_path, args.sub)
    if args.cmd == "dump":
        return _dump(args.db_path)
    return _poll(args.db_path, args.sub_id)


if __name__ == "__main__":
    sys.exit(main())
