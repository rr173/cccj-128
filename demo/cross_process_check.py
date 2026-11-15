#!/usr/bin/env python3
"""跨进程复核：在全新解释器进程里用同一份策略重算判断与见证。

主进程先算出结论并把策略路径、身份、请求写入一个任务文件；子进程用
``sys.executable -I`` 启动（隔离模式，不继承 cwd/PYTHONPATH），读取同一策略，
独立重算 check-scope / check-topic。两边的 policy 摘要与 witness 必须逐字节一致，
从而证明同一份判断可以交给同事在新进程里重现。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from offline_mqtt import AdmissionEngine, load_policy_file  # noqa: E402

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
POLICY_PATH = os.path.join(DEMO_DIR, "policies", "collections.policy.json")
EVIDENCE_DIR = os.path.join(DEMO_DIR, "evidence")

_RUNNER = os.path.join(EVIDENCE_DIR, "_cross_process_runner.py")

# 子进程入口（写成独立文件，用 -I 运行，与本仓库测试的跨进程做法一致）。
_RUNNER_SOURCE = f'''
import json, sys
sys.path.insert(0, {REPO_ROOT!r})
from offline_mqtt import AdmissionEngine, load_policy_file
task = json.load(open(sys.argv[1], encoding="utf-8"))
engine = AdmissionEngine(load_policy_file(task["policy_path"]))
scope = engine.decide_scope(task["principal_id"], task["request_filter"])
out = {{"scope": scope.to_dict()}}
if scope.witness is not None:
    out["topic_recheck"] = engine.decide_topic(
        task["principal_id"], scope.witness.topic).to_dict()
print(json.dumps(out, ensure_ascii=False, sort_keys=True))
'''


def main() -> int:
    os.makedirs(EVIDENCE_DIR, exist_ok=True)
    with open(_RUNNER, "w", encoding="utf-8") as fh:
        fh.write(_RUNNER_SOURCE)

    engine = AdmissionEngine(load_policy_file(POLICY_PATH))
    cases = [
        ("team-plant", "plant/#"),
        ("team-payroll", "plant/#"),
        ("team-plant", "plant/temp/#"),
        ("team-no-rules", "#"),
    ]
    report = []
    for principal, request in cases:
        local = engine.decide_scope(principal, request)
        task = {"policy_path": POLICY_PATH, "principal_id": principal,
                "request_filter": request}
        task_path = os.path.join(EVIDENCE_DIR, f"_task_{principal}_{request.replace('/','_').replace('#','h')}.json")
        with open(task_path, "w", encoding="utf-8") as fh:
            json.dump(task, fh, ensure_ascii=False)
        proc = subprocess.run(
            [sys.executable, "-I", _RUNNER, task_path],
            capture_output=True, text=True, check=False,
        )
        if proc.returncode != 0:
            raise SystemExit(f"子进程失败: {proc.stderr}")
        remote = json.loads(proc.stdout)
        remote_scope = remote["scope"]

        # 摘要一致 + 布尔结论一致 + witness 逐字节一致
        assert remote_scope["policy"]["policy_digest_sha256"] == \
            local.policy_summary["policy_digest_sha256"], "策略摘要跨进程不一致"
        assert remote_scope["safe"] == local.safe, "safe 结论跨进程不一致"
        local_witness = None if local.witness is None else local.witness.to_dict()
        assert remote_scope["witness"] == local_witness, "witness 跨进程不一致"
        if local.witness is not None:
            recheck = remote["topic_recheck"]
            assert recheck["allowed"] is False, "新进程复核 witness 应被拒绝"
            assert recheck["deny_rule_ids"] == list(local.witness.deny_rule_ids)
        report.append({
            "principal_id": principal,
            "request_filter": request,
            "safe": local.safe,
            "witness_topic": None if local.witness is None else local.witness.topic,
            "policy_digest_sha256": local.policy_summary["policy_digest_sha256"],
            "cross_process_identical": True,
            "remote_topic_recheck_denied":
                None if local.witness is None else (not remote["topic_recheck"]["allowed"]),
        })
        os.unlink(task_path)

    out_path = os.path.join(EVIDENCE_DIR, "cross_process.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump({"runner": f"{sys.executable} -I", "cases": report},
                  fh, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\n跨进程复核 {len(report)} 个用例全部一致，证据: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
