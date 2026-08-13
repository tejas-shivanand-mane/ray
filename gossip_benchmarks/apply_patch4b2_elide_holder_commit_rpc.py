#!/usr/bin/env python3
"""Apply Patch 4B-2: eliminate the redundant holder commit RPC.

Run from the root of the Ray repository:

    python gossip_benchmarks/apply_patch4b2_elide_holder_commit_rpc.py

Normal holder admission currently has:

    candidate -> owner: ReportRecoveryCandidate
    owner -> candidate: InstallRecoveryHolder
    owner -> witness(es): UpdateRecoveryWitness
    owner local: CommitHolderAdmission
    owner -> candidate: CommitRecoveryManifest      <-- removed by 4B-2
    owner -> candidate: ReportRecoveryCandidate reply

The final ReportRecoveryCandidate reply already carries committed_manifest, and
the candidate callback already applies that manifest.

After the Benchmark 14/15 correctness fix, losing that reply after a witness ACK
is also safe: the holder stays provisional and can independently confirm the
witnessed manifest during recovery.

This patch deliberately keeps the CommitRecoveryManifest RPC definition,
handler, profiling counters, and test hooks. It only removes the normal-path
send and updates benchmarks that waited for that send.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys


ROOT = Path.cwd()
CORE = ROOT / "src/ray/core_worker/core_worker.cc"
BENCH14 = ROOT / "gossip_benchmarks/14_candidate_commit_durability.py"
BENCH16 = ROOT / "gossip_benchmarks/16_recovery_correctness_suite.py"

PATCH_MARKER = "Patch 4B-2: do not send an explicit CommitRecoveryManifest RPC"
OLD_COMMENT = "// Only a candidate that received InstallRecoveryHolder needs an"

NEW_COMMENT = """        // Patch 4B-2: do not send an explicit CommitRecoveryManifest RPC.
        //
        // The normal ReportRecoveryCandidate reply already carries the
        // committed manifest, and the candidate applies that manifest in its
        // report callback. If the reply is lost after witness publication, the
        // holder remains provisional and can independently confirm the
        // witnessed manifest during recovery.
"""


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(1)


def read_required(path: Path) -> str:
    if not path.exists():
        fail(f"Missing {path}. Run this script from the Ray repository root.")
    return path.read_text()


def find_matching_cpp_brace(text: str, open_brace: int) -> int:
    """Find a matching C++ brace while ignoring strings and comments."""
    if open_brace < 0 or open_brace >= len(text) or text[open_brace] != "{":
        raise ValueError("open_brace must point at '{'")

    depth = 0
    i = open_brace
    state = "code"

    while i < len(text):
        ch = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if state == "code":
            if ch == "/" and nxt == "/":
                state = "line_comment"
                i += 2
                continue
            if ch == "/" and nxt == "*":
                state = "block_comment"
                i += 2
                continue
            if ch == '"':
                state = "string"
                i += 1
                continue
            if ch == "'":
                state = "char"
                i += 1
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return i
            i += 1
            continue

        if state == "line_comment":
            if ch == "\n":
                state = "code"
            i += 1
            continue

        if state == "block_comment":
            if ch == "*" and nxt == "/":
                state = "code"
                i += 2
            else:
                i += 1
            continue

        if state == "string":
            if ch == "\\":
                i += 2
            elif ch == '"':
                state = "code"
                i += 1
            else:
                i += 1
            continue

        if state == "char":
            if ch == "\\":
                i += 2
            elif ch == "'":
                state = "code"
                i += 1
            else:
                i += 1
            continue

    raise ValueError("Unbalanced C++ braces while locating commit-RPC block")


def patch_core(text: str) -> tuple[str, bool]:
    if PATCH_MARKER in text:
        return text, False

    comment_pos = text.find(OLD_COMMENT)
    if comment_pos < 0:
        fail(
            "Could not find the normal-path explicit CommitRecoveryManifest "
            "comment in core_worker.cc. No files changed."
        )

    func_pos = text.rfind(
        "void CoreWorker::FinishRecoveryHolderAdmission(",
        0,
        comment_pos,
    )
    if func_pos < 0:
        fail(
            "Commit-RPC comment was not found inside "
            "FinishRecoveryHolderAdmission. No files changed."
        )

    next_func = text.find("\nvoid CoreWorker::", func_pos + 1)
    if next_func < 0:
        next_func = len(text)

    if not (func_pos < comment_pos < next_func):
        fail("Commit-RPC block bounds are ambiguous. No files changed.")

    if_pos = text.find(
        "if (candidate_needs_commit_rpc)",
        comment_pos,
        next_func,
    )
    if if_pos < 0:
        fail(
            "Could not find candidate_needs_commit_rpc normal commit block. "
            "No files changed."
        )

    brace_pos = text.find("{", if_pos, next_func)
    if brace_pos < 0:
        fail("Could not find opening brace of commit-RPC block.")

    try:
        close_brace = find_matching_cpp_brace(text, brace_pos)
    except ValueError as exc:
        fail(str(exc))

    start = text.rfind("\n", 0, comment_pos) + 1

    end = close_brace + 1
    while end < len(text) and text[end] in " \t":
        end += 1
    if end < len(text) and text[end] == "\n":
        end += 1
    while end < len(text) and text[end] == "\n":
        end += 1

    new_text = text[:start] + NEW_COMMENT + "\n" + text[end:]

    func_start = new_text.find(
        "void CoreWorker::FinishRecoveryHolderAdmission("
    )
    func_end = new_text.find("\nvoid CoreWorker::", func_start + 1)
    if func_end < 0:
        func_end = len(new_text)

    if (
        "candidate_client->CommitRecoveryManifest("
        in new_text[func_start:func_end]
    ):
        fail(
            "Sanity check failed: explicit commit RPC still appears inside "
            "FinishRecoveryHolderAdmission. No files changed."
        )

    return new_text, True


def patch_bench14(text: str) -> tuple[str, bool]:
    changed = False

    pattern = re.compile(
        r"""lambda\s+p:\s*\(\s*
            int\(p\.get\("holder_admissions_committed",\s*0\)\)\s*>=\s*1\s*
            and\s*
            int\(p\.get\("holder_commit_rpcs_completed",\s*0\)\)\s*>=\s*1\s*
            \)""",
        re.VERBOSE,
    )

    replacement = (
        'lambda p: int(\n'
        '                    p.get("holder_admissions_committed", 0)\n'
        '                ) >= 1'
    )

    text2, count = pattern.subn(replacement, text, count=1)
    if count:
        text = text2
        changed = True

    old = "H1 is fully committed before owner failure."
    new = "H1 is fully committed by the owner before owner failure."
    if old in text:
        text = text.replace(old, new, 1)
        changed = True

    return text, changed


def patch_bench16(text: str) -> tuple[str, bool]:
    changed = False

    old_h1 = """            int(p.get("holder_admissions_committed", 0)) >= 1
            and int(p.get("holder_commit_rpcs_completed", 0)) >= 1
            and int(p.get("max_non_owner_holders", 0)) >= 1"""
    new_h1 = """            int(p.get("holder_admissions_committed", 0)) >= 1
            and int(p.get("max_non_owner_holders", 0)) >= 1"""
    if old_h1 in text:
        text = text.replace(old_h1, new_h1, 1)
        changed = True

    old_h2 = """            int(p.get("holder_admissions_committed", 0)) >= 2
            and int(p.get("holder_commit_rpcs_completed", 0)) >= 2
            and int(p.get("max_non_owner_holders", 0)) >= 2"""
    new_h2 = """            int(p.get("holder_admissions_committed", 0)) >= 2
            and int(p.get("max_non_owner_holders", 0)) >= 2"""
    if old_h2 in text:
        text = text.replace(old_h2, new_h2, 1)
        changed = True

    old_gate = """            formation["holder_admissions_committed"] >= 2
            and formation["max_non_owner_holders"] >= 2
            and formation["holder_commit_rpcs_completed"] >= 2"""
    new_gate = """            formation["holder_admissions_committed"] >= 2
            and formation["max_non_owner_holders"] >= 2"""
    if old_gate in text:
        text = text.replace(old_gate, new_gate, 1)
        changed = True

    return text, changed


def main() -> None:
    # Compute everything before writing anything.
    core_original = read_required(CORE)
    bench14_original = read_required(BENCH14)
    bench16_original = BENCH16.read_text() if BENCH16.exists() else None

    core_new, core_changed = patch_core(core_original)
    bench14_new, bench14_changed = patch_bench14(bench14_original)

    bench16_new = None
    bench16_changed = False
    if bench16_original is not None:
        bench16_new, bench16_changed = patch_bench16(bench16_original)

    writes = [
        (CORE, core_new, core_changed),
        (BENCH14, bench14_new, bench14_changed),
    ]

    if bench16_original is not None and bench16_new is not None:
        writes.append((BENCH16, bench16_new, bench16_changed))

    for path, content, changed in writes:
        if changed:
            path.write_text(content)
            print(f"[patched] {path}")
        else:
            print(f"[unchanged/already compatible] {path}")

    if bench16_original is None:
        print(f"[not present] {BENCH16}")

    print()
    print("Patch 4B-2 applied.")
    print()
    print("Review:")
    print(
        "  git diff -- "
        "src/ray/core_worker/core_worker.cc "
        "gossip_benchmarks/14_candidate_commit_durability.py "
        "gossip_benchmarks/16_recovery_correctness_suite.py"
    )
    print("  git diff --check")
    print()
    print("Expected after rebuild during normal holder formation:")
    print("  holder_install_rpcs_*        : one per admitted holder")
    print("  witness_update_rpcs_*        : unchanged")
    print("  holder_commit_rpcs_*         : 0")
    print("  holder_admissions_committed  : unchanged")


if __name__ == "__main__":
    main()
