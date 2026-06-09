#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""chart_lint.py — deterministic high-signal chart-design linter.

"Tell it how to check it." Statically inspects the Plotly chart-builder code in
build_dashboard.py (or any module) for the structural rules in
References/ChartDesign.md. Independently written for our Plotly code — no code
from the source skill (Goodeye Labs' high-signal-chart-workflow, CC BY-NC-ND).

Usage:  uv run --script chart_lint.py <build_dashboard.py>
Exit 0 = clean. Exit 1 = one failed-rule name per stdout line.

Rules checked (per `go.Figure().update_layout(...)` chart):
  legend_not_disabled    every update_layout sets showlegend=False
  title_missing          every update_layout sets title=
  xaxis_title_missing    every update_layout sets an x-axis title
  yaxis_title_missing    every update_layout sets a y-axis title
  hardcoded_markers      a Scatter passes a CONSTANT mode="lines+markers"
                         (markers must be conditional on point count)
File-level:
  direct_label_missing   no add_annotation anywhere (no direct labels / story note)
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path


def _kwarg(call: ast.Call, name: str) -> ast.keyword | None:
    return next((k for k in call.keywords if k.arg == name), None)


def _has_axis_title(call: ast.Call, axis: str) -> bool:
    # accept either `xaxis_title=...` or `xaxis=dict(title=...)`
    if _kwarg(call, f"{axis}_title") is not None:
        return True
    kw = _kwarg(call, axis)
    if kw and isinstance(kw.value, ast.Call):
        return _kwarg(kw.value, "title") is not None
    return False


def lint(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    failed: list[str] = []
    has_annotation = False
    layouts = 0

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        attr = node.func.attr
        if attr == "add_annotation":
            has_annotation = True
        if attr == "update_layout":
            layouts += 1
            if _kwarg(node, "showlegend") is None:
                failed.append("legend_not_disabled")
            if _kwarg(node, "title") is None:
                failed.append("title_missing")
            if not _has_axis_title(node, "xaxis"):
                failed.append("xaxis_title_missing")
            if not _has_axis_title(node, "yaxis"):
                failed.append("yaxis_title_missing")
        if attr == "add_trace" and node.args:
            arg = node.args[0]
            if isinstance(arg, ast.Call):  # go.Scatter(...)
                mode = _kwarg(arg, "mode")
                if (mode and isinstance(mode.value, ast.Constant)
                        and mode.value.value == "lines+markers"):
                    failed.append("hardcoded_markers")

    if layouts == 0:
        failed.append("no_chart_found")
    if not has_annotation:
        failed.append("direct_label_missing")
    # de-dup, stable order
    return list(dict.fromkeys(failed))


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: chart_lint.py <module.py>", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    if not path.exists():
        print("file_missing")
        return 1
    failed = lint(path)
    for name in failed:
        print(name)
    if not failed:
        print("chart-lint OK — all high-signal rules satisfied", file=sys.stderr)
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
