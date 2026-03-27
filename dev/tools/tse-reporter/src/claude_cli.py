"""Claude Code CLI サブプロセス呼び出しユーティリティ

Anthropic API クレジットを消費せず、現在の Claude Code セッションのトークンを使って
Claude を呼び出すためのヘルパー。
"""

import os
import subprocess
from pathlib import Path
from typing import Optional


def run_claude(prompt: str, image_path: Optional[Path] = None) -> str:
    """
    `claude -p` 経由で Claude を呼び出し、出力テキストを返す。

    ANTHROPIC_API_KEY を環境変数から除いて呼び出すことで、
    API クレジットではなく Claude Code のセッション認証を使用する。
    """
    cmd = ["claude", "-p", prompt]
    if image_path is not None:
        cmd += ["--image", str(image_path)]

    # ANTHROPIC_API_KEY を除いた環境変数を渡す（API クレジット消費を防ぐ）
    env = {k: v for k, v in os.environ.items() if k != "ANTHROPIC_API_KEY"}

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(
            f"claude CLI がエラーを返しました (exit {result.returncode}): {err[:500]}"
        )
    return result.stdout.strip()
