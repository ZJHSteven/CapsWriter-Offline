# coding: utf-8
"""
客户端模块

这个包只负责“对外提供名字”，不在导入时一次性把整个客户端应用都加载起来。

为什么要这样做：
- `retry_failed_tasks.py` 这类小脚本只需要少量能力，不应该被音频录制、热键、UI 等模块拖慢。
- 原来的 eager import 会在导入 `util.client` 时立刻加载很多依赖，导致手动重试脚本即使只用 `DiaryWriter` 也会碰到 `sounddevice` 之类的可选包缺失。

实现方式：
- 这里改成懒加载。
- 只有真正访问某个名字时，才去对应模块里导入它。
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

# 这里集中维护“名字 -> 模块路径/属性名”的映射。
# 这样既保留了旧的 `from util.client import XXX` 用法，又不会在包导入时提前加载全部依赖。
_EXPORTS: dict[str, tuple[str, str]] = {
    # 核心状态
    'ClientState': ('util.client.state', 'ClientState'),
    'get_state': ('util.client.state', 'get_state'),
    'console': ('util.client.state', 'console'),
    'WebSocketManager': ('util.client.websocket_manager', 'WebSocketManager'),

    # 音频
    'AudioRecorder': ('util.client.audio', 'AudioRecorder'),
    'AudioStreamManager': ('util.client.audio', 'AudioStreamManager'),
    'AudioFileManager': ('util.client.audio', 'AudioFileManager'),

    # 快捷键
    'Shortcut': ('util.client.shortcut', 'Shortcut'),
    'ShortcutManager': ('util.client.shortcut', 'ShortcutManager'),

    # 输出
    'ResultProcessor': ('util.client.output', 'ResultProcessor'),
    'TextOutput': ('util.client.output', 'TextOutput'),

    # 转录
    'FileTranscriber': ('util.client.transcribe', 'FileTranscriber'),
    'SrtAdjuster': ('util.client.transcribe', 'SrtAdjuster'),

    # 日记
    'DiaryWriter': ('util.client.diary', 'DiaryWriter'),

    # UI
    'TipsDisplay': ('util.client.ui', 'TipsDisplay'),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    """
    按需导出属性。

    当外部写 `from util.client import DiaryWriter` 或 `util.client.DiaryWriter` 时，
    Python 会在这里触发懒加载。
    """
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = _EXPORTS[name]
    value = getattr(import_module(module_name), attr_name)

    # 缓存到模块全局，避免重复 import。
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """让 `dir(util.client)` 也能看到懒加载导出的名字。"""
    return sorted(set(globals()) | set(__all__))
