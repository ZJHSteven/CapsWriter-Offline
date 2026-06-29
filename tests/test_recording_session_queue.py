# coding: utf-8
"""
录音会话队列隔离测试。

这个测试文件只验证本次 bug 的核心数据流，不启动真实麦克风、WebSocket、托盘、
ASR 模型或 GUI。这样做有两个目的：

1. 让测试可以在普通开发机上快速运行，不依赖显卡、模型文件和音频设备。
2. 精确覆盖“begin/data/finish 被多个 recorder 抢同一个全局队列”这个根因。

背景：
- 旧实现中，所有 AudioRecorder 都从 ClientState.queue_in 读取事件。
- 如果旧 recorder 还没退出，新 recorder 又启动，就会出现多个消费者抢一个队列。
- 抢队列会让服务端看到 data 属于新 task_id，但 final 属于旧 task_id。
- 本次修复后，每次录音都有自己的 session_queue，音频流只投递到当前会话队列。
"""

from __future__ import annotations

import asyncio
import base64
import sys
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# 将打包目录里的第三方依赖追加到 sys.path 末尾。
#
# 注意这里必须用 append，不能通过 PYTHONPATH 把 internal 放到最前面：
# internal 目录里带有 asyncio、logging、unittest 等打包运行时文件夹，
# 如果它排在标准库前面，就会遮蔽 Python 自带标准库，导致 `python -m unittest`
# 解析到错误的 unittest 包。
PROJECT_ROOT = Path(__file__).resolve().parents[1]
INTERNAL_DIR = PROJECT_ROOT / "internal"
if INTERNAL_DIR.exists():
    sys.path.append(str(INTERNAL_DIR))

import numpy as np

from config_client import ClientConfig
from core.client.audio.recorder import AudioRecorder
from core.client.audio.stream import AudioStreamManager
from core.client.state import ClientState
from core.client.shortcut.key_mapper import WM_KEYDOWN, WM_KEYUP
from core.client.shortcut.shortcut_manager import ShortcutManager


class FakeKeyboardEventData:
    """
    伪造 Windows 低级键盘事件数据。

    ShortcutManager._check_emulating 只读取 data.flags。
    测试里不需要真实 pynput 事件对象，用这个小对象表达两类情况即可：
    - flags=0x10：程序注入事件，也就是 pynput/SendInput 模拟出来的按键。
    - flags=0x00：真实物理键盘事件。
    """

    def __init__(self, flags: int):
        """保存 Windows 低级键盘钩子 flags 字段。"""
        self.flags = flags


class FakeEmulatorState:
    """
    伪造快捷键补发状态。

    ShortcutManager._check_emulating 只依赖两个方法：
    - is_emulating(key_name)：当前是否处于补发窗口。
    - clear_emulating_flag(key_name)：补发 keyup 到达后清除标志。
    """

    def __init__(self):
        """默认认为 caps_lock 正在补发窗口内。"""
        self.active_keys = {"caps_lock"}
        self.cleared_keys: list[str] = []

    def is_emulating(self, key_name: str) -> bool:
        """返回指定按键是否仍被视为程序补发中。"""
        return key_name in self.active_keys

    def clear_emulating_flag(self, key_name: str) -> None:
        """记录清理动作，并移除补发标志。"""
        self.cleared_keys.append(key_name)
        self.active_keys.discard(key_name)


@dataclass
class FakeWebSocketManager:
    """
    伪造 WebSocket 管理器。

    AudioRecorder 只依赖两个行为：
    - is_connected: 判断是否可以发送消息。
    - send(message): 真正发送协议消息。

    测试里不需要真实网络连接，只要把发出的 AudioMessage 收集起来，
    再检查这些消息的 task_id 和 is_final 即可。
    """

    sent_messages: list[Any]
    is_connected: bool = True

    async def send(self, message) -> bool:
        """记录发送消息，并模拟发送成功。"""
        self.sent_messages.append(message)
        return True


@dataclass
class FakeApp:
    """
    伪造客户端 App。

    AudioRecorder 和 AudioStreamManager 只需要 app.state、app.ws、app.loop。
    这里显式列出这三个字段，避免把完整 CapsWriterClient 拉进测试。
    """

    state: ClientState
    ws: FakeWebSocketManager
    loop: asyncio.AbstractEventLoop


class RecordingSessionQueueTests(unittest.IsolatedAsyncioTestCase):
    """围绕录音会话队列隔离的最小行为测试。"""

    async def asyncSetUp(self) -> None:
        """每个用例都创建全新的状态，避免队列和录音标志互相污染。"""
        self.state = ClientState()
        self.ws = FakeWebSocketManager(sent_messages=[])
        self.app = FakeApp(state=self.state, ws=self.ws, loop=asyncio.get_running_loop())

        # 测试不关心保存 MP3/WAV 文件，关闭后可以避免依赖 ffmpeg 和文件系统副作用。
        self._old_save_audio = ClientConfig.save_audio
        ClientConfig.save_audio = False

    async def asyncTearDown(self) -> None:
        """恢复全局配置，避免影响后续测试或手动运行。"""
        ClientConfig.save_audio = self._old_save_audio
        self.state.stop_recording()

    async def test_audio_callback_writes_only_to_active_session_queue(self) -> None:
        """
        验证音频流只把 data 投递到当前会话队列。

        这覆盖了最关键的修复点：
        - active_queue 是当前录音会话。
        - stale_queue 模拟旧 recorder 仍然存在。
        - 音频回调不应再写入全局 queue_in 或旧队列。
        """
        active_queue: asyncio.Queue = asyncio.Queue()
        stale_queue: asyncio.Queue = asyncio.Queue()

        started = self.state.start_recording(1000.0, active_queue)
        self.assertTrue(started)

        manager = AudioStreamManager.__new__(AudioStreamManager)
        manager.app = self.app

        fake_audio = np.ones((8, 1), dtype=np.float32)
        manager._audio_callback(fake_audio, frames=8, time_info=None, status=None)

        event = await asyncio.wait_for(active_queue.get(), timeout=1)

        self.assertEqual(event["type"], "data")
        self.assertTrue(np.array_equal(event["data"], fake_audio))
        self.assertTrue(stale_queue.empty())
        self.assertTrue(self.state.queue_in.empty())

    async def test_start_recording_rejects_second_active_session(self) -> None:
        """
        验证已有录音时拒绝第二次启动。

        这能防止 CapsLock、鼠标侧键、UDP 控制等入口交错触发时，
        生成多个 recorder 同时读取同一份麦克风数据。
        """
        first_queue: asyncio.Queue = asyncio.Queue()
        second_queue: asyncio.Queue = asyncio.Queue()

        self.assertTrue(self.state.start_recording(1000.0, first_queue))
        self.assertFalse(self.state.start_recording(1001.0, second_queue))
        self.assertIs(self.state.active_recording_queue, first_queue)

    async def test_recorder_uses_one_task_id_for_data_and_final_in_session_queue(self) -> None:
        """
        验证同一录音会话内 data 和 final 使用同一个 task_id。

        这里直接给 AudioRecorder 传 session_queue：
        - begin 建立开始时间。
        - data 触发一次非 final 音频消息。
        - finish 触发最终消息。

        如果 recorder 又去读全局 queue_in，测试会超时或发不出消息。
        如果 task_id 串线，两个消息的 task_id 会不同。
        """
        session_queue: asyncio.Queue = asyncio.Queue()
        recorder = AudioRecorder(self.app)

        await session_queue.put({"type": "begin", "time": 1000.0, "data": None})
        await session_queue.put({
            "type": "data",
            "time": 1001.0,
            "data": np.ones((4800, 1), dtype=np.float32),
        })
        await session_queue.put({"type": "finish", "time": 1002.0, "data": None})

        await asyncio.wait_for(recorder.record_and_send(session_queue), timeout=2)
        # AudioRecorder 内部用 asyncio.create_task() 异步发送 WebSocket 消息。
        # record_and_send 返回只代表录音队列消费完毕，不代表发送协程已经被调度执行。
        # 这里主动让出事件循环，确保两个 _send_message 任务都完成后再断言。
        await asyncio.sleep(0)

        self.assertEqual(len(self.ws.sent_messages), 2)
        self.assertFalse(self.ws.sent_messages[0].is_final)
        self.assertTrue(self.ws.sent_messages[1].is_final)
        self.assertEqual(self.ws.sent_messages[0].task_id, self.ws.sent_messages[1].task_id)

        decoded = base64.b64decode(self.ws.sent_messages[0].data)
        self.assertGreater(len(decoded), 0)


class ShortcutInjectedEventTests(unittest.TestCase):
    """围绕补发按键防自捕获的最小行为测试。"""

    def _manager_with_fake_emulator(self) -> ShortcutManager:
        """
        构造不启动真实监听器的 ShortcutManager 实例。

        这里用 __new__ 跳过 __init__，避免创建 pynput listener、线程池和真实 controller。
        测试目标只是 _check_emulating 的纯逻辑分支。
        """
        manager = ShortcutManager.__new__(ShortcutManager)
        manager._emulator = FakeEmulatorState()
        return manager

    def test_real_keyboard_event_is_not_swallowed_by_emulating_flag(self) -> None:
        """
        验证真实键盘事件不会被补发标志误吞。

        这是为了防止一个边界问题：
        - 程序刚补发过 CapsLock，_emulating_keys 里短暂存在 caps_lock。
        - 用户真实又按了一次 CapsLock。
        - 真实事件 flags 不带 LLKHF_INJECTED，所以不能被当成程序补发。
        """
        manager = self._manager_with_fake_emulator()

        swallowed = manager._check_emulating(
            "caps_lock",
            WM_KEYDOWN,
            data=FakeKeyboardEventData(flags=0x00),
        )

        self.assertFalse(swallowed)
        self.assertEqual(manager._emulator.cleared_keys, [])
        self.assertIn("caps_lock", manager._emulator.active_keys)

    def test_injected_keyboard_keyup_is_swallowed_and_clears_flag(self) -> None:
        """
        验证程序注入的 keyup 会被吞掉，并清理补发标志。

        这正是 CapsWriter 短按补发 CapsLock 时需要的行为：
        补发事件不应该重新触发录音；补发结束后标志也不能残留。
        """
        manager = self._manager_with_fake_emulator()

        swallowed = manager._check_emulating(
            "caps_lock",
            WM_KEYUP,
            data=FakeKeyboardEventData(flags=0x10),
        )

        self.assertTrue(swallowed)
        self.assertEqual(manager._emulator.cleared_keys, ["caps_lock"])
        self.assertNotIn("caps_lock", manager._emulator.active_keys)


if __name__ == "__main__":
    unittest.main()
