# coding: utf-8
"""
按键模拟器。

这个模块只负责一件事：在 CapsWriter 需要“补发”某个按键时，主动向系统
发送一次按键事件。

为什么需要补发：
- 阻塞模式下，CapsWriter 会拦截原始按键，短按时用户仍然期望这个按键本身生效。
- 例如 CapsLock 短按不应该被听写功能吃掉，所以需要程序再模拟一次 CapsLock。

为什么补发要记录状态：
- 程序模拟出来的按键也会被全局键盘监听器看见。
- 如果不标记“这是我自己补发的”，监听器可能把补发事件又当作新的听写触发，
  形成“短按 -> 补发 -> 又触发 -> 又补发”的循环。
"""

import time

from pynput import keyboard, mouse
from . import logger
from core.client.shortcut.key_mapper import KeyMapper


EMULATION_FLAG_TTL = 0.5
"""
补发标志的最大保留时间，单位秒。

正常情况下，标志会在监听到补发按键的 keyup / mouse up 后清掉。
保留一个短超时是为了处理异常情况：如果系统没有把注入事件回调回来，
标志也不能永久留在集合里，否则后续真实按键会被误判。
"""


class ShortcutEmulator:
    """
    快捷键模拟器

    使用常驻的 controller 对象，避免重复创建开销
    """

    def __init__(self):
        """初始化模拟器"""
        self._keyboard_controller = keyboard.Controller()
        self._mouse_controller = mouse.Controller()
        self._emulating_keys = {}

    def _cleanup_expired_flags(self) -> None:
        """
        清理过期的补发标志。

        这里不用后台线程，只在检查或新增标志时顺手清理。
        好处是实现简单，并且不会引入额外的生命周期管理问题。
        """
        now = time.monotonic()
        expired_keys = [
            key_name
            for key_name, started_at in self._emulating_keys.items()
            if now - started_at > EMULATION_FLAG_TTL
        ]
        for key_name in expired_keys:
            self._emulating_keys.pop(key_name, None)

    def is_emulating(self, key_name: str) -> bool:
        """检查是否正在模拟指定按键"""
        self._cleanup_expired_flags()
        return key_name in self._emulating_keys

    def clear_emulating_flag(self, key_name: str) -> None:
        """清除模拟标志"""
        self._emulating_keys.pop(key_name, None)

    def emulate_key(self, key_name: str) -> None:
        """
        异步模拟键盘按键

        Args:
            key_name: 按键名称（如 'caps_lock', 'f12'）
        """
        self._cleanup_expired_flags()
        self._emulating_keys[key_name] = time.monotonic()

        key_obj = KeyMapper.name_to_key(key_name)
        if key_obj is not None:
            self._keyboard_controller.press(key_obj)
            self._keyboard_controller.release(key_obj)
            logger.debug(f"[{key_name}] 补发按键成功")
        else:
            logger.warning(f"[{key_name}] 无法识别的按键，跳过补发")

    def emulate_mouse_click(self, button_name: str) -> None:
        """
        异步模拟鼠标按键

        Args:
            button_name: 鼠标按键名称（'x1' 或 'x2'）
        """
        self._cleanup_expired_flags()
        self._emulating_keys[button_name] = time.monotonic()

        # pynput 鼠标按键对象映射
        button_map = {
            'x1': mouse.Button.x1,
            'x2': mouse.Button.x2
        }

        if button_name in button_map:
            button = button_map[button_name]
            self._mouse_controller.press(button)
            self._mouse_controller.release(button)
            logger.debug(f"[{button_name}] 补发鼠标按键成功")
        else:
            logger.warning(f"[{button_name}] 无法识别的鼠标按键，跳过补发")
