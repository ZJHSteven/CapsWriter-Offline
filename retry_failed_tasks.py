# coding: utf-8
"""
失败任务手动重试脚本（终端交互版）

用途：
1. 列出 `runtime/failed_tasks` 中尚未成功重试的失败任务。
2. 允许用户输入编号选择任务进行重试（也支持 `all` 批量重试）。
3. 重试成功后：
   - 在终端打印结果（便于复制粘贴）
   - 写入文字备份（日记）
   - 标记失败任务状态为 `replayed_success`
   - 删除失败音频（按配置）

注意：
- 本脚本是“整段录音重放重试”，不是继续原来的云端 WebSocket 会话。
- 不会自动向当前前台窗口上屏，避免误输入。
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from config import ServerConfig, ClientConfig
from util.client.diary.diary_writer import DiaryWriter
from util.server.asr_aliyun_realtime import AliyunRealtimeRecognizer
from util.server.failed_task_store import FailedTaskStore


def _build_recognizer() -> AliyunRealtimeRecognizer:
    """按当前项目配置创建阿里云实时识别器（用于重放重试）。"""
    return AliyunRealtimeRecognizer(
        api_key=ServerConfig.aliyun_api_key,
        endpoint=ServerConfig.aliyun_endpoint,
        model=ServerConfig.aliyun_model,
        source_language=ServerConfig.aliyun_source_language,
        max_sentence_silence=ServerConfig.aliyun_max_sentence_silence,
        punctuation_enabled=ServerConfig.aliyun_enable_punctuation,
        itn_enabled=ServerConfig.aliyun_enable_itn,
        connect_timeout=ServerConfig.aliyun_connect_timeout,
        response_timeout=ServerConfig.aliyun_response_timeout,
        finish_confirm_timeout=getattr(ServerConfig, 'aliyun_finish_confirm_timeout', None),
        auto_retry_once=False,  # 手动重试脚本内禁止再次自动重试，避免递归重试链
        retry_backoff_seconds=getattr(ServerConfig, 'aliyun_retry_backoff_seconds', 0.5),
        failed_task_store_enabled=getattr(ServerConfig, 'failed_task_store_enabled', True),
        failed_task_store_dir=getattr(ServerConfig, 'failed_task_store_dir', 'runtime/failed_tasks'),
        failed_audio_delete_on_replay_success=getattr(ServerConfig, 'failed_audio_delete_on_replay_success', True),
        ws_log_verbosity=getattr(ServerConfig, 'aliyun_ws_log_verbosity', 'summary'),
    )


def _print_records(records: list[dict]) -> None:
    """按编号打印失败任务列表。"""
    print("\n当前失败任务列表：")
    for idx, item in enumerate(records, 1):
        task_id = item.get('task_id', '')
        status = item.get('status', '')
        err = item.get('error_code', '')
        created_at = float(item.get('created_at') or 0.0)
        tstr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(created_at)) if created_at else 'N/A'
        salvage = ((item.get('salvage') or {}).get('combined_text') or '')[:60]
        print(f"{idx:>3}. {task_id} | {status} | {err} | {tstr}")
        if salvage:
            print(f"     保底预览: {salvage}")


def _should_show_record(item: dict) -> bool:
    """过滤掉已重试成功的任务，避免列表噪音。"""
    return str(item.get('status') or '') != 'replayed_success'


def _write_text_backup(text: str, time_start: float) -> None:
    """把重试成功结果写入文字备份（日记）。"""
    if not getattr(ClientConfig, 'save_text_backup', True):
        return
    diary = DiaryWriter()
    diary.write(
        f"[手动重试成功] {text}",
        time_start or time.time(),
        None,
        retention_days=getattr(ClientConfig, 'text_backup_retention_days', None),
    )


def _retry_one(store: FailedTaskStore, recognizer: AliyunRealtimeRecognizer, item: dict) -> None:
    """重试单个失败任务。"""
    task_ref = str(item.get('task_id') or '')
    if not task_ref:
        print("跳过：缺少 task_id")
        return

    audio_path = store.find_audio_path(task_ref)
    if not audio_path or not audio_path.exists():
        print(f"跳过 {task_ref}: 找不到 audio.pcm")
        return

    meta = store.load_meta(task_ref) or item
    time_start = float(meta.get('time_start') or time.time())
    samplerate = int(((meta.get('meta') or {}).get('samplerate')) or 16000)

    print(f"\n开始重试: {task_ref}")
    print(f"  音频: {audio_path}")
    print(f"  采样率: {samplerate}")
    store.mark_retrying(task_ref)

    try:
        result = recognizer.transcribe_pcm_file(
            pcm_path=str(audio_path),
            samplerate=samplerate,
            time_start=time_start,
            time_submit=time.time(),
            local_task_id=f"manual-retry-{task_ref}",
            enable_spool=False,
        )
        if result.status != 'success_confirmed':
            store.mark_replay_failed(task_ref, result.error_code or 'retry_failed', result.error_message or '重试失败')
            print(f"重试失败（保底状态）: {result.status} | {result.error_code}")
            if result.salvage_text_finalized:
                print("已定稿保底：")
                print(result.salvage_text_finalized)
            if result.salvage_text_partial:
                print("未定稿保底：")
                print(result.salvage_text_partial)
            return

        print("重试成功，结果如下（可直接复制）：")
        print("=" * 60)
        print(result.text)
        print("=" * 60)
        _write_text_backup(result.text, time_start)
        store.mark_replay_success(
            task_ref,
            delete_audio=getattr(ServerConfig, 'failed_audio_delete_on_replay_success', True),
        )
        print("已写入文字备份，并标记为重试成功。")
    except Exception as e:
        store.mark_replay_failed(task_ref, 'retry_exception', f'{type(e).__name__}: {e}')
        print(f"重试异常: {type(e).__name__}: {e}")


def main() -> int:
    """终端入口。"""
    base_dir = getattr(ServerConfig, 'failed_task_store_dir', 'runtime/failed_tasks')
    store = FailedTaskStore(base_dir)
    records_all = store.list_records()
    records = [item for item in records_all if _should_show_record(item)]
    if not records:
        print("没有待重试的失败任务。")
        return 0

    _print_records(records)
    print("\n输入编号（如 1）、多个编号（如 1,3,5）、`all` 批量重试，或直接回车退出。")
    user_input = input("> ").strip()
    if not user_input:
        return 0

    if user_input.lower() == 'all':
        selected = records
    else:
        indexes: list[int] = []
        for part in user_input.replace('，', ',').split(','):
            part = part.strip()
            if not part:
                continue
            if not part.isdigit():
                print(f"非法输入: {part}")
                return 1
            idx = int(part)
            if idx < 1 or idx > len(records):
                print(f"编号超出范围: {idx}")
                return 1
            indexes.append(idx)
        selected = [records[i - 1] for i in indexes]

    recognizer = _build_recognizer()
    try:
        for item in selected:
            _retry_one(store, recognizer, item)
    finally:
        recognizer.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
