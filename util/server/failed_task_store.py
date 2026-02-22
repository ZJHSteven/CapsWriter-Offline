# coding: utf-8
"""
失败任务持久化存储模块（服务端）。

设计目标：
1. 当实时云端任务超时/异常时，保存“已获得的战果”（文本/句子状态/日志摘要）。
2. 保存可重试音频（PCM16），供自动重试或手动重试脚本使用。
3. 为后续排障提供结构化元数据。
"""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from util.logger import get_logger

logger = get_logger('server')


class FailedTaskStore:
    """
    失败任务存储器。

    目录结构（默认）：
    `runtime/failed_tasks/YYYYMMDD/<task_id>/`
    """

    def __init__(self, base_dir: str = 'runtime/failed_tasks') -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def record_failure(
        self,
        *,
        task_id: str,
        source: str,
        time_start: float,
        error_code: str,
        error_message: str,
        salvage_text_finalized: str,
        salvage_text_partial: str,
        salvage_text_combined: str,
        sentences_summary: Iterable[dict],
        ws_events: Iterable[dict],
        audio_pcm_path: Optional[str],
        metadata_extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        保存失败任务完整上下文，返回失败任务引用 ID（当前直接使用 task_id）。
        """
        record_dir = self._record_dir(task_id, time_start)
        record_dir.mkdir(parents=True, exist_ok=True)

        payload = {
            'task_id': task_id,
            'source': source,
            'time_start': time_start,
            'created_at': time.time(),
            'status': 'failed_pending_retry',
            'error_code': error_code,
            'error_message': error_message,
            'salvage': {
                'finalized_text': salvage_text_finalized or '',
                'partial_text': salvage_text_partial or '',
                'combined_text': salvage_text_combined or '',
            },
        }
        if metadata_extra:
            payload['meta'] = metadata_extra

        self._write_json(record_dir / 'meta.json', payload)
        self._write_json(record_dir / 'sentences.json', list(sentences_summary))

        salvage_file = record_dir / 'salvage.txt'
        salvage_file.write_text(salvage_text_combined or '', encoding='utf-8')

        error_file = record_dir / 'error.txt'
        error_file.write_text(f"{error_code}\n{error_message}\n", encoding='utf-8')

        ws_events_path = record_dir / 'ws_events.jsonl'
        with ws_events_path.open('w', encoding='utf-8') as f:
            for item in ws_events:
                f.write(json.dumps(self._jsonable(item), ensure_ascii=False) + '\n')

        if audio_pcm_path:
            src = Path(audio_pcm_path)
            if src.exists():
                dst = record_dir / 'audio.pcm'
                if src.resolve() != dst.resolve():
                    shutil.copy2(src, dst)

        logger.warning(f"失败任务已保存: task_id={task_id}, dir={record_dir}")
        return task_id

    def mark_retrying(self, task_ref: str) -> None:
        self._update_meta(task_ref, {'status': 'retrying', 'retry_started_at': time.time()})

    def mark_replay_success(self, task_ref: str, delete_audio: bool = True) -> None:
        self._update_meta(task_ref, {'status': 'replayed_success', 'replayed_success_at': time.time()})
        if delete_audio:
            audio_path = self.find_audio_path(task_ref)
            if audio_path and audio_path.exists():
                try:
                    audio_path.unlink()
                except Exception as e:
                    logger.warning(f"删除失败音频失败: {audio_path}, {e}")

    def mark_replay_failed(self, task_ref: str, error_code: str, error_message: str) -> None:
        self._update_meta(task_ref, {
            'status': 'replayed_failed',
            'last_retry_error_code': error_code,
            'last_retry_error_message': error_message,
            'last_retry_failed_at': time.time(),
        })

    def list_records(self) -> list[dict]:
        """
        列出所有失败任务记录（按创建时间倒序）。
        """
        items: list[dict] = []
        for meta_path in self.base_dir.glob('*/*/meta.json'):
            try:
                data = json.loads(meta_path.read_text(encoding='utf-8'))
                data['_record_dir'] = str(meta_path.parent)
                items.append(data)
            except Exception as e:
                logger.warning(f"读取失败任务元数据失败: {meta_path}, {e}")
        items.sort(key=lambda x: float(x.get('created_at') or 0), reverse=True)
        return items

    def find_record_dir(self, task_ref: str) -> Optional[Path]:
        matches = list(self.base_dir.glob(f'*/{task_ref}'))
        return matches[0] if matches else None

    def find_audio_path(self, task_ref: str) -> Optional[Path]:
        record_dir = self.find_record_dir(task_ref)
        if not record_dir:
            return None
        audio = record_dir / 'audio.pcm'
        return audio if audio.exists() else None

    def load_meta(self, task_ref: str) -> Optional[dict]:
        record_dir = self.find_record_dir(task_ref)
        if not record_dir:
            return None
        meta_path = record_dir / 'meta.json'
        if not meta_path.exists():
            return None
        return json.loads(meta_path.read_text(encoding='utf-8'))

    def _record_dir(self, task_id: str, time_start: float) -> Path:
        date_key = time.strftime('%Y%m%d', time.localtime(time_start or time.time()))
        return self.base_dir / date_key / task_id

    def _update_meta(self, task_ref: str, updates: Dict[str, Any]) -> None:
        record_dir = self.find_record_dir(task_ref)
        if not record_dir:
            return
        meta_path = record_dir / 'meta.json'
        if not meta_path.exists():
            return
        try:
            data = json.loads(meta_path.read_text(encoding='utf-8'))
        except Exception:
            data = {}
        data.update(updates)
        self._write_json(meta_path, data)

    @staticmethod
    def _write_json(path: Path, data: Any) -> None:
        path.write_text(
            json.dumps(FailedTaskStore._jsonable(data), ensure_ascii=False, indent=2),
            encoding='utf-8'
        )

    @staticmethod
    def _jsonable(data: Any) -> Any:
        if is_dataclass(data):
            return {k: FailedTaskStore._jsonable(v) for k, v in asdict(data).items()}
        if isinstance(data, dict):
            return {str(k): FailedTaskStore._jsonable(v) for k, v in data.items()}
        if isinstance(data, (list, tuple)):
            return [FailedTaskStore._jsonable(x) for x in data]
        if isinstance(data, Path):
            return str(data)
        return data

