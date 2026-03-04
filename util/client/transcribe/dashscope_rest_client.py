# coding: utf-8
"""
百炼录音文件 REST 异步转写客户端。

设计目标：
1. 把“提交任务 + 轮询任务 + 解析结果”封装成独立模块，避免与主流程耦合。
2. 提供对上层稳定的输出结构：text / text_accu / tokens / timestamps。
3. 当接口字段缺失时，提供可预测的兜底行为，避免直接崩溃。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests

from util.logger import get_logger

logger = get_logger('client')


@dataclass
class TranscribeResult:
    """
    统一的转写结果结构，供 FileTranscriber 直接消费。
    """
    text_display: str
    text_accu: str
    tokens: List[str]
    timestamps: List[float]
    duration_seconds: float


class DashScopeAsrRestClient:
    """
    百炼录音文件识别 REST 客户端（异步任务版）。

    关键流程：
    1. 提交任务（POST /services/audio/asr/transcription, X-DashScope-Async=enable）
    2. 轮询状态（GET /tasks/{task_id}）
    3. SUCCEEDED 后解析 output.results
    """

    def __init__(
        self,
        api_key: str,
        submit_url: str,
        task_url_template: str,
        model: str,
        poll_interval: float,
        poll_timeout: float,
        channel_id: Optional[List[int]] = None,
        vocabulary_id: str = '',
    ) -> None:
        if not api_key:
            raise ValueError("file_rest_api_key 为空，请先配置 DASHSCOPE_API_KEY 或 config.py")

        self.api_key = api_key
        self.submit_url = submit_url
        self.task_url_template = task_url_template
        self.model = model
        self.poll_interval = max(0.2, float(poll_interval))
        self.poll_timeout = max(5.0, float(poll_timeout))
        self.channel_id = channel_id or [0]
        self.vocabulary_id = vocabulary_id

    def _build_submit_headers(self, file_url: str) -> Dict[str, str]:
        """
        构建“提交转写任务”请求头。

        关键点：
        - 常规 URL（http/https）只需异步头 `X-DashScope-Async`；
        - 临时 OSS URL（`oss://`）必须额外添加
          `X-DashScope-OssResourceResolve: enable`，
          否则百炼服务端不会去解析 `oss://` 资源。
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "X-DashScope-Async": "enable",
        }
        if self._is_oss_url(file_url):
            headers["X-DashScope-OssResourceResolve"] = "enable"
        return headers

    async def submit_task(self, file_url: str) -> str:
        """
        提交录音文件转写任务，返回 task_id。
        """
        payload: Dict = {
            "model": self.model,
            "input": {"file_urls": [file_url]},
            "parameters": {
                "channel_id": self.channel_id,
            },
        }
        if self.vocabulary_id:
            payload["parameters"]["vocabulary_id"] = self.vocabulary_id

        logger.info(f"提交百炼文件转写任务: model={self.model}, file_url={file_url}")
        response = await asyncio.to_thread(
            requests.post,
            self.submit_url,
            headers=self._build_submit_headers(file_url),
            json=payload,
            timeout=60,
        )
        self._raise_for_http_error(response, "提交转写任务失败")

        data = response.json()
        task_id = (
            data.get("output", {}).get("task_id")
            or data.get("data", {}).get("task_id")
        )
        if not task_id:
            raise RuntimeError(f"提交任务成功但未返回 task_id: {data}")

        return task_id

    async def wait_for_result(self, task_id: str) -> Dict:
        """
        轮询异步任务，直到 SUCCEEDED / FAILED / 超时。
        """
        task_url = self.task_url_template.format(task_id=task_id)
        logger.info(f"开始轮询转写任务: task_id={task_id}")
        start = time.time()

        while True:
            if time.time() - start > self.poll_timeout:
                raise TimeoutError(f"转写任务超时（>{self.poll_timeout}s），task_id={task_id}")

            response = await asyncio.to_thread(
                requests.get,
                task_url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=30,
            )
            self._raise_for_http_error(response, "查询转写任务失败")

            data = response.json()
            output = data.get("output", {})
            status = output.get("task_status", "")
            logger.debug(f"任务状态: task_id={task_id}, status={status}")

            if status == "SUCCEEDED":
                return data
            if status in ("FAILED", "CANCELED"):
                code = output.get("code", "")
                message = output.get("message", "")
                raise RuntimeError(f"转写任务失败: task_id={task_id}, code={code}, message={message}")

            await asyncio.sleep(self.poll_interval)

    def parse_result(self, task_output: Dict) -> TranscribeResult:
        """
        将百炼任务结果转换为项目内统一格式。
        """
        output = task_output.get("output", {})
        results = output.get("results", [])
        if not results:
            raise RuntimeError(f"任务结果缺少 output.results: {task_output}")

        first = results[0]
        transcripts = self._extract_transcripts(first)
        if not transcripts:
            raise RuntimeError(f"任务结果缺少 transcripts: {task_output}")

        # 目前默认取第一个 transcript（通常对应 channel_id=0）
        transcript = transcripts[0]
        text_display = transcript.get("text", "").strip()
        sentences = transcript.get("sentences", [])

        text_accu = "".join(
            sentence.get("text", "")
            for sentence in sentences
            if isinstance(sentence, dict)
        ).strip()
        if not text_accu:
            text_accu = text_display

        tokens: List[str] = []
        timestamps: List[float] = []
        for sentence in sentences:
            words = sentence.get("words", []) if isinstance(sentence, dict) else []
            for word in words:
                if not isinstance(word, dict):
                    continue
                base_text = str(word.get("text", "")).strip()
                punctuation = str(word.get("punctuation", ""))
                token = f"{base_text}{punctuation}".strip()
                if not token:
                    continue

                begin_ms = word.get("begin_time")
                if begin_ms is None:
                    continue
                try:
                    begin = float(begin_ms) / 1000.0
                except (TypeError, ValueError):
                    continue

                tokens.append(token)
                timestamps.append(begin)

        duration_ms = transcript.get("content_duration_in_milliseconds") or 0
        try:
            duration_seconds = float(duration_ms) / 1000.0
        except (TypeError, ValueError):
            duration_seconds = 0.0

        # 兜底：若 words 缺失，仍保证后续落盘流程可继续。
        if not tokens and text_accu:
            tokens = [ch for ch in text_accu.replace(" ", "")]
            if duration_seconds > 0 and tokens:
                unit = duration_seconds / len(tokens)
                timestamps = [i * unit for i in range(len(tokens))]
            else:
                timestamps = [0.0 for _ in tokens]

        return TranscribeResult(
            text_display=text_display,
            text_accu=text_accu,
            tokens=tokens,
            timestamps=timestamps,
            duration_seconds=duration_seconds,
        )

    def _extract_transcripts(self, result_item: Dict) -> List[Dict]:
        """
        从任务结果中提取 transcripts。

        兼容两种返回形态：
        1. 直接内联 `results[].transcripts`；
        2. 只返回 `results[].transcription_url`，需要二次下载结果 JSON。
        """
        inline_transcripts = result_item.get("transcripts", [])
        if isinstance(inline_transcripts, list) and inline_transcripts:
            return [item for item in inline_transcripts if isinstance(item, dict)]

        transcription_url = str(result_item.get("transcription_url") or "").strip()
        if not transcription_url:
            return []

        logger.info("检测到 transcription_url，开始下载转写明细")
        resp = requests.get(transcription_url, timeout=60)
        self._raise_for_http_error(resp, "下载 transcription_url 失败")

        payload = resp.json()
        payload_data = payload.get("data", payload) if isinstance(payload, dict) else {}
        if not isinstance(payload_data, dict):
            return []

        url_transcripts = payload_data.get("transcripts", [])
        if not isinstance(url_transcripts, list):
            return []
        return [item for item in url_transcripts if isinstance(item, dict)]

    @staticmethod
    def _raise_for_http_error(response: requests.Response, prefix: str) -> None:
        if response.status_code == 200:
            return
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise RuntimeError(f"{prefix}，HTTP={response.status_code}，响应={detail}")

    @staticmethod
    def _is_oss_url(file_url: str) -> bool:
        """
        判断是否为百炼临时 OSS 地址。
        """
        return str(file_url or "").strip().lower().startswith("oss://")
