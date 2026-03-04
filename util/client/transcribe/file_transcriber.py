# coding: utf-8
"""
文件转录模块（独立 REST 通道版本）。

核心目标：
1. 文件转写完全脱离本地实时识别服务端，不再占用实时链路资源。
2. 采用百炼录音文件 REST 异步接口：提交任务 -> 轮询状态 -> 获取结果。
3. 保持原有落盘体验（txt/json/srt/merge），尽量减少用户使用差异。
"""

from __future__ import annotations

import asyncio
import json
import re
import tempfile
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from config import ClientConfig as Config
from util.client.state import console
from util.client.transcribe.dashscope_rest_client import DashScopeAsrRestClient
from util.client.transcribe.file_upload_resolver import FileUploadResolver
from util.tools import srt_from_txt
from util.logger import get_logger

if TYPE_CHECKING:
    from util.client.state import ClientState

logger = get_logger('client')


class FileTranscriber:
    """
    文件转录器（独立通道）。

    处理流程：
    1. （可选）把本地视频/音频预处理成 16k 单声道 wav
    2. 上传文件到可访问 URL（临时签名上传或自定义上传 API）
    3. 提交百炼 REST 异步任务
    4. 轮询任务完成并解析结果
    5. 保存 txt/json/srt/merge 文件
    """

    def __init__(self, state: 'ClientState', file: Path):
        self.state = state
        self.file = file
        self.task_id: Optional[str] = None
        self._prepared_file: Optional[Path] = None
        self._need_cleanup_prepared_file = False
        self._rest_client: Optional[DashScopeAsrRestClient] = None
        self._submit_time: float = 0.0

    async def check(self) -> bool:
        """
        检查转写前置条件。
        """
        if not self.file.exists():
            console.print(f'文件不存在：{self.file}')
            logger.error(f"文件不存在: {self.file}")
            return False

        if Config.file_transcribe_backend != 'aliyun_rest':
            console.print(f"当前 file_transcribe_backend={Config.file_transcribe_backend}，暂不支持。")
            logger.error(f"不支持的文件转写后端: {Config.file_transcribe_backend}")
            return False

        if not Config.file_rest_api_key:
            console.print('\n[bold red]错误：未配置 DASHSCOPE_API_KEY[/bold red]')
            console.print('    请配置环境变量 DASHSCOPE_API_KEY，或在 config.py 里设置 file_rest_api_key。')
            logger.error("未配置 file_rest_api_key")
            return False

        if Config.file_prepare_audio_with_ffmpeg:
            import shutil
            ffmpeg_path = shutil.which('ffmpeg')
            if ffmpeg_path is None:
                console.print('\n[bold red]错误：未检测到 FFmpeg 环境[/bold red]')
                console.print('    该模式下会先把文件转成 wav 再上传，请安装 FFmpeg。')
                logger.error("未检测到 FFmpeg，无法执行文件预处理")
                return False

        # 提前检查上传模式配置，避免运行到中途才报错。
        upload_mode = (Config.file_upload_mode or 'none').lower()
        supported_upload_modes = {'dashscope_temp_oss', 'presigned_put', 'custom_api', 'none'}
        if upload_mode not in supported_upload_modes:
            console.print(f'\n[bold red]错误：不支持的 file_upload_mode={upload_mode}[/bold red]')
            console.print(
                "    支持的模式：dashscope_temp_oss / presigned_put / custom_api / none"
            )
            logger.error(f"不支持的 file_upload_mode: {upload_mode}")
            return False
        if upload_mode == 'presigned_put' and not Config.file_upload_presign_api:
            console.print('\n[bold red]错误：file_upload_mode=presigned_put 但未配置 file_upload_presign_api[/bold red]')
            logger.error("缺少 file_upload_presign_api")
            return False
        if upload_mode == 'custom_api' and not Config.file_upload_api:
            console.print('\n[bold red]错误：file_upload_mode=custom_api 但未配置 file_upload_api[/bold red]')
            logger.error("缺少 file_upload_api")
            return False
        if upload_mode == 'none':
            console.print('\n[bold red]错误：file_upload_mode=none，未配置本地文件上传通道[/bold red]')
            console.print('    请在 config.py 中设置 dashscope_temp_oss / presigned_put / custom_api。')
            logger.error("file_upload_mode=none")
            return False

        return True

    async def _prepare_audio_file(self) -> Path:
        """
        准备待上传文件：
        - 默认使用 ffmpeg 转成 16k 单声道 wav，兼容视频输入与音频格式差异。
        - 若关闭该选项，则直接上传原文件。
        """
        if not Config.file_prepare_audio_with_ffmpeg:
            return self.file

        temp_file = Path(tempfile.mktemp(prefix='cw_rest_', suffix='.wav'))
        ffmpeg_cmd = [
            "ffmpeg", "-y",
            "-i", str(self.file),
            "-vn",              # 忽略视频流，只保留音频
            "-ac", "1",         # 单声道
            "-ar", "16000",     # 16k 采样率
            str(temp_file),
        ]

        logger.info(f"开始预处理音频: {self.file} -> {temp_file}")
        process = await asyncio.create_subprocess_exec(
            *ffmpeg_cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await process.wait()
        if process.returncode != 0:
            raise RuntimeError(f"ffmpeg 预处理失败，退出码={process.returncode}")

        self._prepared_file = temp_file
        self._need_cleanup_prepared_file = True
        return temp_file

    def _build_upload_resolver(self) -> FileUploadResolver:
        return FileUploadResolver(
            mode=Config.file_upload_mode,
            api_key=Config.file_rest_api_key,
            dashscope_policy_url=Config.file_upload_dashscope_policy_url,
            dashscope_model=Config.file_upload_dashscope_model,
            dashscope_policy_timeout=Config.file_upload_dashscope_policy_timeout,
            dashscope_form_timeout=Config.file_upload_dashscope_form_timeout,
            presign_api=Config.file_upload_presign_api,
            presign_timeout=Config.file_upload_presign_timeout,
            put_timeout=Config.file_upload_put_timeout,
            upload_api=Config.file_upload_api,
            upload_timeout=Config.file_upload_timeout,
            upload_result_key=Config.file_upload_result_key,
        )

    def _build_rest_client(self) -> DashScopeAsrRestClient:
        return DashScopeAsrRestClient(
            api_key=Config.file_rest_api_key,
            submit_url=Config.file_rest_submit_url,
            task_url_template=Config.file_rest_task_url_template,
            model=Config.file_rest_model,
            poll_interval=Config.file_rest_poll_interval,
            poll_timeout=Config.file_rest_poll_timeout,
            channel_id=Config.file_rest_channel_id,
            vocabulary_id=Config.file_rest_vocabulary_id,
        )

    async def send(self) -> None:
        """
        发送阶段（独立通道版本）：
        - 准备本地文件
        - 上传得到 URL
        - 提交 REST 异步任务
        """
        self.task_id = str(uuid.uuid1())
        console.print(f'\n任务标识：{self.task_id}')
        console.print(f'    处理文件：{self.file}')

        prepared_file = await self._prepare_audio_file()
        if prepared_file != self.file:
            console.print(f'    预处理文件：{prepared_file.name}')

        resolver = self._build_upload_resolver()
        file_url = await resolver.resolve(prepared_file)
        console.print('    上传完成，已获取可访问 URL')
        logger.info(f"文件上传完成: {prepared_file} -> {file_url}")

        self._rest_client = self._build_rest_client()
        self._submit_time = time.time()
        self.task_id = await self._rest_client.submit_task(file_url)
        console.print(f'    云端任务ID：{self.task_id}')
        logger.info(f"文件转写任务已提交: task_id={self.task_id}")

    async def receive(self) -> None:
        """
        接收阶段（独立通道版本）：
        - 轮询任务直到完成
        - 解析并落盘
        """
        if not self.task_id or not self._rest_client:
            raise RuntimeError("请先调用 send() 提交任务，再调用 receive()")

        try:
            response = await self._rest_client.wait_for_result(self.task_id)
            result = self._rest_client.parse_result(response)
            self._save_outputs(
                text_display=result.text_display,
                text_accu=result.text_accu,
                tokens=result.tokens,
                timestamps=result.timestamps,
            )

            process_duration = time.time() - self._submit_time
            console.print(f'    处理耗时：{process_duration:.2f}s')
            console.print(f'    识别结果：\n[green]{result.text_display}')
            logger.info(
                f"文件转写完成: {self.file}, task_id={self.task_id}, "
                f"耗时={process_duration:.2f}s, 文本长度={len(result.text_display)}"
            )
        finally:
            await self._cleanup_temp_files()

    def _save_outputs(
        self,
        text_display: str,
        text_accu: str,
        tokens: list[str],
        timestamps: list[float],
    ) -> None:
        """
        保存转写结果文件，兼容原有产物格式。
        """
        text_split = re.sub('[，。？]', '\n', text_accu)
        json_filename = self.file.with_suffix('.json')
        txt_filename = self.file.with_suffix('.txt')
        merge_filename = self.file.with_suffix('.merge.txt')

        if Config.file_save_merge:
            with open(merge_filename, 'w', encoding='utf-8') as f:
                f.write(text_accu)
            logger.debug(f"保存合并文本: {merge_filename}")

        if Config.file_save_txt or Config.file_save_srt:
            with open(txt_filename, 'w', encoding='utf-8') as f:
                f.write(text_split)
            logger.debug(f"保存切分文本: {txt_filename}")

        if Config.file_save_json:
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump({'timestamps': timestamps, 'tokens': tokens}, f, ensure_ascii=False)
            logger.debug(f"保存 JSON 结果: {json_filename}")

        if Config.file_save_srt:
            srt_from_txt.one_task(txt_filename)

        if not Config.file_save_txt and txt_filename.exists():
            try:
                txt_filename.unlink()
                logger.debug(f"清理中间 TXT 文件: {txt_filename}")
            except Exception as e:
                logger.warning(f"清理中间 TXT 文件失败: {e}")

    async def _cleanup_temp_files(self) -> None:
        """
        清理预处理产生的临时文件。
        """
        if not self._need_cleanup_prepared_file or not self._prepared_file:
            return
        try:
            if self._prepared_file.exists():
                self._prepared_file.unlink()
                logger.debug(f"已清理临时文件: {self._prepared_file}")
        except Exception as e:
            logger.warning(f"清理临时文件失败: {e}")
        finally:
            self._prepared_file = None
            self._need_cleanup_prepared_file = False
