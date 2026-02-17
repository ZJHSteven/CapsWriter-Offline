# coding: utf-8
"""
本地文件 -> 可访问 URL 解析模块。

背景：
百炼录音文件 REST 接口只接受 file_urls，因此本地文件需要先上传到可访问地址。

支持三种模式：
1) none:          不上传，直接报错提示配置。
2) presigned_put: 调用临时签名 API 获取 upload_url + file_url，再 PUT 上传。
3) custom_api:    调用自定义上传接口（multipart/form-data），接口返回公网 URL。
"""

from __future__ import annotations

import asyncio
import mimetypes
from pathlib import Path
from typing import Dict, Optional

import requests

from util.logger import get_logger

logger = get_logger('client')


class FileUploadResolver:
    """
    文件 URL 解析器：负责把本地文件上传并返回可访问 URL。
    """

    def __init__(
        self,
        mode: str,
        presign_api: str,
        presign_timeout: float,
        put_timeout: float,
        upload_api: str,
        upload_timeout: float,
        upload_result_key: str,
    ) -> None:
        self.mode = (mode or 'none').lower()
        self.presign_api = presign_api
        self.presign_timeout = float(presign_timeout)
        self.put_timeout = float(put_timeout)
        self.upload_api = upload_api
        self.upload_timeout = float(upload_timeout)
        self.upload_result_key = upload_result_key or 'url'

    async def resolve(self, file_path: Path) -> str:
        """
        根据配置模式，将本地文件转成公网可访问 URL。
        """
        if self.mode == 'none':
            raise RuntimeError(
                "file_upload_mode=none，当前未配置上传通道。"
                "请配置 presigned_put 或 custom_api。"
            )
        if self.mode == 'presigned_put':
            return await self._resolve_by_presigned_put(file_path)
        if self.mode == 'custom_api':
            return await self._resolve_by_custom_api(file_path)
        raise RuntimeError(f"不支持的 file_upload_mode: {self.mode}")

    async def _resolve_by_presigned_put(self, file_path: Path) -> str:
        """
        临时签名上传流程：
        1. 向 presign_api 请求临时上传地址
        2. PUT 文件到 upload_url
        3. 返回 file_url 供 ASR REST 使用
        """
        if not self.presign_api:
            raise RuntimeError("file_upload_mode=presigned_put 但 file_upload_presign_api 未配置")

        content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        request_payload = {
            "filename": file_path.name,
            "content_type": content_type,
            "size": file_path.stat().st_size,
        }

        logger.info(f"请求临时签名上传地址: {self.presign_api}")
        presign_resp = await asyncio.to_thread(
            requests.post,
            self.presign_api,
            json=request_payload,
            timeout=self.presign_timeout,
        )
        self._raise_for_http_error(presign_resp, "请求临时签名地址失败")

        data = presign_resp.json()
        upload_url = data.get("upload_url")
        file_url = data.get("file_url") or data.get("url")
        upload_headers = data.get("headers", {}) if isinstance(data.get("headers"), dict) else {}

        if not upload_url or not file_url:
            raise RuntimeError(
                f"临时签名接口返回缺少 upload_url/file_url，响应={data}"
            )

        logger.info("开始执行临时签名 PUT 上传")
        with open(file_path, "rb") as fp:
            put_resp = await asyncio.to_thread(
                requests.put,
                upload_url,
                data=fp,
                headers=upload_headers,
                timeout=self.put_timeout,
            )
        # 预签名 PUT 常见成功码：200/201/204
        if put_resp.status_code not in (200, 201, 204):
            raise RuntimeError(
                f"预签名 PUT 上传失败，HTTP={put_resp.status_code}，响应={put_resp.text}"
            )

        return file_url

    async def _resolve_by_custom_api(self, file_path: Path) -> str:
        """
        自定义上传接口流程（multipart/form-data）：
        - 上传成功后读取 JSON 里的 URL 字段。
        """
        if not self.upload_api:
            raise RuntimeError("file_upload_mode=custom_api 但 file_upload_api 未配置")

        content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        logger.info(f"调用自定义上传接口: {self.upload_api}")

        with open(file_path, "rb") as fp:
            files = {
                "file": (file_path.name, fp, content_type)
            }
            resp = await asyncio.to_thread(
                requests.post,
                self.upload_api,
                files=files,
                timeout=self.upload_timeout,
            )

        self._raise_for_http_error(resp, "自定义上传接口调用失败")
        data = resp.json()

        # 兼容常见字段名，优先用用户配置字段。
        file_url = data.get(self.upload_result_key) or data.get("url") or data.get("file_url")
        if not file_url:
            raise RuntimeError(f"上传接口响应中未找到 URL 字段，响应={data}")
        return file_url

    @staticmethod
    def _raise_for_http_error(response: requests.Response, prefix: str) -> None:
        if response.status_code in (200, 201):
            return
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise RuntimeError(f"{prefix}，HTTP={response.status_code}，响应={detail}")
