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
import uuid
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
        api_key: str,
        dashscope_policy_url: str,
        dashscope_model: str,
        dashscope_policy_timeout: float,
        dashscope_form_timeout: float,
        presign_api: str,
        presign_timeout: float,
        put_timeout: float,
        upload_api: str,
        upload_timeout: float,
        upload_result_key: str,
    ) -> None:
        self.mode = (mode or 'none').lower()
        self.api_key = api_key or ''
        self.dashscope_policy_url = (dashscope_policy_url or '').strip()
        self.dashscope_model = (dashscope_model or 'fun-asr').strip()
        self.dashscope_policy_timeout = float(dashscope_policy_timeout)
        self.dashscope_form_timeout = float(dashscope_form_timeout)
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
                "请配置 dashscope_temp_oss / presigned_put / custom_api。"
            )
        if self.mode == 'dashscope_temp_oss':
            return await self._resolve_by_dashscope_temp_oss(file_path)
        if self.mode == 'presigned_put':
            return await self._resolve_by_presigned_put(file_path)
        if self.mode == 'custom_api':
            return await self._resolve_by_custom_api(file_path)
        raise RuntimeError(f"不支持的 file_upload_mode: {self.mode}")

    async def _resolve_by_dashscope_temp_oss(self, file_path: Path) -> str:
        """
        使用阿里百炼官方临时 OSS 上传本地文件，并返回 `oss://` 地址。

        核心流程（与官方文档一致）：
        1. `GET /api/v1/uploads?action=getPolicy&model=...` 获取上传凭证；
        2. 向 `upload_host` 发 multipart/form-data 上传；
        3. 组装 `oss://{key}`，供后续录音文件 REST 接口使用。
        """
        if not self.api_key:
            raise RuntimeError(
                "file_upload_mode=dashscope_temp_oss 但未配置 DASHSCOPE_API_KEY（file_rest_api_key 为空）"
            )

        policy_url = self.dashscope_policy_url or "https://dashscope.aliyuncs.com/api/v1/uploads"
        policy_model = self.dashscope_model or "fun-asr"
        policy_headers = {
            "Authorization": f"Bearer {self.api_key}",
        }

        logger.info("请求百炼临时 OSS 上传凭证: url=%s model=%s", policy_url, policy_model)
        policy_resp = await asyncio.to_thread(
            requests.get,
            policy_url,
            params={"action": "getPolicy", "model": policy_model},
            headers=policy_headers,
            timeout=self.dashscope_policy_timeout,
        )
        self._raise_for_http_error(policy_resp, "获取百炼临时 OSS 上传凭证失败")

        policy_body = policy_resp.json()
        policy_data = policy_body.get("data", policy_body) if isinstance(policy_body, dict) else {}
        if not isinstance(policy_data, dict):
            raise RuntimeError(f"百炼上传凭证响应格式异常，响应={policy_body}")

        # 兼容不同命名风格（下划线/驼峰），避免字段风格变化导致直接崩溃。
        upload_host = str(policy_data.get("upload_host") or policy_data.get("uploadHost") or "").strip()
        upload_dir = str(policy_data.get("upload_dir") or policy_data.get("uploadDir") or "").strip()
        policy = str(policy_data.get("policy") or "").strip()
        signature = str(policy_data.get("signature") or "").strip()
        oss_access_key_id = str(
            policy_data.get("oss_access_key_id")
            or policy_data.get("ossAccessKeyId")
            or policy_data.get("OSSAccessKeyId")
            or ""
        ).strip()
        security_token = str(
            policy_data.get("x_oss_security_token")
            or policy_data.get("x-oss-security-token")
            or policy_data.get("xOssSecurityToken")
            or ""
        ).strip()

        if not upload_host:
            raise RuntimeError(f"百炼上传凭证缺少 upload_host，响应={policy_body}")
        if not upload_dir:
            raise RuntimeError(f"百炼上传凭证缺少 upload_dir，响应={policy_body}")
        if not policy or not signature or not oss_access_key_id or not security_token:
            raise RuntimeError(
                "百炼上传凭证缺少关键字段（policy/signature/oss_access_key_id/x_oss_security_token），"
                f"响应={policy_body}"
            )

        if not upload_host.startswith(("http://", "https://")):
            upload_host = f"https://{upload_host.lstrip('/')}"

        object_key = self._build_dashscope_object_key(upload_dir, file_path.name)
        content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        form_fields = {
            # key 是 OSS 对象路径，最终 `oss://` 也是基于它来组装。
            "key": object_key,
            "policy": policy,
            "OSSAccessKeyId": oss_access_key_id,
            "Signature": signature,
            "x-oss-security-token": security_token,
            # 让上传接口尽量返回 200，便于统一处理响应状态。
            "success_action_status": "200",
        }

        logger.info("开始上传文件到百炼临时 OSS: host=%s key=%s", upload_host, object_key)
        with open(file_path, "rb") as fp:
            files = {"file": (file_path.name, fp, content_type)}
            upload_resp = await asyncio.to_thread(
                requests.post,
                upload_host,
                data=form_fields,
                files=files,
                timeout=self.dashscope_form_timeout,
            )

        if upload_resp.status_code not in (200, 201, 204):
            raise RuntimeError(
                f"百炼临时 OSS 上传失败，HTTP={upload_resp.status_code}，响应={upload_resp.text}"
            )

        oss_url = f"oss://{object_key}"
        logger.info("百炼临时 OSS 上传完成: %s", oss_url)
        return oss_url

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

    @staticmethod
    def _build_dashscope_object_key(upload_dir: str, filename: str) -> str:
        """
        生成上传对象 key。

        设计取舍：
        - 保留官方给的 `upload_dir` 作为前缀，避免权限范围越界；
        - 文件名追加 UUID 前缀，减少并发上传重名冲突；
        - 最终 key 不以 `/` 开头，便于 `oss://{key}` 直接拼接。
        """
        clean_dir = upload_dir.strip().strip("/")
        unique_name = f"{uuid.uuid4().hex}_{filename}"
        if not clean_dir:
            return unique_name
        return f"{clean_dir}/{unique_name}"
