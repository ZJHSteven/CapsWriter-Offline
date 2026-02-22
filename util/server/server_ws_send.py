import json
import base64
import asyncio
from multiprocessing import Queue

from util.server.server_cosmic import console, Cosmic
from util.server.server_classes import Result
from util.tools.asyncio_to_thread import to_thread
from util.logger import get_logger
from rich import inspect

# 获取日志记录器
logger = get_logger('server')


async def ws_send():

    queue_out = Cosmic.queue_out
    sockets = Cosmic.sockets

    logger.info("WebSocket 发送任务已启动")

    while True:
        try:
            # 获取识别结果（从多进程队列）
            result: Result = await to_thread(queue_out.get)

            # 得到退出的通知
            if result is None:
                logger.info("收到退出通知，停止发送任务")
                return

            # 构建消息
            message = {
                'task_id': result.task_id,
                'duration': result.duration,
                'time_start': result.time_start,
                'time_submit': result.time_submit,
                'time_complete': result.time_complete,
                'text': result.text,               # 主要输出（简单拼接）
                'text_accu': result.text_accu,     # 精确输出（时间戳拼接）
                'tokens': result.tokens,
                'timestamps': result.timestamps,
                'is_final': result.is_final,
                # 下面这些字段用于“失败保底 / 手动重试”链路，旧客户端忽略也不会报错
                'status': getattr(result, 'status', 'success_confirmed'),
                'error_code': getattr(result, 'error_code', ''),
                'error_message': getattr(result, 'error_message', ''),
                'salvage_text_finalized': getattr(result, 'salvage_text_finalized', ''),
                'salvage_text_partial': getattr(result, 'salvage_text_partial', ''),
                'needs_manual_retry': bool(getattr(result, 'needs_manual_retry', False)),
                'retry_task_ref': getattr(result, 'retry_task_ref', ''),
            }

            # 获得 socket
            websocket = next(
                (ws for ws in sockets.values() if str(ws.id) == result.socket_id),
                None,
            )

            if not websocket:
                logger.warning(f"客户端 {result.socket_id} 不存在，跳过发送结果，任务ID: {result.task_id}")
                continue

            # 发送消息
            await websocket.send(json.dumps(message))
            logger.debug(f"发送识别结果，任务ID: {result.task_id}, 文本长度: {len(result.text)}")

            if result.source == 'mic':
                status = getattr(result, 'status', 'success_confirmed')
                if status == 'success_confirmed':
                    console.print(f'识别结果：\n    [green]{result.text}')
                    logger.info(f"麦克风识别结果: {result.text}")
                else:
                    failure_status = getattr(result, 'status', 'unknown')
                    failure_error = getattr(result, 'error_code', '')
                    # 失败保底结果不自动上屏，但在服务端终端明确打印出来，方便人工复制。
                    console.print(
                        f'    [yellow]失败保底状态：{failure_status} error={failure_error}'
                    )
                    if result.salvage_text_finalized:
                        console.print(f'    [cyan]已定稿保底：{result.salvage_text_finalized}')
                    if result.salvage_text_partial:
                        console.print(f'    [yellow]未定稿保底：{result.salvage_text_partial}')
                    if result.retry_task_ref:
                        console.print(f'    [yellow]失败任务引用：{result.retry_task_ref}')
            elif result.source == 'file':
                console.print(f'    转录进度：{result.duration:.2f}s', end='\r')
                logger.debug(f"文件转录进度: {result.duration:.2f}s")
                if result.is_final:
                    console.print('\n    [green]转录完成')
                    logger.info(f"文件转录完成，任务ID: {result.task_id}, 总时长: {result.duration:.2f}s")

        except Exception as e:
            logger.error(f"发送结果时发生错误: {e}", exc_info=True)
            print(e)


