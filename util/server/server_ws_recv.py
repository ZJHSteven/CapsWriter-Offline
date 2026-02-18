# coding: utf-8
"""
WebSocket 接收处理模块

处理客户端发送的音频数据，进行分段和缓冲，提交到识别队列。
"""

import json
import time
from base64 import b64decode

import websockets

from config import ServerConfig as Config
from util.server.server_cosmic import console, Cosmic
from util.server.server_classes import Task
from util.constants import AudioFormat
from util.tools.my_status import Status
from util.logger import get_logger

# 获取日志记录器
logger = get_logger('server')

# 麦克风接收状态指示器
status_mic = Status('正在接收音频', spinner='point')

# 百炼实时链路的传输层分帧（100ms），仅用于网络发送节奏，不是“语义分句”。
ALIYUN_STREAM_FRAME_SECONDS = 0.1
ALIYUN_STREAM_FRAME_BYTES = AudioFormat.seconds_to_bytes(ALIYUN_STREAM_FRAME_SECONDS)


class AudioCache:
    """
    音频缓冲区
    
    用于缓存接收到的音频数据，直到达到分段阈值后提交处理。
    """
    def __init__(self):
        self.chunks: bytes = b''    # 音频数据缓冲
        self.offset: float = 0.0    # 当前偏移时间（秒）
        self.byte_count: int = 0    # 累计接收字节数
    
    @property
    def duration(self) -> float:
        """缓冲区音频时长（秒）"""
        return AudioFormat.bytes_to_seconds(len(self.chunks))
    
    @property
    def total_duration(self) -> float:
        """累计接收的音频总时长（秒）"""
        return AudioFormat.bytes_to_seconds(self.byte_count)
    
    def reset(self) -> None:
        """重置缓冲区"""
        self.chunks = b''
        self.offset = 0.0
        self.byte_count = 0


def _submit_task(
    queue_in,
    source: str,
    data: bytes,
    offset: float,
    task_id: str,
    socket_id: str,
    is_final: bool,
    time_start: float,
) -> None:
    """
    统一封装任务入队逻辑，避免重复构造 Task。

    说明：
    - aliyun 实时模式下不依赖 overlap/offset 做文本拼接，因此 overlap 固定为 0。
    - 本地模型模式仍可使用 offset（日志与时长统计）。
    """
    task = Task(
        source=source,
        data=data,
        offset=offset,
        task_id=task_id,
        socket_id=socket_id,
        overlap=0.0,
        is_final=is_final,
        time_start=time_start,
        time_submit=time.time(),
    )
    queue_in.put(task)


def _handle_aliyun_stream_message(
    queue_in,
    source: str,
    is_final: bool,
    task_id: str,
    socket_id: str,
    time_start: float,
    data: bytes,
    cache: AudioCache,
) -> None:
    """
    百炼实时链路（会话模式）专用处理：
    1. 只做“传输层 100ms 分帧”后入队，不做 60 秒工程切段。
    2. final 消息只作为会话结束信号，触发云端 finish-task。
    """
    global status_mic

    cache.chunks += data
    cache.byte_count += len(data)

    if not is_final:
        if source == 'mic':
            status_mic.start()

        while len(cache.chunks) >= ALIYUN_STREAM_FRAME_BYTES:
            segment_data = cache.chunks[:ALIYUN_STREAM_FRAME_BYTES]
            cache.chunks = cache.chunks[ALIYUN_STREAM_FRAME_BYTES:]
            _submit_task(
                queue_in=queue_in,
                source=source,
                data=segment_data,
                offset=cache.offset,
                task_id=task_id,
                socket_id=socket_id,
                is_final=False,
                time_start=time_start,
            )
            cache.offset += ALIYUN_STREAM_FRAME_SECONDS
        return

    # is_final=True：先把残留数据送完，再发送一个空数据 final 控制任务。
    if source == 'mic':
        status_mic.stop()

    if cache.chunks:
        remain_data = cache.chunks
        _submit_task(
            queue_in=queue_in,
            source=source,
            data=remain_data,
            offset=cache.offset,
            task_id=task_id,
            socket_id=socket_id,
            is_final=False,
            time_start=time_start,
        )
        cache.offset += AudioFormat.bytes_to_seconds(len(remain_data))

    _submit_task(
        queue_in=queue_in,
        source=source,
        data=b'',
        offset=cache.offset,
        task_id=task_id,
        socket_id=socket_id,
        is_final=True,
        time_start=time_start,
    )

    logger.debug(
        f"提交百炼实时会话结束信号，任务ID: {task_id}, 累计时长: {cache.total_duration:.2f}s"
    )
    cache.reset()


async def message_handler(websocket, message: dict, cache: AudioCache) -> None:
    """
    处理客户端发送的音频消息
    
    根据消息中的分段参数，将音频数据分段后提交到识别队列。
    """
    global status_mic
    source = message['source']
    is_final = message['is_final']
    is_start = not bool(cache.chunks)
    if source == 'mic':
        queue_in = Cosmic.queue_in_mic
    elif source == 'file':
        queue_in = Cosmic.queue_in_file
    else:
        logger.warning(f"未知音频来源 source={source}，按 file 队列处理")
        queue_in = Cosmic.queue_in_file

    # 获取 id
    task_id = message['task_id']
    socket_id = str(websocket.id)

    try:
        # base64 解码音频数据（float32, 16kHz, mono）
        data = b64decode(message['data'])

        # 云端实时模式：不走本地 60 秒工程分段，改为会话流式入队。
        if Config.model_type.lower() == 'aliyun_realtime' and source == 'mic':
            _handle_aliyun_stream_message(
                queue_in=queue_in,
                source=source,
                is_final=is_final,
                task_id=task_id,
                socket_id=socket_id,
                time_start=message['time_start'],
                data=data,
                cache=cache,
            )
            return

        # 其余模式保留旧逻辑（本地模型 / 旧文件链路）
        seg_duration = message['seg_duration']
        seg_overlap = message['seg_overlap']
        seg_threshold = seg_duration + seg_overlap * 2

        cache.chunks += data
        cache.byte_count += len(data)

        if not is_final:
            # 打印状态消息
            if source == 'mic':
                status_mic.start()
            if source == 'file' and is_start:
                console.print('正在接收音频文件...')
                logger.info(f"开始接收音频文件，任务ID: {task_id}")

            # 若缓冲已达到分段阈值，将片段作为任务提交
            segment_bytes = AudioFormat.seconds_to_bytes(seg_duration + seg_overlap)
            stride_bytes = AudioFormat.seconds_to_bytes(seg_duration)
            
            while cache.duration >= seg_threshold:
                segment_data = cache.chunks[:segment_bytes]
                cache.chunks = cache.chunks[stride_bytes:]
                
                task = Task(
                    source=source,
                    data=segment_data,
                    offset=cache.offset,
                    task_id=task_id,
                    socket_id=socket_id,
                    overlap=seg_overlap,
                    is_final=False,
                    time_start=message['time_start'],
                    time_submit=time.time()
                )
                cache.offset += seg_duration
                queue_in.put(task)
                logger.debug(
                    f"提交音频片段，任务ID: {task_id}, "
                    f"偏移: {cache.offset}s, 缓冲区: {len(cache.chunks)} bytes"
                )

        else:  # is_final
            # 打印状态消息
            if source == 'mic':
                status_mic.stop()
            elif source == 'file':
                print(f'音频文件接收完毕，时长 {cache.total_duration:.2f}s')
                logger.info(f"音频文件接收完毕，任务ID: {task_id}, 时长: {cache.total_duration:.2f}s")

            # 提交最终片段
            task = Task(
                source=source,
                data=cache.chunks,
                offset=cache.offset,
                task_id=task_id,
                socket_id=socket_id,
                overlap=seg_overlap,
                is_final=True,
                time_start=message['time_start'],
                time_submit=time.time()
            )
            queue_in.put(task)
            logger.debug(f"提交最终片段，任务ID: {task_id}, 数据大小: {len(cache.chunks)} bytes")

            # 重置缓冲区
            cache.reset()

    except Exception as e:
        logger.error(f"音频数据处理错误，任务ID: {task_id}: {e}", exc_info=True)
        raise


async def ws_recv(websocket) -> None:
    """
    WebSocket 接收主函数
    
    处理单个客户端连接，接收音频数据并分发处理。
    """
    global status_mic

    # 登记 socket 到连接池
    sockets = Cosmic.sockets
    sockets_id = Cosmic.sockets_id
    socket_id = str(websocket.id)
    sockets[socket_id] = websocket
    sockets_id.append(socket_id)
    console.print(f'接客了：{websocket}\n', style='yellow')
    logger.info(f"新客户端连接: {websocket}, ID: {socket_id}")

    # 创建音频缓冲区
    cache = AudioCache()

    # 接收并处理消息
    try:
        async for message in websocket:
            # 解析 JSON 消息
            message = json.loads(message)
            # 处理音频数据
            await message_handler(websocket, message, cache)

        console.print("ConnectionClosed...")
        logger.info(f"客户端正常关闭连接: {socket_id}")
        
    except websockets.ConnectionClosed:
        console.print("ConnectionClosed...")
        logger.warning(f"客户端连接已关闭: {socket_id}")
    except websockets.InvalidState:
        console.print("InvalidState...")
        logger.error(f"WebSocket 状态异常: {socket_id}")
    except Exception as e:
        console.print("Exception:", e)
        logger.error(f"WebSocket 接收异常，客户端ID {socket_id}: {e}", exc_info=True)
    finally:
        # 清理资源
        status_mic.stop()
        status_mic.on = False
        sockets.pop(socket_id, None)
        if socket_id in sockets_id:
            sockets_id.remove(socket_id)
        
        # 清理识别结果缓存，防止内存泄漏
        from util.server.server_recognize import clear_results_by_socket_id
        clear_results_by_socket_id(socket_id)
        
        logger.debug(f"客户端资源已清理: {socket_id}")
