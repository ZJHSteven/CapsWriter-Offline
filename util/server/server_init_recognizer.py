import time
from multiprocessing import Queue
import queue
import signal
import atexit
from platform import system
from config import ServerConfig as Config
from util.model_config import ParaformerArgs, ModelPaths, SenseVoiceArgs, FunASRNanoGGUFArgs
from util.server.server_check_model import check_model
from util.server.server_cosmic import console
from util.server.server_recognize import recognize
from util.server.asr_aliyun_realtime import AliyunRealtimeRecognizer
from util.tools.empty_working_set import empty_current_working_set
from util.logger import get_logger

# 获取日志记录器
logger = get_logger('server')

# 全局变量，用于跟踪资源状态
_resources_initialized = False


def cleanup_recognizer_resources():
    """清理识别器资源"""
    global _resources_initialized

    if not _resources_initialized:
        return

    logger.debug("识别子进程资源清理完成")


def signal_handler(signum, frame):
    """
    识别子进程的信号处理器

    优雅地退出识别进程。
    """
    signal_name = signal.Signals(signum).name
    logger.info(f"识别子进程收到信号 {signal_name} ({signum})，准备退出...")

    # 清理资源
    cleanup_recognizer_resources()

    # 退出进程
    logger.debug("识别子进程退出")
    exit(0)





def init_recognizer(queue_in_mic: Queue, queue_in_file: Queue, queue_out: Queue, sockets_id):
    global _resources_initialized

    logger.info("识别子进程启动")
    logger.debug(f"系统平台: {system()}")

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.debug("识别子进程信号处理器已注册")

    # 注册 atexit 处理器
    atexit.register(cleanup_recognizer_resources)

    model_type = Config.model_type.lower()
    sherpa_onnx = None

    # 云端模式不依赖 sherpa-onnx，本地模式才需要导入
    if model_type in ('fun_asr_nano', 'sensevoice', 'paraformer'):
        with console.status("载入模块中…", spinner="bouncingBall", spinner_style="yellow"):
            import sherpa_onnx as _sherpa_onnx
            sherpa_onnx = _sherpa_onnx
        console.print('[green4]模块加载完成', end='\n\n')
        logger.info("Sherpa-ONNX 模块加载完成")
    else:
        logger.info(f"当前模式 {model_type} 不需要加载 Sherpa-ONNX")

    # 载入语音模型
    console.print('[yellow]语音模型载入中', end='\r'); t1 = time.time()
    logger.info(f"开始加载语音模型，类型: {Config.model_type}")

     # 检查模型文件
    check_model()

    # 根据配置选择模型类型
    try:
        if model_type == 'aliyun_realtime':
            logger.debug("使用阿里云百炼实时 ASR")
            recognizer = AliyunRealtimeRecognizer(
                api_key=Config.aliyun_api_key,
                endpoint=Config.aliyun_endpoint,
                model=Config.aliyun_model,
                source_language=Config.aliyun_source_language,
                max_sentence_silence=Config.aliyun_max_sentence_silence,
                punctuation_enabled=Config.aliyun_enable_punctuation,
                itn_enabled=Config.aliyun_enable_itn,
                connect_timeout=Config.aliyun_connect_timeout,
                response_timeout=Config.aliyun_response_timeout,
            )
        elif model_type == 'fun_asr_nano':
            logger.debug("使用 Fun-ASR-Nano 模型")
            # 延迟导入本地 GGUF 引擎，避免在云端模式下触发不必要的模块加载与循环导入
            from util.fun_asr_gguf import create_asr_engine
            # recognizer = sherpa_onnx.OfflineRecognizer.from_funasr_nano(
            #     **{key: value for key, value in FunASRNanoArgs.__dict__.items() if not key.startswith('_')}
            # )
            recognizer = create_asr_engine(
                **{key: value for key, value in FunASRNanoGGUFArgs.__dict__.items() if not key.startswith('_')}
            )
        elif model_type == 'sensevoice':
            logger.debug("使用 SenseVoice 模型")
            recognizer = sherpa_onnx.OfflineRecognizer.from_sense_voice(
                **{key: value for key, value in SenseVoiceArgs.__dict__.items() if not key.startswith('_')}
            )
        elif model_type == 'paraformer':
            logger.debug("使用 Paraformer 模型")
            recognizer = sherpa_onnx.OfflineRecognizer.from_paraformer(
                **{key: value for key, value in ParaformerArgs.__dict__.items() if not key.startswith('_')}
            )
        else:
            error_msg = (
                f"不支持的模型类型: {Config.model_type}，"
                "请选择 'aliyun_realtime'、'fun_asr_nano'、'sensevoice' 或 'paraformer'"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
    except Exception as e:
        logger.error(f"模型加载失败: {e}", exc_info=True)
        raise

    console.print(f'[green4]语音模型载入完成 ({model_type})', end='\n\n')
    logger.info(f"语音模型加载完成 ({model_type})，耗时: {time.time() - t1:.2f}s")

    # 载入标点模型（仅 Paraformer 需要）
    punc_model = None
    if model_type == 'paraformer':
        logger.info("开始加载标点模型")
        console.print('[yellow]标点模型载入中', end='\r')
        config = sherpa_onnx.OfflinePunctuationConfig(
            model=sherpa_onnx.OfflinePunctuationModelConfig(
                ct_transformer=ModelPaths.punc_model_dir.as_posix()
            ),
        )
        punc_model = sherpa_onnx.OfflinePunctuation(config)
        console.print(f'[green4]标点模型载入完成 (CT-Transformer)', end='\n\n')
        logger.info("标点模型加载完成")

    console.print(f'模型加载耗时 {time.time() - t1 :.2f}s', end='\n\n')

    # 清空物理内存工作集
    if system() == 'Windows':
        empty_current_working_set()
        logger.debug("已清空物理内存工作集")

    queue_out.put(True)  # 通知主进程加载完了
    logger.info("识别器初始化完成，开始处理任务")

    # 标记资源已初始化
    _resources_initialized = True

    while True:
        # 任务调度策略：
        # 1. 优先处理麦克风队列，保证实时听写响应
        # 2. 文件转录走次级队列，避免长任务持续占用主链路
        try:
            task = queue_in_mic.get_nowait()
        except queue.Empty:
            try:
                task = queue_in_file.get(timeout=1)
            except queue.Empty:
                continue

        # 检查退出信号
        if task is None:
            logger.info("收到退出信号，识别子进程正在停止...")
            break

        if task.socket_id not in sockets_id:    # 检查任务所属的连接是否存活
            logger.debug(f"任务所属连接已断开，跳过处理，任务ID: {task.task_id}")
            continue

        result = recognize(recognizer, punc_model, task)   # 执行识别
        queue_out.put(result)      # 返回结果

    # 清理完成
    logger.info("识别子进程已退出")
