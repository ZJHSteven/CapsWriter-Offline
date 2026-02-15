# 百炼接入与资料策略（2026-02-15）

## 1. 已确认的官方资料入口

- 实时语音识别（WebSocket）：
  `https://help.aliyun.com/zh/model-studio/fun-asr-realtime-websocket-api`
- 录音文件识别（RESTful）：
  `https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-restful-api`
- 实时 SDK 示例（含延迟指标）：
  `https://help.aliyun.com/zh/model-studio/fun-asr-realtime-python-sdk`

## 2. Context7 可用数据源

当前已验证可直接使用：

- `/websites/help_aliyun_zh_model-studio`
  - 覆盖百炼文档主站，包含 Fun-ASR 实时与录音文件接口说明。
- `/modelscope/funasr`
  - 覆盖开源 FunASR 运行时协议与示例，可作为 SDK/协议侧补充参考。

## 3. 是否需要新建 Context 资料库

结论：**当前阶段不需要**。

原因：

1. 现有 Context7 已能覆盖本次改造所需关键信息（端点、模型名、协议、限制）。
2. 本次目标是落地迁移，优先减少资料平台建设成本，先把工程改造闭环跑通。

## 4. 是否需要新建 Skill

结论：**建议后续建立一个“阿里云百炼通用 Skill”，但不阻塞本次迁移**。

建议在以下场景启动 Skill 化：

1. 除 ASR 外，还要接入百炼的 TTS、翻译、向量检索、Agent 编排。
2. 团队有重复性的“查文档 + 生成代码模板 + 鉴权排错”需求。
3. 希望把 API 约束、重试策略、错误码排查流程标准化。

## 5. 本项目当前执行策略

1. 直接使用 Context7 + 官方文档，驱动本次 ASR 改造。
2. 先完成实时链路迁移与并发问题修复。
3. 稳定后再将共性流程抽取为 Skill，覆盖更广的百炼能力。
