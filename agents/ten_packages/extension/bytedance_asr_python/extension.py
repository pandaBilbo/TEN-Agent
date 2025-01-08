#
# This file is part of TEN Framework, an open source project.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file for more information.
#

from dataclasses import dataclass
from ten_ai_base.config import BaseConfig
from .asr_client import (
    AsrWsClient,
    generate_full_default_header,
    generate_audio_default_header,
    generate_last_audio_default_header,
    parse_response,
    ByteDanceWebSocketClient,
)
import asyncio
import uuid
import json
import gzip
import websockets
import traceback

from ten import (
    AudioFrame,
    VideoFrame,
    AsyncExtension,
    AsyncTenEnv,
    Cmd,
    StatusCode,
    CmdResult,
    Data,
)

DATA_OUT_TEXT_DATA_PROPERTY_TEXT = "text"
DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL = "is_final"
DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID = "stream_id"
DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT = "end_of_segment"

@dataclass
class ByteDanceASRConfig(BaseConfig):
    api_key: str = ""
    token: str = ""
    cluster: str = ""
    sample_rate: int = 16000
    channels: int = 1
    bits: int = 16

class ByteDanceASRExtension(AsyncExtension):
    def __init__(self, name: str):
        super().__init__(name)
        self.config: ByteDanceASRConfig = None
        self.client = None
        self.ten_env: AsyncTenEnv = None
        self.stream_id = -1
        self.loop = None
        self.buffer = bytearray()
        self.seg_duration = 100
        self.last_text = ""

    async def on_start(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("ByteDance ASR starting")
        self.loop = asyncio.get_event_loop()
        self.ten_env = ten_env
        
        self.config = await ByteDanceASRConfig.create_async(ten_env=ten_env)
        if not all([self.config.api_key, self.config.token, self.config.cluster]):
            ten_env.log_error("Missing required configuration")
            return

        # 计算分片大小 (100ms 的数据)
        bytes_per_sample = self.config.bits // 8
        size_per_sec = self.config.channels * bytes_per_sample * self.config.sample_rate
        self.chunk_size = int(size_per_sec * self.seg_duration / 1000)

        self.client = ByteDanceWebSocketClient(self.config)
        
        # 设置回调
        async def on_result(payload):
            if 'result' in payload and isinstance(payload['result'], list) and len(payload['result']) > 0:
                asr_result = payload['result'][0]
                
                # 检查是否有 utterances
                if 'utterances' in asr_result and isinstance(asr_result['utterances'], list):
                    for utterance in asr_result['utterances']:
                        text = utterance.get('text', '')
                        is_final = utterance.get('definite', False)  # 使用 definite 字段判断是否为最终结果
                        
                        if text and text != self.last_text:
                            await self._send_text(
                                text=text,
                                is_final=is_final,
                                stream_id=self.stream_id
                            )
                            self.last_text = text

        async def on_error(error):
            self.ten_env.log_error(f"ASR error: {error}")
            await asyncio.sleep(0.2)
            await self.client.start()

        self.client.on('result', on_result)
        self.client.on('error', on_error)
        
        # 启动客户端
        await self.client.start()

    async def on_audio_frame(self, ten_env: AsyncTenEnv, audio_frame: AudioFrame) -> None:
        if not self.client or not self.client.connected:
            ten_env.log_error("WebSocket not connected")
            return

        frame_buf = audio_frame.get_buf()
        if not frame_buf:
            return

        try:
            self.stream_id = audio_frame.get_property_int("stream_id")
        except Exception as e:
            self.ten_env.log_error(f"Failed to get stream_id: {str(e)}")
            return
        
        # 缓存音频数据
        self.buffer.extend(frame_buf)
        
        # 当缓冲区达到指定大小时发送
        while len(self.buffer) >= self.chunk_size:
            chunk = self.buffer[:self.chunk_size]
            self.buffer = self.buffer[self.chunk_size:]
            await self.client.send(bytes(chunk))

    async def on_stop(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("ByteDance ASR stopping")
        if self.client:
            await self.client.close()
            self.client = None

    async def _send_text(self, text: str, is_final: bool, stream_id: int) -> None:
        if not text.strip():
            return
            
        self.ten_env.log_info(f"Sending text: [{text}], is_final: {is_final}, stream_id: {stream_id}")
            
        data = Data.create("text_data")
        data.set_property_bool(DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL, is_final)
        data.set_property_string(DATA_OUT_TEXT_DATA_PROPERTY_TEXT, text)
        data.set_property_int(DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID, stream_id)
        data.set_property_bool(DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT, is_final)
        
        await self.ten_env.send_data(data)

    async def _start_listen(self) -> None:
        self.ten_env.log_info("start and listen bytedance asr")

        self.client = ByteDanceWebSocketClient(self.config)

        async def on_open():
            self.ten_env.log_info("WebSocket connected")
            self.connected = True

        async def on_close():
            self.ten_env.log_info("WebSocket closed")
            self.connected = False
            await asyncio.sleep(0.2)
            self.loop.create_task(self._start_listen())

        async def on_message(payload):
            if 'result' in payload and isinstance(payload['result'], list) and len(payload['result']) > 0:
                asr_result = payload['result'][0]
                if 'utterances' in asr_result and isinstance(asr_result['utterances'], list):
                    for utterance in asr_result['utterances']:
                        text = utterance.get('text', '')
                        is_final = utterance.get('definite', False)
                        
                        if text and text != self.last_text:
                            await self._send_text(
                                text=text,
                                is_final=is_final,
                                stream_id=self.stream_id
                            )
                            self.last_text = text

        async def on_error(error):
            self.ten_env.log_error(f"ASR error: {error}")

        self.client.on('open', on_open)
        self.client.on('close', on_close)
        self.client.on('result', on_message)
        self.client.on('error', on_error)

        result = await self.client.start()
        if not result:
            self.ten_env.log_error("Failed to connect")
            await asyncio.sleep(0.2)
            self.loop.create_task(self._start_listen())
        else:
            self.ten_env.log_info("Successfully connected")
