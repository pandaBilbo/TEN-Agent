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
    parse_response
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
        self.ws = None
        self.reqid = None
        self.loop = None
        self.connected = False
        self.stopped = False
        self.buffer = bytearray()
        self.seg_duration = 100  # 改为100ms，更适合实时处理
        self.last_text = ""  # 添加这行，用于记录上一次的文本

    async def on_start(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("ByteDance ASR starting")
        self.loop = asyncio.get_event_loop()
        self.ten_env = ten_env
        
        self.config = await ByteDanceASRConfig.create_async(ten_env=ten_env)
        if not all([self.config.api_key, self.config.token, self.config.cluster]):
            ten_env.log_error("Missing required configuration")
            return

        self.client = AsrWsClient(
            cluster=self.config.cluster,
            appid=self.config.api_key,
            token=self.config.token,
            sample_rate=self.config.sample_rate,
            bits=self.config.bits,
            channel=self.config.channels,
        )

        # 计算分片大小 (100ms 的数据)
        bytes_per_sample = self.config.bits // 8
        size_per_sec = self.config.channels * bytes_per_sample * self.config.sample_rate
        self.chunk_size = int(size_per_sec * self.seg_duration / 1000)  # 应该是 3200 bytes

        self.loop.create_task(self._start_listen())

    async def _start_listen(self) -> None:
        try:
            self.reqid = str(uuid.uuid4())
            request_params = self.client.construct_request(self.reqid)
            payload_bytes = gzip.compress(str.encode(json.dumps(request_params)))
            
            full_client_request = bytearray(generate_full_default_header())
            full_client_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
            full_client_request.extend(payload_bytes)
            
            headers = self.client.token_auth()
            self.ws = await websockets.connect(
                self.client.ws_url,
                extra_headers=headers,
                max_size=1000000000
            )
            
            await self.ws.send(full_client_request)
            self.connected = True
            self.ten_env.log_info("WebSocket connected successfully")
            
            # 开始监听响应
            while not self.stopped:
                try:
                    res = await self.ws.recv()
                    result = parse_response(res)
                    self.ten_env.log_info(f"Received response: {result}")
                    
                    if result and 'payload_msg' in result:
                        payload = result['payload_msg']
                        if payload.get('code') != 1000:
                            self.ten_env.log_error(f"Error response: {payload}")
                            continue
                            
                        # 检查是否有识别结果
                        if 'result' in payload:
                            text = ""
                            is_final = False
                            
                            # 处理最终结果
                            if isinstance(payload['result'], list) and len(payload['result']) > 0:
                                asr_result = payload['result'][0]
                                text = asr_result.get('text', '')
                                is_final = True
                            # 处理中间结果
                            elif isinstance(payload['result'], dict):
                                text = payload['result'].get('text', '')
                                is_final = False
                            
                            # 只有当文本发生变化时才发送
                            if text and text != self.last_text:
                                self.ten_env.log_info(f"Processing ASR result: {text} (is_final: {is_final})")
                                await self._send_text(
                                    text=text,
                                    is_final=is_final,
                                    stream_id=self.stream_id
                                )
                                self.last_text = text
                except Exception as e:
                    if not self.stopped:
                        self.ten_env.log_error(f"WebSocket error: {str(e)}\n{traceback.format_exc()}")
                        await asyncio.sleep(0.2)
                        self.loop.create_task(self._start_listen())
                    break
                    
        except Exception as e:
            if not self.stopped:
                self.ten_env.log_error(f"Failed to connect: {str(e)}")
                await asyncio.sleep(0.2)
                self.loop.create_task(self._start_listen())

    async def on_audio_frame(self, ten_env: AsyncTenEnv, audio_frame: AudioFrame) -> None:
        ten_env.log_info("on_audio_frame")
        if not self.ws or not self.connected:
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
        ten_env.log_info(f"buffer length: {len(self.buffer)}")
        ten_env.log_info(f"chunk_size: {self.chunk_size}")
        while len(self.buffer) >= self.chunk_size:
            try:
                chunk = self.buffer[:self.chunk_size]
                self.buffer = self.buffer[self.chunk_size:]
                
                payload_bytes = gzip.compress(bytes(chunk))
                audio_only_request = bytearray(generate_audio_default_header())
                audio_only_request.extend((len(payload_bytes)).to_bytes(4, 'big'))
                audio_only_request.extend(payload_bytes)
                
                await self.ws.send(audio_only_request)
            except Exception as e:
                ten_env.log_error(f"Failed to send audio: {str(e)}")
                self.connected = False
                break

    async def on_stop(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("ByteDance ASR stopping")
        self.stopped = True
        if self.ws:
            await self.ws.close()
            self.ws = None
        if self.client:
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
