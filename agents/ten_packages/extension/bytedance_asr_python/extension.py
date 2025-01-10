#
# This file is part of TEN Framework, an open source project.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file for more information.
#

from dataclasses import dataclass
from ten_ai_base.config import BaseConfig
from .asr_client import (
    ByteDanceWebSocketClient,
)
import asyncio
import time
from ten import (
    AudioFrame,
    AsyncExtension,
    AsyncTenEnv,
    Data,
    Cmd 
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
        self.last_text = ""
        self.chunk_queue = asyncio.Queue()
        self.send_interval = 20
        self.sending_task = None
        self.chunk_size = 3200
    
    async def on_cmd(self, ten_env: AsyncTenEnv, cmd: Cmd) -> None:
        cmd_name = cmd.get_name()

    async def on_start(self, ten_env: AsyncTenEnv) -> None:
        self.loop = asyncio.get_event_loop()
        self.ten_env = ten_env
        
        self.config = await ByteDanceASRConfig.create_async(ten_env=ten_env)
        if not all([self.config.api_key, self.config.token, self.config.cluster]):
            ten_env.log_error("Missing required configuration")
            return

        self.client = ByteDanceWebSocketClient(self.config)
        
        async def on_result(payload):
            if 'result' in payload and isinstance(payload['result'], list) and len(payload['result']) > 0:
                asr_result = payload['result'][0]
                self.ten_env.log_info(f"bytedance asr on_result: {asr_result}")
                if 'utterances' in asr_result and isinstance(asr_result['utterances'], list):
                    for utterance in asr_result['utterances']:
                        text = utterance.get('text', '')
                        is_final = utterance.get('definite', False)
                        
                        if is_final:
                            self.ten_env.log_info("ASR_TEST_POINT_IS_FINAL:{}".format(
                                int(time.time() * 1000)
                            ))

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
        
        await self.client.start()
        self.sending_task = self.loop.create_task(self._process_chunks())

    async def _process_chunks(self):
        while True:
            try:
                chunks = []
                for _ in range(4):
                    if self.chunk_queue.empty():
                        break
                    chunk = await self.chunk_queue.get()
                    if chunk:
                        chunks.append(chunk)
                if chunks:
                    combined_chunk = b''.join(chunks)
                    self.ten_env.log_info(f"Sending combined chunk of size: {len(combined_chunk)} bytes")
                    await self.client.send(combined_chunk)
                
                await asyncio.sleep(self.send_interval / 1000)
            except Exception as e:
                self.ten_env.log_error(f"Error processing chunks: {str(e)}")
                await asyncio.sleep(0.1)

    async def on_audio_frame(self, ten_env: AsyncTenEnv, audio_frame: AudioFrame) -> None:
        ten_env.log_info("ASR_TEST_POINT_ON_AUDIO_FRAME:{}".format(
            int(time.time() * 1000)
        ))
        if not self.client or not self.client.connected:
            ten_env.log_error("WebSocket not connected")
            return

        frame_buf = audio_frame.get_buf()
        if not frame_buf or len(frame_buf) == 0:
            ten_env.log_error("Empty audio frame")
            return

        try:
            self.stream_id = audio_frame.get_property_int("stream_id")
        except Exception as e:
            self.ten_env.log_error(f"Failed to get stream_id: {str(e)}")
            return
    
        await self.chunk_queue.put(frame_buf)

    async def on_stop(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("ByteDance ASR stopping")
        if self.sending_task:
            self.sending_task.cancel()
            try:
                await self.sending_task
            except asyncio.CancelledError:
                pass
        if self.client:
            await self.client.close()
            self.client = None

    async def _send_text(self, text: str, is_final: bool, stream_id: int) -> None:
        if not text.strip():
            return
            
        data = Data.create("text_data")
        data.set_property_bool(DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL, is_final)
        data.set_property_string(DATA_OUT_TEXT_DATA_PROPERTY_TEXT, text)
        data.set_property_int(DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID, stream_id)
        data.set_property_bool(DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT, is_final)
        
        await self.ten_env.send_data(data)

    async def _start_listen(self) -> None:
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
