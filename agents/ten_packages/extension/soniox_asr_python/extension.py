#
# This file is part of TEN Framework, an open source project.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file for more information.
#
from dataclasses import dataclass
from ten_ai_base.config import BaseConfig
from soniox.transcribe_live import transcribe_stream
from soniox.speech_service import SpeechClient
import asyncio
from collections import deque
import threading
import queue
import time

from ten import (
    AudioFrame,
    AsyncExtension,
    AsyncTenEnv,
    Data,
)

@dataclass
class SonioxASRConfig(BaseConfig):
    api_key: str = ""
    sample_rate: int = 16000
    channels: int = 1

DATA_OUT_TEXT_DATA_PROPERTY_TEXT = "text"
DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL = "is_final"
DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID = "stream_id"
DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT = "end_of_segment"

class SonioxASRExtension(AsyncExtension):
    def __init__(self, name: str):
        super().__init__(name)
        self.config: SonioxASRConfig = None
        self.client = None
        self.ten_env: AsyncTenEnv = None
        self.stream_id = -1
        self.running = False
        self.audio_queue = queue.Queue()
        self.transcription_thread = None
        self.loop = None
        self.current_text = "" 

    def iter_audio(self):
        while self.running:
            try:
                audio_data = self.audio_queue.get(timeout=0.1)  
                yield audio_data
            except queue.Empty:
                self.ten_env.log_info("No new audio data")
                continue
        self.ten_env.log_info("soniox_asr_python: ASR end")
        yield b''


    def run_transcription(self):
        try:
            self.ten_env.log_info("soniox_asr_python: Starting transcription")
            all_final_tokens = []

            for result in transcribe_stream(
                self.iter_audio(),
                self.client,
                model="en_v2_lowlatency",
                include_nonfinal=True,
                audio_format="pcm_s16le",
                sample_rate_hertz=16000,
                num_audio_channels=1,
            ):
                final_tokens = []
                nonfinal_tokens = []
                for word in result.words:
                    if word.is_final:
                        final_tokens.append(word.text)
                    else:
                        nonfinal_tokens.append(word.text)

                 # Append current final tokens to all final tokens.
                all_final_tokens += final_tokens

                # Print all final tokens and current non-final tokens.
                all_final_tokens_str = "".join(all_final_tokens)
                nonfinal_tokens_str = "".join(nonfinal_tokens)
                
                self.ten_env.log_info(f"Final: {all_final_tokens_str}")
                self.ten_env.log_info(f"Non-final: {nonfinal_tokens_str}")

                if final_tokens:  
                    is_final = nonfinal_tokens_str.strip() == ""
                    if is_final:
                        self.ten_env.log_info("ASR_TEST_POINT_IS_FINAL:{}".format(
                            int(time.time() * 1000)
                        ))
                    future = asyncio.run_coroutine_threadsafe(
                        self._send_text(
                            all_final_tokens_str, 
                            is_final, 
                            self.stream_id,
                        ),
                        self.loop
                    )
                    if is_final:
                        # self.ten_env.log_info("ASR_TEST_POINT_IS_FINAL:{}".format(
                        #     int(time.time() * 1000)
                        # ))
                        all_final_tokens = []
                        all_final_tokens_str = ""
                        nonfinal_tokens_str = ""
                        nonfinal_tokens = []
                    future.result(timeout=1)

        except Exception as e:
            self.ten_env.log_error(f"soniox_asr_python: Transcription error: {str(e)}")

    async def on_start(self, ten_env: AsyncTenEnv) -> None:
        self.ten_env = ten_env
        self.config = await SonioxASRConfig.create_async(ten_env=ten_env)
        self.loop = asyncio.get_event_loop()
        
        self.ten_env.log_info("soniox_asr_python: Starting Soniox ASR")
        self.client = SpeechClient(api_key=self.config.api_key)
        
        self.running = True
        self.transcription_thread = threading.Thread(target=self.run_transcription)
        self.transcription_thread.daemon = True
        self.transcription_thread.start()
        
        self.ten_env.log_info("soniox_asr_python: Soniox ASR started successfully")

    async def on_stop(self, ten_env: AsyncTenEnv) -> None:
        self.running = False
        if self.transcription_thread:
            self.transcription_thread.join(timeout=1.0)
        if self.client:
            await self.client.close()

    async def on_audio_frame(
        self, ten_env: AsyncTenEnv, audio_frame: AudioFrame
    ) -> None:
        ten_env.log_info("ASR_TEST_POINT_ON_AUDIO_FRAME:{}".format(
            int(time.time() * 1000)
        ))
        if not self.client or not self.running:
            return

        frame_buf = audio_frame.get_buf()
        if not frame_buf or len(frame_buf) == 0:
            ten_env.log_error("Empty audio frame")
            return
            
        self.stream_id = audio_frame.get_property_int("stream_id")
        speech_end = audio_frame.get_property_bool("end_of_speech")
        if speech_end:
            self.running = False
        # Convert frame_buf to bytes if it's not already
        audio_bytes = bytes(frame_buf)
        
        try:
            self.audio_queue.put_nowait(audio_bytes)
        except queue.Full:
            self.ten_env.log_error("soniox_asr_python: Audio queue is full, dropping frame")

    async def _send_text(self, text: str, is_final: bool, stream_id: int) -> None:
        if not text.strip():
            return
        
        data = Data.create("text_data")
        data.set_property_bool(DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL, is_final)
        data.set_property_string(DATA_OUT_TEXT_DATA_PROPERTY_TEXT, text)
        data.set_property_int(DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID, stream_id)
        data.set_property_bool(DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT, is_final)
        
        await self.ten_env.send_data(data)
