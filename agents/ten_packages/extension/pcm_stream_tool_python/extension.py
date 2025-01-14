#
# This file is part of TEN Framework, an open source project.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file for more information.
#
from dataclasses import dataclass
from ten_ai_base.config import BaseConfig
import asyncio
import os
import time
import os
import subprocess

from ten import (
    AudioFrame,
    VideoFrame,
    AudioFrameDataFmt,
    AsyncExtension,
    AsyncTenEnv,
    Cmd,
    StatusCode,
    CmdResult,
    Data,
)

@dataclass
class PcmStreamToolConfig(BaseConfig):
    pcm_file: str = ""


class PCMStreamToolExtension(AsyncExtension):
    def __init__(self, name: str):
        super().__init__(name)
        self.config: PcmStreamToolConfig = None
        self.pcm_queue = asyncio.Queue()
        self.loop = None
        self.stream_id = 1000

    async def on_init(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_debug("on_init")
        await super().on_init(ten_env)

    async def on_start(self, ten_env: AsyncTenEnv) -> None:
        self.config = await PcmStreamToolConfig.create_async(ten_env=ten_env)
        self.loop = asyncio.get_event_loop()
        self.loop.create_task(self.start_get_pcm_stream(ten_env))

    async def on_stop(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("on_stop")
        await super().on_stop(ten_env)

    async def on_cmd(self, ten_env: AsyncTenEnv, cmd: Cmd) -> None:
        cmd_name = cmd.get_name()

    async def on_data(self, ten_env: AsyncTenEnv, data: Data) -> None:
        data_name = data.get_name()
        ten_env.log_debug("on_data name {}".format(data_name))
        await super().on_data(ten_env, data)

    async def start_get_pcm_stream(self, ten_env: AsyncTenEnv) -> None:
        await asyncio.sleep(1.5)

        chunk_size = int(16000 * 2 * 1 * 0.01)
        pcm_path = os.path.join(os.path.dirname(__file__), self.config.pcm_file)
        async def read_and_send_pcm():
            try:
                end_of_speech = False

                with open(pcm_path, 'rb') as pcm_file:
                    while True:
                        chunk = pcm_file.read(chunk_size)
                        if not chunk:
                            ten_env.log_info("pcm file read end!!!")
                            await asyncio.sleep(180)

                            ten_env.log_info("exit app start")
                            close_app_cmd = Cmd.create("ten:close_app")
                            close_app_cmd.set_dest("localhost", None, None, None)
                            await ten_env.send_cmd(close_app_cmd)
                            ten_env.log_info("exit app end")
                            break

                        audio_frame = AudioFrame.create("pcm_frame")
                        audio_frame.set_sample_rate(16000)
                        audio_frame.set_bytes_per_sample(2)
                        audio_frame.set_number_of_channels(1)
                        audio_frame.set_data_fmt(AudioFrameDataFmt.INTERLEAVE)
                        audio_frame.set_samples_per_channel(len(chunk) // 2)
                        audio_frame.alloc_buf(len(chunk))

                        buff = audio_frame.lock_buf()
                        buff[:] = chunk
                        audio_frame.unlock_buf(buff)

                        audio_frame.set_property_int("stream_id", self.stream_id)

                        if pcm_file.tell() == os.fstat(pcm_file.fileno()).st_size:
                            end_of_speech = True
                        audio_frame.set_property_bool("end_of_speech", end_of_speech)
                        # ten_env.log_info("pcm_stream_tool_python send_audio_frame index:{}".format(
                        #    int(time.time() * 1000)
                        # ))
                        ten_env.log_info(f"pcm_stream_tool_python send_audio_frame end_of_speech:{end_of_speech}")
                        await ten_env.send_audio_frame(audio_frame)
                        #ten_env.log_info("pcm_stream_tool_python send_audio_frame end index before sleep:{}".format(
                        #    int(time.time() * 1000)
                        #))
                        await asyncio.sleep(0.01)
                        #ten_env.log_info("pcm_stream_tool_python send_audio_frame end index after sleep:{}".format(
                        #    int(time.time() * 1000)
                        #))

            except Exception as e:
                ten_env.log_error(f"Error processing PCM data: {str(e)}")

        asyncio.create_task(read_and_send_pcm())