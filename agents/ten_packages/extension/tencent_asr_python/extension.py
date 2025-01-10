from ten import (
    AsyncExtension,
    AsyncTenEnv,
    Cmd,
    Data,
    AudioFrame,
    StatusCode,
    CmdResult,
)

import asyncio
import time
from dataclasses import dataclass

from ten_ai_base.config import BaseConfig
from .tencent_asr import TencetASR
from .asr_result import AsrResult

DATA_OUT_TEXT_DATA_PROPERTY_TEXT = "text"
DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL = "is_final"
DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID = "stream_id"
DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT = "end_of_segment"

FRAME_SIZE = 1280  # 16kHz * 2bytes * 0.04s


@dataclass
class TencentASRConfig(BaseConfig):
    app_id: str = ""
    secret_id: str = ""
    secret_key: str = ""
    engine_model_type: str = ""


class TencentASRExtension(AsyncExtension):
    def __init__(self, name: str):
        super().__init__(name)

        self.client = None
        self.config: TencentASRConfig = None
        self.ten_env: AsyncTenEnv = None
        self.audio_frame_queue = asyncio.Queue()
        self.stream_id = 0
        self.frame_buffer = bytearray()

    async def on_init(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("TencentASRExtension on_init")

    async def on_start(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("tencent_asr on_start")
        self.ten_env = ten_env

        self.config = await TencentASRConfig.create_async(ten_env=ten_env)
        ten_env.log_info(f"config: {self.config}")

        if (
            not self.config.app_id
            or not self.config.secret_id
            or not self.config.secret_key
        ):
            ten_env.log_error("missing app_id or secret_id or secret_key")
            return

        self.client = TencetASR(self.audio_frame_queue, self.config)
        self.client.create_asr_client(ten_env)
        self.client.start(ten_env=ten_env)

        asyncio.create_task(self.process_asr_result(ten_env=ten_env))
        ten_env.log_info("starting async_tencent_asr_wrapper thread")

    async def on_audio_frame(self, ten_env: AsyncTenEnv, frame: AudioFrame) -> None:
        ten_env.log_info("ASR_TEST_POINT_ON_AUDIO_FRAME:{}".format(
            int(time.time() * 1000)
        ))

        if not self.client:
            return
        buf = frame.get_buf()
        self.stream_id = frame.get_property_int("stream_id")
        await self.audio_frame_queue.put(buf)

    async def on_stop(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("on_stop")

        if self.client:
            self.client.stop(ten_env=ten_env)

    async def on_cmd(self, ten_env: AsyncTenEnv, cmd: Cmd) -> None:
        cmd_json = cmd.to_json()
        ten_env.log_info(f"on_cmd json: {cmd_json}")

        cmd_result = CmdResult.create(StatusCode.OK)
        cmd_result.set_property_string("detail", "success")
        await ten_env.return_result(cmd_result, cmd)

    async def process_asr_result(self, ten_env: AsyncTenEnv) -> None:
        self.ten_env.log_info("start and listen tencent asr")
        while True:
            ten_env.log_info("get asr result")
            asr_result = await self.client.get_asr_result()
            ten_env.log_info(f"get asr result {asr_result}")
            if asr_result is None:
                break

            if asr_result.is_final:
                ten_env.log_info("ASR_TEST_POINT_IS_FINAL:{}".format(
                    int(time.time() * 1000)
                ))
                
            await self.create_and_send_data(self.ten_env, asr_result)

    async def create_and_send_data(self, ten_env: AsyncTenEnv, asr_result: AsrResult):
        try:
            text = ""
            for word in asr_result.words:
                text += word.text

            ten_env.log_info(f"tencent_asr on asr result {asr_result}, {text}")
            data = Data.create("text_data")
            data.set_property_string(
                DATA_OUT_TEXT_DATA_PROPERTY_TEXT, asr_result.voice_text_str
            )
            data.set_property_int(DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID, self.stream_id)
            data.set_property_bool(
                DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT, asr_result.is_final
            )
            data.set_property_bool(
                DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL, asr_result.is_final
            )
            await ten_env.send_data(data)
        except Exception as e:
            ten_env.log_error(f"error on create_and_send_data {e}")
