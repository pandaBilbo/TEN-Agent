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
import dashscope
from dashscope.audio.asr import Recognition, RecognitionCallback, RecognitionResult
from dataclasses import dataclass
from ten_ai_base.config import BaseConfig

DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT = "end_of_segment"
DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL = "is_final"
DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID = "stream_id"
DATA_OUT_TEXT_DATA_PROPERTY_TEXT = "text"


@dataclass
class AliyunASRConfig(BaseConfig):
    api_key: str = ""
    disfluency_removal_enabled: bool = False
    format: str = "pcm"
    model: str = "paraformer-realtime-v2"
    sample_rate: int = 16000


class AliyunASRExtension(AsyncExtension):
    def __init__(self, name: str):
        super().__init__(name)

        self.config: AliyunASRConfig = None
        self.loop = None
        self.recognition = None
        self.stopped = False
        self.stream_id = -1
        self.ten_env: AsyncTenEnv = None

    async def on_init(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("AliyunASRExtension on_init")

    async def on_start(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("on_start")
        self.loop = asyncio.get_event_loop()
        self.ten_env = ten_env

        self.config = await AliyunASRConfig.create_async(ten_env=ten_env)
        ten_env.log_info(
            f"on_start, api_key: {self.config.api_key[:10]}***, format: {self.config.format}, model: {self.config.model}, sample_rate: {self.config.sample_rate}"
        )

        if not self.config.api_key:
            ten_env.log_error("get property api_key failed")
            return

        # Set dashscope API key
        dashscope.api_key = self.config.api_key

        # Initialize Recognition
        self.recognition = Recognition(
            callback=Callback(ten_env, self),
            disfluency_removal_enabled=self.config.disfluency_removal_enabled,
            format=self.config.format,
            model=self.config.model,
            sample_rate=self.config.sample_rate,
        )

        # Start streaming recognition
        self.start_recognition()

    async def on_audio_frame(self, ten_env: AsyncTenEnv, frame: AudioFrame) -> None:
        ten_env.log_info("[ASR_TEST_POINT_ON_AUDIO_FRAME]")

        if self.stopped:
            return

        frame_buf = frame.get_buf()
        if not frame_buf:
            self.ten_env.log_warn("send_frame: empty pcm_frame detected.")
            return

        self.stream_id = frame.get_property_int("stream_id")

        try:
            # Send audio frame to streaming recognition
            self.recognition.send_audio_frame(frame_buf)
        except Exception as e:
            self.ten_env.log_error(f"Error processing audio frame: {e}")

    async def on_stop(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("on_stop")

        self.stopped = True

        if self.recognition:
            try:
                self.recognition.stop()
                ten_env.log_info("Recognition stopped successfully")
            except Exception as e:
                ten_env.log_error(f"Error stopping recognition: {e}")

    async def on_cmd(self, ten_env: AsyncTenEnv, cmd: Cmd) -> None:
        cmd_name = cmd.get_name()
        ten_env.log_info(f"on_cmd processing cmd {cmd_name}")

        cmd_result = CmdResult.create(StatusCode.OK)
        cmd_result.set_property_string("detail", "success")
        await ten_env.return_result(cmd_result, cmd)

    async def _send_text(self, text: str, is_final: bool, stream_id: int) -> None:
        stable_data = Data.create("text_data")
        stable_data.set_property_bool(DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL, is_final)
        stable_data.set_property_string(DATA_OUT_TEXT_DATA_PROPERTY_TEXT, text)
        stable_data.set_property_int(DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID, stream_id)
        stable_data.set_property_bool(
            DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT, is_final
        )
        await self.ten_env.send_data(stable_data)

    def start_recognition(self):
        try:
            self.recognition.start()
            self.ten_env.log_info("AliyunASRExtension started successfully")
        except Exception as e:
            self.ten_env.log_error(f"Failed to start recognition: {e}")


class Callback(RecognitionCallback):
    def __init__(self, ten_env: AsyncTenEnv, extension: AliyunASRExtension):
        self.ten_env = ten_env
        self.extension = extension

    def on_open(self) -> None:
        self.ten_env.log_info(f"callback on_open")

    def on_close(self) -> None:
        self.ten_env.log_info(f"callback on_close")

        if not self.stopped:
            self.ten_env.log_warn(
                "AliyunASRExtension connection closed unexpectedly. Reconnecting..."
            )
            self.extension.start_recognition()

    def on_event(self, result: RecognitionResult) -> None:
        sentence = result.get_sentence()
        self.ten_env.log_info(f"callback on_event, sentence: {sentence}")

        is_final = sentence.get("sentence_end", False)

        if is_final:
            self.ten_env.log_info("[ASR_TEST_POINT_IS_FINAL]")

        text = sentence.get("text", "")
        if text:
            asyncio.run_coroutine_threadsafe(
                self.extension._send_text(
                    text=text, is_final=is_final, stream_id=self.extension.stream_id
                ),
                self.extension.loop,
            )
