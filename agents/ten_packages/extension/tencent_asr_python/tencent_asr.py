import asyncio
from dataclasses import dataclass
import json
from ten import AsyncTenEnv
from ten_ai_base.config import BaseConfig
from .tencent_asr_sdk.common import credential
from .asr_result import AsrResult, Word
from .tencent_asr_sdk import speech_recognizer


def get_asr_result_from_response(response, base_time: int) -> AsrResult:
    result = response["result"]

    sentence_start_time = base_time + result["start_time"]
    sentence_duration = result["end_time"] - result["start_time"]
    sentence_is_final = result["slice_type"] == 2
    sentence_language = "zh-CN"
    words = []

    word_list = result["word_list"]
    for word in word_list:
        words.append(
            Word(
                text_time=base_time + word["start_time"],
                duration_ms=word["end_time"] - word["start_time"],
                text=word["word"],
                is_final=word["stable_flag"] == 1,
            )
        )
    return AsrResult(
        text_time=sentence_start_time,
        language=sentence_language,
        slice_type=result["slice_type"],
        duration_ms=sentence_duration,
        words=words,
        voice_text_str=result["voice_text_str"],
        is_final=sentence_is_final,
    )


class AsyncIteratorCallback(speech_recognizer.SpeechRecognitionListener):
    def __init__(self, ten_env: AsyncTenEnv, queue: asyncio.Queue, on_failure):
        self.ten_env = ten_env
        self.first_frame_timestamp = 0
        self.loop = asyncio.get_event_loop()
        self.queue = queue
        self.on_failure = on_failure

    # def on_audio_frame(self, audio_frame: AudioFrame):
    #     if self.first_frame_timestamp == 0:
    #         self.first_frame_timestamp = audio_frame.get_timestamp()

    def on_recognition_start(self, response):
        self.ten_env.log_info(f"OnRecognitionStart,voice_id:{response['voice_id']}")

    def on_sentence_begin(self, response):
        rsp_str = json.dumps(response, ensure_ascii=False)

        self.ten_env.log_debug(
            f"OnRecognitionSentenceBegin, voice_id:{response['voice_id']}, rsp:{rsp_str}"
        )

        asr_result = get_asr_result_from_response(response, self.first_frame_timestamp)

        asyncio.run_coroutine_threadsafe(self.queue.put(asr_result), self.loop)

    def on_recognition_result_change(self, response):
        rsp_str = json.dumps(response, ensure_ascii=False)

        self.ten_env.log_info(
            f"OnResultChange, voice_id:{response['voice_id']}, rsp:{rsp_str}"
        )

        asr_result = get_asr_result_from_response(response, self.first_frame_timestamp)

        self.ten_env.log_info(f"OnSentenceEnd, asr result:{asr_result}")

        asyncio.run_coroutine_threadsafe(self.queue.put(asr_result), self.loop)

    def on_sentence_end(self, response):
        rsp_str = json.dumps(response, ensure_ascii=False)
        self.ten_env.log_info(
            f"OnSentenceEnd, voice_id:{response['voice_id']}, rsp:{rsp_str}"
        )

        asr_result = get_asr_result_from_response(response, self.first_frame_timestamp)
        self.ten_env.log_info(f"OnSentenceEnd, asr result:{asr_result}")

        asyncio.run_coroutine_threadsafe(self.queue.put(asr_result), self.loop)

    def on_recognition_complete(self, response):
        self.ten_env.log_info(f"OnRecognitionComplete, voice_id:{response['voice_id']}")

    def on_fail(self, response):
        rsp_str = json.dumps(response, ensure_ascii=False)
        self.ten_env.log_info(f"OnFail, voice_id:{response['voice_id']}, rsp:{rsp_str}")
        if self.on_failure:
            self.on_failure(self.ten_env)


@dataclass
class TencentASRConfig(BaseConfig):
    app_id: str = ""
    secret_id: str = ""
    secret_key: str = ""
    engine_model_type: str = ""


FRAME_DURATION = 0.04  # 40ms


class TencetASR:
    def __init__(
        self, audio_frame_queue: asyncio.Queue, config: TencentASRConfig
    ) -> None:
        self.config = config
        self.recognizer = None
        self.audio_frame_queue = audio_frame_queue
        self.queue = asyncio.Queue()
        self.listener = None

    def create_asr_client(self, ten_env: AsyncTenEnv):
        credential_var = credential.Credential(
            self.config.secret_id, self.config.secret_key
        )
        self.listener = AsyncIteratorCallback(
            ten_env=ten_env, queue=self.queue, on_failure=self.stop
        )
        ten_env.log_info(f"create_asr_client, config:{self.config}")
        recognizer = speech_recognizer.SpeechRecognizer(
            appid=self.config.app_id,
            credential=credential_var,
            engine_model_type=self.config.engine_model_type,
            listener=self.listener,
        )
        recognizer.set_filter_modal(1)
        recognizer.set_convert_num_mode(0)
        recognizer.set_word_info(0)
        recognizer.set_voice_format(1)
        recognizer.set_need_vad(1)

        self.recognizer = recognizer
        asyncio.create_task(self.send_audio_frame(ten_env))

    def start(self, ten_env: AsyncTenEnv) -> None:
        if self.recognizer:
            self.recognizer.start()
        else:
            ten_env.log_warn("ASR client is not initialized")

    async def send_audio_frame(self, ten_env: AsyncTenEnv) -> None:
        while True:
            try:
                frame_buf = await self.audio_frame_queue.get()
                # self.listener.on_audio_frame(frame)
                if not frame_buf:
                    ten_env.log_warn("send_frame: empty pcm_frame detected.")
                    return
                # ten_env.log_info(f"send_frame: {len(frame_buf)}")
                if not self.recognizer:
                    ten_env.log_warn("send_frame: recognizer is not initialized.")
                    return

                self.recognizer.write(frame_buf)
                # await asyncio.sleep(FRAME_DURATION)
            except asyncio.QueueEmpty:
                break

    # def send_audio_data(self, frame_buf: bytes) -> None:
    #     self.recognizer.write(frame_buf)

    def stop(self, ten_env: AsyncTenEnv) -> None:
        if self.recognizer:
            try:
                self.recognizer.stop()
            except Exception as e:
                ten_env.log_warn(f"stop recognizer err {e}")
            finally:
                self.recognizer = None
                ten_env.log_info("recognizer stopped")
        else:
            ten_env.log_info("recognizer stopped already")

    async def get_asr_result(self) -> AsrResult:
        return await self.queue.get()
