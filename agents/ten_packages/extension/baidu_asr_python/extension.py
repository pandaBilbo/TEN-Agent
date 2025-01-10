from ten import (
    AsyncExtension,
    AsyncTenEnv,
    Cmd,
    Data,
    AudioFrame,
    StatusCode,
    CmdResult,
)
from typing import Union
import asyncio
import json
import threading
import time
import uuid
import websocket
from dataclasses import dataclass
from ten_ai_base.config import BaseConfig
import concurrent.futures

DATA_OUT_TEXT_DATA_PROPERTY_END_OF_SEGMENT = "end_of_segment"
DATA_OUT_TEXT_DATA_PROPERTY_IS_FINAL = "is_final"
DATA_OUT_TEXT_DATA_PROPERTY_STREAM_ID = "stream_id"
DATA_OUT_TEXT_DATA_PROPERTY_TEXT = "text"


@dataclass
class BaiduASRConfig(BaseConfig):
    app_id: Union[int, str] = 0
    api_key: str = ""
    chunk_len: int = int(16000 * 2 / 1000 * 160)  # 5120 bytes for 160ms
    chunk_ms: int = 160  # 160ms per chunk
    format: str = "pcm"
    model: Union[int, str] = "15372"
    sample_rate: int = 16000
    url: str = "wss://vop.baidu.com/realtime_asr"


class BaiduASRExtension(AsyncExtension):
    def __init__(self, name: str):
        super().__init__(name)

        self.audio_frame = bytearray()
        self.audio_queue = asyncio.Queue()
        self.config: BaiduASRConfig = None
        self.loop = None
        self.stopped = False
        self.stream_id = -1
        self.ten_env: AsyncTenEnv = None
        self.ws = None
        self.ws_thread = None

    async def on_init(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("BaiduASRExtension on_init")

    async def on_start(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("on_start")

        self.loop = asyncio.get_event_loop()
        self.ten_env = ten_env

        self.config = await BaiduASRConfig.create_async(ten_env=ten_env)
        ten_env.log_info(
            f"on_start, api_key: {self.config.api_key[:10]}***, app_id: {self.config.app_id}, format: {self.config.format}, model: {self.config.model}, sample_rate: {self.config.sample_rate}"
        )

        if not all([self.config.api_key, self.config.app_id]):
            ten_env.log_error("Missing required credentials (api_key, app_id)")
            return

        # Start WebSocket connection
        await self.start_websocket()

    async def on_audio_frame(self, ten_env: AsyncTenEnv, frame: AudioFrame) -> None:
        ten_env.log_info("ASR_TEST_POINT_ON_AUDIO_FRAME:{}".format(
            int(time.time() * 1000)
        ))

        if self.stopped:
            return

        frame_buf = frame.get_buf()
        if not frame_buf:
            self.ten_env.log_warn("send_frame: empty audio_frame detected.")
            return

        self.stream_id = frame.get_property_int("stream_id")

        try:
            await self.audio_queue.put(frame_buf)
        except Exception as e:
            self.ten_env.log_error(f"Error queueing audio frame: {e}")

    async def on_stop(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_info("on_stop")
        self.stopped = True

        if self.ws:
            try:
                # Send finish frame
                finish_frame = {"type": "FINISH"}
                self.ws.send(json.dumps(finish_frame))
                ten_env.log_info("Sent FINISH frame")

                # Signal audio sending thread to stop
                await self.audio_queue.put(None)

                # Close WebSocket connection
                self.ws.close()
                self.ws = None

                if self.ws_thread:
                    self.ws_thread.join(timeout=2)
                    self.ws_thread = None

                ten_env.log_info("WebSocket connection closed successfully")
            except Exception as e:
                ten_env.log_error(f"Error closing WebSocket connection: {e}")

    async def on_cmd(self, ten_env: AsyncTenEnv, cmd: Cmd) -> None:
        cmd_name = cmd.get_name()
        ten_env.log_info(f"on_cmd processing cmd {cmd_name}")

        if cmd_name == "cancel":
            if self.ws:
                try:
                    cancel_frame = {"type": "CANCEL"}
                    self.ws.send(json.dumps(cancel_frame))
                    ten_env.log_info("Sent CANCEL frame")
                except Exception as e:
                    ten_env.log_error(f"Error sending cancel frame: {e}")

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

    def on_ws_message(self, ws, message):
        """WebSocket message callback"""
        try:
            response_data = json.loads(message)
            self.ten_env.log_info(f"Received response: {message}")

            # Extract result from response
            text = response_data.get("result", "")
            is_final = response_data.get("type", "") == "FIN_TEXT"

            if text:
                if is_final:
                    self.ten_env.log_info("ASR_TEST_POINT_IS_FINAL:{}".format(
                        int(time.time() * 1000)
                    ))
                asyncio.run_coroutine_threadsafe(
                    self._send_text(text, is_final, self.stream_id), self.loop
                )
        except Exception as e:
            self.ten_env.log_error(f"Error processing response: {e}")

    def on_ws_error(self, ws, error):
        """WebSocket error callback"""
        self.ten_env.log_error(f"WebSocket error: {error}")

    def on_ws_close(self, ws, close_status_code, close_msg):
        """WebSocket close callback"""
        self.ten_env.log_warn(f"WebSocket closed: {close_status_code} - {close_msg}")

        if not self.stopped:
            self.ten_env.log_info("Attempting to reconnect...")
            future = asyncio.run_coroutine_threadsafe(self.start_websocket(), self.loop)
            try:
                future.result(timeout=10)  # Wait for reconnection with timeout
            except Exception as e:
                self.ten_env.log_error(f"Reconnection failed: {e}")

    def on_ws_open(self, ws):
        """WebSocket open callback"""
        try:
            # Send start frame
            start_frame = {
                "type": "START",
                "data": {
                    "appid": int(self.config.app_id),
                    "appkey": self.config.api_key,
                    "dev_pid": int(self.config.model),
                    "cuid": str(uuid.uuid4()),
                    "format": self.config.format,
                    "sample": self.config.sample_rate,
                },
            }

            body = json.dumps(start_frame)
            ws.send(body)
            self.ten_env.log_info(f"Sent START frame: {body}")

            # Start audio sending thread
            self.ws_thread = threading.Thread(target=self.send_audio_frames)
            self.ws_thread.daemon = True
            self.ws_thread.start()

        except Exception as e:
            self.ten_env.log_error(f"Error in on_open: {e}")

    async def start_websocket(self):
        try:
            # Generate unique session ID
            url = f"{self.config.url}?sn={str(uuid.uuid1())}"

            # Initialize WebSocket with callbacks
            self.ws = websocket.WebSocketApp(
                url,
                on_message=self.on_ws_message,
                on_error=self.on_ws_error,
                on_close=self.on_ws_close,
                on_open=self.on_ws_open,
            )

            # Run WebSocket in a separate thread
            ws_thread = threading.Thread(target=self.ws.run_forever)
            ws_thread.daemon = True
            ws_thread.start()

            self.ten_env.log_info(f"WebSocket connection initiated: {url}")

        except Exception as e:
            self.ten_env.log_error(f"Failed to start WebSocket connection: {e}")

    def send_audio_frames(self):
        """Thread function to send audio frames"""
        while not self.stopped and self.ws and self.ws.sock:
            try:
                # Get frame from queue with timeout
                try:
                    # Use run_coroutine_threadsafe to get frame from asyncio.Queue
                    frame = asyncio.run_coroutine_threadsafe(
                        self.audio_queue.get(), self.loop
                    ).result(timeout=1)
                except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
                    continue

                if frame is None:
                    # Send any remaining audio frames
                    if len(self.audio_frame) > 0:
                        self.ws.send(
                            bytes(self.audio_frame), websocket.ABNF.OPCODE_BINARY
                        )
                        self.ten_env.log_info(
                            f"Sent final audio frame, size: {len(self.audio_frame)}"
                        )
                    continue

                # Accumulate frames
                self.audio_frame.extend(frame)

                # Send if audio frame size reaches or exceeds chunk_len
                if len(self.audio_frame) >= self.config.chunk_len:
                    self.ten_env.log_debug(
                        f"Sending audio frame, size: {len(self.audio_frame)}"
                    )
                    self.ws.send(bytes(self.audio_frame), websocket.ABNF.OPCODE_BINARY)
                    self.audio_frame = bytearray()  # Reset audio frame
                    # time.sleep(self.config.chunk_ms / 1000.0)  # Control sending rate

            except Exception as e:
                self.ten_env.log_error(f"Error sending audio frame: {e}")
                break
