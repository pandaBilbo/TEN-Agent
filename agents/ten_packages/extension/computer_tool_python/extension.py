#
# This file is part of TEN Framework, an open source project.
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file for more information.
#
import json
from typing import Any, Dict
from ten_ai_base.llm_tool import AsyncLLMToolBaseExtension
from ten_ai_base.types import LLMToolMetadata, LLMToolMetadataParameter, LLMToolResult

from ten import (
    AsyncTenEnv,
    Cmd,
    Data
)

START_APP_TOOL_NAME = "start_app"
START_APP_TOOL_DESCRIPTION = "start an application with name"

SAVE_TO_NOTEBOOK_TOOL_NAME = "save_to_notebook"
SAVE_TO_NOTEBOOK_TOOL_DESCRIPTION = "call this whenever you need to save anything to notebook"    

class ComputerToolExtension(AsyncLLMToolBaseExtension):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.apps_list = {
            "WeChat": "com.tencent.xinWeChat"
        }

    async def on_init(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_debug("on_init")
        await super().on_init(ten_env)

    async def on_start(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_debug("on_start")
        await super().on_start(ten_env)

    async def on_stop(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_debug("on_stop")
        await super().on_stop(ten_env)

    async def on_deinit(self, ten_env: AsyncTenEnv) -> None:
        ten_env.log_debug("on_deinit")
        await super().on_deinit(ten_env)

    async def on_cmd(self, ten_env: AsyncTenEnv, cmd: Cmd) -> None:
        cmd_name = cmd.get_name()
        ten_env.log_debug("on_cmd name {}".format(cmd_name))

        await super().on_cmd(ten_env, cmd)

    def get_tool_metadata(self, ten_env: AsyncTenEnv) -> list[LLMToolMetadata]:
        ten_env.log_info("get_computer_tool_metadata")
        return [
            LLMToolMetadata(
                name=START_APP_TOOL_NAME,
                description=START_APP_TOOL_DESCRIPTION,
                parameters=[
                    LLMToolMetadataParameter(
                        name="name",
                        type="string",
                        description="The application name to start",
                        required=True,
                    )
                ]
            ),
            LLMToolMetadata(
                name=SAVE_TO_NOTEBOOK_TOOL_NAME,
                description=SAVE_TO_NOTEBOOK_TOOL_DESCRIPTION,
                parameters=[
                    LLMToolMetadataParameter(
                        name="text",
                        type="string",
                        description="The text to save",
                        required=True,
                    )
                ]
            )
        ]

    async def run_tool(self, ten_env: AsyncTenEnv, name: str, args: dict) -> LLMToolResult:
        ten_env.log_info(f"run_tool name {name}")
        if name == START_APP_TOOL_NAME:
            app_name = args.get("name")
            if app_name not in self.apps_list:
                return {"content": json.dumps({"text": f"app {app_name} not found"})}

            result = await self._start_application(args, ten_env)
            return {"content": json.dumps(result)}
        elif name == SAVE_TO_NOTEBOOK_TOOL_NAME:
            text = args.get("text")
            ten_env.log_info(f"will save text to notebook {text}")

            result = await self._save_to_notebook(args, ten_env)
            return {"content": json.dumps(result)}

    async def _start_application(self, args: dict, ten_env: AsyncTenEnv) -> Any:
        app_name = args.get("name")
        app_bundle_id = self.apps_list.get(app_name)
        ten_env.log_info(f"will start app {app_name} with bundle id {app_bundle_id}")
        self._send_data(ten_env, "start_app", {"app_bundle_id": app_bundle_id, "app_name": app_name})
        return {"text": f"just say '{app_name} is starting'"}

    async def _save_to_notebook(self, args: dict, ten_env: AsyncTenEnv) -> Any:
        text = args.get("text")
        ten_env.log_info(f"will save text to notebook {text}")
        self._send_data(ten_env, "save_to_notebook", args)
        return {"text": f"just say 'the content has been saved to notebook'"}

    def _send_data(self, ten_env: AsyncTenEnv, action: str, data: Dict[str, Any]):
        try:
            action_data = json.dumps({
                "data_type": "action",
                "action": action,
                "data": data
            })
            output_data = Data.create("data")
            output_data.set_property_buf("data", action_data.encode("utf-8"))
            ten_env.send_data(output_data)
            ten_env.log_info(f"send data {action_data}")
        except Exception as err:
            ten_env.log_warn(f"send data error {err}")

