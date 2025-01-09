from ten import (
    Addon,
    register_addon_as_extension,
    TenEnv,
)


@register_addon_as_extension("tencent_asr_python")
class TencentASRExtensionAddon(Addon):
    def on_create_instance(self, ten: TenEnv, addon_name: str, context) -> None:
        from .extension import TencentASRExtension

        ten.log_info("on_create_instance")
        ten.on_create_instance_done(TencentASRExtension(addon_name), context)
