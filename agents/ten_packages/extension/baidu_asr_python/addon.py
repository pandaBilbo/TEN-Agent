from ten import (
    Addon,
    register_addon_as_extension,
    TenEnv,
)


@register_addon_as_extension("baidu_asr_python")
class BaiduASRExtensionAddon(Addon):
    def on_create_instance(self, ten: TenEnv, addon_name: str, context) -> None:
        from .extension import BaiduASRExtension

        ten.log_info("on_create_instance")
        ten.on_create_instance_done(BaiduASRExtension(addon_name), context)
