from dataclasses import dataclass
from typing import List


@dataclass
class Word:
    text_time: int
    duration_ms: int
    text: str
    is_final: bool


@dataclass
class AsrResult:
    text_time: int = 0
    language: str = ""
    slice_type: int = 0
    duration_ms: int = 0
    words: List[Word] = None
    voice_text_str: str = ""
    is_final: bool = False
