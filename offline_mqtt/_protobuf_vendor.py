"""把仓库内置的纯 Python protobuf 运行库加入 ``sys.path``。

本仓库不假设机器上安装了 ``google.protobuf``（也不要求 protoc、不联网）：
发布时把官方纯 Python wheel 解包到 ``vendor/protobuf/``，本模块在导入
``google.protobuf`` 之前把该目录插入 ``sys.path``。若运行环境已经安装了
protobuf，则优先使用环境自带版本（vendor 目录追加在末尾）。
"""

from __future__ import annotations

import os
import sys

_VENDOR_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "vendor",
    "protobuf",
)

if os.path.isdir(_VENDOR_DIR) and _VENDOR_DIR not in sys.path:
    # append 而非 insert(0)：环境里若已有官方 protobuf，保持其优先级。
    sys.path.append(_VENDOR_DIR)
