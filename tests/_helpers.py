"""共享测试工具。"""

import os
import tempfile


class TempDbMixin:
    def setUp(self):  # noqa: N802 - unittest 命名
        fd, self.db_path = tempfile.mkstemp(prefix="mqtt_", suffix=".db")
        os.close(fd)
        # 让文件以干净状态开始
        os.unlink(self.db_path)

    def tearDown(self):  # noqa: N802
        for suffix in ("", "-wal", "-shm"):
            p = self.db_path + suffix
            if os.path.exists(p):
                os.unlink(p)
