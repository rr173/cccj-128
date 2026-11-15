#!/usr/bin/env python3
"""测试运行器：可在 ``python -I`` 隔离模式下运行整套 unittest。

-I 模式不会把 cwd 自动加入 sys.path，因此在导入前显式注入仓库根目录。
"""

import os
import sys
import unittest

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, REPO_ROOT)

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=os.path.join(REPO_ROOT, "tests"),
                            top_level_dir=REPO_ROOT)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
