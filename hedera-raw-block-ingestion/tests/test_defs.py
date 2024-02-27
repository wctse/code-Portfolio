import os

os.environ['IS_TEST'] = 'True'

def test_defs_imports():
    from pipelines.core.extract import defs
    pass
