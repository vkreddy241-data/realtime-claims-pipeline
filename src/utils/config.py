import os
from functools import lru_cache
from pathlib import Path
from typing import Optional
import yaml

@lru_cache(maxsize=4)
def load_config(env: str = "dev", path: Optional[str] = None) -> dict:
    if path:
        config_file = Path(path)
    else:
        repo_root   = Path(__file__).resolve().parents[3]
        config_file = repo_root / "configs" / f"{env}.yaml"
    if not config_file.exists():
        raise FileNotFoundError(f"Config not found: {config_file}")
    with open(config_file) as f:
        cfg = yaml.safe_load(f)
    return _apply_env_overrides(cfg)

def _apply_env_overrides(cfg: dict) -> dict:
    def _replace(obj):
        if isinstance(obj, dict):
            return {k: _replace(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_replace(i) for i in obj]
        if isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
            return os.environ.get(obj[2:-1], obj)
        return obj
    return _replace(cfg)
