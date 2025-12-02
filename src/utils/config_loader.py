import yaml
from pathlib import Path
from typing import Dict, Any

def load_config(config_path: str = "configs/base.yaml") -> Dict[str, Any]:
    """
    Loads YAML config and ensures paths are absolute relative to project root.
    """
    # Find the project root
    project_root = Path(__file__).parent.parent.parent
    
    full_path = project_root / config_path
    
    if not full_path.exists():
        raise FileNotFoundError(f"Config file not found at {full_path}")

    with open(full_path, "r") as f:
        config = yaml.safe_load(f)

    return config