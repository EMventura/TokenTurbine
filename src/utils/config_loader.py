import yaml
from pathlib import Path
from typing import Dict, Any

def load_config(config_path: str = "configs/base.yaml") -> Dict[str, Any]:
    """
    Loads YAML config and ensures paths are absolute relative to project root.
    """
    # Find the project root (assuming this script is in src/utils/)
    # Adjust .parent calls depending on your actual structure
    project_root = Path(__file__).parent.parent.parent
    
    full_path = project_root / config_path
    
    if not full_path.exists():
        raise FileNotFoundError(f"Config file not found at {full_path}")

    with open(full_path, "r") as f:
        config = yaml.safe_load(f)

    # OPTIONAL: Resolution logic (if you want to make paths absolute automatically)
    # This prevents "File not found" errors when running from different dirs
    # config['ingestion']['input_path'] = str(project_root / config['ingestion']['input_path'])
    
    return config