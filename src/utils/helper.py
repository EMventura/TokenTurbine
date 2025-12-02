import os
import logging
import ray

logger = logging.getLogger("TokenTurbine")

def ensure_directory(path: str):
    """Ensure a directory exists, create if needed."""
    os.makedirs(path, exist_ok=True)

def compute_count_safe(ds: ray.data.Dataset, label: str = "dataset") -> int:
    """
    Safely compute dataset count with error handling.
    
    Returns count or -1 if counting fails.
    """
    try:
        count = ds.count()
        logger.info(f"{label}: {count:,} documents")
        return count
    except Exception as e:
        logger.warning(f"Could not count {label}: {e}")
        return -1