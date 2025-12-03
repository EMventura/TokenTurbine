import glob
import logging
import os

logger = logging.getLogger("TokenTurbine") 

def consolidate_json_shards(temp_dir: str, output_path: str):
    """
    Consolidate Ray's sharded JSON output into a single JSONL file.
    
    Args:
        temp_dir: Directory containing JSON shards
        output_path: Desired output file path
    """
    json_files = sorted(glob.glob(os.path.join(temp_dir, "*.json")))
    
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {temp_dir}")
    
    logger.info(f"Consolidating {len(json_files)} shard(s) into single file...")
    
    with open(output_path, 'w', encoding='utf-8') as outfile:
        for json_file in json_files:
            logger.debug(f"  Merging: {os.path.basename(json_file)}")
            with open(json_file, 'r', encoding='utf-8') as infile:
                for line in infile:
                    outfile.write(line)
    
    logger.info(f"Consolidated to: {output_path}")


def prepare_for_export(batch):
    """Select only required columns"""
    columns_to_keep = ['doc_id', 'text']
            
    for col in ['url', 'source']:
        if col in batch.column_names:
            columns_to_keep.append(col)
            
    return batch.select(columns_to_keep)