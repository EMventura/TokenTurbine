import glob
import logging
import os

# def consolidate_to_single_jsonl(output_path: str, shard_pattern: str):
#     """
#     Consolidate Ray's sharded output into a single JSONL file.
    
#     Args:
#         output_path: Desired single output file path
#         shard_pattern: Pattern to find shard files (e.g., "data/processed/*.json")
#     """
    
#     logger.info("Consolidating shard files into single JSONL...")
    
#     output_dir = os.path.dirname(output_path)
#     shard_files = sorted(glob.glob(os.path.join(output_dir, "*.json")))
    
#     if not shard_files:
#         logger.error(f"No shard files found in {output_dir}")
#         return
    
#     logger.info(f"Found {len(shard_files)} shard files to consolidate")
    
#     with open(output_path, 'w', encoding='utf-8') as outfile:
#         for shard_file in shard_files:
#             logger.info(f"  Merging: {os.path.basename(shard_file)}")
#             with open(shard_file, 'r', encoding='utf-8') as infile:
#                 for line in infile:
#                     outfile.write(line)
#             # Delete shard file after merging
#             os.remove(shard_file)
    
#     logger.info(f"✅ Consolidated to: {output_path}")

def prepare_for_export(batch):
    """Select only required columns"""
    columns_to_keep = ['doc_id', 'text']
            
    # Optionally keep metadata for traceability
    for col in ['url', 'source']:
        if col in batch.column_names:
            columns_to_keep.append(col)
            
    return batch.select(columns_to_keep)