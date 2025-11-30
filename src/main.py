import argparse
import logging
import ray
import sys
import os
import glob
import shutil
from data_load import IngestionStep
from filtering import QualityFilterStep
from deduplication import DeduplicationStep
from tokenization import TokenizationStep
from utils.config_loader import load_config
from utils.single_jsonl import prepare_for_export


# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("TokenTurbine") 

def ensure_directory(path: str):
    """Ensure a directory exists, create if needed."""
    os.makedirs(path, exist_ok=True)

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
    
    logger.info(f"✅ Consolidated to: {output_path}")

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

def main(config_path: str):
    # 2. Load Configuration (The Single Source of Truth)
    try:
        config = load_config(config_path)
        logger.info(f"Configuration loaded from {config_path}")
    except FileNotFoundError as e:
        logger.critical(f"Config file not found: {config_path}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Failed to load config: {e}")
        return

    # 3. Initialize Ray (Systems Level)
    # We pass the address from config if we want to connect to a cluster later
    ray_address = config['system'].get('ray_address', 'auto')
    if ray.is_initialized():
        ray.shutdown()
    
    try:
        ray.init(address=ray_address, ignore_reinit_error=True)
        logger.info(f"✅ Ray initialized at {ray_address}")
    except Exception as e:
        logger.critical(f"Failed to initialize Ray: {e}")
        sys.exit(1)
    
    compute_counts = config.get('compute_counts', True)

    try:
        # ==========================================
        # STEP 1: INGESTION
        # ==========================================
        logger.info("="*60)
        logger.info("Starting Step 1: Ingestion")
        logger.info("="*60)

        try:
            dataset = IngestionStep(config['ingestion']).run()
        except Exception as e:
                logger.critical(f"Ingestion failed: {e}", exc_info=True)
                sys.exit(1)

        # Dependency Injection: We pass ONLY the 'ingestion' part of the config
        #ingestion_conf = config['ingestion']
        
        #ingest_step = IngestionStep(ingestion_conf)
        #dataset = ingest_step.run()

        # Count INPUT (Optional, usually cheap for JSONL/Parquet metadata)
        # If this is too slow on 10TB, we skip it and just use file size.
        # try:
        #     total_input_rows = dataset.count()
        #     logger.info(f"Initial Input Rows: {total_input_rows:,}")
        # except:
        #     total_input_rows = None
        
        if compute_counts:
            initial_count = compute_count_safe(dataset, "After ingestion")

        # ==========================================
        # STEP 2: LANGUAGE & QUALITY FILTERING (The Funnel)
        # ==========================================
        if config['filtering']['enabled']:
            logger.info("="*60)
            logger.info("Starting Step 2: Filtering")
            logger.info("="*60)
            try:
                dataset = QualityFilterStep(config['filtering']).run(dataset)
            except Exception as e:
                logger.critical(f"Filtering failed: {e}", exc_info=True)
                sys.exit(1)

            if compute_counts:
                after_filter_count = compute_count_safe(dataset, "After filtering")

        # ==========================================
        # STEP 3: DEDUPLICATION 
        # ==========================================
        if config['deduplication']['enabled']:
            logger.info("="*60)
            logger.info("Starting Step 3: Deduplication")
            logger.info("="*60)

            try:
                dataset = DeduplicationStep(config['deduplication']).run(dataset)
            except Exception as e:
                    logger.critical(f"Deduplication failed: {e}", exc_info=True)
                    sys.exit(1)

            if compute_counts:
                after_dedup_count = compute_count_safe(dataset, "After deduplication")
        # ==========================================
        # STEP 4: EXPORT CLEANED TEXT
        # ==========================================
        logger.info("="*60)
        logger.info("Starting Step 4: Export Cleaned Dataset")
        logger.info("="*60)
        output_path = config['paths'].get('output_file', 'data/processed/cleaned_dataset.jsonl')
        temp_output_dir = output_path + "_temp"
        
        # Ensure output directory exists
        ensure_directory(os.path.dirname(output_path))
        ensure_directory(temp_output_dir)
        
        try:
            export_ds = dataset.map_batches(
                prepare_for_export,
                batch_format="pyarrow"
            )

            # Write to temp directory
            logger.info(f"Writing to temporary directory: {temp_output_dir}")
            export_ds.write_json(temp_output_dir, force_ascii=False)

            # Consolidate shards into single file
            consolidate_json_shards(temp_output_dir, output_path)

            # Get file size for reporting
            file_size_mb = os.path.getsize(output_path) / (1024**2)
            logger.info(f"Output file size: {file_size_mb:.1f} MB")

        except Exception as e:
            logger.critical(f"Export failed: {e}", exc_info=True)
            sys.exit(1)
        finally:
            # Clean up temp directory
            if os.path.exists(temp_output_dir):
                shutil.rmtree(temp_output_dir)
                logger.debug(f"Cleaned up temp directory: {temp_output_dir}")

        # # This forces all data into one partition, creating a single output file
        # logger.info("Repartitioning to single file...")
        # export_ds_single = export_ds.repartition(1) 
        
        # # Write to temp directory (Ray creates shards)
        # logger.info(f"Writing dataset...")
        # os.makedirs(temp_output_dir, exist_ok=True)
        # export_ds.write_json(temp_output_dir)

        # Ray creates a file like: "data_0.json" in the temp directory
        # Find and move it to the desired output path
        # json_files = glob.glob(os.path.join(temp_output_dir, "*.json"))
        
        # if len(json_files) == 1:
        #     # Move the single file to final location
        #     shutil.move(json_files[0], output_path)
        #     logger.info(f"✅ Single JSONL file created: {output_path}")
        # elif len(json_files) > 1:
        #     # Fallback: if somehow multiple files were created, consolidate
        #     logger.warning(f"Expected 1 file but found {len(json_files)}, consolidating...")
        #     with open(output_path, 'w', encoding='utf-8') as outfile:
        #         for json_file in sorted(json_files):
        #             with open(json_file, 'r', encoding='utf-8') as infile:
        #                 for line in infile:
        #                     outfile.write(line)
        #     logger.info(f"✅ Consolidated to: {output_path}")
        # else:
        #     logger.error("No output files found!")
        #     raise FileNotFoundError(f"No JSON files in {temp_output_dir}")
        
        # # Clean up temp directory
        # shutil.rmtree(temp_output_dir)

        # ==========================================
        # STEP 5: TOKENIZATION 
        # ==========================================
        if config['tokenization']['enabled']:
            logger.info("="*60)
            logger.info("Starting Step 5: Tokenization")
            logger.info("="*60)
            tokenization_dir = config['paths']['processed_dir']
            
            # Clean tokenization directory if it exists (for reproducibility)
            if os.path.exists(tokenization_dir):
                logger.info(f"Cleaning existing tokenization dir: {tokenization_dir}")
                shutil.rmtree(tokenization_dir)
            
            # Run Tokenization (Writes to disk)
            # Note: We capture the returned DS, but the heavy work happens during write
            try:
                TokenizationStep(config['tokenization']).run(dataset, tokenization_dir)
            except Exception as e:
                logger.critical(f"Tokenization failed: {e}", exc_info=True)
                sys.exit(1)

        # ==========================================
        # FINAL REPORT 
        # ==========================================
        logger.info("\n" + "="*60)
        logger.info("🎉 PIPELINE COMPLETE!")
        logger.info("="*60)
        logger.info(f"Status:           SUCCESS")
        logger.info(f"Output file:      {output_path}")
        logger.info(f"File size:        {file_size_mb:.1f} MB")
        
        if compute_counts:
            if 'initial_count' in locals() and initial_count > 0:
                logger.info(f"Initial docs:     {initial_count:,}")
            if 'after_filter_count' in locals() and after_filter_count > 0:
                logger.info(f"After filtering:  {after_filter_count:,}")
            if 'after_dedup_count' in locals() and after_dedup_count > 0:
                logger.info(f"After dedup:      {after_dedup_count:,}")
                if initial_count > 0:
                    retention = (after_dedup_count / initial_count * 100)
                    logger.info(f"Retention rate:   {retention:.1f}%")
        
        if config['tokenization']['enabled']:
            logger.info(f"Tokenized output: {tokenization_dir}")
        
        logger.info("="*60)
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        ray.shutdown()
        logger.info("Ray shutdown successfully.")

if __name__ == "__main__":
    # This allows you to run: python main.py --config configs/experiment_1.yaml
    parser = argparse.ArgumentParser(description="TokenTurbine: LLM Data Prep Pipeline")
    parser.add_argument(
        "--config", 
        type=str, 
        default="configs/base.yaml", 
        help="Path to the YAML configuration file"
    )
    
    args = parser.parse_args()
    main(args.config)