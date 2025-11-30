import ray
import logging
import tiktoken
import statistics
import pyarrow as pa
import pyarrow.compute as pc
from typing import Dict, Any

logger = logging.getLogger("TokenTurbine")

class TokenizationWorker:
    """
    Worker Actor that handles the tokenization logic.
    Using an Actor allows us to cache the tokenizer in memory (avoiding reload overhead).
    """
    def __init__(self, config: Dict[str, Any]):
        self.model_name = config.get("tokenizer_model", "gpt2")
        self.keep_text = config.get("keep_text", True)
        self.add_special_tokens = config.get("add_special_tokens", False)
        self.max_length = config.get("max_length", None)  # For truncation
        self.tokenizer = None

        # Statistics
        self.total_tokens = 0
        self.total_docs = 0
        self.docs_truncated = 0
        self.empty_token_docs = 0
        self.very_long_docs = 0

    def _get_tokenizer(self):
        """Lazy load the tokenizer."""
        if self.tokenizer is None:
            try:
                # Disabling special tokens to prevent injection attacks
                # (e.g. users injecting <|endoftext|> to break context)
                self.tokenizer = tiktoken.get_encoding(self.model_name)
            except Exception:
                logger.warning(f"Could not load {self.model_name}, falling back to gpt2")
                self.tokenizer = tiktoken.get_encoding("gpt2")
        return self.tokenizer

    def __call__(self, batch: pa.Table) -> pa.Table:
        """
        Tokenizes a batch of text using PyArrow for jagged list support.
        """

        # Validation
        if 'text' not in batch.column_names or len(batch) == 0:
            return batch.slice(0, 0)

        enc = self._get_tokenizer()
        texts = batch['text'].to_pylist()

        all_token_ids = []
        all_token_counts = []

        # Determine allowed special tokens
        if self.add_special_tokens:
            allowed_special = 'all'  # Allow all special tokens
        else:
            allowed_special = set()  # Prevent special token injection
        
        for text in texts:
            try:
                # Encode text
                token_ids = enc.encode(text, allowed_special=allowed_special)

                # Validation: Check for encoding issues
                if len(token_ids) == 0:
                    self.empty_token_docs += 1
                    if self.empty_token_docs <= 5:  # Log first few
                        logger.warning(
                            f"Document produced 0 tokens (may be empty or invalid). "
                            f"Text length: {len(text)}"
                        )

                # Validation: Check for unusually long documents
                if len(token_ids) > 100000:  # 100k tokens is very long
                    self.very_long_docs += 1
                    if self.very_long_docs <= 5:  # Log first few
                        logger.warning(
                            f"Document has {len(token_ids):,} tokens (unusually long). "
                            f"Consider chunking for training."
                        )

                # Truncate if necessary
                if self.max_length and len(token_ids) > self.max_length:
                    token_ids = token_ids[:self.max_length]
                    self.docs_truncated += 1
                
                all_token_ids.append(token_ids)
                all_token_counts.append(len(token_ids))
                
                # Update statistics
                self.total_tokens += len(token_ids)
                self.total_docs += 1
                
            except Exception as e:
                logger.error(f"Tokenization failed for document: {e}")
                all_token_ids.append([])
                all_token_counts.append(0)
                # Add empty token list for failed documents
                self.empty_token_docs += 1
        
        # Periodic logging
        if self.total_docs % 10000 == 0 and self.total_docs > 0:
            avg_tokens = self.total_tokens / self.total_docs
            logger.info(
                f"Tokenization progress: {self.total_docs:,} docs, "
                f"{self.total_tokens:,} total tokens, "
                f"avg {avg_tokens:.1f} tokens/doc, "
                f"{self.docs_truncated} truncated"
            )

        # Build output table
        output_dict = {
            'doc_id': batch['doc_id'],
            # pa.list_ handles jagged arrays perfectly
            'input_ids': pa.array(all_token_ids, type=pa.list_(pa.int64())),
            'token_count': pa.array(all_token_counts, type=pa.int64()),
        }
        
        # Preserve metadata columns
        metadata_cols = ['source', 'url', 'ingest_time']
        for col in metadata_cols:
            if col in batch.column_names:
                output_dict[col] = batch[col]
        
        # Optionally keep text (useful for debugging, but increases size)
        if self.keep_text:
            output_dict['text'] = batch['text']
        
        return pa.Table.from_pydict(output_dict)

class TokenizationStep:
    """
    Orchestrator for the Tokenization Step.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.enabled = config.get("enabled", True)
        # We can override output_path via config, or pass it in run()
        self.output_path = config.get("output_path", "data/processed/tokenized")
        # New config options
        self.export_format = config.get("export_format", "parquet")

        # Validate export format
        if self.export_format not in ["parquet", "jsonl", "json"]:
            logger.warning(
                f"Unknown export_format '{self.export_format}', "
                f"defaulting to 'parquet'"
            )
            self.export_format = "parquet"

    def _log_statistics(self, ds: ray.data.Dataset):
        """Log tokenization statistics."""
        logger.info("Computing final statistics...")
        
        # Smart Sampling: Don't scan 10TB for a log message
        sample_size = 10000
        sample = ds.take(sample_size) if ds.count() > sample_size else ds.take_all()
        
        token_counts = [doc['token_count'] for doc in sample]
        
        if token_counts:
            avg_tokens = statistics.mean(token_counts)
            max_tokens = max(token_counts)
            
            logger.info("="*60)
            logger.info("Tokenization Statistics (Sampled):")
            logger.info(f"  Avg tokens/doc:  {avg_tokens:.1f}")
            logger.info(f"  Max tokens:      {max_tokens:,}")
            logger.info("="*60)

    def run(self, ds: ray.data.Dataset, output_path: str) -> ray.data.Dataset:
        """
        Execute tokenization and export.
        
        Args:
            ds: Input dataset
            output_path: Output path (overrides config if provided)
            
        Returns:
            Tokenized dataset
        """
        if not self.enabled:
            logger.info("Tokenization disabled. Skipping export.")
            return

        target_path = output_path or self.output_path
        model_name = self.config.get("tokenizer_model", "gpt2")

        logger.info("="*60)
        logger.info(f"Starting Tokenization (Model: {model_name})")
        logger.info(f"  Model: {model_name}")
        logger.info(f"  Format: {self.export_format}")
        logger.info(f"  Output: {target_path}")
        logger.info("="*60)
        
        # 1. Tokenize using Actors
        tokenized_ds = ds.map_batches(
            TokenizationWorker,
            batch_format="pyarrow",
            batch_size=2000,
            fn_constructor_args=[self.config],
            compute=ray.data.ActorPoolStrategy(min_size=1, max_size=4)
        )
        
        # 2. Write to Disk (Sharded)
        # Ray automatically splits the data into multiple files (shards) based on blocks.
        # We use Snappy compression which is the industry standard for Parquet/ML.
        try:
            if self.export_format == "jsonl":
                logger.info(f"Writing JSONL to {target_path}...")
                # ray.data.write_json handles the newline delimiting automatically
                tokenized_ds.write_json(target_path, force_ascii=False)
            else:
                logger.info(f"Writing Parquet files to {target_path}...")
                tokenized_ds.write_parquet(target_path, compression="snappy")

            # Read back purely for verification/stats (lazy read)
            # In a real 10TB run, you might skip this or rely on write logs.
            logger.info(f"✅ Tokenization complete! Data saved to {target_path}")

        except Exception as e:
            logger.error(f"Failed to write tokenized data: {e}")
            raise e
        
        logger.info("="*60)
        logger.info("Tokenization completed")
        logger.info("="*60)

        return tokenized_ds