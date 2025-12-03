import ray
import logging
import re
from typing import Dict, Any, Set
import pyarrow as pa
import pyarrow.compute as pc
from datasketch import MinHash, MinHashLSH

logger = logging.getLogger("TokenTurbine")


class MinHashDeduplicator:
    """
    Worker class for MinHash-based deduplication.
    Maintains a global LSH index across batches (when using single actor).
    
    Note: Memory usage grows with dataset size. For very large datasets (>10M docs),
    consider using disk-backed LSH or processing in chunks.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.num_perm = config.get("num_perm", 128)
        self.threshold = config.get("threshold", 0.85)
        self.ngram_size = config.get("ngram_size", 5)
        self.seed = config.get("seed", 42)
        self.max_lsh_items = config.get("max_lsh_items", 1_000_000)
        
        # Global LSH index (persists across batches)
        self.lsh = MinHashLSH(threshold=self.threshold, num_perm=self.num_perm)
        self.seen_docs = set()
        
        # Statistics
        self.total_processed = 0
        
        logger.info(
            f"MinHash LSH initialized: num_perm={self.num_perm}, "
            f"threshold={self.threshold}, ngram_size={self.ngram_size}"
        )
    
    def _get_shingles(self, text: str) -> Set[str]:
        """Create word n-gram shingles from text."""

        # Normalize: lowercase and remove punctuation
        text = re.sub(r'[^\w\s]', '', text.lower())
        tokens = text.split()
        
        if len(tokens) < self.ngram_size:
            # For very short texts, use entire text as single shingle
            return {text} if text else set()
        
        # Create n-grams
        shingles = {
            " ".join(tokens[i:i + self.ngram_size])
            for i in range(len(tokens) - self.ngram_size + 1)
        }
        
        return shingles
    
    def __call__(self, batch: pa.Table) -> pa.Table:
        """
        Process batch: compute MinHash and filter duplicates using LSH.
        """
        if 'text' not in batch.column_names or len(batch) == 0:
            return batch
        
        doc_ids = batch['doc_id'].to_pylist()
        texts = batch['text'].to_pylist()
        
        docs_to_keep = []
        
        for doc_id, text in zip(doc_ids, texts):
            self.total_processed += 1
            
            # Check memory limit
            if len(self.seen_docs) >= self.max_lsh_items:
                if self.total_processed == self.max_lsh_items + 1:  # Log once
                    logger.warning(
                        f"⚠️  LSH index reached capacity ({self.max_lsh_items:,} items). "
                        f"Accepting remaining documents without deduplication. "
                        f"Consider: (1) increasing max_lsh_items, (2) processing in chunks, "
                        f"or (3) using disk-backed LSH for larger datasets."
                    )
                docs_to_keep.append(doc_id)
                continue
            
            # Compute MinHash signature
            m = MinHash(num_perm=self.num_perm, seed=self.seed)
            shingles = self._get_shingles(text)
            
            # Handle empty shingles
            if not shingles:
                docs_to_keep.append(doc_id)  # Keep documents with no shingles
                continue
            
            for shingle in shingles:
                m.update(shingle.encode('utf8'))
            
            # Query LSH for similar documents
            similar_docs = self.lsh.query(m)
            
            if not similar_docs:
                # No similar document found, keep this one
                self.lsh.insert(doc_id, m)
                self.seen_docs.add(doc_id)
                docs_to_keep.append(doc_id)
        
        # Filter batch to keep only non-duplicate documents
        if not docs_to_keep:
            return batch.slice(0, 0)  # Empty batch with schema
        
        keep_mask = pc.is_in(batch['doc_id'], pa.array(docs_to_keep))
        return batch.filter(keep_mask)


class ExactDeduplicator:
    """
    Exact deduplication using a set to track seen doc_ids.
    Since doc_id is xxhash of text, identical texts have identical doc_ids.
    """
    
    def __init__(self):
        self.seen_ids = set()
        self.total_processed = 0
        
    def __call__(self, batch: pa.Table) -> pa.Table:
        """
        Filter out documents with doc_ids we've already seen.
        """
        if 'doc_id' not in batch.column_names or len(batch) == 0:
            return batch
        
        doc_ids = batch['doc_id'].to_pylist()
        
        # Find which doc_ids are new (not seen before)
        new_doc_ids = []
        for doc_id in doc_ids:
            self.total_processed += 1
            
            if doc_id not in self.seen_ids:
                self.seen_ids.add(doc_id)
                new_doc_ids.append(doc_id)
            
        # Filter batch to keep only new documents
        if not new_doc_ids:
            return batch.slice(0, 0)  
        
        keep_mask = pc.is_in(batch['doc_id'], pa.array(new_doc_ids))
        return batch.filter(keep_mask)


class DeduplicationStep:
    """
    Two-stage deduplication pipeline:
    1. Exact deduplication (based on doc_id hash) - Fast
    2. Fuzzy deduplication (MinHash LSH) - Slower but catches near-duplicates
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.enabled = config.get("enabled", True)
        
        # Validate configuration
        if self.enabled:
            num_perm = config.get("num_perm", 128)
            threshold = config.get("threshold", 0.85)
            
            if num_perm < 64 or num_perm > 256:
                logger.warning(
                    f"num_perm={num_perm} is outside typical range (64-256). "
                    f"This may affect accuracy or performance."
                )
            
            if threshold < 0.7 or threshold > 0.95:
                logger.warning(
                    f"threshold={threshold} is outside typical range (0.7-0.9). "
                    f"Low thresholds may remove non-duplicates!"
                )
    
    def _exact_dedup(self, ds: ray.data.Dataset) -> ray.data.Dataset:
        """
        Exact deduplication by tracking seen doc_ids.
        Since doc_id is xxhash of text, identical texts have same ID.
        """
        logger.info("Stage 1: Exact deduplication...")
        
        # Use map_batches with a stateful actor
        # Single actor maintains a set of seen doc_ids across all batches
        deduped = ds.map_batches(
            ExactDeduplicator,
            batch_format="pyarrow",
            batch_size=2000,
            compute=ray.data.ActorPoolStrategy(
                min_size=1,
                max_size=1  # Single actor to maintain global state
            )
        )
        
        return deduped
    
    def _fuzzy_dedup(self, ds: ray.data.Dataset) -> ray.data.Dataset:
        """
        Fuzzy deduplication using MinHash LSH.
        """
        logger.info("Stage 2: Fuzzy deduplication with MinHash LSH...")
        
        # Use single actor to maintain global LSH index across all batches
        deduped = ds.map_batches(
            MinHashDeduplicator,
            batch_format="pyarrow",
            batch_size=2000, 
            fn_constructor_args=[self.config],
            compute=ray.data.ActorPoolStrategy(
                min_size=1,
                max_size=1 
            )
        )
        
        return deduped
    
    def run(self, ds: ray.data.Dataset) -> ray.data.Dataset:
        """Execute the deduplication pipeline."""
        if not self.enabled:
            logger.info("Deduplication disabled, skipping...")
            return ds
        
        # Stage 1: Exact deduplication
        ds = self._exact_dedup(ds)
        
        # Stage 2: Fuzzy deduplication
        ds = self._fuzzy_dedup(ds)
        
        logger.info("="*60)
        logger.info("Deduplication completed")
        logger.info("="*60)
        
        return ds