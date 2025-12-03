import ray
import logging
import fasttext
import pyarrow as pa
import pyarrow.compute as pc
from typing import Dict, Any, List, Tuple
import re

logger = logging.getLogger("TokenTurbine")

class FastTextPredictor:
    """
    The Worker Class (Actor) that handles Language Identification, PII detection, and Toxicity filtering.
    Combines model-based (FastText) and heuristic-based (Regex/Keywords) filters.
    Ray will instantiate this class on every worker node.
    """
    def __init__(self, config: Dict[str, Any]):
        self.lang_model_path = config.get("language_model_path", "data/lid.176.bin")
        self.target_lang = config.get("target_language", "__label__en")
        self.min_lang_score = config.get("min_lang_score", 0.65)
        self.max_punc_ratio = config.get("max_punc_ratio", 0.3)
        self.sample_chars = config.get("sample_chars_for_langid", 1000)
        self.enable_pii = config.get("enable_pii", True)
        self.pii_action = config.get("pii_action", "redact") # "redact" or "drop"
        self.enable_toxicity = config.get("enable_toxicity", True)
        self.max_toxic_ratio = config.get("max_toxic_ratio", 0.001)
        self.model = None

        # Compiled Regex for performance
        # Email: Standard wide coverage
        self.email_re = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        # IPv4: Simple octet check
        self.ip_re = re.compile(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b')
        # Phone: US/International formats 
        self.phone_re = re.compile(r'\b(?:\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b')

        # Toxicity: Focus on hate speech and slurs rather than all profanity
        # Filter out common false positives like "kill time", "sex education"
        toxic_words = config.get("toxic_words", [
            "nigger", "nigga", "chink", "kike", "fag", "faggot", "tranny",
            "retard", "spic", "wetback", "gook", "cunt", "whore", "slut"
        ])

        # Pre-compile regex for all toxic words: \b(bad1|bad2|bad3)\b
        if toxic_words:
            self.toxicity_re = re.compile(
                r'\b(' + '|'.join(map(re.escape, toxic_words)) + r')\b', 
                re.IGNORECASE
            )
        else:
            self.toxicity_re = None

    def _load_model(self):
        """Lazy loading of the FastText model on the worker node."""
        if self.model is None:
            try:
                fasttext.FastText.eprint = lambda x: None
                self.model = fasttext.load_model(self.lang_model_path)
            except FileNotFoundError as e:
                logger.critical(f"FastText model not found at {self.lang_model_path}")
                raise e
            except Exception as e:
                logger.critical(f"Failed to load FastText model: {e}")
                raise e

    def _smart_sample(self, text: str, sample_size: int) -> str:
        """
        Smart sampling that avoids headers/boilerplate.
        Takes sample from 25% mark to avoid common header patterns.
        """
        if len(text) <= sample_size:
            return text
        
        # Start from 25% into document to skip headers
        start = len(text) // 4
        end = start + sample_size
        
        # Ensure we don't exceed text length
        if end > len(text):
            start = max(0, len(text) - sample_size)
            end = len(text)
        
        return text[start:end]

    def _predict_language(self, texts: List[str]) -> Tuple[List[str], List[float]]:
        """
        Batch prediction using FastText with optimizations.
        """
        self._load_model()
        
        # We clean the text only for the model's eyes. 
        # We do not overwrite the actual dataframe column yet.

        cleaned_for_model = []
        for t in texts:
            # Smart sampling from middle of document
            sample = self._smart_sample(t, self.sample_chars)
            # Normalize whitespace for model
            sample = sample.replace("\n", " ").replace("\r", " ")
            sample = re.sub(r'\s+', ' ', sample).strip() 
            cleaned_for_model.append(sample)
            
        # k=1 returns the top language guess
        try:
            labels, scores = self.model.predict(cleaned_for_model, k=1)
            flat_labels = [l[0] for l in labels]
            flat_scores = [s[0] for s in scores]
            return flat_labels, flat_scores
        except Exception as e:
            logger.error(f"FastText prediction failed: {e}")
            return ["__label__unknown"] * len(texts), [0.0] * len(texts)

    def _handle_pii(self, text: str) -> Tuple[str, bool]:
        """
        Detects PII.
        Returns: (processed_text, has_pii_flag)
        """
        has_pii = False
        
        # Helper to track detection without modifying text if action is 'drop'
        def check_and_replace(pattern, replacement, txt):
            nonlocal has_pii
            if self.pii_action == "drop":
                if pattern.search(txt):
                    has_pii = True
                return txt # Don't waste time replacing if we drop
            else:
                # Redact
                new_txt, count = pattern.subn(replacement, txt)
                if count > 0: 
                    has_pii = True
                return new_txt

        text = check_and_replace(self.email_re, "<EMAIL>", text)
        if has_pii and self.pii_action == "drop": return text, True
        
        text = check_and_replace(self.ip_re, "<IP>", text)
        if has_pii and self.pii_action == "drop": return text, True
        
        text = check_and_replace(self.phone_re, "<PHONE>", text)
        
        return text, has_pii

    def _check_toxicity(self, text: str) -> bool:
        """Returns True if text contains toxic keywords based on ratio threshold.."""

        if not self.toxicity_re:
            return False

        # If threshold is 0, use search (Fastest)
        if self.max_toxic_ratio <= 0.0:
            return bool(self.toxicity_re.search(text))
            
        # Otherwise, count matches to calculate density
        matches = self.toxicity_re.findall(text)
        if not matches:
            return False
            
        # Approximate word count 
        # We use a simple split to approximate total tokens
        word_count = len(text.split())
        if word_count == 0:
            return True # Edge case: contains toxic word but no whitespace? Drop it.
            
        ratio = len(matches) / word_count
        
        # Drop only if ratio exceeds threshold
        return ratio > self.max_toxic_ratio

    def _compute_punc_ratio(self, text_array: pa.Array) -> pa.Array:
        """
        Vectorized punctuation ratio calculation using PyArrow.
        Ratio = (Non-AlphaNumeric Chars) / (Total Chars)
        This approximates the ratio by:
        1. Counting total characters (utf8_length)
        2. Counting alphanumeric characters (regex match)
        3. Computing ratio = (total - alphanum - spaces) / total
        """
    
        texts = text_array.to_pylist()
        
        punc_ratios = []
        for text in texts:
            if len(text) == 0:
                punc_ratios.append(1.0)
                continue
            alphanum_space_count = sum(1 for c in text if c.isalnum() or c.isspace())
            punc_ratio = (len(text) - alphanum_space_count) / len(text)
            punc_ratios.append(punc_ratio)
        
        return pa.array(punc_ratios, type=pa.float64())
    
    def __call__(self, batch: pa.Table) -> pa.Table:
        """
        Ray calls this method for every batch.
        """
        if 'text' not in batch.column_names or len(batch) == 0:
            return batch

        initial_rows = len(batch) 

        # Language Identification
        texts = batch['text'].to_pylist()
        labels, scores = self._predict_language(texts)
        
        # Add temporary columns
        batch = batch.append_column("lang_label", pa.array(labels, type=pa.string()))
        batch = batch.append_column("lang_score", pa.array(scores, type=pa.float64()))
        
        # Filter by Language
        is_target = pc.equal(batch['lang_label'], self.target_lang)
        is_confident = pc.greater(batch['lang_score'], self.min_lang_score)
        keep_mask = pc.and_(is_target, is_confident)

        batch = batch.filter(keep_mask)
        
        if len(batch) == 0:
            return batch.drop(['lang_label', 'lang_score'])

        # Content Safety (PII & Toxicity) 
        # Switch to Python loop for Regex logic
        
        texts = batch['text'].to_pylist()
        safe_texts = []
        valid_indices = []
        
        for i, text in enumerate(texts):
            # Toxicity Check (Drop immediately)
            if self.enable_toxicity and self._check_toxicity(text):
                continue # Drop toxic docs
                
            # PII Handling
            if self.enable_pii:
                text, found_pii = self._handle_pii(text)
                if found_pii:
                    if self.pii_action == "drop":
                        continue # Drop PII docs if configured
            
            safe_texts.append(text)
            valid_indices.append(i)
            
        if not safe_texts:
            return batch.slice(0, 0).drop(['lang_label', 'lang_score'])

        # Reconstruct batch with filtered/redacted text
        indices_array = pa.array(valid_indices)
        batch = batch.take(indices_array)
        
        # Replace the 'text' column with the Redacted version
        # (PyArrow tables are immutable, so we drop and append, or replace)
        batch = batch.drop(["text"])
        batch = batch.append_column("text", pa.array(safe_texts, type=pa.string()))

        # Quality Heuristics (Punctuation) ---
        punc_ratios = self._compute_punc_ratio(batch['text'])
        batch = batch.append_column("punc_ratio", punc_ratios)
        
        quality_mask = pc.less_equal(batch['punc_ratio'], self.max_punc_ratio)
        quality_filtered = len(batch) - pc.sum(pc.cast(quality_mask, pa.int64())).as_py()
        batch = batch.filter(quality_mask)

        # Cleanup
        return batch.drop(['lang_label', 'lang_score', 'punc_ratio'])

class QualityFilterStep:
    """
    The Orchestrator (Pipeline Step).
    Runs on the Driver and configures the pipeline.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def run(self, ds: ray.data.Dataset) -> ray.data.Dataset:
        logger.info("Configuring Language, PII & Toxicity Filtering...")
    
        filtered_ds = ds.map_batches(
            FastTextPredictor, 
            batch_format="pyarrow",
            batch_size=2000,
            fn_constructor_args=[self.config], 
            compute=ray.data.ActorPoolStrategy(min_size=1, max_size=4)
        )
        
        logger.info("="*60)
        logger.info("Filtering completed")
        logger.info("="*60)

        return filtered_ds