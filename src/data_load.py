import ray
import logging
import xxhash
import pyarrow as pa
import pyarrow.compute as pc
import html
from typing import Dict, Any
from datetime import datetime
from bs4 import BeautifulSoup
import re
import unicodedata
import os
import warnings

# Suppress spurious BeautifulSoup warnings
# 1. MarkupResemblesLocatorWarning: Text looks like a URL (we don't care, we treat it as text)
# 2. XMLParsedAsHTMLWarning: Text looks like XML but we parse as HTML (fine for stripping tags)
try:
    from bs4 import MarkupResemblesLocatorWarning
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
except ImportError:
    pass

try:
    from bs4.builder import XMLParsedAsHTMLWarning
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
except ImportError:
    pass

logger = logging.getLogger("TokenTurbine")

class IngestionWorker:
    """
    Worker Actor that handles the actual data ingestion and cleaning logic.
    """
    def __init__(self, config: Dict[str, Any]):
        self.min_text_length = config.get("min_text_length", 100)
        self.normalize_text = config.get("normalize_text", True)
        self.source_name = config.get("source_name", "unknown")
        self.filter_code = config.get("filter_code", True)
        
        # Compile regexes for performance
        # Catches common JS/CSS/Code patterns:
        # - Lines ending in { or ;
        # - Keywords like 'function', 'var', 'const', 'console.log'
        self.code_keyword_re = re.compile(
            r'\b(function|var|const|let|=>|console\.log|document\.getElementById|'
            r'window\.|return|import|export|class|def|printf|iostream)\b'
        )
        self.symbol_density_re = re.compile(r'[\{\}\(\);=<>]')

    def _get_output_schema(self) -> pa.Schema:
        return pa.schema([
            ('doc_id', pa.string()),
            ('text', pa.string()),
            ('url', pa.string()),
            ('char_count', pa.int64()),
            ('word_count', pa.int64()),
            ('source', pa.string()),
            ('ingest_time', pa.float64())
        ])

    def _empty_table(self) -> pa.Table:
        return pa.Table.from_pydict({
            'doc_id': [], 'text': [], 'url': [], 
            'char_count': [], 'word_count': [], 
            'source': [], 'ingest_time': []
        }).cast(self._get_output_schema())

    def _is_code_like(self, text: str) -> bool:
        """
        Heuristic to check if text is likely code/boilerplate.
        Returns True if it looks like code.
        """
        if not text: return False

        # Quick check: if no code patterns found, skip expensive operations
        if not self.code_keyword_re.search(text):
            return False
        
        # 1. Check for Programming Keywords
        # If we find > 5 distinct programming keywords, it's likely code
        keywords_found = len(self.code_keyword_re.findall(text))
        if keywords_found > 5:
            return True

        # 2. Symbol Density Check (Code uses lots of { } ; = )
        # Calculate ratio of code-symbols to total length
        symbol_count = len(self.symbol_density_re.findall(text))
        ratio = symbol_count / len(text)
        
        # Threshold: If > 5% of characters are code syntax, drop it.
        if ratio > 0.05: 
            return True
            
        return False

    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize text: Remove HTML, normalize unicode, and normalize whitespace.
        """
        if not text: return ""
        
        try:
            # 1. Parse and extract text from HTML
            soup = BeautifulSoup(text, 'html.parser')
            # Remove script and style elements
            for element in soup(['script', 'style', 'header', 'footer', 'nav', 'noscript']):
                element.decompose()
            # Get text
            text = soup.get_text()
        except Exception as e:
            # Fallback: if BeautifulSoup fails, use regex
            logger.debug(f"BeautifulSoup failed, using regex fallback: {e}")
            text = re.sub(r'<[^>]+>', '', text)
        
        # 2. Basic Decodes
        # HTML entities that BeautifulSoup might have missed like &nbsp;, &amp;, etc.)
        text = html.unescape(text)
        # Normalize unicode (NFC form - canonical composition)
        text = unicodedata.normalize('NFC', text)
        
        # 3. Remove control characters (except newlines and tabs)
        text = ''.join(
            char for char in text 
            if unicodedata.category(char)[0] != 'C' or char in '\n\t'
        )
    
        # 4. Whitespace Normalization (Refined)
        # Replace non-breaking spaces with standard space
        text = text.replace('\u00a0', ' ')
        
        # Replace tabs with spaces
        text = text.replace('\t', ' ')
        
        # 5. Line Processing
        # Split into lines to process indentation (strip each line)
        # This fixes "      var programFilesVar" -> "var programFilesVar"
        lines = [line.strip() for line in text.split('\n')]
        
        # Join: This results in single \n for lines, and \n\n for paragraphs
        text = '\n'.join(lines)

        # Replace 3+ newlines with 2 (Standardize Paragraphs)
        # This fixes "text\n\n\n\ntext" -> "text\n\ntext"
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Final Cleanup: Replace multiple spaces within a line
        text = re.sub(r'[ \t]+', ' ', text)

        return text.strip()

    def __call__(self, batch: pa.Table) -> pa.Table:
        if 'text' not in batch.column_names:
            return self._empty_table()
        
        texts = batch['text'].to_pylist()
        
        # 1. Cleaning & Normalization
        if self.normalize_text:
            cleaned_texts = [self._clean_text(text) for text in texts]
        else:
            cleaned_texts = texts
            
        # 2. Filter Logic (Length & Code Detection)
        valid_indices = []
        final_texts = []
        
        for i, text in enumerate(cleaned_texts):

            # Length check
            if not text or len(text) < self.min_text_length:
                continue
            
            # Code check 
            if self.filter_code and self._is_code_like(text):
                continue
                
            valid_indices.append(i)
            final_texts.append(text)
            
        if not final_texts:
            return self._empty_table()

        # 3. Reconstruct Batch (Filtering other columns based on valid indices)
        # We grab the corresponding rows for metadata
        indices_array = pa.array(valid_indices)
        filtered_batch = batch.take(indices_array)
        
        # 4. Generate IDs and Metrics
        doc_ids = [xxhash.xxh64(t.encode('utf-8')).hexdigest() for t in final_texts]
        
        # Use pyarrow compute on the clean list
        text_array = pa.array(final_texts, type=pa.string())
        char_counts = pc.utf8_length(text_array)
        word_counts = pc.list_value_length(pc.utf8_split_whitespace(text_array))
        
        row_count = len(final_texts)
        timestamp = datetime.utcnow().timestamp()
        
        urls = filtered_batch['url'] if 'url' in filtered_batch.column_names else pa.nulls(row_count, type=pa.string())

        return pa.Table.from_pydict({
            'doc_id': pa.array(doc_ids, type=pa.string()),
            'text': text_array,
            'url': urls,
            'char_count': char_counts,
            'word_count': word_counts,
            'source': pa.repeat(self.source_name, row_count),
            'ingest_time': pa.repeat(timestamp, row_count)
        }).cast(self._get_output_schema())


class IngestionStep:
    """
    Orchestration class for the Ingestion Pipeline.
    """
    def __init__(self, config: Dict[str, Any]):
        self.input_path = config.get("input_path")
        if not self.input_path:
            raise ValueError("Config missing 'input_path' in ingestion config")
        
        self.source_name = self.input_path.split('/')[-1]
        self.batch_size = config.get("batch_size", 2000)

        # Build worker config
        self.worker_config = {
            'min_text_length': config.get('min_text_length', 100),
            'normalize_text': config.get('normalize_text', True),
            'filter_code': config.get('filter_code', True),
            'source_name': self.source_name
        }

    def run(self) -> ray.data.Dataset:
        """Execute the ingestion pipeline with parallel workers."""
        logger.info(f"Reading JSONL from {self.input_path}")
        
        try:
            ds = ray.data.read_json(self.input_path)
        except FileNotFoundError as e:
            logger.critical(f"Input file not found: {self.input_path}")
            raise e
        except Exception as e:
            logger.critical(f"Could not read file: {e}")
            raise e
        
        processed_ds = ds.map_batches(
            IngestionWorker, 
            batch_format="pyarrow",
            batch_size=self.batch_size,
            fn_constructor_args=[self.worker_config], 
            compute=ray.data.ActorPoolStrategy(min_size=1, max_size=4)
        )

        logger.info("="*60)
        logger.info("Ingestion completed")
        logger.info("="*60)
        
        return processed_ds