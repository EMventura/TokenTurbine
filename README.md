# TokenTurbine
An end-to-end data pipeline for LLM pre-training. TokenTurbine ingests a sample of unprepared data, cleans and normalizes text, performs deduplication and quality filtering, and outputs a training-ready dataset. The default dataset can be downloaded from ([here](https://s3.us-east-1.amazonaws.com/mainpipe.maincode.com/mainpipe_data_v1.jsonl)). Built with Ray for distributed processing.

###  Pipeline stages:

- **Data Ingestion** - Load and normalize text. Initial low-cost filtering.
- **Quality Filtering** - Language detection, PII handling, toxicity filtering
- **Deduplication** - Exact and fuzzy duplicate removal using MinHash LSH
- **Tokenization** - Fast tokenization with tiktoken
- **Export** - Training-ready format (single JSONL file by default, Parquet available for tokens)

### Key Features

**Distributed Processing** - Distributed processing with Ray  
**Production-Ready** - Containerized with Docker for reproducible runs  
**Configurable** - YAML-based configuration for easy experimentation  
**Efficient** - Vectorized operations with PyArrow for high throughput  
**Observable** - Comprehensive logging and statistics tracking  

---

## Prerequisites

- **Docker** 20.10+ and **Docker Compose** 2.0+ ([Install Docker](https://docs.docker.com/get-docker/))
- At least **6GB RAM** and **1GB disk space** (or 3x input data size if using a different dataset).

**Verify prerequisites**:

```bash
docker --version           # Should be 20.10+
docker-compose --version   # Should be 2.0+
```

---

## Installation & Usage

TokenTurbine offers **3 setup options** - choose the one that fits your workflow:

Option 1: **Script-based** (Recommended)
Option 2: **Makefile** 
Option 3: **Manual Setup**

### Option 1: use script download_data.sh
```bash
# 1. Clone the repository
git clone https://github.com/EMventura/TokenTurbine.git 
cd TokenTurbine

# 2. Make download script executable
chmod +x scripts/download_data.sh

# 3. Run the download script
./scripts/download_data.sh

# 4. Build the Docker image
docker-compose build

# 5. Run the pipeline
docker-compose up

# 6. Access results
ls -lh data/processed/cleaned_dataset.jsonl
```

### Option 2: Makefile

```bash
# 1. Clone the repository
git clone 
cd TokenTurbine

# 2. Download data
make download

# 3. Build the container
make build

# 4. Run the pipeline
make run

# 5. Access results
ls -lh data/processed/cleaned_dataset.jsonl
```

### Option 3: Manual download

```bash
# 1. Clone the repository
git clone 
cd TokenTurbine

# 2. Create data directory
mkdir -p data/raw

# 3. Download the dataset manually
curl -L "https://s3.us-east-1.amazonaws.com/mainpipe.maincode.com/mainpipe_data_v1.jsonl" \
  -o data/raw/mainpipe_data_v1.jsonl

# 4. Verify the download
ls -lh data/raw/mainpipe_data_v1.jsonl

# 5. Build the Docker image
docker-compose build

# 6. Run the pipeline
docker-compose up

# 7. Access results
ls -lh data/processed/cleaned_dataset.jsonl
```

**That's it!** Your cleaned dataset is ready at `data/processed/cleaned_dataset.jsonl`

## Configuration

Edit `configs/base.yaml` to customize the pipeline:

```yaml
# Example: Adjust filtering thresholds
filtering:
  enabled: true
  min_lang_score: 0.65        # Language confidence (0-1)
  max_punc_ratio: 0.3         # Max punctuation ratio
  enable_pii: true
  pii_action: "redact"        # "redact" or "drop"
  enable_toxicity: true

# Example: Tune deduplication
deduplication:
  enabled: true
  num_perm: 128               # MinHash permutations (64-256)
  threshold: 0.85             # Similarity threshold (0.7-0.95)
  max_lsh_items: 1000000      # Memory limit

# Example: Enable tokenization
tokenization:
  enabled: true
  tokenizer_model: "gpt2"     # "gpt2", "cl100k_base", etc.
  max_length: 2048
  export_format: "parquet"    # "parquet" or "jsonl"
```

See `configs/base.yaml` for all available options.

---

## Pipeline Stages

### 1. Ingestion
- Loads JSONL input files
- Cleans HTML and normalizes Unicode
- Filters code-like content and short documents
- Generates document IDs with xxhash

### 2. Quality Filtering
- **Language Detection**: FastText-based filtering (176 languages)
- **PII Handling**: Detects and redacts emails, phone numbers, IPs
- **Toxicity Filter**: Removes hate speech and toxic content
- **Quality Heuristics**: Punctuation ratio, character distribution

### 3. Deduplication
- **Exact Dedup**: Hash-based duplicate removal
- **Fuzzy Dedup**: MinHash LSH for near-duplicate detection
- Configurable similarity threshold (Jaccard distance)

### 4. Tokenization (Optional)
- Fast BPE tokenization with tiktoken
- Multiple tokenizer support (GPT-2, GPT-4, custom)
- Configurable sequence length and truncation
- Exports to Parquet or JSONL

### 5. Export
- Single consolidated JSONL file
- Preserves document metadata (source, URL, timestamps)
- Optional tokenized shards for training

---

## Project Structure

```
TokenTurbine/
├── src/
│   ├── main.py                 # Pipeline orchestrator
│   ├── data_load.py            # Ingestion stage
│   ├── filtering.py            # Quality filtering
│   ├── deduplication.py        # Dedup stage
│   ├── tokenization.py         # Tokenization stage
│   └── utils/
│       ├── config_loader.py    # Config management
│       └── single_jsonl.py     # Export utilities
├── configs/
│   └── base.yaml               # Default configuration
├── scripts/                    # Helper scripts (For download data option 1)
│   └── download_data.sh
├── data/
│   ├── raw/                    # Input data 
│   ├── processed/              # Output data
│   └── reports/                # Metrics (future)
├── Dockerfile                  # Container definition
├── docker-compose.yml          # Container orchestration
├── requirements.txt            # Python dependencies
├── Makefile                    # Helper commands
└── README.md                   # This file
```

---

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make download` | Download the input dataset |
| `make build` | Build Docker image |
| `make run` | Run pipeline with default config |
| `make shell` | Open interactive shell in container |
| `make logs` | View pipeline logs |
| `make stop` | Stop running containers |
| `make clean` | Remove containers and images |
| `make clean-data` | RRemove all processed data |
| `make validate` | Verify installation |

Run `make help` to see all available commands.


## Troubleshooting

### Common Issues

**Issue:** Out of Memory errors  
**Solution:** Increase Docker memory limit (Settings → Resources → Memory → 6GB+)

**Issue:** Pipeline runs slowly  
**Solution:** Reduce `batch_size` in config or increase CPU allocation

**Issue:** "FastText model not found"  
**Solution:** Model should be auto-downloaded. If not: `cd data && wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin`

**Issue:** Permission denied on data files  
**Solution:** `chmod -R 755 data/` or run with your user: `docker-compose run --user $(id -u):$(id -g) pipeline`

**Issue:** Data file not found (FileNotFoundError: data/raw/mainpipe_data_v1.jsonl)
**Solution:** Download data file either manually, using the download_data.sh script or with make download

**Issue:** Old containers interfering (ERROR: 'ContainerConfig')
**Solution:** 
```bash
# Clean everything
docker-compose down -v

docker-compose build --no-cache

docker-compose up
```

---

## Advanced Usage

### Custom Configurations

Create multiple configs for different experiments:

```bash
# Create custom config
cp configs/base.yaml configs/experiment_1.yaml

# Edit thresholds
vim configs/experiment_1.yaml

# Run with custom config
docker-compose run --rm pipeline --config configs/experiment_1.yaml

```
### Use your own data

```bash
# Copy your data file
cp /path/to/your/data.jsonl data/raw/mainpipe_data_v1.jsonl

# Run pipeline
docker-compose up
```

## Output Format

### Cleaned Dataset (`data/processed/cleaned_dataset.jsonl`)

```json
{
  "doc_id": "a1b2c3d4e5f6...",
  "text": "This is the cleaned document text...",
  "url": "https://example.com/page",
  "source": "mainpipe_data_v1.jsonl"
}
```

### Tokenized Dataset (`data/processed/tokenized/*.parquet`)

Parquet files containing:
- `doc_id`: Document identifier
- `input_ids`: List of token IDs
- `token_count`: Number of tokens
- `text`: Original text (optional)
- `source`, `url`: Metadata

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## Acknowledgments

Built with:
- [Ray](https://ray.io/) - Distributed computing framework
- [FastText](https://fasttext.cc/) - Language identification
- [tiktoken](https://github.com/openai/tiktoken) - Fast tokenization
- [DataSketch](https://github.com/ekzhu/datasketch) - MinHash LSH
- [PyArrow](https://arrow.apache.org/docs/python/) - Columnar data processing

---

## Contact

For questions or issues:
- Review troubleshooting section above
- Open an issue on GitHub

---

## Roadmap

### Current Version (v1.0)
- Basic pipeline (ingestion, filtering, dedup, tokenization)
- Docker containerization
- Configuration system
- Quality filters (language, PII, toxicity)
- MinHash LSH deduplication

### Future Enhancements
- [ ] Inspectability dashboard (metrics, histograms)
- [ ] Multi-node Ray cluster support
- [ ] Advanced quality filters (perplexity, document structure)
- [ ] Data mixing and sampling strategies
- [ ] Resume from failures (robust checkpointing)

---