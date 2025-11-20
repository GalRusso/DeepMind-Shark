# KB Unstructured Data Pipeline

## Developer Notes

### Pipeline Overview
This data pipeline processes unstructured documents through a 4-stage ETL process:

1. **Landing** (`landing_kb_unstructured.all`): Raw file ingestion from Google Drive with metadata
2. **Silver Chunking** (`silver_kb_unstructured.chunking`): Document parsing and text chunking using Docling
3. **Silver Vectorizing** (`silver_kb_unstructured.vectorizing`): Embedding generation for chunks
4. **Gold** (`gold_kb_unstructured.gold`): Final unified table joining all stages

### Key Components
- **Parallel Processing**: Chunking uses hash-based distribution across 5 parallel tasks
- **File Filtering**: Excel files >1MB are filtered out during landing to prevent processing issues
- **Incremental Processing**: Each stage only processes new/unprocessed records using LEFT ANTI joins

### Data Flow
```
Google Drive → Landing Table → Chunking (5 parallel tasks) → Vectorizing → Gold Table → Pinecone
```

## ⚠️ Known Caveats

### Duplicate Chunks Issue
**Problem**: The chunking process can create duplicate records with different `created_at` timestamps.

**Root Cause**: Task retries combined with append-only writes can process the same file multiple times.

**Symptoms**: 
- Multiple rows with identical `landing_id`, `chunk_id`, `text`, `metadata` but different `created_at`
- Query result discrepancies between direct silver table joins vs. full pipeline joins

**Mitigation**: 
- Dedup when creating Gold

### Orphaned Records
Files processed before filter implementation may leave "orphaned" chunks in silver tables when their landing records are removed by newer filters. 