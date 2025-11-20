# Pipeline Investigation Notes

## File Count Discrepancy (2025-11-04)

**Issue**: ~2,500 files downloaded ? ~2,000 files in landing table

**Root Cause**: Multiple Monday items (different `item_id`s) reference the same file content (same `file_hash`).

**Impact**:
- `dropDuplicates(["file_hash"])` in `landing.py` line 232 keeps only 1 record per unique file
- ~149 Monday item associations are dropped
- This is **data-level issue** - Monday board has duplicate file references across items

