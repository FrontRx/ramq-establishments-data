# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository processes healthcare establishment data from Quebec's RAMQ (Régie de l'assurance maladie du Québec) system. It extracts data from PDFs, enriches it with Google Places API, and creates comprehensive CSV files with healthcare facility information.

## Common Development Commands

### Running the Data Pipeline

1. **Extract RAMQ data from PDFs:**
   ```bash
   python scripts/extract_ramq_pdf.py
   ```

2. **Enrich with Google Places API:**
   ```bash
   python scripts/enrich_with_google_places.py
   ```

3. **Merge the datasets:**
   ```bash
   python scripts/merge_data.py
   ```

4. **Find fax numbers with web search:**
   ```bash
   python scripts/find_fax_numbers_with_keywords_fixed.py
   ```

### Dependencies Required
- Python 3.6+
- poppler-utils (for PDF text extraction via `pdftotext` command)
- Python packages: requests, openai, python-dotenv

## Architecture and Data Flow

### Data Processing Pipeline
1. **PDF Extraction** (`extract_ramq_pdf.py`): Downloads RAMQ PDFs from all Quebec regions, extracts establishment codes, names, addresses, and care unit categories
2. **Google Places Enrichment** (`enrich_with_google_places.py`): Takes extracted data and searches Google Places API for additional details (phone, coordinates, website, place ID)
3. **Data Merging** (`merge_data.py`): Combines original RAMQ data with enriched Google Places data into a comprehensive dataset
4. **Fax Number Extraction** (`find_fax_numbers_with_keywords_fixed.py`): Uses OpenAI API with web search to find fax numbers for establishments

### Key Data Structures

**Primary CSV Fields:**
- `code`: 5-digit RAMQ establishment identifier
- `ramq_id`: Same as code, used for cross-referencing
- `name`: Establishment name
- `address`: Physical address
- `region`: Quebec region
- `categories`: Types of care units (comma-separated)
- `id`: Google Place ID
- `fax_number`: Extracted fax number
- `is_fax_enabled`: Binary flag (1 or 0)

### Important Implementation Details

- **API Keys**: The Google Places API key is currently hardcoded in `enrich_with_google_places.py`. OpenAI API key should be set as environment variable `OPENAI_API_KEY`
- **Rate Limiting**: Google Places enrichment includes exponential backoff and processes in small batches to avoid API limits
- **Data Files**: Main data files are stored in `data/` directory, archives in `archive/`
- **Logging**: Enrichment progress is logged to `data/enrichment_progress.log`
- **Hardcoded Paths**: Scripts use absolute paths like `/home/ubuntu/ramq_data/` that may need adjustment for local development

### Error Handling Patterns

- All scripts include retry logic for API calls
- PDF extraction handles missing or malformed PDFs gracefully
- Enrichment scripts save progress incrementally to temporary files
- Web search functions have fallback patterns for finding fax numbers

## Testing Approach

No formal test suite exists. Testing is done by:
1. Running scripts on sample data
2. Verifying CSV output formats
3. Checking enrichment logs for errors