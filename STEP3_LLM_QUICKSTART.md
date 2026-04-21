# Step 3 LLM Quickstart

This project now supports three extraction modes:

- `--mode rule` : offline baseline extractor
- `--mode llm`  : OpenAI LLM extractor using structured outputs
- `--mode auto` : prefer LLM if `OPENAI_API_KEY` is set, otherwise fall back to rules

## 1. Install dependencies

```bash
pip install -r requirements.txt
```

## 2. Set your API key

```bash
export OPENAI_API_KEY=YOUR_KEY
```

Or create a `.env` file based on `.env.example`.

## 3. Run extraction

```bash
python scripts/extract_narratives.py --mode auto
```

Force LLM mode:

```bash
python scripts/extract_narratives.py --mode llm
```

Force offline rule mode:

```bash
python scripts/extract_narratives.py --mode rule
```

## 4. Notes

- LLM settings live in `app/config/llm_config.json`
- Prompt template lives in `app/prompts/narrative_extraction_prompt.md`
- If LLM mode errors and fallback is enabled, the pipeline will automatically use the rule extractor
