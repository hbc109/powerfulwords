from __future__ import annotations
import csv, hashlib, json, re
from pathlib import Path
from typing import List
from docx import Document as DocxDocument
from pypdf import PdfReader

def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")

def read_docx_file(path: Path) -> str:
    doc = DocxDocument(str(path))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())

def read_pdf_file(path: Path) -> str:
    reader = PdfReader(str(path))
    pages = []
    for page in reader.pages:
        try:
            txt = page.extract_text() or ""
        except Exception:
            txt = ""
        if txt.strip():
            pages.append(txt)
    return "\n\n".join(pages)

def read_excel_file(path: Path) -> str:
    """Render every sheet as plain text. Each sheet becomes a section
    headed by its name; rows become tab-separated lines so headers and
    cell values stay aligned for the narrative extractor."""
    import pandas as pd
    sheets = pd.read_excel(path, sheet_name=None, dtype=str)
    parts = []
    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            continue
        df = df.fillna("")
        parts.append(f"## Sheet: {sheet_name}")
        parts.append("\t".join(str(c) for c in df.columns))
        for _, row in df.iterrows():
            parts.append("\t".join(str(v) for v in row.values))
        parts.append("")
    return "\n".join(parts)


def extract_text_from_file(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".txt":
        return read_text_file(path)
    if suffix == ".docx":
        return read_docx_file(path)
    if suffix == ".pdf":
        return read_pdf_file(path)
    if suffix in (".xlsx", ".xls"):
        return read_excel_file(path)
    raise ValueError(f"Unsupported file type: {suffix}")

def clean_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()

def estimate_tokens(text: str) -> int:
    return max(1, int(len(text) / 4))

def chunk_text(text: str, chunk_chars: int = 1800, overlap_chars: int = 250) -> List[dict]:
    if not text.strip():
        return []
    chunks = []
    start = 0
    idx = 0
    n = len(text)
    while start < n:
        end = min(n, start + chunk_chars)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append({"chunk_index": idx, "text": chunk, "token_estimate": estimate_tokens(chunk)})
            idx += 1
        if end >= n:
            break
        start = max(end - overlap_chars, 0)
    return chunks

def read_manifest_csv(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
