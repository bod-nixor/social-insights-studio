#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path

import pdfplumber
from pypdf import PdfReader


MAX_BYTES = 20 * 1024 * 1024
MAX_PAGES = 80
A4_WIDTH = 595.28
A4_HEIGHT = 841.89


def verify_pdf(pdf_path: Path):
    errors = []
    reader = PdfReader(str(pdf_path))
    page_count = len(reader.pages)
    if not 1 <= page_count <= MAX_PAGES:
        errors.append(f"page count {page_count} outside 1..{MAX_PAGES}")
    if pdf_path.stat().st_size > MAX_BYTES:
        errors.append(f"file exceeds {MAX_BYTES} bytes")
    root = reader.trailer.get("/Root", {})
    if root.get("/OpenAction") is not None:
        errors.append("document has an OpenAction")
    names = root.get("/Names")
    if names and names.get_object().get("/EmbeddedFiles") is not None:
        errors.append("document contains embedded files")

    all_text = []
    with pdfplumber.open(str(pdf_path)) as document:
        if len(document.pages) != page_count:
            errors.append("independent page counts disagree")
        for index, page in enumerate(document.pages, start=1):
            if abs(page.width - A4_WIDTH) > 1 or abs(page.height - A4_HEIGHT) > 1:
                errors.append(f"page {index} is not A4")
            text = page.extract_text() or ""
            all_text.append(text)
            if len(text.strip()) < 20:
                errors.append(f"page {index} is blank or nearly blank")
            for char in page.chars:
                if (
                    char.get("x0", 0) < -0.5
                    or char.get("x1", 0) > page.width + 0.5
                    or char.get("top", 0) < -0.5
                    or char.get("bottom", 0) > page.height + 0.5
                ):
                    errors.append(f"page {index} has text outside page bounds")
                    break
            annotations = reader.pages[index - 1].get("/Annots") or []
            if annotations:
                errors.append(f"page {index} contains annotations or links")

    extracted = "\n".join(all_text)
    if re.search(r"\b(?:null|undefined)\b", extracted, re.IGNORECASE):
        errors.append("raw null or undefined text found")
    if re.search(r"(?:https?|file|ftp)://", extracted, re.IGNORECASE):
        errors.append("uncontrolled URL found in visible text")
    if "Social Insights Studio" not in extracted:
        errors.append("brand text missing")
    if "Methodology and data notes" not in extracted:
        errors.append("methodology section missing")
    if not re.search(rf"\b{page_count}\s*/\s*{page_count}\b", extracted):
        errors.append("final page footer count missing")

    sample_name = pdf_path.name
    if sample_name == "all-platform-report.pdf":
        for provider in ["TikTok", "YouTube", "Facebook Pages", "Instagram", "Website Analytics"]:
            if provider not in extracted:
                errors.append(f"all-platform sample missing {provider}")
    if sample_name == "tiktok-no-content-report.pdf" and "No eligible stored content" not in extracted:
        errors.append("no-content empty state missing")
    if sample_name == "missing-metric-report.pdf" and "Unavailable" not in extracted:
        errors.append("missing-metric state not visible")
    if sample_name == "long-content-report.pdf" and extracted.count("Title or path") < 2:
        errors.append("paginated content table did not repeat its header")

    return {
        "file": str(pdf_path.resolve()),
        "bytes": pdf_path.stat().st_size,
        "pages": page_count,
        "text_characters": len(extracted),
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(description="Verify generated Social Insights Studio PDF samples.")
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    paths = sorted(args.directory.glob("*.pdf"))
    if not paths:
        raise SystemExit("no PDF samples found")
    results = [verify_pdf(path) for path in paths]
    print(json.dumps({"results": results}, indent=2))
    if any(result["errors"] for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
