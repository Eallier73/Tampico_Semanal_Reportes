#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import string
from pathlib import Path


ACCENT_REPLACEMENTS = {
    "á": "a",
    "é": "e",
    "í": "i",
    "ó": "o",
    "ú": "u",
    "Á": "a",
    "É": "e",
    "Í": "i",
    "Ó": "o",
    "Ú": "u",
    "ñ": "n",
    "Ñ": "n",
    "ü": "u",
    "Ü": "u",
}


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATOS_DIR = REPO_ROOT / "Datos" / "2026_W14_Datos"
TARGET_FILES = ("material_comentarios.txt", "material_institucional.txt")


def _replace_accents(text: str) -> str:
    for original, replacement in ACCENT_REPLACEMENTS.items():
        text = text.replace(original, replacement)
    return text


def normalize_facebook(text: str) -> str:
    text = _replace_accents(text or "")
    text = text.lower()
    text = re.sub(r"@\w+", " ", text)
    text = re.sub(r"#(\w+)", r" \1 ", text)
    text = re.sub(r"https?:\/\/\S*", " ", text)
    text = re.sub(r"www\.\S+", " ", text)
    text = re.sub(r"\S+\.com\b", " ", text)
    text = re.sub(r"\S+\.mx\b", " ", text)
    text = re.sub(r"\bhttp\b", " ", text)
    text = re.sub(r"\bhttps\b", " ", text)
    text = re.sub(r"\bcom\b", " ", text)
    text = re.sub(r"\bfacebook\b", " ", text)
    text = re.sub(r"\bfb\b", " ", text)
    text = re.sub(r"\bme gusta\b", " ", text)
    text = re.sub(r"\bcompartir\b", " ", text)
    text = re.sub(r"[" + re.escape(string.punctuation + string.digits) + "]", " ", text)
    text = "".join(char for char in text if ord(char) < 128)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_twitter(text: str) -> str:
    text = _replace_accents(text or "")
    text = text.lower()
    text = re.sub(r"@\w+", " ", text)
    text = re.sub(r"#(\w+)", r" \1 ", text)
    text = re.sub(r"https?:\/\/\S*", " ", text)
    text = re.sub(r"www\.\S+", " ", text)
    text = re.sub(r"\S+\.com\b", " ", text)
    text = re.sub(r"\S+\.mx\b", " ", text)
    text = re.sub(r"\bhttp\b", " ", text)
    text = re.sub(r"\bhttps\b", " ", text)
    text = re.sub(r"\bcom\b", " ", text)
    text = re.sub(r"\brt\b", " ", text)
    text = re.sub(r"\bvia\b", " ", text)
    text = re.sub(r"[" + re.escape(string.punctuation + string.digits) + "]", " ", text)
    text = "".join(char for char in text if ord(char) < 128)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_youtube(text: str) -> str:
    text = _replace_accents(text or "")
    text = text.lower()
    text = re.sub(r"https?:\/\/\S*", " ", text)
    text = re.sub(r"\bhttp\b", " ", text)
    text = re.sub(r"\bhttps\b", " ", text)
    text = re.sub(r"\bcom\b", " ", text)
    text = re.sub(r"\.\s*com\b", " ", text)
    text = re.sub(r"[" + re.escape(string.punctuation + string.digits) + "]", " ", text)
    text = "".join(char for char in text if ord(char) < 128)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def detect_dialect(path: Path) -> csv.Dialect:
    with open(path, "r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
        sample = handle.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;")
    except csv.Error:
        return csv.get_dialect("excel")


def read_dict_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    dialect = detect_dialect(path)
    with open(path, "r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle, dialect=dialect)
        fieldnames = [field.strip().lower() for field in (reader.fieldnames or [])]
        rows = []
        for row in reader:
            normalized_row = {}
            for key, value in (row or {}).items():
                if key is None:
                    continue
                normalized_row[key.strip().lower()] = (value or "").strip().strip('"')
            rows.append(normalized_row)
    return rows, fieldnames


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    unique = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def words_to_lines(items: list[str], words_per_line: int) -> list[str]:
    words = " ".join(items).split()
    return [
        " ".join(words[index:index + words_per_line])
        for index in range(0, len(words), words_per_line)
        if words[index:index + words_per_line]
    ]


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        if lines:
            handle.write("\n".join(lines) + "\n")


def read_lines(path: Path) -> list[str]:
    with open(path, "r", encoding="utf-8-sig", errors="ignore") as handle:
        return [line.strip() for line in handle if line.strip()]


def get_normalizer(name: str):
    if name == "facebook":
        return normalize_facebook
    if name == "twitter":
        return normalize_twitter
    if name == "youtube":
        return normalize_youtube
    raise ValueError(f"Normalizador no soportado: {name}")


def clean_target_file(path: Path, normalizer_name: str, words_per_line: int | None = None) -> tuple[int, int]:
    normalizer = get_normalizer(normalizer_name)
    source_lines = read_lines(path)
    cleaned = [normalizer(line) for line in source_lines]
    cleaned = dedupe_keep_order([line for line in cleaned if line])

    if words_per_line and words_per_line > 0:
        cleaned = words_to_lines(cleaned, words_per_line)

    write_lines(path, cleaned)
    return len(source_lines), len(cleaned)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Limpia solo material_comentarios.txt y material_institucional.txt en una carpeta semanal de Datos"
    )
    parser.add_argument(
        "--datos-dir",
        default=str(DEFAULT_DATOS_DIR),
        help=f"Carpeta semanal de Datos (default: {DEFAULT_DATOS_DIR})",
    )
    parser.add_argument(
        "--comentarios-normalizer",
        choices=["facebook", "twitter", "youtube"],
        default="twitter",
        help="Normalizador para material_comentarios.txt",
    )
    parser.add_argument(
        "--institucional-normalizer",
        choices=["facebook", "twitter", "youtube"],
        default="facebook",
        help="Normalizador para material_institucional.txt",
    )
    parser.add_argument(
        "--words-per-line",
        type=int,
        default=None,
        help="Opcional: recompone la salida en bloques de N palabras por línea",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    datos_dir = Path(args.datos_dir)

    if not datos_dir.exists() or not datos_dir.is_dir():
        raise SystemExit(f"No existe la carpeta de datos: {datos_dir}")

    target_paths = {
        "material_comentarios.txt": args.comentarios_normalizer,
        "material_institucional.txt": args.institucional_normalizer,
    }

    print(f"Carpeta objetivo: {datos_dir}")
    for filename, normalizer_name in target_paths.items():
        path = datos_dir / filename
        if not path.exists():
            print(f"  - Omitido (no existe): {path}")
            continue

        before_count, after_count = clean_target_file(path, normalizer_name, args.words_per_line)
        print(
            f"  - {filename}: {before_count} lineas -> {after_count} lineas "
            f"(normalizador: {normalizer_name})"
        )


if __name__ == "__main__":
    main()