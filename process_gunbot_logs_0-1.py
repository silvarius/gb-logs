#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
process_gunbot_logs.py
Traitement et mise en forme des fichiers de logs Gunbot.

Usage : placer ce script dans le même répertoire que les fichiers gunbot_logs*,
        puis : python3 process_gunbot_logs.py

Format de sortie :
  - Les tableaux ASCII Gunbot sont supprimés.
  - Chaque cycle est présenté ainsi :
      ████ séparateur (simple ou double) ████
      GB_xx.x.x PAIR@exchange Round #N YYYY/MM/DD HH:MM:SS
      ████ séparateur (simple ou double) ████
      [données de la stratégie...]

      ████ séparateur du cycle suivant ████
      ...

  - Séparateur DOUBLE (##) : premier cycle après l'ouverture d'une bougie.
  - Séparateur SIMPLE (#)  : tous les autres cycles.
  - Pas de séparateur de fin de cycle (il fait double emploi avec le début du suivant).
"""

import os
import re
import gzip
import shutil
import subprocess
import sys
from datetime import datetime, timezone


# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────

SCRIPT_NAME = os.path.basename(__file__)

# Regex de la ligne d'en-tête de cycle
# Ex : GB_30.9.9 USD-ETH@hyperliquid Round #288 2026/04/26 19:15:46
CYCLE_HEADER_RE = re.compile(
    r'^GB[_\s]\S+\s+\S+@\S+\s+Round\s+#\d+\s+(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})'
)

# Timeframes courants (en minutes) — toute valeur entière positive est acceptée
TIMEFRAMES_HINT = ["1", "3", "5", "10", "15", "30", "60", "120", "240", "360", "720", "1440"]

SEP_SINGLE = "#" * 80
SEP_DOUBLE = "#" * 80 + "\n" + "#" * 80


# ─────────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────────

def find_latest_gunbot_log(directory: str) -> str | None:
    """Retourne le chemin du fichier gunbot_logs* le plus récent du répertoire."""
    candidates = [
        f for f in os.listdir(directory)
        if f.startswith("gunbot_logs") and os.path.isfile(os.path.join(directory, f))
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda f: os.path.getmtime(os.path.join(directory, f)),
        reverse=True
    )
    return os.path.join(directory, candidates[0])


def decompress_if_needed(filepath: str, directory: str) -> tuple[str, bool]:
    """
    Si le fichier est un .gz, le décompresse dans le répertoire.
    Retourne (chemin_fichier_texte, a_été_décompressé).
    """
    if filepath.endswith(".gz"):
        dest = os.path.join(directory, os.path.basename(filepath[:-3]))
        print(f"  Décompression de {os.path.basename(filepath)}"
              f" -> {os.path.basename(dest)} ...")
        with gzip.open(filepath, "rb") as f_in, open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        return dest, True
    return filepath, False


def ask_timeframe() -> int:
    """Demande à l'utilisateur le timeframe et retourne la valeur en minutes."""
    print("\nTimeframes courants (en minutes) :")
    print("  " + "  ".join(TIMEFRAMES_HINT))
    print("  (toute autre valeur entière positive est acceptée)")
    while True:
        raw = input("Timeframe de référence (en minutes) : ").strip()
        try:
            val = int(raw)
            if val > 0:
                return val
        except ValueError:
            pass
        print("  X Valeur invalide, entrez un entier positif.")


def ask_filter_mode() -> bool:
    """
    Demande le mode de filtrage des cycles.
    Retourne True si on ne garde que le premier cycle par bougie.
    """
    print("\nFiltrage des cycles :")
    print("  1 - Conserver tous les cycles")
    print("  2 - Conserver uniquement le premier cycle apres chaque ouverture de bougie")
    while True:
        raw = input("Votre choix [1/2] : ").strip()
        if raw == "1":
            return False
        if raw == "2":
            return True
        print("  X Entrez 1 ou 2.")


def parse_cycle_timestamp(ts_str: str) -> datetime:
    """Convertit '2026/04/26 19:15:46' en objet datetime (naif, UTC implicite)."""
    return datetime.strptime(ts_str.strip(), "%Y/%m/%d %H:%M:%S")


def candle_open_time(dt: datetime, tf_minutes: int) -> datetime:
    """Retourne l'heure d'ouverture de la bougie dans laquelle tombe dt."""
    total_seconds = int(dt.timestamp())
    tf_seconds = tf_minutes * 60
    candle_start_ts = (total_seconds // tf_seconds) * tf_seconds
    return datetime.fromtimestamp(candle_start_ts, tz=timezone.utc).replace(tzinfo=None)


def is_table_line(line: str) -> bool:
    """Detecte une ligne appartenant au tableau ASCII Gunbot."""
    return bool(re.match(r'^[┌├└│┐┤┘─═╔╠╚║╗╣╝╞╡╤╧╪╫╬]+', line.strip()))


# ─────────────────────────────────────────────
# PARSING DES CYCLES
# ─────────────────────────────────────────────

def parse_cycles(text: str) -> list[dict]:
    """
    Decoupe le texte brut en liste de cycles.
    Chaque cycle : { 'header': str, 'timestamp': datetime, 'body': [str] }
    """
    cycles = []
    current_header = None
    current_ts = None
    current_body: list[str] = []

    for raw_line in text.splitlines():
        line = raw_line.rstrip('\r')
        m = CYCLE_HEADER_RE.match(line.strip())
        if m:
            if current_header is not None:
                cycles.append({
                    "header":    current_header,
                    "timestamp": current_ts,
                    "body":      current_body,
                })
            current_header = line.strip()
            current_ts     = parse_cycle_timestamp(m.group(1))
            current_body   = []
        else:
            if current_header is not None:
                current_body.append(line)

    # Dernier cycle
    if current_header is not None:
        cycles.append({
            "header":    current_header,
            "timestamp": current_ts,
            "body":      current_body,
        })

    return cycles


# ─────────────────────────────────────────────
# FILTRAGE
# ─────────────────────────────────────────────

def filter_cycles_by_candle(cycles: list[dict], tf_minutes: int) -> list[dict]:
    """
    Ne conserve que le premier cycle de chaque tranche de bougie
    (celui dont le timestamp depasse pour la premiere fois l'ouverture de la bougie).
    """
    seen: set = set()
    kept = []
    for cycle in cycles:
        key = candle_open_time(cycle["timestamp"], tf_minutes)
        if key not in seen:
            seen.add(key)
            kept.append(cycle)
    return kept


# ─────────────────────────────────────────────
# FORMATAGE DE LA SORTIE
# ─────────────────────────────────────────────

def format_output(cycles: list[dict], tf_minutes: int) -> str:
    """
    Construit le texte final a partir de la liste de cycles a afficher.

    Format de chaque cycle :
        <sep>
        <header>
        <sep>
        <body>

    Pas de separateur de fin : la ligne vide entre deux cycles suffit,
    et le separateur d'ouverture du cycle suivant joue ce role visuellement.

    sep = SEP_DOUBLE pour le premier cycle de chaque bougie, SEP_SINGLE sinon.
    """
    # Identifier les timestamps des premiers cycles de chaque bougie
    first_of_candle: set = set()
    seen: set = set()
    for c in cycles:
        key = candle_open_time(c["timestamp"], tf_minutes)
        if key not in seen:
            seen.add(key)
            first_of_candle.add(c["timestamp"])

    parts = []
    for cycle in cycles:
        sep = SEP_DOUBLE if cycle["timestamp"] in first_of_candle else SEP_SINGLE

        # Supprimer les lignes de tableau, puis les lignes vides en tete/queue
        body = [l for l in cycle["body"] if not is_table_line(l)]
        while body and not body[0].strip():
            body.pop(0)
        while body and not body[-1].strip():
            body.pop()

        block = sep + "\n" + cycle["header"] + "\n" + sep + "\n" + "\n".join(body)
        parts.append(block)

    return "\n\n".join(parts) + "\n"


# ─────────────────────────────────────────────
# NETTOYAGE DU REPERTOIRE
# ─────────────────────────────────────────────

def cleanup_directory(directory: str, keep_files: set[str]) -> None:
    """Supprime tous les fichiers du repertoire sauf ceux listes dans keep_files."""
    for fname in sorted(os.listdir(directory)):
        fpath = os.path.join(directory, fname)
        if os.path.isfile(fpath) and os.path.abspath(fpath) not in keep_files:
            try:
                os.remove(fpath)
                print(f"  Supprime : {fname}")
            except OSError as e:
                print(f"  X Impossible de supprimer {fname} : {e}")


# ─────────────────────────────────────────────
# POINT D'ENTREE
# ─────────────────────────────────────────────

def main() -> None:
    script_dir  = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.abspath(__file__)

    print("=" * 60)
    print("  Gunbot Log Processor")
    print("=" * 60)

    # 1. Trouver le fichier log le plus recent
    log_path = find_latest_gunbot_log(script_dir)
    if log_path is None:
        print("\nX Aucun fichier 'gunbot_logs*' trouve dans le repertoire du script.")
        sys.exit(1)
    print(f"\nFichier selectionne : {os.path.basename(log_path)}")

    # 2. Decompresser si necessaire
    text_path, _ = decompress_if_needed(log_path, script_dir)

    # 3. Lecture
    print(f"  Lecture de {os.path.basename(text_path)} ...")
    with open(text_path, "r", encoding="utf-8", errors="replace") as f:
        raw_text = f.read()

    # 4. Questions utilisateur
    tf_minutes  = ask_timeframe()
    filter_mode = ask_filter_mode()

    # 5. Parsing
    print("\n  Analyse des cycles ...")
    all_cycles = parse_cycles(raw_text)
    print(f"  {len(all_cycles)} cycle(s) detecte(s).")

    if not all_cycles:
        print("X Aucun cycle trouve. Verifiez le format du fichier.")
        sys.exit(1)

    # 6. Filtrage
    if filter_mode:
        cycles_to_render = filter_cycles_by_candle(all_cycles, tf_minutes)
        print(f"  {len(cycles_to_render)} cycle(s) conserve(s) "
              f"apres filtrage (TF {tf_minutes} min).")
    else:
        cycles_to_render = all_cycles
        print(f"  Tous les cycles conserves (TF {tf_minutes} min "
              f"pour la distinction visuelle).")

    # 7. Nom du fichier de sortie
    first_ts  = all_cycles[0]["timestamp"].strftime("%Y%m%d_%H%M%S")
    base_name = os.path.splitext(os.path.basename(text_path))[0]
    if base_name.endswith(".log"):
        base_name = base_name[:-4]
    suffix          = "processed" if filter_mode else "processed_all"
    output_filename = f"{base_name}_{suffix}_{first_ts}.txt"
    output_path     = os.path.join(script_dir, output_filename)

    # 8. Formatage et ecriture
    print(f"\n  Ecriture de {output_filename} ...")
    content = format_output(cycles_to_render, tf_minutes)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  OK Fichier cree ({os.path.getsize(output_path):,} octets).")

    # 9. Nettoyage du repertoire
    print("\n  Nettoyage du repertoire ...")
    keep = {os.path.abspath(output_path), script_path}
    cleanup_directory(script_dir, keep)

    # 10. Ouverture dans VIM
    print(f"\n  Ouverture dans VIM ...")
    subprocess.run(["vim", output_path])


if __name__ == "__main__":
    main()
