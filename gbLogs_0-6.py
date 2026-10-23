#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gbLogs_0-6.py
Traitement et mise en forme des fichiers de logs Gunbot generes par pm2.
Version "serveur" : le script est place directement dans le repertoire
pm2/logs et travaille sur les fichiers de logs presents sur disque
(fini l'archive zip en entree).
Version cross-platform : Linux et Windows.

Usage :
  placer ce script dans le repertoire pm2/logs, puis : python3 gbLogs_0-6.py

Selection des fichiers a traiter :
  - L'utilisateur saisit une date/heure de debut et une date/heure de fin
    (par defaut : hier 00:00:00 -> hier 23:59:59).
  - Deux modes de selection, selon la saisie utilisateur :

    MODE JOURNALIER (par defaut, ou des lors que debut=00:00:00 et
    fin=23:59:59, ce qui est le cas des valeurs par defaut) :
      pm2 genere un fichier -out horodate a 00:00:00 a chaque bascule de
      jour calendaire, pour peu que l'instance soit active a ce moment-la.
      Ce fichier est le meilleur point de depart pour une journee : il
      contient tous les cycles voulus pour demarrer la journee.
      Pour chaque instance :
        * le PREMIER fichier traite est celui date <date_debut> 00:00:00 ;
        * le DERNIER fichier traite est le fichier date
          <date_fin + 1 jour> 00:00:00 lui-meme, inclus en totalite : la
          rotation de minuit vide le buffer pm2 de la journee qui se
          termine dans ce nouveau fichier, qui contient donc les derniers
          cycles de <date_fin> avant d'enchainer sur ceux du lendemain.
        Exemple : date_debut = date_fin = 10/08/2026 -> on traite
        gbp1-out__2026-08-10_00-00-00.log jusqu'a
        gbp1-out__2026-08-11_00-00-00.log inclus (ce dernier fichier peut
        donc deborder legerement sur la journee du 11/08).
      Si l'un des deux fichiers 00:00:00 attendus est absent pour une
      instance donnee (l'instance etait desactivee au moment de la
      bascule de jour concernee), on bascule pour CETTE INSTANCE
      uniquement sur le mode plage precise (ci-dessous), applique a
      l'intervalle [date_debut 00:00:00, date_fin 23:59:59].

    MODE PLAGE PRECISE (des que l'utilisateur saisit une heure de debut
    ou de fin differente de 00:00:00/23:59:59) :
      * tous les fichiers -out dont l'horodatage est compris dans
        l'intervalle [debut, fin] ;
      * le fichier -out immediatement anterieur au debut (le plus proche
        avant la borne, s'il existe) ;
      * le fichier -out immediatement posterieur a la fin (le plus proche
        apres la borne, s'il existe).
      Si aucun fichier ne tombe dans l'intervalle, l'instance est ignoree
      (pas de fichier limitrophe recupere dans ce cas : cela eviterait de
      remonter un fichier arbitrairement eloigne pour une instance inactive
      sur la periode demandee).
      Exemple : bornes 10:00:00 -> 11:00:00, avec des fichiers a 08:00, 09:00,
      10:15, 10:40, 10:55, 12:00, 13:00 -> fichiers retenus : 09:00 (le plus
      proche avant), 10:15/10:40/10:55 (dans l'intervalle), 12:00 (le plus
      proche apres). Les fichiers a 08:00 et 13:00 sont ecartes.

  - Les fichiers <instance>-error__*.log (erreurs / timeouts pm2) sont
    toujours ignores : ils ne contiennent pas de cycles de trading.
  - Le contenu retenu est celui de la totalite des fichiers selectionnes
    (aucun decoupage cycle par cycle sur les bornes exactes) : les cycles
    en dehors de l'intervalle mais presents dans un fichier limitrophe
    selectionne sont donc conserves.

Traitement, par instance :
  1. Les fichiers selectionnes sont decompresses si besoin (.gz), nettoyes
     des codes ANSI (couleurs pm2), puis decoupes en cycles.
  2. L'ensemble des cycles est fusionne, deduplique et trie chronologiquement.
  3. Le meme traitement que les versions precedentes (suppression des
     tableaux ASCII, filtrage optionnel au 1er cycle par bougie, separateurs
     simple/double) est applique a la liste de cycles fusionnee.

Sortie :
  - Une archive .zip deposee dans le meme repertoire (pm2/logs), contenant
    un fichier .txt fusionne par instance, nomme :
    gunbot_logs_<instance>_<date>_<heure_debut>_<heure_fin>.txt
    (date/heures = periode couverte par les cycles reellement trouves pour
    cette instance, ex : gunbot_logs_gbp1_2026-08-09_100000_180000.txt)
  - Les fichiers sources ne sont ni modifies ni supprimes.
"""

import gzip
import os
import platform
import re
import sys
from datetime import datetime, timedelta, timezone
from datetime import time as dtime
import zipfile


# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────

IS_WINDOWS = platform.system() == "Windows"

# Regex de la ligne d'en-tete de cycle (une fois les codes ANSI retires)
# Ex : GB_30.9.9 USD-BTC@hyperliquid Round #140982 2026/08/09 03:48:53
CYCLE_HEADER_RE = re.compile(
    r'^GB[_\s]\S+\s+\S+@\S+\s+Round\s+#\d+\s+(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})'
)

# Codes d'echappement ANSI (couleurs/fond ajoutes par pm2)
ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')

# Nom de fichier pm2 : <instance>-out__YYYY-MM-DD_HH-MM-SS.log[.gz]
#                  ou  <instance>-error__YYYY-MM-DD_HH-MM-SS.log[.gz]
PM2_FILENAME_RE = re.compile(
    r'^(?P<instance>.+)-(?P<stream>out|error)__'
    r'(?P<ts>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.log(\.gz)?$'
)

TIMEFRAMES_HINT = ["1", "3", "5", "10", "15", "30", "60", "120", "240", "360", "720", "1440"]

SEP_SINGLE = "#" * 80
SEP_DOUBLE = "#" * 80 + "\n" + "#" * 80

# Formats de saisie acceptes pour les dates/heures utilisateur
DATETIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"]
DATE_ONLY_FORMATS = ["%Y-%m-%d", "%d/%m/%Y"]


# ─────────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────────

def strip_ansi(text: str) -> str:
    """Supprime les codes d'echappement ANSI (couleurs pm2)."""
    return ANSI_RE.sub("", text)


def parse_datetime_input(raw: str, is_end: bool):
    """
    Parse une saisie utilisateur en datetime.
    Accepte 'YYYY-MM-DD HH:MM:SS', 'DD/MM/YYYY HH:MM:SS',
    ou une date seule (heure forcee a 00:00:00 pour un debut,
    23:59:59 pour une fin). Retourne None si la saisie est invalide.
    """
    raw = raw.strip()
    if not raw:
        return None
    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    for fmt in DATE_ONLY_FORMATS:
        try:
            d = datetime.strptime(raw, fmt)
            return d.replace(hour=23, minute=59, second=59) if is_end \
                else d.replace(hour=0, minute=0, second=0)
        except ValueError:
            continue
    return "INVALID"


def ask_date_range() -> tuple[datetime, datetime]:
    """
    Demande a l'utilisateur les bornes de date/heure a traiter.
    Par defaut : hier 00:00:00 -> hier 23:59:59.
    """
    yesterday = (datetime.now() - timedelta(days=1)).date()
    default_start = datetime.combine(yesterday, datetime.min.time())
    default_end = datetime.combine(yesterday, datetime.max.time()).replace(microsecond=0)

    print("\nPeriode a traiter (format 'YYYY-MM-DD HH:MM:SS' ou 'DD/MM/YYYY HH:MM:SS') :")
    print(f"  Par defaut, debut = {default_start.strftime('%Y-%m-%d %H:%M:%S')}")
    while True:
        raw = input(f"Date/heure de debut [{default_start.strftime('%Y-%m-%d %H:%M:%S')}] : ")
        if not raw.strip():
            start_dt = default_start
            break
        parsed = parse_datetime_input(raw, is_end=False)
        if parsed == "INVALID" or parsed is None:
            print("  X Format invalide.")
            continue
        start_dt = parsed
        break

    print(f"  Par defaut, fin = {default_end.strftime('%Y-%m-%d %H:%M:%S')}")
    while True:
        raw = input(f"Date/heure de fin [{default_end.strftime('%Y-%m-%d %H:%M:%S')}] : ")
        if not raw.strip():
            end_dt = default_end
            break
        parsed = parse_datetime_input(raw, is_end=True)
        if parsed == "INVALID" or parsed is None:
            print("  X Format invalide.")
            continue
        if parsed < start_dt:
            print("  X La date de fin doit etre posterieure a la date de debut.")
            continue
        end_dt = parsed
        break

    return start_dt, end_dt


def ask_timeframe() -> int:
    """Demande a l'utilisateur le timeframe et retourne la valeur en minutes."""
    print("\nTimeframes courants (en minutes) :")
    print("  " + "  ".join(TIMEFRAMES_HINT))
    print("  (toute autre valeur entiere positive est acceptee)")
    while True:
        raw = input("Timeframe de reference (en minutes) : ").strip()
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
# DECOUVERTE DES FICHIERS SUR DISQUE
# ─────────────────────────────────────────────

def scan_out_files(directory: str) -> dict[str, list[tuple[datetime, str]]]:
    """
    Parcourt le repertoire et regroupe les fichiers -out par instance.
    Les fichiers -error sont ignores.
    Retourne { instance: [(timestamp_nom_fichier, chemin_complet), ...] }
    trie par timestamp croissant pour chaque instance.
    """
    groups: dict[str, list[tuple[datetime, str]]] = {}
    for fname in os.listdir(directory):
        fpath = os.path.join(directory, fname)
        if not os.path.isfile(fpath):
            continue
        m = PM2_FILENAME_RE.match(fname)
        if not m or m.group("stream") != "out":
            continue
        instance = m.group("instance")
        ts = datetime.strptime(m.group("ts"), "%Y-%m-%d_%H-%M-%S")
        groups.setdefault(instance, []).append((ts, fpath))

    for instance in groups:
        groups[instance].sort(key=lambda t: t[0])

    return groups


def select_files_in_range(
    files: list[tuple[datetime, str]], start: datetime, end: datetime
) -> list[tuple[datetime, str]]:
    """
    Selectionne les fichiers dont l'horodatage tombe dans [start, end],
    plus le fichier le plus proche juste avant start et celui le plus
    proche juste apres end (un seul de chaque cote), s'ils existent.

    Si aucun fichier ne tombe dans [start, end], l'instance est consideree
    comme inactive sur la periode demandee : on ne va PAS chercher un
    fichier limitrophe (aussi proche soit-il chronologiquement dans la
    liste), sous peine de remonter un fichier arbitrairement ancien/recent
    sans rapport avec la periode demandee. L'elargissement aux fichiers
    limitrophes n'a de sens que pour completer une fenetre deja pertinente.
    """
    in_range = [(ts, f) for ts, f in files if start <= ts <= end]
    if not in_range:
        return []

    before_candidates = [(ts, f) for ts, f in files if ts < start]
    after_candidates = [(ts, f) for ts, f in files if ts > end]

    selected = list(in_range)
    if before_candidates:
        selected.append(max(before_candidates, key=lambda t: t[0]))
    if after_candidates:
        selected.append(min(after_candidates, key=lambda t: t[0]))

    selected.sort(key=lambda t: t[0])
    return selected


def is_whole_day_range(start: datetime, end: datetime) -> bool:
    """
    Determine si la plage saisie correspond a des journees completes
    (heure de debut = 00:00:00 et heure de fin = 23:59:59), ce qui
    declenche le mode journalier base sur les fichiers 00:00:00 de pm2.
    Toute autre heure saisie declenche le mode plage precise.
    """
    return start.time() == dtime(0, 0, 0) and end.time() == dtime(23, 59, 59)


def select_files_daily(
    files: list[tuple[datetime, str]], start_date, end_date
) -> list[tuple[datetime, str]] | None:
    """
    Mode journalier : le premier fichier retenu est celui date exactement
    <start_date> 00:00:00, le dernier est celui date <end_date + 1 jour>
    00:00:00, INCLUS EN TOTALITE (i.e. tous les fichiers dont l'horodatage
    est dans [start_date 00:00:00, end_date+1 00:00:00]).

    Ce dernier fichier est inclus entier et non exclu : la rotation de
    minuit vide le buffer pm2 de la journee qui se termine dans ce nouveau
    fichier, qui contient donc aussi les derniers cycles de end_date (en
    plus des premiers cycles du lendemain).

    Retourne None si le fichier 00:00:00 de start_date ou celui de
    end_date+1 n'existe pas pour cette instance : dans ce cas l'appelant
    doit basculer sur le mode de secours (select_files_in_range).
    """
    start_midnight = datetime.combine(start_date, dtime(0, 0, 0))
    next_day_midnight = datetime.combine(end_date + timedelta(days=1), dtime(0, 0, 0))

    has_start_file = any(ts == start_midnight for ts, _ in files)
    has_next_day_file = any(ts == next_day_midnight for ts, _ in files)
    if not has_start_file or not has_next_day_file:
        return None

    selected = [
        (ts, f) for ts, f in files
        if start_midnight <= ts <= next_day_midnight
    ]
    selected.sort(key=lambda t: t[0])
    return selected


def read_file_text(filepath: str) -> str:
    """Lit un fichier de log (decompresse .gz si besoin) en texte UTF-8."""
    if filepath.endswith(".gz"):
        with gzip.open(filepath, "rb") as f:
            raw_bytes = f.read()
    else:
        with open(filepath, "rb") as f:
            raw_bytes = f.read()
    return raw_bytes.decode("utf-8", errors="replace")


# ─────────────────────────────────────────────
# PARSING DES CYCLES
# ─────────────────────────────────────────────

def parse_cycles(text: str) -> list[dict]:
    """
    Decoupe le texte brut (deja nettoye des codes ANSI) en liste de cycles.
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

    if current_header is not None:
        cycles.append({
            "header":    current_header,
            "timestamp": current_ts,
            "body":      current_body,
        })

    return cycles


def collect_instance_cycles(filepaths: list[str]) -> list[dict]:
    """
    Lit tous les fichiers -out selectionnes pour une instance, les nettoie,
    les decoupe en cycles, fusionne le tout et trie chronologiquement.
    Deduplique les cycles identiques (meme en-tete) pouvant apparaitre a la
    frontiere entre deux fichiers issus d'une rotation pm2.
    """
    all_cycles: list[dict] = []
    for filepath in filepaths:
        text = read_file_text(filepath)
        text = strip_ansi(text)
        all_cycles.extend(parse_cycles(text))

    all_cycles.sort(key=lambda c: c["timestamp"])

    deduped: list[dict] = []
    seen_headers: set[str] = set()
    for cycle in all_cycles:
        if cycle["header"] in seen_headers:
            continue
        seen_headers.add(cycle["header"])
        deduped.append(cycle)

    return deduped


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
    sep = SEP_DOUBLE pour le premier cycle de chaque bougie, SEP_SINGLE sinon.
    """
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

        body = [l for l in cycle["body"] if not is_table_line(l)]
        while body and not body[0].strip():
            body.pop(0)
        while body and not body[-1].strip():
            body.pop()

        block = sep + "\n" + cycle["header"] + "\n" + sep + "\n" + "\n".join(body)
        parts.append(block)

    return "\n\n".join(parts) + "\n"


def build_output_filename(instance: str, cycles: list[dict]) -> str:
    """
    Construit le nom de fichier de sortie :
      gunbot_logs_<instance>_<date>_<heure_debut>_<heure_fin>.txt
    date/heures = periode couverte par les cycles reellement trouves pour
    l'instance (avant filtrage eventuel par bougie).
    """
    start_dt = cycles[0]["timestamp"]
    end_dt   = cycles[-1]["timestamp"]
    date_str  = start_dt.strftime("%Y-%m-%d")
    start_str = start_dt.strftime("%H%M%S")
    end_str   = end_dt.strftime("%H%M%S")
    return f"gunbot_logs_{instance}_{date_str}_{start_str}_{end_str}.txt"


# ─────────────────────────────────────────────
# POINT D'ENTREE
# ─────────────────────────────────────────────

def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))

    print("=" * 60)
    print("  Gunbot Log Processor (pm2 / serveur)")
    print(f"  OS detecte : {'Windows' if IS_WINDOWS else platform.system()}")
    print(f"  Repertoire : {script_dir}")
    print("=" * 60)

    # 1. Scanner le repertoire pour trouver les fichiers -out par instance
    groups = scan_out_files(script_dir)
    if not groups:
        print("\nX Aucun fichier '<instance>-out__...log' trouve dans le repertoire.")
        sys.exit(1)

    instances = sorted(groups.keys())
    print(f"\nInstance(s) detectee(s) : {', '.join(instances)}")
    for instance in instances:
        print(f"  - {instance} : {len(groups[instance])} fichier(s) au total")

    # 2. Periode a traiter
    start_dt, end_dt = ask_date_range()
    print(f"\nPeriode retenue : {start_dt.strftime('%Y-%m-%d %H:%M:%S')} "
          f"-> {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")

    daily_mode = is_whole_day_range(start_dt, end_dt)
    if daily_mode:
        print("  Mode journalier (fichiers 00:00:00 de pm2) - "
              "repli automatique par instance si besoin.")
    else:
        print("  Mode plage precise (heures personnalisees saisies).")

    # 3. Questions utilisateur (appliquees a toutes les instances)
    tf_minutes  = ask_timeframe()
    filter_mode = ask_filter_mode()

    # 4. Traitement par instance
    output_entries: list[tuple[str, str]] = []  # (nom_fichier, contenu)
    for instance in instances:
        selected = None
        if daily_mode:
            selected = select_files_daily(groups[instance], start_dt.date(), end_dt.date())
            if selected is None:
                print(f"\n  Instance {instance} : fichier(s) 00:00:00 manquant(s) "
                      f"(instance probablement inactive a la bascule de jour) "
                      f"-> repli sur le mode plage precise pour cette instance.")
                selected = select_files_in_range(groups[instance], start_dt, end_dt)
            else:
                print(f"\n  Instance {instance} : mode journalier applique "
                      f"(fichiers 00:00:00 trouves).")
        else:
            selected = select_files_in_range(groups[instance], start_dt, end_dt)

        if not selected:
            print(f"  Instance {instance} : aucun fichier dans/autour de la periode, ignoree.")
            continue

        print(f"\n  Traitement de l'instance {instance} "
              f"({len(selected)} fichier(s) selectionne(s)) :")
        for ts, fpath in selected:
            print(f"    - {os.path.basename(fpath)}")

        all_cycles = collect_instance_cycles([f for _, f in selected])
        print(f"    {len(all_cycles)} cycle(s) detecte(s).")

        if not all_cycles:
            print("    X Aucun cycle trouve pour cette instance, ignoree.")
            continue

        if filter_mode:
            cycles_to_render = filter_cycles_by_candle(all_cycles, tf_minutes)
            print(f"    {len(cycles_to_render)} cycle(s) conserve(s) "
                  f"apres filtrage (TF {tf_minutes} min).")
        else:
            cycles_to_render = all_cycles
            print(f"    Tous les cycles conserves (TF {tf_minutes} min "
                  f"pour la distinction visuelle).")

        output_filename = build_output_filename(instance, all_cycles)
        content = format_output(cycles_to_render, tf_minutes)
        output_entries.append((output_filename, content))

    if not output_entries:
        print("\nX Aucune instance n'a pu etre traitee.")
        sys.exit(1)

    # 5. Ecriture de l'archive zip de sortie, dans le meme repertoire
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_zip_path = os.path.join(script_dir, f"gunbot_logs_processed_{run_ts}.zip")

    with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as out_zf:
        for filename, content in output_entries:
            out_zf.writestr(filename, content)

    print(f"\n  OK Archive de sortie creee : {os.path.basename(output_zip_path)}")
    for filename, _ in output_entries:
        print(f"    - {filename}")


if __name__ == "__main__":
    main()
