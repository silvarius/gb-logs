#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gbLogs_0-7.py
Traitement et mise en forme des fichiers de logs Gunbot generes par pm2.
Version "serveur" : le script est place directement dans le repertoire
pm2/logs et travaille sur les fichiers de logs presents sur disque.
Version cross-platform : Linux et Windows.

Usage :
  placer ce script dans le repertoire pm2/logs, puis : python3 gbLogs_0-7.py

CHANGEMENT MAJEUR PAR RAPPORT A LA 0-6 : detection des cycles
--------------------------------------------------------------
Les strategies "custom" (ex : maret_gunbot, executees sur des paires
alimentees en websockets, comme USDC-HYPE@hyperliquid) ne generent PAS la
ligne texte "GB_x.x.x PAIR@exchange Round #N YYYY/MM/DD HH:MM:SS" qui
precedait auparavant le tableau ASCII pour les strategies natives (ex :
stepgrid). Seul le tableau ASCII est encore emis, et il est strictement
identique dans sa structure (colonnes Round / Exchange / Pair / Strategy /
Timestamp sur la 2e ligne de donnees) que la strategie soit native ou
custom.

Verifie sur des logs reels (gbd2, strategie maret_gunbot 0-1-9, paire
USDC-HYPE@hyperliquid) : malgre le websocket, le tableau continue d'etre
emis a intervalle regulier (~5s, cf. boucle de polling du script de
strategie) - le websocket ne fait que changer la SOURCE des donnees
utilisees a l'interieur de chaque cycle (cache tague "source":"ws" /
"ws-candle" dans les lignes [WS_DEBUG]), il ne supprime pas la notion de
cycle ni la generation du tableau. La detection de cycle peut donc etre
unifiee pour les deux types de strategies :

  - Un cycle commence a la bordure superieure du tableau (ligne "┌─...─┐"),
    qu'une ligne texte "GB_..." la precede ou non.
  - Les informations de cycle (version, round, exchange, pair, strategy,
    timestamp) sont lues directement sur la ligne de donnees du tableau
    (juste apres la ligne d'en-tetes de colonnes), AVANT que le tableau ne
    soit ecarte du corps du cycle.
  - Le tableau (bordures + lignes de donnees) n'est jamais copie dans le
    corps du cycle : il est uniquement analyse puis saute jusqu'a sa
    bordure inferieure ("└─...─┘"). Le corps du cycle commence juste apres.
  - L'eventuelle ligne texte "GB_..." heritee des strategies natives, si
    elle est presente, est desormais purement redondante avec les
    informations du tableau : elle est filtree du corps du cycle (elle
    finirait sinon, du fait de l'inversion d'ancrage, en fin de corps du
    cycle PRECEDENT).
  - La cle d'unicite utilisee pour le dedoublonnage aux frontieres de
    rotation pm2 est desormais la ligne de donnees brute du tableau
    (au lieu de l'ancienne ligne texte).

SELECTION DE PAIRE(S)
----------------------
Une meme instance pm2 peut heberger plusieurs paires simultanement (ex :
gbd2 a fait tourner USDC-BTC ET USDC-HYPE dans le meme fichier, la premiere
etant un residu de test en erreur permanente - API 422, aucune sortie de
strategie custom, aucun "reached end of user code"). Le tableau donnant
desormais la paire de facon fiable pour CHAQUE cycle, gbLogs peut filtrer
par paire :
  - Si une seule paire est detectee pour l'instance -> traitement normal,
    nommage de fichier inchange (retro-compatible avec la 0-6).
  - Si plusieurs paires sont detectees -> l'utilisateur choisit
    interactivement la ou les paires a conserver (ou toutes). Si plusieurs
    paires sont conservees, un fichier .txt distinct est genere par paire
    dans l'archive de sortie, avec la paire ajoutee au nom de fichier.

Le reste (recherche des fichiers -out par instance, mode journalier /
plage precise, filtrage optionnel au 1er cycle par bougie, separateurs
simple/double, suppression des codes ANSI, ignorance des fichiers
-error) est repris a l'identique de la 0-6.

Sortie :
  - Une archive .zip deposee dans le meme repertoire (pm2/logs), contenant
    un fichier .txt fusionne par instance (ou par instance+paire si
    plusieurs paires conservees), nomme :
      gunbot_logs_<instance>_<date>_<heure_debut>_<heure_fin>.txt
      gunbot_logs_<instance>_<paire>_<date>_<heure_debut>_<heure_fin>.txt
  - Les fichiers sources ne sont ni modifies ni supprimes.
"""

import gzip
import os
import platform
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from datetime import time as dtime
import zipfile


# ─────────────────────────────────────────────
# CONSTANTES
# ─────────────────────────────────────────────

IS_WINDOWS = platform.system() == "Windows"

# Codes d'echappement ANSI (couleurs/fond ajoutes par pm2)
ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')

# Ancienne ligne d'en-tete de cycle des strategies natives (ex : stepgrid).
# Desormais purement redondante avec le tableau : filtree du corps du
# cycle si elle est rencontree (cf. note en tete de fichier).
LEGACY_HEADER_RE = re.compile(
    r'^GB[_\s]\S+\s+\S+@\S+\s+Round\s+#\d+\s+\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}$'
)

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


def ask_pair_selection(pair_counts: "Counter[str]") -> set[str]:
    """
    Demande a l'utilisateur quelle(s) paire(s) conserver lorsque plusieurs
    paires distinctes sont detectees dans les cycles d'une meme instance.
    Retourne l'ensemble des paires a conserver.
    """
    pairs = sorted(pair_counts.keys())
    print("\n  Plusieurs paires detectees dans les logs de cette instance :")
    for idx, p in enumerate(pairs, start=1):
        print(f"    {idx} - {p} ({pair_counts[p]} cycle(s))")
    all_idx = len(pairs) + 1
    print(f"    {all_idx} - Toutes les paires (un fichier .txt distinct par paire)")
    print("    (plusieurs numeros separes par des virgules acceptes, ex : 1,3)")
    while True:
        raw = input("  Paire(s) a conserver : ").strip()
        if not raw:
            continue
        if raw == str(all_idx):
            return set(pairs)
        try:
            idxs = [int(x.strip()) for x in raw.split(",")]
            if idxs and all(1 <= x <= len(pairs) for x in idxs):
                return {pairs[x - 1] for x in idxs}
        except ValueError:
            pass
        print("    X Selection invalide.")


def parse_cycle_timestamp(ts_str: str) -> datetime:
    """Convertit '2026/04/26 19:15:46' en objet datetime (naif, UTC implicite)."""
    return datetime.strptime(ts_str.strip(), "%Y/%m/%d %H:%M:%S")


def candle_open_time(dt: datetime, tf_minutes: int) -> datetime:
    """Retourne l'heure d'ouverture de la bougie dans laquelle tombe dt."""
    total_seconds = int(dt.timestamp())
    tf_seconds = tf_minutes * 60
    candle_start_ts = (total_seconds // tf_seconds) * tf_seconds
    return datetime.fromtimestamp(candle_start_ts, tz=timezone.utc).replace(tzinfo=None)


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
    fichier limitrophe.
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
    """
    return start.time() == dtime(0, 0, 0) and end.time() == dtime(23, 59, 59)


def select_files_daily(
    files: list[tuple[datetime, str]], start_date, end_date
) -> list[tuple[datetime, str]] | None:
    """
    Mode journalier : le premier fichier retenu est celui date exactement
    <start_date> 00:00:00, le dernier est celui date <end_date + 1 jour>
    00:00:00, INCLUS EN TOTALITE.

    Retourne None si l'un des deux fichiers 00:00:00 attendus n'existe pas
    pour cette instance : l'appelant doit alors basculer sur le mode de
    secours (select_files_in_range).
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
# PARSING DES CYCLES (base sur le tableau ASCII)
# ─────────────────────────────────────────────

def _is_table_top(line: str) -> bool:
    s = line.strip()
    return s.startswith("┌") and s.endswith("┐")


def _is_table_bottom(line: str) -> bool:
    s = line.strip()
    return s.startswith("└") and s.endswith("┘")


def extract_cycle_metadata(data_row: str) -> dict | None:
    """
    Extrait les informations de cycle (version, round, exchange, pair,
    strategy, timestamp) a partir de la 2e ligne de donnees du tableau
    ASCII (celle qui suit la ligne d'en-tetes de colonnes) :

      │                   │ GB 31.5.5  │ 258   │ hyperliquid │ USDC-HYPE │ custom/custom │ 2026/08/26 17:12:42 │

    Colonnes attendues, dans l'ordre : (label, Version, Round, Exchange,
    Pair, Strategy, Timestamp). Retourne None si la ligne ne matche pas la
    structure attendue (tableau malforme/tronque).
    """
    cells = [c.strip() for c in data_row.strip().strip("│").split("│")]
    if len(cells) != 7:
        return None
    _, version, round_s, exchange, pair, strategy, ts_s = cells
    try:
        timestamp = parse_cycle_timestamp(ts_s)
    except ValueError:
        return None
    version_num = version.replace("GB", "").strip()
    header = f"GB_{version_num} {pair}@{exchange} Round #{round_s} {ts_s}"
    return {
        "header": header,
        "timestamp": timestamp,
        "round": round_s,
        "exchange": exchange,
        "pair": pair,
        "strategy": strategy,
        "raw_key": data_row.strip(),
    }


def parse_cycles(text: str) -> list[dict]:
    """
    Decoupe le texte brut (deja nettoye des codes ANSI) en liste de cycles,
    en se basant sur le tableau ASCII plutot que sur une eventuelle ligne
    texte "GB_..." (absente pour les strategies custom/websockets, cf.
    note en tete de fichier).

    Chaque cycle : { header, timestamp, round, exchange, pair, strategy,
                     raw_key, body }
    Le tableau lui-meme (bordures + lignes de donnees) n'est jamais copie
    dans le corps du cycle : uniquement analyse puis saute.
    """
    lines = text.splitlines()
    n = len(lines)
    cycles: list[dict] = []
    current_meta: dict | None = None
    current_body: list[str] = []
    in_table = False
    i = 0

    while i < n:
        raw_line = lines[i].rstrip("\r")

        if not in_table and _is_table_top(raw_line):
            # Nouveau tableau => on cloture le cycle en cours (s'il existe)
            if current_meta is not None:
                cycles.append({**current_meta, "body": current_body})
                current_body = []

            # La ligne de donnees attendue est 3 lignes plus bas :
            #   i   = bordure superieure
            #   i+1 = ligne d'en-tetes de colonnes
            #   i+2 = separateur
            #   i+3 = ligne de donnees (Round/Exchange/Pair/Strategy/Timestamp)
            if i + 3 < n:
                current_meta = extract_cycle_metadata(lines[i + 3].rstrip("\r"))
            else:
                current_meta = None  # tableau tronque en fin de fichier

            in_table = True
            i += 1
            continue

        if in_table:
            if _is_table_bottom(raw_line):
                in_table = False
            i += 1
            continue

        # Ligne normale, hors tableau
        if current_meta is not None:
            if not LEGACY_HEADER_RE.match(raw_line.strip()):
                current_body.append(raw_line)
        i += 1

    if current_meta is not None:
        cycles.append({**current_meta, "body": current_body})

    return cycles


def collect_instance_cycles(filepaths: list[str]) -> list[dict]:
    """
    Lit tous les fichiers -out selectionnes pour une instance, les nettoie,
    les decoupe en cycles, fusionne le tout et trie chronologiquement.
    Deduplique les cycles identiques (meme ligne de donnees de tableau)
    pouvant apparaitre a la frontiere entre deux fichiers issus d'une
    rotation pm2.
    """
    all_cycles: list[dict] = []
    for filepath in filepaths:
        text = read_file_text(filepath)
        text = strip_ansi(text)
        all_cycles.extend(parse_cycles(text))

    all_cycles.sort(key=lambda c: c["timestamp"])

    deduped: list[dict] = []
    seen_keys: set[str] = set()
    for cycle in all_cycles:
        if cycle["raw_key"] in seen_keys:
            continue
        seen_keys.add(cycle["raw_key"])
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
    Le corps de chaque cycle ne contient deja plus le tableau ASCII (jamais
    copie par parse_cycles) : pas de filtrage supplementaire necessaire ici.
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

        body = list(cycle["body"])
        while body and not body[0].strip():
            body.pop(0)
        while body and not body[-1].strip():
            body.pop()

        block = sep + "\n" + cycle["header"] + "\n" + sep + "\n" + "\n".join(body)
        parts.append(block)

    return "\n\n".join(parts) + "\n"


def build_output_filename(instance: str, cycles: list[dict], pair_suffix: str | None = None) -> str:
    """
    Construit le nom de fichier de sortie :
      gunbot_logs_<instance>_<date>_<heure_debut>_<heure_fin>.txt
      gunbot_logs_<instance>_<paire>_<date>_<heure_debut>_<heure_fin>.txt
    date/heures = periode couverte par les cycles reellement trouves
    (avant filtrage eventuel par bougie). pair_suffix n'est ajoute que
    lorsque plusieurs paires distinctes sont conservees pour l'instance
    (retro-compatibilite avec la 0-6 dans le cas courant a une seule paire).
    """
    start_dt = cycles[0]["timestamp"]
    end_dt = cycles[-1]["timestamp"]
    date_str = start_dt.strftime("%Y-%m-%d")
    start_str = start_dt.strftime("%H%M%S")
    end_str = end_dt.strftime("%H%M%S")
    if pair_suffix:
        return f"gunbot_logs_{instance}_{pair_suffix}_{date_str}_{start_str}_{end_str}.txt"
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
    tf_minutes = ask_timeframe()
    filter_mode = ask_filter_mode()

    # 4. Traitement par instance
    output_entries: list[tuple[str, str]] = []  # (nom_fichier, contenu)
    for instance in instances:
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

        # 4bis. Selection de paire(s) si plusieurs paires distinctes
        pair_counts = Counter(c["pair"] for c in all_cycles)
        if len(pair_counts) > 1:
            print(f"    {len(pair_counts)} paire(s) distincte(s) detectee(s) : "
                  f"{', '.join(f'{p} ({n})' for p, n in sorted(pair_counts.items()))}")
            selected_pairs = ask_pair_selection(pair_counts)
        else:
            selected_pairs = set(pair_counts.keys())

        kept_cycles = [c for c in all_cycles if c["pair"] in selected_pairs]
        if not kept_cycles:
            print("    X Aucun cycle conserve apres selection de paire, instance ignoree.")
            continue

        distinct_pairs_kept = sorted(set(c["pair"] for c in kept_cycles))
        if len(distinct_pairs_kept) == 1:
            groups_by_pair = {None: kept_cycles}
        else:
            groups_by_pair = {}
            for c in kept_cycles:
                groups_by_pair.setdefault(c["pair"], []).append(c)

        for pair_suffix, group_cycles in groups_by_pair.items():
            group_cycles.sort(key=lambda c: c["timestamp"])

            if filter_mode:
                cycles_to_render = filter_cycles_by_candle(group_cycles, tf_minutes)
                label = pair_suffix or instance
                print(f"    [{label}] {len(cycles_to_render)} cycle(s) conserve(s) "
                      f"apres filtrage (TF {tf_minutes} min).")
            else:
                cycles_to_render = group_cycles

            output_filename = build_output_filename(instance, group_cycles, pair_suffix)
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
