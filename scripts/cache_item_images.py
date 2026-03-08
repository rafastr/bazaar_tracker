from __future__ import annotations

import argparse
import html
import os
import re
import sqlite3
import time
import urllib.parse

from typing import Optional, Tuple
from requests.adapters import HTTPAdapter

import certifi
import requests
import urllib3

# Identify ourselves politely; keep requests rate-limited with --sleep
UA = "BazaarTracker/0.1 (local image cache builder; respectful scraping)"

CARD_CANON_RE = re.compile(
    r'(/card/[^/"<>\s]+/[^"<>\s]+)',
    re.IGNORECASE,
)

CARD_ABS_RE = re.compile(
    r'(https?://(?:global\.)?bazaardb\.gg/card/[^/"<>\s]+/[^"<>\s]+)',
    re.IGNORECASE,
)

CDN_IMAGE_RE = re.compile(
    r"(https?://s\.bazaardb\.gg/[^\"'<>]+?\.(?:webp|png|jpe?g))",
    re.IGNORECASE,
)

OG_IMAGE_RE = re.compile(
    r'<meta\s+property=["\']og:image["\']\s+content=["\'](?P<url>https?://s\.bazaardb\.gg/[^"\']+)["\']',
    re.IGNORECASE,
)

CARD_TITLE_RE = re.compile(
    r"<h1[^>]*>\s*(?P<title>[^<]+?)\s*</h1>",
    re.IGNORECASE | re.DOTALL,
)

TITLE_TAG_RE = re.compile(
    r"<title>\s*(?P<title>[^<]+?)\s*</title>",
    re.IGNORECASE | re.DOTALL,
)

CARD_URL_OVERRIDES = {
    "Temple Expedition Ticket": "https://bazaardb.gg/card/lsqnp5wd48wb1vk6gvwgmqty2s/Temple-Expedition-Ticket",
    "Crash Site Ticket": "https://bazaardb.gg/card/gd3jys9fcffktvvx18qky8s4m9/Crash-Site-Ticket",
}


def _clean_url(u: str) -> str:
    u = html.unescape(u)
    u = u.replace("\\/", "/")
    u = u.replace("\\u0027", "'")
    u = re.sub(r"\s+", "", u).strip()
    u = u.rstrip("\\")
    u = u.rstrip('"')
    u = u.rstrip("'")
    return u.split("?")[0]


def _normalize_search_html(text: str) -> str:
    text = text.replace("\\/", "/")
    text = text.replace("\\u0027", "'")
    text = text.replace("\\u0026", "&")
    text = html.unescape(text)
    return text


def _norm_name(text: str) -> str:
    text = html.unescape(text)
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _search_variants(name: str) -> list[str]:
    variants: list[str] = []
    seen = set()

    def add(s: str) -> None:
        s = s.strip()
        if s and s not in seen:
            seen.add(s)
            variants.append(s)

    add(name)
    add(name.replace("'", ""))
    add(name.replace("'", " "))
    add(re.sub(r"'s\b", "s", name, flags=re.IGNORECASE))
    add(re.sub(r"['’]", "", name))

    return variants


def _extract_card_name(card_html: str) -> Optional[str]:
    m = CARD_TITLE_RE.search(card_html)
    if m:
        return html.unescape(m.group("title")).strip()

    m = TITLE_TAG_RE.search(card_html)
    if m:
        title = html.unescape(m.group("title")).strip()
        title = title.split(" - ")[0].strip()
        if title:
            return title

    return None


def _extract_candidate_card_urls(search_html: str) -> list[str]:
    search_html = _normalize_search_html(search_html)

    seen = set()
    out = []

    for path in CARD_CANON_RE.findall(search_html):
        url = _clean_url("https://bazaardb.gg" + path)
        if "/card/" in url and url not in seen:
            seen.add(url)
            out.append(url)

    for url in CARD_ABS_RE.findall(search_html):
        url = _clean_url(url)
        if "/card/" in url and url not in seen:
            seen.add(url)
            out.append(url)

    return out


def score_image_url(u: str) -> int:
    u2 = u.lower()
    if u2.endswith(".webp"):
        return 3
    if u2.endswith(".png"):
        return 2
    if u2.endswith(".jpg") or u2.endswith(".jpeg"):
        return 1
    return 0


def build_session(insecure: bool = False) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": UA,
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    s.verify = False if insecure else certifi.where()

    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=20,
        max_retries=1,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    return s


def fetch_text(session: requests.Session, url: str, timeout: int) -> str:
    r = session.get(
        url,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=(10, timeout)
    )
    r.raise_for_status()
    return r.text


def fetch_bytes(session: requests.Session, url: str, timeout: int) -> bytes:
    r = session.get(
        url,
        headers={
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        },
        timeout=(10, timeout)
    )
    r.raise_for_status()
    return r.content


def resolve_bazaardb_image_url(
    session: requests.Session,
    name: str,
    timeout: int,
    debug: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    search_urls: list[str] = []

    for variant in _search_variants(name):
        quoted_variant = f'"{variant}"'
        search_urls.extend(
            [
                f"https://bazaardb.gg/search?c=items&q={urllib.parse.quote(quoted_variant)}",
                f"https://bazaardb.gg/search?c=items&q={urllib.parse.quote(variant)}",
                f"https://bazaardb.gg/search?q={urllib.parse.quote(quoted_variant)}",
                f"https://bazaardb.gg/search?q={urllib.parse.quote(variant)}",
            ]
        )

    wanted = _norm_name(name)
    tried_card_urls = set()

    override_url = CARD_URL_OVERRIDES.get(name)
    if override_url:
        try:
            card_html = fetch_text(session, override_url, timeout=timeout)
            m_og = OG_IMAGE_RE.search(card_html)
            if m_og:
                return override_url, _clean_url(m_og.group("url"))

            cdn_urls = [_clean_url(m.group(1)) for m in CDN_IMAGE_RE.finditer(card_html)]
            if cdn_urls:
                cdn_urls.sort(key=score_image_url, reverse=True)
                return override_url, cdn_urls[0]

            return override_url, None
        except requests.RequestException:
            pass

    for search_url in search_urls:
        try:
            search_html = fetch_text(session, search_url, timeout=timeout)
            if debug:
                print(f"[DEBUG] search_url={search_url}")
                print(f"[DEBUG] html_len={len(search_html)}")
                print(f"[DEBUG] has_/card_={'/card/' in search_html}")
                print(f"[DEBUG] first_300={search_html[:300]!r}")
        except requests.RequestException:
            continue

        candidate_urls = _extract_candidate_card_urls(search_html)
        if debug:
            print(f"[DEBUG] candidates={candidate_urls[:10]}")

        if not candidate_urls:
            continue

        for card_url in candidate_urls:
            if card_url in tried_card_urls:
                continue
            tried_card_urls.add(card_url)

            if debug:
                print(f"[DEBUG] trying_card_url={card_url}")

            try:
                card_html = fetch_text(session, card_url, timeout=timeout)
            except requests.RequestException:
                continue

            card_name = _extract_card_name(card_html)
            if debug:
                print(f"[DEBUG] card_url={card_url}")
                print(f"[DEBUG] card_name={card_name!r}")

            if not card_name:
                continue

            if _norm_name(card_name) != wanted:
                continue

            m_og = OG_IMAGE_RE.search(card_html)
            if m_og:
                return card_url, _clean_url(m_og.group("url"))

            cdn_urls = [_clean_url(m.group(1)) for m in CDN_IMAGE_RE.finditer(card_html)]
            if not cdn_urls:
                return card_url, None

            cdn_urls.sort(key=score_image_url, reverse=True)
            return card_url, cdn_urls[0]

    return None, None


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def ensure_image_path_column(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(templates)")
    cols = {row[1] for row in cur.fetchall()}
    if "image_path" not in cols:
        cur.execute("ALTER TABLE templates ADD COLUMN image_path TEXT")
        conn.commit()


def ensure_ignored_column(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(templates)")
    cols = {row[1] for row in cur.fetchall()}
    if "ignored" not in cols:
        cur.execute("ALTER TABLE templates ADD COLUMN ignored INTEGER DEFAULT 0")
        conn.commit()


def clear_image_path(cur: sqlite3.Cursor, conn: sqlite3.Connection, template_id: str) -> None:
    cur.execute("UPDATE templates SET image_path=NULL WHERE template_id=?", (template_id,))
    conn.commit()


def build_image_paths(out_dir: str, template_id: str) -> tuple[str, str]:
    filename = f"{template_id}.webp"
    disk_path = os.path.join(out_dir, filename)
    db_path = os.path.join("assets", "images", "items", filename).replace("\\", "/")
    return disk_path, db_path


def cache_item_images(
    db_path: str,
    out_dir: str = "assets/images/items",
    sleep: float = 0.7,
    limit: int = 0,
    force: bool = False,
    timeout: int = 30,
    insecure: bool = False,
    debug: bool = False,
) -> dict:

    if insecure:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = build_session(insecure=insecure)

    ensure_dir(out_dir)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        ensure_image_path_column(conn)
        ensure_ignored_column(conn)

        cur = conn.cursor()

        cur.execute(
            "SELECT template_id, name, image_path FROM templates "
            "WHERE ignored=0 ORDER BY name ASC"
        )
        rows = cur.fetchall()

        total = len(rows)
        print(f"Need images for: {total} items")

        ok = 0
        skipped = 0
        fixed = 0
        unresolved = 0
        failed = 0

        for idx, r in enumerate(rows, start=1):
            template_id = str(r["template_id"])
            name = str(r["name"])
            existing_image_path = r["image_path"]

            disk_path, db_path_img = build_image_paths(out_dir, template_id)

            # Prefer canonical disk location. If it exists, repair DB path if needed.
            if (not force) and os.path.exists(disk_path):
                if existing_image_path != db_path_img:
                    cur.execute(
                        "UPDATE templates SET image_path=? WHERE template_id=?",
                        (db_path_img, template_id),
                    )
                    conn.commit()
                    fixed += 1
                    print(f"[{idx}/{total}] [FIX] {name} -> {db_path_img}")
                else:
                    skipped += 1
                    print(f"[{idx}/{total}] [SKIP] {name}")
                continue

            # If DB already points somewhere valid, skip.
            if (not force) and existing_image_path:
                existing_disk_path = os.path.normpath(existing_image_path)
                if os.path.exists(existing_disk_path):
                    skipped += 1
                    print(f"[{idx}/{total}] [SKIP] {name}")
                    continue

            try:
                card_url, img_url = resolve_bazaardb_image_url(
                    session,
                    name,
                    timeout=timeout,
                    debug=debug,
                )

                if not img_url:
                    cur.execute(
                        "UPDATE templates SET ignored=1, image_path=NULL WHERE template_id=?",
                        (template_id,),
                    )
                    conn.commit()

                    unresolved += 1
                    print(f"[{idx}/{total}] [IGNORED] {name} card={card_url}")
                    time.sleep(sleep)
                    continue

                data = fetch_bytes(session, img_url, timeout=max(timeout, 60))

                with open(disk_path, "wb") as f:
                    f.write(data)

                cur.execute(
                    "UPDATE templates SET image_path=? WHERE template_id=?",
                    (db_path_img, template_id),
                )
                conn.commit()

                ok += 1
                print(f"[{idx}/{total}] [OK] {name} -> {db_path_img}")

                if limit and ok >= limit:
                    break

            except (requests.RequestException, OSError) as e:
                if force:
                    clear_image_path(cur, conn, template_id)

                failed += 1
                print(f"[{idx}/{total}] [FAIL] {name}: {e}")

            time.sleep(sleep)

        return {
            "ok": True,
            "message": "Item image cache updated",
            "downloaded": ok,
            "skipped": skipped,
            "fixed": fixed,
            "unresolved": unresolved,
            "failed": failed,
        }

    finally:
        conn.close()


def main() -> None:

    ap = argparse.ArgumentParser(description="Cache BazaarDB item images locally keyed by template_id")

    ap.add_argument("--db", required=True)
    ap.add_argument("--out-dir", default="assets/images/items")
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--insecure", action="store_true")
    ap.add_argument("--debug", action="store_true")

    args = ap.parse_args()

    result = cache_item_images(
        db_path=args.db,
        out_dir=args.out_dir,
        sleep=args.sleep,
        limit=args.limit,
        force=args.force,
        timeout=args.timeout,
        insecure=args.insecure,
        debug=args.debug,
    )

    print(result)


if __name__ == "__main__":
    main()
