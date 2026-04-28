"""
İkinci geçiş — DuckDuckGo versiyonu.
Boş kayıtların Instagram'ını domain heuristiği + DDG ile bul. API key gerektirmez.

Kurulum: pip install duckduckgo-search

Kullanım:
  python second_pass_ddg.py              # Default 200 kayıt
  python second_pass_ddg.py --max 50     # Sadece 50 kayıt
"""
import os
import re
import json
import time
import argparse
from urllib.parse import urlparse
from ddgs import DDGS

INPUT_FILE = "diamond_manufacturers_merged.json"
OUTPUT_FILE = "diamond_manufacturers_enriched.json"

# Aggregator/dizin domainleri
DIRECTORY_DOMAINS = {
    "iyifirma.com", "dijitalrehber.com", "sahibinden.com", "n11.com",
    "hepsiburada.com", "trendyol.com", "g.page", "goo.gl",
    "maps.google.com", "facebook.com", "linkedin.com",
    "yandex.com", "tumisyeri.com", "yelp.com", "foursquare.com",
    "wixsite.com", "blogspot.com", "wordpress.com",
}
IG_BLACKLIST = {
    "explore", "p", "reel", "reels", "tv", "stories", "accounts", "share",
    # Popüler resmi hesaplar / generic match trap'leri
    "instagram", "meta", "facebook", "whatsapp", "google", "youtube",
    "twitter", "tiktok", "spotify", "netflix", "amazon", "apple",
    "official", "support", "help", "about", "press",
}
INSTAGRAM_REGEX = re.compile(
    r"(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9_.]+)/?",
    re.IGNORECASE,
)
NAME_STOP_WORDS = {
    "kuyumculuk", "kuyumcu", "pırlanta", "pirlanta", "mücevher", "mucevher",
    "altın", "altin", "gümüş", "gumus", "flagship", "şube", "sube",
    "atölyesi", "atolyesi", "tasarım", "tasarim", "imalat", "üretim", "uretim",
    "ltd", "şti", "sti", "as", "a.ş", "ticaret", "the",
}


def domain_root(url: str) -> str | None:
    if not url:
        return None
    try:
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        if netloc in DIRECTORY_DOMAINS:
            return None
        parts = netloc.split(".")
        if len(parts) < 2:
            return None
        root = parts[0]
        if len(root) < 4:
            return None
        return root
    except Exception:
        return None


def name_keywords(name: str) -> str | None:
    if not name:
        return None
    words = [w for w in name.split() if w.lower() not in NAME_STOP_WORDS and len(w) > 2]
    return " ".join(words[:2]) if words else None


def ddg_search(query: str, ddgs: DDGS, max_results: int = 5) -> list[str]:
    """site:instagram.com {query} → handle listesi"""
    full_q = f"site:instagram.com {query}"
    try:
        results = list(ddgs.text(full_q, max_results=max_results, region="tr-tr"))
    except Exception as e:
        print(f"    ! DDG error: {e}")
        return []
    handles = []
    for r in results:
        # DDG result: {'title':..., 'href':..., 'body':...}
        href = r.get("href", "")
        m = INSTAGRAM_REGEX.search(href)
        if m:
            h = m.group(1)
            if h.lower() not in IG_BLACKLIST and h not in handles:
                handles.append(h)
    return handles


def best_match(handles: list[str], domain: str | None, name_kw: str | None) -> str | None:
    if not handles:
        return None
    if domain:
        for h in handles:
            if domain in h.lower():
                return h
    if name_kw:
        first = name_kw.split()[0].lower()
        if len(first) >= 4:
            for h in handles:
                if first in h.lower():
                    return h
    return None


def main(max_records: int):
    src = OUTPUT_FILE if os.path.exists(OUTPUT_FILE) else INPUT_FILE
    print(f"Kaynak: {src}")
    with open(src, encoding="utf-8") as f:
        data = json.load(f)

    targets = [
        e for e in data
        if not e.get("instagram_handle")
        and not e.get("emails")
        and e.get("website")
    ]
    print(f"Hedef (boş + website'li): {len(targets)} | Max: {max_records}\n")

    found = 0
    processed = 0

    with DDGS() as ddgs:
        for i, e in enumerate(targets, 1):
            if processed >= max_records:
                print(f"\n⚠ {max_records} kayıt limiti — duruluyor.")
                break

            domain = domain_root(e["website"])
            name_kw = name_keywords(e.get("name", ""))

            if not domain and not name_kw:
                continue
            processed += 1

            ig = None

            # 1) Domain heuristiği
            if domain:
                handles = ddg_search(domain, ddgs)
                ig = best_match(handles, domain, name_kw)
                time.sleep(1.5)  # DDG'a nazik ol

            # 2) İsim fallback
            if not ig and name_kw:
                handles = ddg_search(f'"{name_kw}"', ddgs)
                ig = best_match(handles, domain, name_kw)
                time.sleep(1.5)

            status = f"@{ig}" if ig else "—"
            print(f"  [{i}/{len(targets)}] {e['name'][:45]:45} → {status}")

            if ig:
                e["instagram_handle"] = ig
                e["instagram_url"] = f"https://instagram.com/{ig}"
                e["instagram_source"] = "second_pass_ddg"
                found += 1
                with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    total_ig = sum(1 for e in data if e.get("instagram_handle"))
    print(f"\n✓ Bu run: {found} yeni IG | İşlenen: {processed} | Toplam IG: {total_ig} | → {OUTPUT_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=115,
                        help="Bu run'da işlenecek max kayıt")
    args = parser.parse_args()
    main(args.max)