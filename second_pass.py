"""
İkinci geçiş: boş kayıtların Instagram'ını domain heuristiği + CSE ile bul.

Mantık:
  1. Boş + website'i olan kayıtları al
  2. Domain root çıkar (pelinkuyumculuk.com.tr → pelinkuyumculuk)
  3. site:instagram.com {domain_root} CSE araması
  4. Bulamazsa isim bazlı CSE araması (fallback)
  5. Sonucu validate et (domain root handle içinde olmalı)

Kullanım:
  python second_pass.py              # Default 90 API call (CSE quota güvenli)
  python second_pass.py --max 50     # Sadece 50 API call yap
  python second_pass.py --max 200    # Quota'n yüksekse
"""
import os
import re
import json
import time
import argparse
import requests
from urllib.parse import urlparse

CSE_API_KEY = os.getenv("GOOGLE_CSE_API_KEY")
CSE_ID = os.getenv("GOOGLE_CSE_ID")

INPUT_FILE = "diamond_manufacturers_merged.json"
OUTPUT_FILE = "diamond_manufacturers_enriched.json"

# Aggregator/dizin domainleri — bunlardan handle çıkartmaya çalışma
DIRECTORY_DOMAINS = {
    "iyifirma.com", "dijitalrehber.com", "sahibinden.com", "n11.com",
    "hepsiburada.com", "trendyol.com", "g.page", "goo.gl",
    "maps.google.com", "facebook.com", "linkedin.com",
    "yandex.com", "tumisyeri.com", "yelp.com", "foursquare.com",
    "wixsite.com", "blogspot.com", "wordpress.com",
}

IG_BLACKLIST = {"explore", "p", "reel", "reels", "tv", "stories", "accounts", "share"}
INSTAGRAM_REGEX = re.compile(
    r"(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9_.]+)/?",
    re.IGNORECASE,
)

# Türkçe stop words — isim aramasında çıkartılır
NAME_STOP_WORDS = {
    "kuyumculuk", "kuyumcu", "pırlanta", "pirlanta", "mücevher", "mucevher",
    "altın", "altin", "gümüş", "gumus", "flagship", "şube", "sube",
    "atölyesi", "atolyesi", "tasarım", "tasarim", "imalat", "üretim", "uretim",
    "ltd", "şti", "sti", "as", "a.ş", "ticaret", "the",
}


def domain_root(url: str) -> str | None:
    """https://tufankuyumculuk.com.tr/ → 'tufankuyumculuk'"""
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
        if len(root) < 4:  # çok kısa ise generic olabilir
            return None
        return root
    except Exception:
        return None


def name_keywords(name: str) -> str | None:
    """'Pelin Kuyumculuk Flagship' → 'Pelin Flagship' (stop word'ler atılmış)"""
    if not name:
        return None
    words = [w for w in name.split() if w.lower() not in NAME_STOP_WORDS and len(w) > 2]
    return " ".join(words[:2]) if words else None


def cse_search(query: str) -> list[str]:
    """site:instagram.com {query} → handle listesi"""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": CSE_API_KEY,
        "cx": CSE_ID,
        "q": f"site:instagram.com {query}",
        "num": 5,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        # Auth/quota hatalarında derhal dur — 90 boş çağrı yapma
        if r.status_code in (401, 403):
            raise SystemExit(
                f"\n✗ AUTH HATASI ({r.status_code}): {r.text[:300]}\n"
                f"  • GOOGLE_CSE_API_KEY ve GOOGLE_CSE_ID env'lerini kontrol et\n"
                f"  • Custom Search API'yi Cloud Console'da Enable et"
            )
        if r.status_code == 429:
            raise SystemExit(f"\n✗ Quota doldu (429). Yarın tekrar dene.")
        if r.status_code != 200:
            print(f"    ! CSE {r.status_code}: {r.text[:100]}")
            return []
        handles = []
        for item in r.json().get("items", []):
            m = INSTAGRAM_REGEX.search(item.get("link", ""))
            if m:
                h = m.group(1)
                if h.lower() not in IG_BLACKLIST and h not in handles:
                    handles.append(h)
        return handles
    except SystemExit:
        raise
    except Exception as e:
        print(f"    ! CSE error: {e}")
        return []


def best_match(handles: list[str], domain: str | None, name_kw: str | None) -> str | None:
    """Validate: handle, domain root veya isim kelimesini içermeli (false match önleme)."""
    if not handles:
        return None
    if domain:
        # Domain root handle içinde geçiyorsa → güçlü eşleşme
        for h in handles:
            if domain in h.lower():
                return h
    if name_kw:
        first = name_kw.split()[0].lower()
        if len(first) >= 4:
            for h in handles:
                if first in h.lower():
                    return h
    # Hiçbiri eşleşmediyse riskli, boş dön
    return None


def main(max_calls: int):
    # Pre-flight: env değişkenleri var mı?
    if not CSE_API_KEY or not CSE_ID:
        raise SystemExit(
            "✗ GOOGLE_CSE_API_KEY veya GOOGLE_CSE_ID env değişkeni yok.\n"
            "  PowerShell:  $env:GOOGLE_CSE_API_KEY='...' ; $env:GOOGLE_CSE_ID='...'\n"
            "  Bash:        export GOOGLE_CSE_API_KEY=... ; export GOOGLE_CSE_ID=..."
        )

    # Resumable: enriched varsa onu oku (önceki run'dan kaldığımız yer)
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
    print(f"Hedef (boş + website'li): {len(targets)} | Max API çağrı: {max_calls}\n")

    found = 0
    api_calls = 0

    for i, e in enumerate(targets, 1):
        if api_calls >= max_calls:
            print(f"\n⚠ {max_calls} API çağrısı limiti — duruluyor. Yarın devam et.")
            break

        domain = domain_root(e["website"])
        name_kw = name_keywords(e.get("name", ""))

        if not domain and not name_kw:
            continue

        ig = None

        # 1) Domain heuristiği (en güçlü sinyal)
        if domain:
            handles = cse_search(domain)
            api_calls += 1
            ig = best_match(handles, domain, name_kw)

        # 2) İsim fallback
        if not ig and name_kw and api_calls < max_calls:
            handles = cse_search(f'"{name_kw}"')
            api_calls += 1
            ig = best_match(handles, domain, name_kw)

        status = f"@{ig}" if ig else "—"
        print(f"  [{i}/{len(targets)}] [{api_calls} call] {e['name'][:45]:45} → {status}")

        if ig:
            e["instagram_handle"] = ig
            e["instagram_url"] = f"https://instagram.com/{ig}"
            e["instagram_source"] = "second_pass_cse"
            found += 1
            # Her bulduğumuzda kaydet (crash'a karşı)
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

        time.sleep(0.3)

    # Son kayıt
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    total_ig = sum(1 for e in data if e.get("instagram_handle"))
    print(f"\n✓ Bu run: {found} yeni IG | API: {api_calls} | Toplam IG: {total_ig} | → {OUTPUT_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=10,
                        help="Max API çağrı (CSE free: 100/gün)")
    args = parser.parse_args()
    main(args.max)