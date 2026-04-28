"""
Pırlanta üretim/imalat işletmelerinin Instagram adreslerini toplar.
Pipeline: Google Places API → Website scrape → Google Custom Search fallback
"""
import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# ============ CONFIG ============
PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
CSE_API_KEY = os.getenv("GOOGLE_CSE_API_KEY")
CSE_ID = os.getenv("GOOGLE_CSE_ID")

SEARCH_QUERIES = [
    "pırlanta üretim",
    "pırlanta imalat",
    "pırlanta imalatçısı",
]

# Türkiye'nin kuyumculuk açısından önemli şehirleri
CITIES = [
    "İstanbul", "Ankara", "İzmir", "Bursa", "Antalya",
    "Gaziantep", "Konya", "Kayseri", "Adana", "Mersin",
    "Şanlıurfa", "Diyarbakır", "Eskişehir", "Samsun", "Trabzon",
]

OUTPUT_FILE = "diamond_manufacturers.json"
INSTAGRAM_REGEX = re.compile(
    r"(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9_.]+)/?",
    re.IGNORECASE,
)
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
)
# Filtrelenecek genel hesaplar (paylaş butonu vs.)
IG_BLACKLIST = {"explore", "p", "reel", "reels", "tv", "stories", "accounts", "share"}
# Email false positive filtreleri
EMAIL_BLACKLIST_EXT = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico")
EMAIL_BLACKLIST_DOMAINS = ("sentry.io", "wixpress.com", "example.com", "domain.com")
# İletişim sayfalarında aranacak path'ler
CONTACT_PATHS = ["/iletisim", "/contact", "/iletisim-bilgileri", "/contact-us", "/kontakt"]


# ============ GOOGLE PLACES (NEW) ============
def search_places(query: str, max_pages: int = 3) -> list[dict]:
    """Places API (New) Text Search — sayfa başı 20, max 60 sonuç."""
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": PLACES_API_KEY,
        "X-Goog-FieldMask": (
            "places.id,places.displayName,places.formattedAddress,"
            "places.websiteUri,places.nationalPhoneNumber,"
            "nextPageToken"
        ),
    }
    results, page_token = [], None
    for _ in range(max_pages):
        body = {"textQuery": query, "languageCode": "tr", "regionCode": "TR"}
        if page_token:
            body["pageToken"] = page_token
        r = requests.post(url, headers=headers, json=body, timeout=15)
        if r.status_code != 200:
            print(f"  ! Places error {r.status_code}: {r.text[:120]}")
            break
        data = r.json()
        results.extend(data.get("places", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(2)  # nextPageToken aktivasyonu için bekleme
    return results


# ============ WEBSITE SCRAPE ============
def _fetch(url: str) -> str | None:
    try:
        r = requests.get(
            url, timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0)"},
            allow_redirects=True,
        )
        return r.text if r.status_code == 200 else None
    except Exception:
        return None


def _find_instagram(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    candidates = [a.get("href", "") for a in soup.find_all("a", href=True)]
    candidates.append(html)
    for text in candidates:
        m = INSTAGRAM_REGEX.search(text)
        if m and m.group(1).lower() not in IG_BLACKLIST:
            return m.group(1)
    return None


def _find_emails(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    found = set()
    # Önce mailto: linklerini topla (en güvenilir)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("mailto:"):
            email = href[7:].split("?")[0].strip()
            if "@" in email:
                found.add(email.lower())
    # Sonra raw HTML'de regex ile ara
    for m in EMAIL_REGEX.finditer(html):
        found.add(m.group(0).lower())
    # Filtrele
    return [
        e for e in found
        if not e.endswith(EMAIL_BLACKLIST_EXT)
        and not any(d in e for d in EMAIL_BLACKLIST_DOMAINS)
    ]


def extract_from_site(url: str) -> tuple[str | None, list[str]]:
    """Ana sayfa + iletişim sayfalarından IG handle ve email listesi döner."""
    ig_handle, emails = None, []
    html = _fetch(url)
    if html:
        ig_handle = _find_instagram(html)
        emails.extend(_find_emails(html))

    # IG bulunduysa email aramayı atla (gerek yok), bulunmadıysa contact path'leri dene
    if not ig_handle or not emails:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in CONTACT_PATHS:
            html = _fetch(base + path)
            if not html:
                continue
            if not ig_handle:
                ig_handle = _find_instagram(html)
            emails.extend(_find_emails(html))
            if ig_handle and emails:
                break

    # Dedup, sıralı
    emails = list(dict.fromkeys(emails))
    return ig_handle, emails


# ============ GOOGLE CUSTOM SEARCH FALLBACK ============
def search_instagram_via_google(business_name: str) -> str | None:
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": CSE_API_KEY,
        "cx": CSE_ID,
        "q": f'site:instagram.com "{business_name}"',
        "num": 3,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return None
        for item in r.json().get("items", []):
            m = INSTAGRAM_REGEX.search(item.get("link", ""))
            if m and m.group(1).lower() not in IG_BLACKLIST:
                return m.group(1)
    except Exception as e:
        print(f"  ! CSE error: {e}")
    return None


# ============ MAIN ============
def main():
    seen_ids = set()
    enriched = []

    for city in CITIES:
        for q in SEARCH_QUERIES:
            query = f"{q} {city}"
            print(f"\n[Places] {query}")
            places = search_places(query)
            print(f"  → {len(places)} sonuç")

            for p in places:
                pid = p.get("id")
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)

                name = p.get("displayName", {}).get("text", "")
                website = p.get("websiteUri")
                ig_handle, emails = None, []
                ig_source = None

                if website:
                    ig_handle, emails = extract_from_site(website)
                    if ig_handle:
                        ig_source = "website"

                # IG bulunamadıysa Google CSE fallback
                if not ig_handle and name:
                    ig_handle = search_instagram_via_google(name)
                    if ig_handle:
                        ig_source = "google_search"

                enriched.append({
                    "place_id": pid,
                    "name": name,
                    "address": p.get("formattedAddress"),
                    "phone": p.get("nationalPhoneNumber"),
                    "website": website,
                    "instagram_handle": ig_handle,
                    "instagram_url": f"https://instagram.com/{ig_handle}" if ig_handle else None,
                    "instagram_source": ig_source,
                    "emails": emails if not ig_handle else [],  # IG yoksa email göster
                    "city_query": city,
                })
                ig_or_mail = ig_handle or (emails[0] if emails else "—")
                print(f"  • {name} → {ig_or_mail}")
                time.sleep(0.5)  # rate limit nezaketi

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    total = len(enriched)
    with_ig = sum(1 for e in enriched if e["instagram_handle"])
    with_mail = sum(1 for e in enriched if e["emails"])
    print(f"\n✓ {total} işletme | {with_ig} IG | {with_mail} sadece email | → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()