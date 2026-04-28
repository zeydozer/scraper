"""
Tüm part JSON dosyalarını birleştirir ve Instagram handle'a göre tekilleştirir.
Kullanım: python merge_parts.py
"""
import json
import glob
from urllib.parse import urlparse

INPUT_GLOB = "diamond_manufacturers_part*.json"
OUTPUT_FILE = "diamond_manufacturers_merged.json"


def domain_of(url: str | None) -> str | None:
    if not url:
        return None
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except Exception:
        return None


def main():
    files = sorted(glob.glob(INPUT_GLOB))
    if not files:
        raise SystemExit(f"Dosya bulunamadı: {INPUT_GLOB}")

    all_entries = []
    for f in files:
        with open(f, encoding="utf-8") as fp:
            data = json.load(fp)
            all_entries.extend(data)
            print(f"  • {f}: {len(data)} kayıt")

    print(f"\nToplam ham kayıt: {len(all_entries)}")

    seen_ig = set()       # instagram handle (lowercase)
    seen_place = set()    # google place_id
    seen_domain = set()   # website domain
    merged = []
    dropped = {"ig": 0, "place": 0, "domain": 0}

    for e in all_entries:
        ig = (e.get("instagram_handle") or "").lower().strip()
        pid = e.get("place_id")
        dom = domain_of(e.get("website"))

        # 1. IG handle dedup (en güçlü kriter)
        if ig:
            if ig in seen_ig:
                dropped["ig"] += 1
                continue
            seen_ig.add(ig)

        # 2. place_id dedup (cross-part güvenliği)
        if pid:
            if pid in seen_place:
                dropped["place"] += 1
                continue
            seen_place.add(pid)

        # 3. Domain dedup (IG yoksa ve website varsa)
        if not ig and dom:
            if dom in seen_domain:
                dropped["domain"] += 1
                continue
            seen_domain.add(dom)

        merged.append(e)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    with_ig = sum(1 for e in merged if e.get("instagram_handle"))
    with_mail = sum(1 for e in merged if e.get("emails"))
    with_neither = sum(1 for e in merged if not e.get("instagram_handle") and not e.get("emails"))

    print(f"\n--- DEDUP ---")
    print(f"  IG çakışması: {dropped['ig']}")
    print(f"  place_id çakışması: {dropped['place']}")
    print(f"  domain çakışması: {dropped['domain']}")
    print(f"\n--- SONUÇ ---")
    print(f"  Toplam tekil: {len(merged)}")
    print(f"  Instagram'ı olan: {with_ig}")
    print(f"  Sadece email: {with_mail}")
    print(f"  Hiçbiri yok: {with_neither}")
    print(f"\n→ {OUTPUT_FILE}")


if __name__ == "__main__":
    main()