"""
Mesaj listesini hazırla — alakasız işletmeleri ve şüpheli IG eşleşmelerini ayıkla.

Filtreler:
  1. İsim filtresi: AVM, fabrika, market, restoran vb. açıkça pırlanta/kuyumcu olmayanlar
  2. Handle filtresi: IG handle'ı, işletme adı veya domain ile en az 4 harf ortak değilse şüpheli

Kullanım: python filter_for_messaging.py
Çıktı:
  - diamond_manufacturers_clean.json      → mesajlaşmaya hazır liste
  - diamond_manufacturers_rejected.json   → filtre dışı kalanlar (review için)
"""
import re
import json
import unicodedata
from urllib.parse import urlparse

INPUT_FILE = "diamond_manufacturers_enriched.json"
CLEAN_FILE = "diamond_manufacturers_clean.json"
REJECTED_FILE = "diamond_manufacturers_rejected.json"

# İsmi açıkça kuyumcu/pırlanta DIŞI gösteren kelimeler
NEGATIVE_KW = [
    "avm", "alışveriş merkezi", "yaşam merkezi",
    "fabrika", "fabrikası", "sanayi sit",
    "çinko", "plastik", "metal sanayi",
    "yöresel", "doğal ürün", "kuruyemiş", "bakliyat",
    "market", "hipermarket", "süpermarket", "süper market",
    "restoran", "restorant", "kafe ", "cafe ", "kahvecisi",
    "otel", "hotel ", "pansiyon", "konaklama",
    "eczane", "nalbur", "manav", "kasap",
    "dingil", "döküm",
    "tekstil", "giyim", "ayakkabı",
    "ofis ", "büro ",
]

# İsmi kuyumcu/pırlanta olduğunu gösteren kelimeler (varsa istisna olarak tut)
POSITIVE_KW = [
    "kuyumcu", "kuyumculuk", "pırlanta", "pirlanta",
    "mücevher", "mucevher", "mücevherat", "mucevherat",
    "altın", "altin", "gümüş", "gumus",
    "diamond", "jewelry", "jeweler", "jewellery",
    "tektaş", "tektas", "yüzük", "tasarım", "tasarim",
]

# Anlamlı isim kelimesini belirlemek için atılacak stop words
NAME_STOP_WORDS = {
    "kuyumculuk", "kuyumcu", "pırlanta", "pirlanta", "mücevher", "mucevher",
    "mücevherat", "mucevherat", "altın", "altin", "gümüş", "gumus",
    "flagship", "şube", "sube", "atölyesi", "atolyesi",
    "tasarım", "tasarim", "imalat", "üretim", "uretim",
    "ltd", "şti", "sti", "as", "a.ş", "ticaret", "the", "ve", "and",
    "store", "shop",
}


def normalize(s: str) -> str:
    """'AY MÜCEVHER' → 'aymucevher' (lowercase, diacritics strip, alphanumeric only)"""
    if not s:
        return ""
    s = s.lower()
    # Türkçe ı→i normalize
    s = s.replace("ı", "i").replace("İ", "i")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", s)


def has_substring_overlap(a: str, b: str, min_len: int = 4) -> bool:
    """a ve b arasında en az min_len uzunluğunda ortak alt-string var mı?"""
    a, b = normalize(a), normalize(b)
    if not a or not b or min(len(a), len(b)) < min_len:
        return False
    for length in range(min(len(a), len(b)), min_len - 1, -1):
        for i in range(len(a) - length + 1):
            if a[i:i + length] in b:
                return True
    return False


def is_relevant_business(name: str) -> tuple[bool, str]:
    """İşletme adı pırlanta/kuyumcu için alakalı mı?"""
    n = name.lower() if name else ""
    has_pos = any(kw in n for kw in POSITIVE_KW)
    has_neg = next((kw for kw in NEGATIVE_KW if kw in n), None)

    if has_pos:
        return True, "ok"
    if has_neg:
        return False, f"name_negative:{has_neg.strip()}"
    return True, "ok_neutral"  # Belirsiz isimler tut (false positive azalt)


def is_handle_related(handle: str, name: str, website: str | None) -> tuple[bool, str]:
    """Handle, name veya domain ile ortak substring/prefix/initials'a sahip mi?"""
    if not handle:
        return True, "no_handle"

    h_norm = normalize(handle)

    # 1. Standart 4+ karakter substring overlap
    if has_substring_overlap(handle, name):
        return True, "name_match"

    # 2. Domain ile substring overlap
    if website:
        try:
            domain = urlparse(website).netloc
            if has_substring_overlap(handle, domain):
                return True, "domain_match"
        except Exception:
            pass

    # 3. Prefix match: handle, name'in anlamlı bir kelimesiyle başlıyor mu?
    # ("DC Kuyumculuk" → @dcdiamondstore, "JWD Gold" → @jwdjewelery)
    significant_words = [
        normalize(w) for w in name.split()
        if w.lower() not in NAME_STOP_WORDS and len(w) > 1
    ]
    for w in significant_words:
        if w and len(w) >= 2 and h_norm.startswith(w):
            return True, f"prefix_match:{w}"

    # 4. Initials match: handle, ismin baş harfleriyle başlıyor mu?
    # ("International Gemological Institute - Turkey" → @igiworldwide, IGI=ilk 3)
    initials = normalize("".join(
        w[0] for w in name.split() if w and w[0].isalpha()
    ))
    # 3-6 harflik prefix'leri uzundan kısaya doğru dene
    for n in range(min(len(initials), 6), 2, -1):
        if h_norm.startswith(initials[:n]):
            return True, f"initials_match:{initials[:n]}"

    return False, "handle_unrelated"


def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    clean, rejected = [], []
    stats = {"name_negative": 0, "handle_unrelated": 0, "kept": 0}

    for e in data:
        name = e.get("name", "")
        handle = e.get("instagram_handle")
        website = e.get("website")

        # 1. İsim filtresi
        ok_name, name_reason = is_relevant_business(name)
        if not ok_name:
            e["_rejected_reason"] = name_reason
            rejected.append(e)
            stats["name_negative"] += 1
            continue

        # 2. Handle filtresi (sadece IG'si olanlar için)
        if handle:
            ok_handle, handle_reason = is_handle_related(handle, name, website)
            if not ok_handle:
                e["_rejected_reason"] = handle_reason
                rejected.append(e)
                stats["handle_unrelated"] += 1
                continue

        clean.append(e)
        stats["kept"] += 1

    with open(CLEAN_FILE, "w", encoding="utf-8") as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)
    with open(REJECTED_FILE, "w", encoding="utf-8") as f:
        json.dump(rejected, f, ensure_ascii=False, indent=2)

    # Özet
    clean_ig = sum(1 for e in clean if e.get("instagram_handle"))
    clean_mail = sum(1 for e in clean if e.get("emails"))
    clean_neither = sum(1 for e in clean if not e.get("instagram_handle") and not e.get("emails"))

    print(f"--- FİLTRELEME ---")
    print(f"  İsim filtresine takılanlar: {stats['name_negative']}")
    print(f"  Handle alakasız: {stats['handle_unrelated']}")
    print(f"\n--- TEMİZ LİSTE ({stats['kept']}) ---")
    print(f"  Instagram'ı olan:  {clean_ig}  ← DM hedefleri")
    print(f"  Sadece email:      {clean_mail}  ← Mail hedefleri")
    print(f"  İletişim yok:      {clean_neither}")
    print(f"  Toplam ulaşılabilir: {clean_ig + clean_mail}")
    print(f"\n→ {CLEAN_FILE}")
    print(f"→ {REJECTED_FILE}  (gözden geçir, yanlış elenen varsa söyle)")


if __name__ == "__main__":
    main()