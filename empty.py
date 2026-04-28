"""
Boş kayıtlardan örnek göster — Instagram ve email olmayanları incele.
Kullanım: python inspect_empty.py
"""
import json
import random

INPUT_FILE = "diamond_manufacturers_merged.json"
SAMPLE_SIZE = 20

with open(INPUT_FILE, encoding="utf-8") as f:
    data = json.load(f)

empty = [e for e in data if not e.get("instagram_handle") and not e.get("emails")]
with_site = [e for e in empty if e.get("website")]
without_site = [e for e in empty if not e.get("website")]

print(f"Toplam boş kayıt: {len(empty)}")
print(f"  Website'i olanlar: {len(with_site)}  ← scraper kaçırmış olabilir")
print(f"  Website'i olmayanlar: {len(without_site)}  ← Google'da kayıtlı değil")

# Stratified sample: yarı website'li, yarı website'siz
random.seed(42)
n_with = min(SAMPLE_SIZE // 2, len(with_site))
n_without = min(SAMPLE_SIZE - n_with, len(without_site))
sample = random.sample(with_site, n_with) + random.sample(without_site, n_without)

print(f"\n{'='*70}")
print(f"WEBSITE'İ OLAN ({n_with} örnek) — bu kritik, scraper neden bulamadı?")
print('='*70)
for i, e in enumerate(sample[:n_with], 1):
    print(f"\n{i}. {e['name']}")
    print(f"   📍 {e.get('address', '—')}")
    print(f"   🌐 {e.get('website')}")
    print(f"   📞 {e.get('phone', '—')}")

print(f"\n{'='*70}")
print(f"WEBSITE'İ OLMAYAN ({n_without} örnek)")
print('='*70)
for i, e in enumerate(sample[n_with:], 1):
    print(f"\n{i}. {e['name']}")
    print(f"   📍 {e.get('address', '—')}")
    print(f"   📞 {e.get('phone', '—')}")