## Local

```bash
pip install -r requirements.txt
python scraper.py
```

## Docker Compose

`.env` dosyasini asagidaki degiskenlerle doldurun:

```env
GOOGLE_PLACES_API_KEY=...
GOOGLE_CSE_API_KEY=...
GOOGLE_CSE_ID=...
```

Calistirmak icin:

```bash
docker compose run --rm scraper
```

Farkli bir parcayi calistirmak icin:

```bash
docker compose run --rm scraper python scraper.py --part 2
```

Cikti dosyasi proje klasorune `diamond_manufacturers_part1.json` gibi yazilir.

Part dosyalarini birlestirmek icin:

```bash
docker compose run --rm merge
```

Birlesik cikti `diamond_manufacturers_merged.json` olarak yazilir.
