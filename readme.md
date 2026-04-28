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

Cikti dosyasi proje klasorune `diamond_manufacturers.json` olarak yazilir.
