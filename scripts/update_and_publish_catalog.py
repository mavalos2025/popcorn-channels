#!/usr/bin/env python3
"""
Popcorn Play - Health Check y Generador de Catálogo Autónomo para GitHub Actions.
Este script se ejecuta dentro del entorno de GitHub Actions (o en local) cada 6 horas:
1. Realiza Health Check asíncrono para verificar streams .m3u8 activos.
2. Descarta señales caídas y preserva el catálogo 100% funcional de Toda América.
3. Genera:
   - 'canales.json' (catálogo maestro minificado)
   - 'countries.json' y 'countries/{CODE}.json'
   - 'categories.json' y 'categories/{GENERO}.json' (deportes, noticias, infantiles, etc.)
   - 'config.json' (archivo central de Control Remoto de la App)
"""

import os
import sys
import json
import asyncio
import datetime
import logging
from typing import Dict, List, Any, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("CatalogSyncCI")

# Directorio raíz del repositorio en GitHub Actions
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Taxonomía Canónica de Géneros
CATEGORY_TAXONOMY: Dict[str, Dict[str, Any]] = {
    "deportes": {
        "id": "deportes",
        "name": "Deportes",
        "icon": "⚽",
        "keywords": ["sports", "sport", "deporte", "deportes", "futbol", "football", "soccer", "racing", "nba", "baseball", "motor", "golf", "tennis"]
    },
    "noticias": {
        "id": "noticias",
        "name": "Noticias",
        "icon": "📰",
        "keywords": ["news", "noticia", "noticias", "information", "informacion", "weather", "clima", "parlamentario", "canal del congreso"]
    },
    "infantiles": {
        "id": "infantiles",
        "name": "Infantiles",
        "icon": "🧸",
        "keywords": ["kids", "children", "animation", "infantil", "infantiles", "cartoon", "disney", "anime", "dibujos"]
    },
    "peliculas": {
        "id": "peliculas",
        "name": "Películas y Cine",
        "icon": "🎬",
        "keywords": ["movie", "movies", "cinema", "cine", "peliculas", "films", "hollywood"]
    },
    "musica": {
        "id": "musica",
        "name": "Música",
        "icon": "🎵",
        "keywords": ["music", "musica", "radio", "hits", "rock", "pop", "classical", "jazz", "cumbia", "salsa"]
    },
    "entretenimiento": {
        "id": "entretenimiento",
        "name": "Entretenimiento",
        "icon": "🎭",
        "keywords": ["entertainment", "entretenimiento", "series", "comedy", "comedia", "novela", "drama", "lifestyle", "reality"]
    },
    "cultura": {
        "id": "cultura",
        "name": "Cultura y Educación",
        "icon": "📚",
        "keywords": ["documentary", "documentales", "culture", "cultura", "education", "educacion", "science", "ciencia", "history", "historia", "nature", "religion", "religious", "catolica", "cristiana"]
    },
    "general": {
        "id": "general",
        "name": "General y Variedades",
        "icon": "📺",
        "keywords": ["general", "variety", "variedades", "family", "local", "regional"]
    }
}

COUNTRY_SLUGS: Dict[str, str] = {
    "PE": "peru", "MX": "mexico", "AR": "argentina", "CO": "colombia",
    "CL": "chile", "US": "usa", "BR": "brasil", "EC": "ecuador",
    "VE": "venezuela", "UY": "uruguay", "PY": "paraguay", "BO": "bolivia",
    "CR": "costarica", "PA": "panama", "DO": "dominicana", "GT": "guatemala",
    "CA": "canada", "CU": "cuba", "SV": "elsalvador", "HN": "honduras",
    "JM": "jamaica", "NI": "nicaragua", "PR": "puertorico", "TT": "trinidad"
}


def classify_channel_category(group: str, name: str = "") -> str:
    combined = f"{group or ''} {name or ''}".lower()
    for cat_id, meta in CATEGORY_TAXONOMY.items():
        if cat_id == "general":
            continue
        for kw in meta["keywords"]:
            if kw in combined:
                return cat_id
    return "general"


async def verify_stream_url(client: Any, url: str, semaphore: asyncio.Semaphore, timeout: float = 3.0) -> bool:
    """Verifica si un stream .m3u8 está activo."""
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return False

    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 14; Mobile; K) AppleWebKit/537.36 Chrome/131.0.6778.260 Mobile Safari/537.36",
        "Accept": "*/*"
    }

    async with semaphore:
        try:
            resp = await client.head(url, headers=headers, timeout=timeout, follow_redirects=True)
            if resp.status_code in [200, 204, 206, 301, 302, 307, 308]:
                return True
            if resp.status_code in [405, 403]:
                # Algunos servidores no aceptan HEAD, intentar GET de 1 fragmento
                resp_get = await client.get(url, headers={**headers, "Range": "bytes=0-1024"}, timeout=timeout, follow_redirects=True)
                return resp_get.status_code in [200, 206]
            return False
        except Exception:
            return False


async def clean_channel_list(channels: List[Dict[str, Any]], concurrency: int = 50) -> List[Dict[str, Any]]:
    """Ejecuta verificación concurrente de streams si httpx está disponible."""
    try:
        import httpx
    except ImportError:
        logger.warning("httpx no instalado, omitiendo verificación en vivo de URLs.")
        return channels

    logger.info(f"Iniciando verificación de salud (Health Check) en {len(channels)} canales...")
    semaphore = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=concurrency)

    async with httpx.AsyncClient(limits=limits, verify=False) as client:
        tasks = [verify_stream_url(client, ch.get("stream_url", ""), semaphore) for ch in channels]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    healthy_channels = []
    for ch, is_alive in zip(channels, results):
        if is_alive is True:
            healthy_channels.append(ch)

    logger.info(f"Canales activos comprobados: {len(healthy_channels)} / {len(channels)} (Descartados {len(channels) - len(healthy_channels)} caídos)")
    # Si por alguna razón la red en GitHub Actions falló masivamente (>70% drop), preservar los canales existentes por resiliencia
    if len(healthy_channels) < (len(channels) * 0.3):
        logger.warning("Caída anómala detectada en Health Check; preservando catálogo existente para evitar cortes.")
        return channels

    return healthy_channels


def sync_catalog():
    master_file = os.path.join(REPO_ROOT, "canales.json")
    if not os.path.exists(master_file):
        logger.error(f"No se encontró 'canales.json' en {REPO_ROOT}")
        sys.exit(1)

    with open(master_file, "r", encoding="utf-8") as f:
        master_channels: List[Dict[str, Any]] = json.load(f)

    logger.info(f"Total canales actuales en catálogo: {len(master_channels)}")

    # 1. Limpieza de streams caídos
    active_channels = asyncio.run(clean_channel_list(master_channels, concurrency=60))

    # Reclasificar categorías y asegurar campos
    for ch in active_channels:
        ch["category_id"] = classify_channel_category(ch.get("group", ""), ch.get("name", ""))

    # 2. Guardar 'canales.json' maestro actualizado
    with open(master_file, "w", encoding="utf-8") as f:
        json.dump(active_channels, f, ensure_ascii=False, separators=(',', ':'))
    logger.info(f"✓ Guardado 'canales.json' ({len(active_channels)} canales)")

    # 3. Guardar 'countries.json' y por país
    countries_dir = os.path.join(REPO_ROOT, "countries")
    os.makedirs(countries_dir, exist_ok=True)

    channels_by_country: Dict[str, List[Dict[str, Any]]] = {}
    for ch in active_channels:
        ccode = (ch.get("country_code") or "US").upper()
        channels_by_country.setdefault(ccode, []).append(ch)

    countries_summary = []
    # Leer países existentes para preservar banderas y nombres
    existing_countries_file = os.path.join(REPO_ROOT, "countries.json")
    existing_meta: Dict[str, Dict[str, Any]] = {}
    if os.path.exists(existing_countries_file):
        try:
            with open(existing_countries_file, "r", encoding="utf-8") as f:
                for c in json.load(f):
                    existing_meta[c["code"]] = c
        except Exception:
            pass

    for ccode, chs in channels_by_country.items():
        base_meta = existing_meta.get(ccode, {
            "code": ccode,
            "name": chs[0].get("country_name") or ccode,
            "flag": "📺",
            "region_id": chs[0].get("region_id") or "latin-america"
        })
        country_obj = {
            "code": ccode,
            "name": base_meta.get("name", ccode),
            "flag": base_meta.get("flag", "📺"),
            "region_id": base_meta.get("region_id", "latin-america"),
            "channels_count": len(chs)
        }
        countries_summary.append(country_obj)

        payload = {
            "country": country_obj,
            "total_channels": len(chs),
            "channels": chs
        }

        # Guardar countries/{CODE}.json
        with open(os.path.join(countries_dir, f"{ccode}.json"), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))

        # Guardar {slug}.json
        slug = COUNTRY_SLUGS.get(ccode)
        if slug:
            with open(os.path.join(REPO_ROOT, f"{slug}.json"), "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))

    # Prioridad de países
    priority_codes = ["MX", "AR", "CO", "PE", "CL", "US", "BR", "EC", "VE", "UY", "PY", "BO", "CR", "PA", "DO", "GT"]
    countries_summary.sort(key=lambda c: (0, priority_codes.index(c["code"])) if c["code"] in priority_codes else (1, c["name"]))

    with open(os.path.join(REPO_ROOT, "countries.json"), "w", encoding="utf-8") as f:
        json.dump(countries_summary, f, ensure_ascii=False, separators=(',', ':'))
    logger.info(f"✓ Guardado 'countries.json' ({len(countries_summary)} países)")

    # 4. Guardar 'categories.json' y por género
    categories_dir = os.path.join(REPO_ROOT, "categories")
    os.makedirs(categories_dir, exist_ok=True)

    channels_by_category: Dict[str, List[Dict[str, Any]]] = {cat: [] for cat in CATEGORY_TAXONOMY.keys()}
    for ch in active_channels:
        cat_id = ch.get("category_id") or "general"
        if cat_id in channels_by_category:
            channels_by_category[cat_id].append(ch)
        else:
            channels_by_category["general"].append(ch)

    categories_summary = []
    for cat_id, meta in CATEGORY_TAXONOMY.items():
        cat_chs = channels_by_category.get(cat_id, [])
        cat_meta = {
            "id": cat_id,
            "name": meta["name"],
            "icon": meta["icon"],
            "channels_count": len(cat_chs)
        }
        categories_summary.append(cat_meta)

        cat_payload = {
            "category": cat_meta,
            "total_channels": len(cat_chs),
            "channels": cat_chs
        }

        with open(os.path.join(categories_dir, f"{cat_id}.json"), "w", encoding="utf-8") as f:
            json.dump(cat_payload, f, ensure_ascii=False, separators=(',', ':'))

        with open(os.path.join(REPO_ROOT, f"{cat_id}.json"), "w", encoding="utf-8") as f:
            json.dump(cat_payload, f, ensure_ascii=False, separators=(',', ':'))

    with open(os.path.join(REPO_ROOT, "categories.json"), "w", encoding="utf-8") as f:
        json.dump(categories_summary, f, ensure_ascii=False, separators=(',', ':'))
    logger.info(f"✓ Guardado 'categories.json' ({len(categories_summary)} categorías)")

    # 5. Actualizar 'config.json' con timestamp de última sincronización
    config_file = os.path.join(REPO_ROOT, "config.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
    else:
        cfg = {}

    cfg["last_updated"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    logger.info("✓ Actualizado 'config.json' con nuevo timestamp de sincronización.")

    logger.info("============================================================")
    logger.info("  PUBLICACIÓN AUTÓNOMA FINALIZADA EXITOSAMENTE")
    logger.info("============================================================")


if __name__ == "__main__":
    sync_catalog()
