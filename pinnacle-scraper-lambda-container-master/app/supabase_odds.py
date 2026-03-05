import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

# Load .env from repo root when running locally (one .env at AWS_Lambda_Odds_Scraper_Github root is enough)
def _load_env_from_root():
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    cwd = Path.cwd()
    for parent in [cwd] + list(cwd.parents):
        env_file = parent / ".env"
        if env_file.is_file():
            load_dotenv(env_file)
            break

_load_env_from_root()

logging.basicConfig(
    format="%(asctime)s %(levelname)s --- %(name)s : %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("supabase_odds")

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
    if not url or not key:
        logger.warning(
            "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set; Supabase save disabled."
        )
        return None
    try:
        from supabase import create_client

        _client = create_client(url, key)
        return _client
    except ImportError:
        logger.warning("supabase package not installed; pip install supabase")
        return None


def save_run(
    source: str,
    sport: str,
    country: str,
    league: str,
    odds_list: List[Any],
    scraped_at: Optional[datetime] = None,
) -> bool:
    """
    Insert one row into odds_scraper_output.

    source: 'swisslos' | 'pinnacle'
    odds_list: raw list/dict from scraper.get_odds()
    """
    client = _get_client()
    if client is None:
        return False
    scraped_at = scraped_at or datetime.utcnow()
    row = {
        "scraped_at": scraped_at.isoformat(),
        "source": source,
        "sport": sport,
        "country": country,
        "league": league,
        "payload": odds_list,
    }
    try:
        client.table("odds_scraper_output").insert(row).execute()
        logger.info("Inserted into Supabase: %s %s/%s/%s", source, sport, country, league)
        return True
    except Exception as e:
        logger.error("Supabase insert failed: %s", e)
        return False

