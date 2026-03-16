import logging

import requests

from etl.config import settings

logger = logging.getLogger(__name__)


def get_json(url: str, headers: dict | None = None, params: dict | None = None) -> dict:
    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=settings.request_timeout_seconds
    )
    response.raise_for_status()
    return response.json()