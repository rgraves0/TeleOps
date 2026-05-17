from __future__ import annotations

import logging
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SERPER_API_KEY = os.getenv(
    "SERPER_API_KEY"
)

SERPER_ENDPOINT = (
    "https://google.serper.dev/search"
)


async def search(
    query: str
) -> dict:

    if not SERPER_API_KEY:
        raise RuntimeError(
            "SERPER_API_KEY missing"
        )

    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "q": query
    }

    async with httpx.AsyncClient(
        timeout=30
    ) as client:

        response = await client.post(
            SERPER_ENDPOINT,
            headers=headers,
            json=payload
        )

    if response.status_code != 200:

        logger.error(
            "Web search failed: %s",
            response.text
        )

        raise RuntimeError(
            f"Search API failed: "
            f"{response.status_code}"
        )

    data = response.json()

    organic_results = (
        data.get(
            "organic",
            []
        )
    )

    simplified_results = []

    for item in organic_results[:3]:

        simplified_results.append(
            {
                "title": item.get(
                    "title"
                ),
                "snippet": item.get(
                    "snippet"
                ),
                "link": item.get(
                    "link"
                )
            }
        )

    logger.info(
        "Web search completed query=%s",
        query
    )

    return {
        "query": query,
        "results": simplified_results
    }


PLUGIN_NAME = "websearch"

PLUGIN_DESCRIPTION = (
    "Internet web search plugin"
)

PLUGIN_VERSION = "1.0.0"
