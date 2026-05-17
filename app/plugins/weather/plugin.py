from __future__ import annotations

import logging
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

WEATHER_API_KEY = os.getenv(
    "OPENWEATHER_API_KEY"
)

BASE_URL = (
    "https://api.openweathermap.org/data/2.5/weather"
)


async def get_weather(
    city: str
) -> dict:

    if not WEATHER_API_KEY:
        raise RuntimeError(
            "OPENWEATHER_API_KEY missing"
        )

    params = {
        "q": city,
        "appid": WEATHER_API_KEY,
        "units": "metric"
    }

    async with httpx.AsyncClient(
        timeout=20
    ) as client:

        response = await client.get(
            BASE_URL,
            params=params
        )

    if response.status_code != 200:

        logger.error(
            "Weather API failed: %s",
            response.text
        )

        raise RuntimeError(
            f"Weather API error: "
            f"{response.status_code}"
        )

    data = response.json()

    weather = (
        data.get("weather", [{}])[0]
    )

    main = data.get("main", {})
    wind = data.get("wind", {})

    result = {
        "city": data.get("name"),
        "country": data.get(
            "sys",
            {}
        ).get("country"),
        "description": weather.get(
            "description"
        ),
        "temperature": main.get("temp"),
        "feels_like": main.get(
            "feels_like"
        ),
        "humidity": main.get(
            "humidity"
        ),
        "wind_speed": wind.get(
            "speed"
        )
    }

    logger.info(
        "Weather fetched city=%s",
        city
    )

    return result


PLUGIN_NAME = "weather"

PLUGIN_DESCRIPTION = (
    "Weather information plugin"
)

PLUGIN_VERSION = "1.0.0"
