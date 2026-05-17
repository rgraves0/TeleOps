from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


class WeatherPluginError(Exception):
    pass


class WeatherPlugin:
    def __init__(self):
        self.geo_url = (
            "https://geocoding-api.open-meteo.com"
            "/v1/search"
        )

        self.weather_url = (
            "https://api.open-meteo.com"
            "/v1/forecast"
        )

        self.timeout = 20

    async def get_weather(
        self,
        city: str
    ) -> str:
        if not city.strip():
            raise WeatherPluginError(
                "City name is empty"
            )

        location = await self._geocode_city(
            city
        )

        latitude = location["latitude"]
        longitude = location["longitude"]

        weather_data = (
            await self._fetch_weather(
                latitude=latitude,
                longitude=longitude
            )
        )

        current = weather_data[
            "current_weather"
        ]

        weather_text = (
            f"Weather Report\n\n"
            f"City: {location['name']}\n"
            f"Country: {location.get('country', 'Unknown')}\n"
            f"Temperature: {current['temperature']}°C\n"
            f"Wind Speed: {current['windspeed']} km/h\n"
            f"Weather Code: {current['weathercode']}\n"
            f"Time: {current['time']}"
        )

        return weather_text

    async def _geocode_city(
        self,
        city: str
    ) -> dict:
        params = {
            "name": city,
            "count": 1,
            "language": "en",
            "format": "json"
        }

        async with httpx.AsyncClient(
            timeout=self.timeout
        ) as client:
            response = await client.get(
                self.geo_url,
                params=params
            )

        if response.status_code >= 400:
            raise WeatherPluginError(
                f"Geocoding failed: "
                f"{response.status_code}"
            )

        data = response.json()

        results = data.get(
            "results",
            []
        )

        if not results:
            raise WeatherPluginError(
                f"City not found: {city}"
            )

        return results[0]

    async def _fetch_weather(
        self,
        latitude: float,
        longitude: float
    ) -> dict:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current_weather": True
        }

        async with httpx.AsyncClient(
            timeout=self.timeout
        ) as client:
            response = await client.get(
                self.weather_url,
                params=params
            )

        if response.status_code >= 400:
            raise WeatherPluginError(
                f"Weather request failed: "
                f"{response.status_code}"
            )

        return response.json()


plugin = WeatherPlugin()
