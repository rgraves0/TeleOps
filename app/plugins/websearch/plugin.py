from __future__ import annotations

import logging
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class WebSearchError(Exception):
    pass


class WebSearchPlugin:
    def __init__(self):
        self.base_url = (
            "https://html.duckduckgo.com/html/"
        )

        self.timeout = 20

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 "
                "(X11; Linux x86_64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/125.0 Safari/537.36"
            )
        }

    async def search(
        self,
        query: str,
        max_results: int = 5
    ) -> str:
        if not query.strip():
            raise WebSearchError(
                "Search query is empty"
            )

        encoded_query = quote_plus(
            query
        )

        url = (
            f"{self.base_url}"
            f"?q={encoded_query}"
        )

        async with httpx.AsyncClient(
            timeout=self.timeout,
            headers=self.headers,
            follow_redirects=True
        ) as client:
            response = await client.get(
                url
            )

        if response.status_code >= 400:
            raise WebSearchError(
                f"Search request failed: "
                f"{response.status_code}"
            )

        soup = BeautifulSoup(
            response.text,
            "lxml"
        )

        results = []

        result_blocks = soup.select(
            ".result"
        )

        for block in result_blocks:
            title_element = block.select_one(
                ".result__title"
            )

            snippet_element = (
                block.select_one(
                    ".result__snippet"
                )
            )

            link_element = block.select_one(
                ".result__url"
            )

            if not title_element:
                continue

            title = (
                title_element.get_text(
                    " ",
                    strip=True
                )
            )

            snippet = ""

            if snippet_element:
                snippet = (
                    snippet_element.get_text(
                        " ",
                        strip=True
                    )
                )

            link = ""

            if link_element:
                link = (
                    link_element.get_text(
                        " ",
                        strip=True
                    )
                )

            result_text = (
                f"Title: {title}\n"
                f"Snippet: {snippet}\n"
                f"Source: {link}"
            )

            results.append(
                result_text
            )

            if len(results) >= max_results:
                break

        if not results:
            return (
                "No search results found."
            )

        return "\n\n".join(results)

    async def quick_search(
        self,
        query: str
    ) -> str:
        return await self.search(
            query=query,
            max_results=3
        )


plugin = WebSearchPlugin()
