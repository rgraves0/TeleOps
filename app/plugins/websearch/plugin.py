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
        self.search_url = (
            "https://html.duckduckgo.com/html/"
        )

        self.timeout = 20

        self.max_results = 5

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 "
                "(X11; Linux x86_64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,"
                "application/xhtml+xml,"
                "application/xml;q=0.9,"
                "*/*;q=0.8"
            ),
            "Accept-Language": (
                "en-US,en;q=0.9"
            ),
            "Connection": "keep-alive"
        }

    async def search(
        self,
        query: str,
        max_results: int | None = None
    ) -> str:
        cleaned_query = query.strip()

        if not cleaned_query:
            raise WebSearchError(
                "Search query is empty"
            )

        limit = (
            max_results
            if max_results is not None
            else self.max_results
        )

        html = await self._fetch_results(
            cleaned_query
        )

        results = self._parse_results(
            html=html,
            limit=limit
        )

        if not results:
            return (
                "No search results found."
            )

        formatted_results = []

        for index, result in enumerate(
            results,
            start=1
        ):
            block = (
                f"[{index}] "
                f"{result['title']}\n"
                f"{result['snippet']}\n"
                f"Source: {result['url']}"
            )

            formatted_results.append(
                block
            )

        return "\n\n".join(
            formatted_results
        )

    async def quick_search(
        self,
        query: str
    ) -> str:
        return await self.search(
            query=query,
            max_results=3
        )

    async def _fetch_results(
        self,
        query: str
    ) -> str:
        encoded_query = quote_plus(
            query
        )

        url = (
            f"{self.search_url}"
            f"?q={encoded_query}"
        )

        logger.info(
            "Web search query: %s",
            query
        )

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                headers=self.headers,
                follow_redirects=True
            ) as client:
                response = await client.get(
                    url
                )

        except httpx.HTTPError as exc:
            logger.exception(
                "HTTP request failed: %s",
                exc
            )

            raise WebSearchError(
                "Search request failed"
            ) from exc

        if response.status_code >= 400:
            raise WebSearchError(
                f"Search failed with "
                f"status code "
                f"{response.status_code}"
            )

        return response.text

    def _parse_results(
        self,
        html: str,
        limit: int
    ) -> list[dict[str, str]]:
        soup = BeautifulSoup(
            html,
            "html.parser"
        )

        results = []

        result_blocks = soup.select(
            ".result"
        )

        for block in result_blocks:
            title_element = (
                block.select_one(
                    ".result__title"
                )
            )

            snippet_element = (
                block.select_one(
                    ".result__snippet"
                )
            )

            link_element = (
                block.select_one(
                    "a.result__a"
                )
            )

            if (
                title_element is None
                or link_element is None
            ):
                continue

            title = (
                title_element.get_text(
                    separator=" ",
                    strip=True
                )
            )

            snippet = ""

            if snippet_element:
                snippet = (
                    snippet_element.get_text(
                        separator=" ",
                        strip=True
                    )
                )

            url = (
                link_element.get(
                    "href",
                    ""
                ).strip()
            )

            cleaned_result = {
                "title": (
                    self._clean_text(
                        title
                    )
                ),
                "snippet": (
                    self._clean_text(
                        snippet
                    )
                ),
                "url": url
            }

            results.append(
                cleaned_result
            )

            if len(results) >= limit:
                break

        return results

    def _clean_text(
        self,
        text: str
    ) -> str:
        cleaned = " ".join(
            text.split()
        )

        return cleaned.strip()


plugin = WebSearchPlugin()
