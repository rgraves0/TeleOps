from __future__ import annotations

import logging
from urllib.parse import (
    unquote,
)

import httpx
from bs4 import (
    BeautifulSoup,
)

logger = logging.getLogger(__name__)


class WebSearchPlugin:
    def __init__(self) -> None:
        self.base_url = (
            "https://html.duckduckgo.com/html/"
        )

        self.timeout = 30

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 "
                "(X11; Linux x86_64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/124.0.0.0 "
                "Safari/537.36"
            ),
            "Accept": (
                "text/html,"
                "application/xhtml+xml,"
                "application/xml;q=0.9,"
                "image/avif,"
                "image/webp,*/*;q=0.8"
            ),
            "Accept-Language": (
                "en-US,en;q=0.9"
            ),
            "Cache-Control": (
                "no-cache"
            ),
            "Pragma": (
                "no-cache"
            ),
            "Referer": (
                "https://duckduckgo.com/"
            ),
            "Connection": (
                "keep-alive"
            ),
        }

    async def search(
        self,
        query: str,
        max_results: int = 5
    ) -> str:
        cleaned_query = query.strip()

        if not cleaned_query:
            return (
                "Search query is empty."
            )

        logger.info(
            "Web search started "
            "query=%s",
            cleaned_query
        )

        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                headers=self.headers
            ) as client:
                response = await client.post(
                    self.base_url,
                    data={
                        "q": cleaned_query
                    }
                )

            response.raise_for_status()

            soup = BeautifulSoup(
                response.text,
                "lxml"
            )

            result_blocks = soup.select(
                ".result"
            )

            if not result_blocks:
                logger.info(
                    "No search results found "
                    "query=%s",
                    cleaned_query
                )

                return (
                    "No search results found."
                )

            parsed_results = []

            for block in result_blocks[
                :max_results
            ]:
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

                url_element = (
                    block.select_one(
                        ".result__url"
                    )
                )

                title = ""

                snippet = ""

                source_url = ""

                if title_element:
                    title = (
                        title_element
                        .get_text(
                            " ",
                            strip=True
                        )
                    )

                if snippet_element:
                    snippet = (
                        snippet_element
                        .get_text(
                            " ",
                            strip=True
                        )
                    )

                if url_element:
                    source_url = (
                        url_element
                        .get_text(
                            " ",
                            strip=True
                        )
                    )

                    source_url = unquote(
                        source_url
                    )

                if (
                    not title
                    and not snippet
                ):
                    continue

                cleaned_result = (
                    f"Title: {title}\n"
                    f"Snippet: {snippet}\n"
                    f"Source: {source_url}"
                )

                parsed_results.append(
                    cleaned_result
                )

            if not parsed_results:
                return (
                    "No search results found."
                )

            formatted_output = (
                f"Search Query: "
                f"{cleaned_query}\n\n"
            )

            for index, result in enumerate(
                parsed_results,
                start=1
            ):
                formatted_output += (
                    f"[Result {index}]\n"
                    f"{result}\n\n"
                )

            logger.info(
                "Web search completed "
                "query=%s results=%s",
                cleaned_query,
                len(parsed_results)
            )

            return formatted_output.strip()

        except httpx.TimeoutException:
            logger.exception(
                "Web search timeout "
                "query=%s",
                cleaned_query
            )

            return (
                "Search request timed out."
            )

        except httpx.HTTPError as exc:
            logger.exception(
                "HTTP error during "
                "web search: %s",
                exc
            )

            return (
                "Unable to fetch "
                "search results right now."
            )

        except Exception as exc:
            logger.exception(
                "Unexpected web search "
                "error: %s",
                exc
            )

            return (
                "An unexpected error "
                "occurred during search."
            )


plugin = WebSearchPlugin()
