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
        self.search_url = (
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
            "Connection": (
                "keep-alive"
            ),
            "Referer": (
                "https://duckduckgo.com/"
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
            "Starting web search "
            "query=%s",
            cleaned_query
        )

        try:
            async with httpx.AsyncClient(
                headers=self.headers,
                timeout=self.timeout,
                follow_redirects=True
            ) as client:
                response = await client.post(
                    self.search_url,
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
                    "No results found "
                    "query=%s",
                    cleaned_query
                )

                return (
                    "No search results found."
                )

            formatted_results = []

            for result in result_blocks[
                :max_results
            ]:
                title_element = (
                    result.select_one(
                        ".result__title"
                    )
                )

                snippet_element = (
                    result.select_one(
                        ".result__snippet"
                    )
                )

                url_element = (
                    result.select_one(
                        ".result__url"
                    )
                )

                title = ""

                snippet = ""

                source = ""

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
                    source = (
                        url_element
                        .get_text(
                            " ",
                            strip=True
                        )
                    )

                    source = unquote(
                        source
                    )

                if (
                    not title
                    and not snippet
                ):
                    continue

                cleaned_result = (
                    f"Title: {title}\n"
                    f"Snippet: {snippet}\n"
                    f"Source: {source}"
                )

                formatted_results.append(
                    cleaned_result
                )

            if not formatted_results:
                return (
                    "No search results found."
                )

            final_output = (
                f"Search Query: "
                f"{cleaned_query}\n\n"
            )

            for index, item in enumerate(
                formatted_results,
                start=1
            ):
                final_output += (
                    f"[Result {index}]\n"
                    f"{item}\n\n"
                )

            logger.info(
                "Search completed "
                "query=%s results=%s",
                cleaned_query,
                len(formatted_results)
            )

            return final_output.strip()

        except httpx.TimeoutException:
            logger.exception(
                "Search timeout "
                "query=%s",
                cleaned_query
            )

            return (
                "Search request timed out."
            )

        except httpx.HTTPError as exc:
            logger.exception(
                "HTTP error during "
                "search: %s",
                exc
            )

            return (
                "Unable to fetch "
                "search results right now."
            )

        except Exception as exc:
            logger.exception(
                "Unexpected search "
                "error: %s",
                exc
            )

            return (
                "An unexpected error "
                "occurred during search."
            )


plugin = WebSearchPlugin()
