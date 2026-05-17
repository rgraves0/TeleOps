def format_ai_response(
    response_text: str
) -> str:

    if not response_text:
        return "No response generated."

    cleaned = str(
        response_text
    ).strip()

    # =====================================================
    # ESCAPE TELEGRAM HTML SPECIAL CHARACTERS
    # =====================================================

    cleaned = (
        cleaned
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

    # =====================================================
    # NORMALIZE NEWLINES
    # =====================================================

    cleaned = cleaned.replace(
        "\r\n",
        "\n"
    )

    cleaned = cleaned.replace(
        "\r",
        "\n"
    )

    # =====================================================
    # REMOVE UNSUPPORTED HTML TAGS
    # =====================================================

    cleaned = cleaned.replace(
        "<br>",
        "\n"
    )

    cleaned = cleaned.replace(
        "<br/>",
        "\n"
    )

    cleaned = cleaned.replace(
        "<br />",
        "\n"
    )

    # =====================================================
    # TELEGRAM MESSAGE LIMIT SAFETY
    # =====================================================

    MAX_TELEGRAM_LENGTH = 3500

    if len(cleaned) > MAX_TELEGRAM_LENGTH:

        cleaned = (
            cleaned[
                :MAX_TELEGRAM_LENGTH
            ]
            + "\n\n..."
        )

    return cleaned


# =========================================================
# AI CHAT CORE
# =========================================================

async def ai_chat_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    if update.message is None:
        return

    if update.effective_user is None:
        return

    telegram_user_id = (
        update.effective_user.id
    )

    user_message = (
        update.message.text or ""
    ).strip()

    if not user_message:
        return

    ai_mode = context.user_data.get(
        "ai_mode",
        False
    )

    if not ai_mode:
        return

    chat_id = update.effective_chat.id

    processing_message = (
        await update.message.reply_text(
            text="🤖 <b>Processing request...</b>",
            parse_mode=ParseMode.HTML
        )
    )

    processing_stage = (
        detect_processing_stage(
            user_message
        )
    )

    await update_processing_message(
        processing_message,
        processing_stage
    )

    stop_event = asyncio.Event()

    typing_task = asyncio.create_task(
        typing_indicator_loop(
            context=context,
            chat_id=chat_id,
            stop_event=stop_event
        )
    )

    try:

        result = (
            await ai_service.process_user_message(
                telegram_user_id=telegram_user_id,
                message=user_message
            )
        )

        response_text = result.get(
            "response",
            "No response generated."
        )

        formatted_response = (
            format_ai_response(
                response_text
            )
        )

        response_type = result.get(
            "type",
            "chat"
        )

        header = ""

        if response_type == "workflow":

            header = (
                "⚡ <b>Workflow Completed</b>\n\n"
            )

        elif response_type == "tool":

            header = (
                "🛠 <b>Task Completed</b>\n\n"
            )

        elif response_type == "error":

            header = (
                "⚠️ <b>Processing Issue</b>\n\n"
            )

        final_message = (
            f"{header}{formatted_response}"
        )

        try:

            await processing_message.edit_text(
                text=final_message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )

        except Exception:

            logger.exception(
                "Telegram HTML formatting failed"
            )

            safe_text = (
                final_message
                .replace("<b>", "")
                .replace("</b>", "")
            )

            safe_text = (
                safe_text
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&amp;", "&")
            )

            await processing_message.edit_text(
                text=safe_text[:3500],
                disable_web_page_preview=True
            )

        return

    except Exception as exc:

        logger.exception(
            "AI chat handler failed: %s",
            exc
        )

        try:

            await processing_message.edit_text(
                text=(
                    "⚠️ <b>Something went wrong.</b>\n\n"
                    "Please try again."
                ),
                parse_mode=ParseMode.HTML
            )

        except Exception:

            await processing_message.edit_text(
                text=(
                    "⚠️ Something went wrong.\n\n"
                    "Please try again."
                )
            )

    finally:

        stop_event.set()

        typing_task.cancel()

        with contextlib.suppress(
            asyncio.CancelledError
        ):
            await typing_task
