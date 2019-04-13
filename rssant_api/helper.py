def shorten(text, width, placeholder='...'):
    if not text:
        return text
    if len(text) <= width:
        return text
    return text[: max(0, width - len(placeholder))] + placeholder
