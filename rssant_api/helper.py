def shorten(text, width, placeholder='...'):
    if len(text) <= width:
        return text
    return text[: max(0, width - len(placeholder))] + placeholder
