def parse_values(values_str):
    """Parse SQL values string"""
    # Simplified parsing - for demonstration
    values = []
    current = []
    in_quotes = False
    escape = False

    for char in values_str:
        if escape:
            current.append(char)
            escape = False
            continue

        if char == '\\':
            escape = True
            continue

        if char in ['\'', '"']:
            in_quotes = not in_quotes
            continue

        if char == ',' and not in_quotes:
            values.append(''.join(current).strip())
            current = []
            continue

        current.append(char)

    if current:
        values.append(''.join(current).strip())

    return values
