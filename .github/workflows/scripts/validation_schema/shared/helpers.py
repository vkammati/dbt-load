def check_mutual_exclusive(model, fields: list[str]):
    if [getattr(model, field) for field in fields].count(None) < len(fields) - 1:
        raise ValueError(
            f"Only ONE of these fields can be set at the same time: {', '.join(fields)}"
        )
