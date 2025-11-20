from typing import Any

from langchain_core.messages.ai import UsageMetadata


def merge_description_and_tags(description: str | None, tags: Any) -> str:
    parts: list[str] = []

    if tags:
        if isinstance(tags, dict):
            parts.extend(str(v) for v in tags.values() if v)
        elif isinstance(tags, list | tuple | set):
            parts.extend(str(t) for t in tags if t)
        else:
            parts.append(str(tags))

    if description:
        parts.append(str(description))

    return " ".join(parts)


def categorize_aspect_ratio(width: int, height: int) -> str:
    ratio = width / height

    known_ratios = {
        "6:13": 6 / 13,
        "9:16": 9 / 16,
        "3:5": 3 / 5,
        "2:3": 2 / 3,
        "1:1": 1.0,
        "19:16": 19 / 16,
        "5:4": 5 / 4,
        "4:3": 4 / 3,
        "11:8": 11 / 8,
        "1.41:1": 2**0.5,
        "143:100": 143 / 100,
        "3:2": 3 / 2,
        "14:9": 14 / 9,
        "8:5": 8 / 5,
        "1.618:1": 1.618,
        "5:3": 5 / 3,
        "16:9": 16 / 9,
        "37:20": 37 / 20,
        "19:10": 19 / 10,
        "2:1": 2.0,
        "11:5": 11 / 5,
        "64:27": 64 / 27,
        "12:5": 12 / 5,
        "2.35:1": 2.35,
        "69:25": 69 / 25,
        "32:9": 32 / 9,
        "4:1": 4.0,
    }

    closest_ratio = "unknown"
    min_diff = float("inf")

    for label, known in known_ratios.items():
        diff = abs(ratio - known)
        if diff < min_diff and diff < 0.05:
            min_diff = diff
            closest_ratio = label

    return closest_ratio


GEMINI_PRICES = {
    "gemini-2.5-pro": {
        "input": 1.25,
        "output": 10.00,
        "input_large": 2.50,
        "output_large": 15.00,
        "threshold": 200_000,
    },
    "gemini-2.5-flash": {"input": 0.30, "output": 2.50, "input_audio": 1.00},
    "gemini-2.5-flash-lite": {"input": 0.10, "output": 0.40, "input_audio": 0.50},
    "gemini-1.5-pro": {
        "input": 1.25,
        "output": 5.00,
        "input_large": 2.50,
        "output_large": 10.00,
        "threshold": 128_000,
    },
    "gemini-1.5-flash": {
        "input": 0.075,
        "output": 0.30,
        "input_large": 0.15,
        "output_large": 0.60,
        "threshold": 128_000,
    },
    "gemini-1.5-flash-8b": {
        "input": 0.0375,
        "output": 0.15,
        "input_large": 0.075,
        "output_large": 0.30,
        "threshold": 128_000,
    },
    "gemini-embedding": {"input": 0.15, "output": 0.0},
}

OPENAI_PRICES = {
    "gpt-4o": {
        "model_id": "gpt-4o-2024-08-06",
        "input_per_million_tokens_usd": 2.50,
        "cached_input_per_million_tokens_usd": 1.25,
        "output_per_million_tokens_usd": 10.00,
    },
    "whisper-1": {"price_per_minute_usd": 0.006},
}


def calculate_openai_cost(model, usage: UsageMetadata | dict) -> float:
    total_prompt_tokens = usage.get("input_tokens", 0)
    total_completion_tokens = usage.get("output_tokens", 0)

    pricing = OPENAI_PRICES.get(model)
    if not pricing:
        raise ValueError(f"Failed to calculate cost due to unsupported model: {model}")

    input_cost = (total_prompt_tokens / 1_000_000) * pricing["input_per_million_tokens_usd"]
    output_cost = (total_completion_tokens / 1_000_000) * pricing["output_per_million_tokens_usd"]

    return input_cost + output_cost


def calculate_gemini_cost(model, usage: UsageMetadata | dict) -> float:
    prompt_tokens = usage.get("input_tokens", 0)
    candidate_tokens = usage.get("output_tokens", 0)

    if model not in GEMINI_PRICES:
        raise ValueError(f"Failed to calculate cost due to unknown model: {model}")

    model_info = GEMINI_PRICES[model]

    if "threshold" in model_info:
        if prompt_tokens > model_info["threshold"]:
            input_price = model_info["input_large"]
            output_price = model_info["output_large"]
        else:
            input_price = model_info["input"]
            output_price = model_info["output"]
    else:
        input_price = model_info["input"]
        output_price = model_info["output"]

    cost = (prompt_tokens / 1_000_000) * input_price
    cost += (candidate_tokens / 1_000_000) * output_price

    return round(cost, 6)


def calculate_cost(usage_metadata: dict[str, UsageMetadata | dict]) -> float:
    total_cost = 0.0
    for model, usage in usage_metadata.items():
        if model in OPENAI_PRICES:
            cost = calculate_openai_cost(model, usage)
        elif model in GEMINI_PRICES:
            cost = calculate_gemini_cost(model, usage)
        else:
            cost = 0.0
        total_cost += cost
    return float(total_cost)


def calculate_transcription_cost(model, duration_sec: float) -> float:
    pricing_info = OPENAI_PRICES.get(model)

    if pricing_info is None or "price_per_minute_usd" not in pricing_info:
        print(f"Failed to calculate transcription cost: no price available for transcription model '{model}'")
        return 0.0

    rate_per_minute = pricing_info["price_per_minute_usd"]
    duration_min = duration_sec / 60.0
    cost = duration_min * rate_per_minute

    return round(cost, 6)
