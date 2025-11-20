from collections import defaultdict

from pydantic import BaseModel


class VideoProfileResult(BaseModel):
    abuse_categories: list[str] | None = None
    abuse_categories_conf: list[float] | None = None
    abuse_categories_reasoning: list[str] | None = None
    adversary_levels: list[str] | None = None
    adversary_levels_conf: list[float] | None = None
    adversary_levels_reasoning: list[str] | None = None


def consolidate_abuse_categories(
    abuse_categories: list[str | None],
    abuse_categories_conf: list[int | None],
    abuse_categories_reasoning: list[str | None],
    adversary_levels: list[str | None],
    adversary_levels_conf: list[int | None],
    adversary_levels_reasoning: list[str | None],
) -> VideoProfileResult:
    if not (
        len(abuse_categories)
        == len(abuse_categories_conf)
        == len(abuse_categories_reasoning)
        == len(adversary_levels)
        == len(adversary_levels_conf)
        == len(adversary_levels_reasoning)
    ):
        raise ValueError("All input lists must have the same length")

    combined_scores = defaultdict(
        lambda: {"label_conf": [], "label_reasoning": [], "adv_conf": [], "adv_reasoning": []}
    )

    for label, label_conf, label_reason, adv_level, adv_conf, adv_reason in zip(
        abuse_categories,
        abuse_categories_conf,
        abuse_categories_reasoning,
        adversary_levels,
        adversary_levels_conf,
        adversary_levels_reasoning,
        strict=True,
    ):
        if all(val is None for val in (label, label_conf, adv_level, adv_conf)):
            continue
        if label is not None and adv_level is None:
            raise ValueError(
                f"label is not None and adv_level is None: {label}, {label_conf}, {adv_level}, {adv_conf}. Please check upstream results"
            )

        if adv_level is not None:
            if label is None:
                label = ""
                label_conf = 50
            if adv_conf is None:
                adv_conf = 50

        key = (label, adv_level)
        combined_scores[key]["label_conf"].append(label_conf)
        combined_scores[key]["label_reasoning"].append(label_reason)
        combined_scores[key]["adv_conf"].append(adv_conf)
        combined_scores[key]["adv_reasoning"].append(adv_reason)

    label_to_keys = defaultdict(list)
    for label, adv_level in combined_scores:
        label_to_keys[label].append((label, adv_level))

    def adversary_priority(level):
        if isinstance(level, str):
            if "full_" in level:
                return 0
            elif "semi_" in level:
                return 1
            elif "non_" in level:
                return 2
        return 3

    unique_abuse_categories = []
    unique_label_confidences = []
    unique_label_reasonings = []
    unique_adversary_levels = []
    unique_adv_confidences = []
    unique_adv_reasonings = []

    for label, keys in label_to_keys.items():
        keys.sort(key=lambda k: adversary_priority(k[1]))
        preferred_key = keys[0]

        scores = combined_scores[preferred_key]

        unique_abuse_categories.append(preferred_key[0])
        unique_label_confidences.append(round(sum(scores["label_conf"]) / len(scores["label_conf"]), 2))
        unique_label_reasonings.append(scores["label_reasoning"][0])
        unique_adversary_levels.append(preferred_key[1])
        unique_adv_confidences.append(round(sum(scores["adv_conf"]) / len(scores["adv_conf"]), 2))
        unique_adv_reasonings.append(scores["adv_reasoning"][0])

    return VideoProfileResult(
        abuse_categories=unique_abuse_categories,
        abuse_categories_conf=unique_label_confidences,
        abuse_categories_reasoning=unique_label_reasonings,
        adversary_levels=unique_adversary_levels,
        adversary_levels_conf=unique_adv_confidences,
        adversary_levels_reasoning=unique_adv_reasonings,
    )
