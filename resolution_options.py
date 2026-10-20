"""Shared resolution choices, stable ordering and truthful output labels."""

VIDEO_QUALITIES = ("max", "4320", "2160", "1440", "1080", "720", "480", "360", "240", "144")
PLAYLIST_QUALITIES = ("max", "2160", "1440", "1080", "720", "480", "360", "240", "144")


def ordered_qualities(selected):
    return tuple(key for key in VIDEO_QUALITIES if key in selected)


def quality_name(key):
    return "Best" if key == "max" else ("Audio" if key == "audio" else f"{key}p")


def quality_summary(selected):
    return ", ".join(quality_name(key) for key in selected)


def video_choices(session=None):
    if session is None:
        return PLAYLIST_QUALITIES
    heights = {str(item.height) for item in session.analysis.available_qualities}
    return tuple(key for key in VIDEO_QUALITIES
                 if (key == "max" and session.analysis.max_quality) or key in heights)


def resolution_tag(key, height):
    actual = f"{nominal_height(height)}p" if height else "source"
    return f"best_{actual}" if key == "max" else actual


def nominal_height(height):
    """Name lightly cropped encodes by their standard resolution tier.

    Keep nonstandard sizes unchanged and never promote a genuinely lower
    resolution (for example, 960p is not 1080p). Actual pixel dimensions remain
    unchanged in the media and Telegram's video attributes.
    """
    height = int(height or 0)
    for tier in (4320, 2160, 1440, 1080, 720, 480, 360, 240, 144):
        if abs(height - tier) <= max(2, min(8, tier // 100)):
            return tier
    return height


def resolution_label(tag):
    if tag.startswith("best_"):
        return "Best · " + ("Source quality" if tag[5:] == "source" else tag[5:])
    return "Source quality" if tag == "source" else tag


def stream_identity(plan):
    # Reuse the same extracted metadata for all variants, so URLs/format IDs
    # identify identical selections even when requested heights differ.
    return tuple((fmt.get("format_id"), fmt.get("url"), fmt.get("vcodec"), fmt.get("acodec"))
                 for fmt in plan.formats)
