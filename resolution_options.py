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
    actual = f"{int(height)}p" if height else "source"
    return f"best_{actual}" if key == "max" else actual


def stream_identity(plan):
    # Reuse the same extracted metadata for all variants, so URLs/format IDs
    # identify identical selections even when requested heights differ.
    return tuple((fmt.get("format_id"), fmt.get("url"), fmt.get("vcodec"), fmt.get("acodec"))
                 for fmt in plan.formats)
