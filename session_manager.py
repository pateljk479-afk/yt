import time
import math
import uuid
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field

from downloader import VideoQualityAnalysis
import config


@dataclass
class PlaylistSession:
    session_id: str
    chat_id: int
    playlist_title: str
    playlist_url: str
    entries: List[dict]
    selected_indices: Set[int] = field(default_factory=set)
    page: int = 0
    per_page: int = 10
    message_id: Optional[int] = None
    created_at: float = field(default_factory=time.time)

    @property
    def total_entries(self) -> int:
        return len(self.entries)

    @property
    def total_pages(self) -> int:
        if not self.entries:
            return 1
        return math.ceil(len(self.entries) / self.per_page)

    def get_page_entries(self) -> List[Tuple[int, dict]]:
        """Get (index, entry) for the current page."""
        start = self.page * self.per_page
        end = min(start + self.per_page, len(self.entries))
        return [(i, self.entries[i]) for i in range(start, end)]

    def toggle(self, index: int) -> bool:
        """Toggle selection for a given video index. Returns True if now selected."""
        if 0 <= index < len(self.entries):
            if index in self.selected_indices:
                self.selected_indices.remove(index)
                return False
            else:
                self.selected_indices.add(index)
                return True
        return False

    def select_all(self):
        """Select all videos in playlist."""
        self.selected_indices = set(range(len(self.entries)))

    def deselect_all(self):
        """Deselect all videos."""
        self.selected_indices.clear()

    def get_selected_entries(self) -> List[dict]:
        """Return list of selected entries in original order."""
        return [self.entries[i] for i in sorted(self.selected_indices)]


@dataclass
class VideoSession:
    session_id: str
    chat_id: int
    video_url: str
    analysis: VideoQualityAnalysis
    message_id: Optional[int] = None
    created_at: float = field(default_factory=time.time)


class SessionManager:
    """In-memory session manager with automatic TTL pruning."""

    def __init__(self, ttl_seconds: int = config.SESSION_TTL_SECONDS):
        self.ttl_seconds = ttl_seconds
        self._playlist_sessions: Dict[str, PlaylistSession] = {}
        self._video_sessions: Dict[str, VideoSession] = {}

    def _generate_id(self) -> str:
        return uuid.uuid4().hex[:8]

    def create_playlist_session(
        self,
        chat_id: int,
        playlist_title: str,
        playlist_url: str,
        entries: List[dict],
        message_id: Optional[int] = None,
    ) -> PlaylistSession:
        self.cleanup_expired()
        session_id = self._generate_id()
        session = PlaylistSession(
            session_id=session_id,
            chat_id=chat_id,
            playlist_title=playlist_title,
            playlist_url=playlist_url,
            entries=entries,
            message_id=message_id,
        )
        self._playlist_sessions[session_id] = session
        return session

    def get_playlist_session(self, session_id: str) -> Optional[PlaylistSession]:
        return self._playlist_sessions.get(session_id)

    def create_video_session(
        self,
        chat_id: int,
        video_url: str,
        analysis: VideoQualityAnalysis,
        message_id: Optional[int] = None,
    ) -> VideoSession:
        self.cleanup_expired()
        session_id = self._generate_id()
        session = VideoSession(
            session_id=session_id,
            chat_id=chat_id,
            video_url=video_url,
            analysis=analysis,
            message_id=message_id,
        )
        self._video_sessions[session_id] = session
        return session

    def get_video_session(self, session_id: str) -> Optional[VideoSession]:
        return self._video_sessions.get(session_id)

    def remove_session(self, session_id: str):
        self._playlist_sessions.pop(session_id, None)
        self._video_sessions.pop(session_id, None)

    def cleanup_expired(self):
        """Remove sessions older than TTL."""
        now = time.time()
        expired_pl = [k for k, v in self._playlist_sessions.items() if now - v.created_at > self.ttl_seconds]
        for k in expired_pl:
            self._playlist_sessions.pop(k, None)

        expired_vid = [k for k, v in self._video_sessions.items() if now - v.created_at > self.ttl_seconds]
        for k in expired_vid:
            self._video_sessions.pop(k, None)


# Global singleton instance
session_manager = SessionManager()
