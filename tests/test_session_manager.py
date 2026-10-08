import time
from session_manager import PlaylistSession, SessionManager


def test_playlist_session_pagination():
    # 45 items in playlist
    entries = [{"id": f"vid_{i}", "title": f"Video Number {i}"} for i in range(45)]
    session = PlaylistSession(
        session_id="test1234",
        chat_id=12345,
        playlist_title="Test Playlist 45",
        playlist_url="https://youtube.com/playlist?list=test",
        entries=entries,
        per_page=10,
    )

    # 45 entries / 10 per page = 5 pages
    assert session.total_entries == 45
    assert session.total_pages == 5

    # Page 0 should have 10 items (0..9)
    session.page = 0
    p0 = session.get_page_entries()
    assert len(p0) == 10
    assert p0[0][0] == 0
    assert p0[-1][0] == 9

    # Page 4 (last page) should have 5 items (40..44)
    session.page = 4
    p4 = session.get_page_entries()
    assert len(p4) == 5
    assert p4[0][0] == 40
    assert p4[-1][0] == 44


def test_playlist_selection_and_select_all():
    entries = [{"id": f"vid_{i}", "title": f"Video Number {i}"} for i in range(15)]
    session = PlaylistSession(
        session_id="test_sel",
        chat_id=12345,
        playlist_title="Test Playlist",
        playlist_url="https://youtube.com/playlist?list=test",
        entries=entries,
    )

    # Toggle individual items
    assert session.toggle(2) is True
    assert 2 in session.selected_indices
    assert len(session.selected_indices) == 1

    # Toggle off
    assert session.toggle(2) is False
    assert 2 not in session.selected_indices
    assert len(session.selected_indices) == 0

    # Select all
    session.select_all()
    assert len(session.selected_indices) == 15
    selected = session.get_selected_entries()
    assert len(selected) == 15

    # Deselect all
    session.deselect_all()
    assert len(session.selected_indices) == 0


def test_session_manager_lifecycle():
    manager = SessionManager(ttl_seconds=1)
    entries = [{"id": "v1", "title": "Vid 1"}]

    session = manager.create_playlist_session(
        chat_id=100,
        playlist_title="Test",
        playlist_url="https://yt.com",
        entries=entries,
    )
    sid = session.session_id
    assert manager.get_playlist_session(sid) is not None

    # Remove manually
    manager.remove_session(sid)
    assert manager.get_playlist_session(sid) is None

    # Expiration TTL test
    session2 = manager.create_playlist_session(
        chat_id=100,
        playlist_title="Test2",
        playlist_url="https://yt.com",
        entries=entries,
    )
    session2.created_at = time.time() - 10  # 10s ago, TTL is 1s
    manager.cleanup_expired()
    assert manager.get_playlist_session(session2.session_id) is None
