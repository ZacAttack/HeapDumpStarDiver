# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Zac Policzer

"""Session state for the HeapDumpStarDiver MCP server."""

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import duckdb


def _find_binary() -> Path | None:
    """Locate the HeapDumpStarDiver binary."""
    # 1. Env var override
    env = os.environ.get("HEAP_DUMP_STAR_DIVER_BINARY_OVERRIDE")
    if env:
        p = Path(env)
        if p.is_file():
            return p

    # 2. Relative to this repo (target/release/)
    repo_root = Path(__file__).resolve().parent.parent
    release = repo_root / "target" / "release" / "HeapDumpStarDiver"
    if release.is_file():
        return release

    # 3. On PATH
    on_path = shutil.which("HeapDumpStarDiver")
    if on_path:
        return Path(on_path)

    return None


@dataclass
class HeapSession:
    """A single heap dump analysis session with its own DuckDB connection."""
    session_id: str
    parquet_dir: Path
    hprof_path: Path | None = None
    connection: duckdb.DuckDBPyConnection | None = field(default=None, repr=False)

    def open(self) -> None:
        """Open the DuckDB connection for this session."""
        if self.connection is None:
            self.connection = duckdb.connect()

    def close(self) -> None:
        """Close the DuckDB connection, keep files on disk."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    @property
    def is_active(self) -> bool:
        return self.connection is not None


class SessionManager:
    """Manages multiple named heap dump sessions."""

    def __init__(self):
        self.sessions: dict[str, HeapSession] = {}
        self.rust_binary: Path | None = _find_binary()

    def _make_session_id(self, hprof_path: Path) -> str:
        """Derive a session ID from the HPROF filename, handling collisions."""
        base = hprof_path.stem  # e.g. "service-a-20240315" from "service-a-20240315.hprof"
        if base not in self.sessions:
            return base
        # Append incrementing suffix on collision
        n = 2
        while f"{base}_{n}" in self.sessions:
            n += 1
        return f"{base}_{n}"

    def create_session(
        self,
        parquet_dir: Path,
        hprof_path: Path | None = None,
        session_id: str | None = None,
    ) -> HeapSession:
        """Create and open a new session."""
        if session_id is None:
            if hprof_path is not None:
                session_id = self._make_session_id(hprof_path)
            else:
                session_id = self._make_session_id(parquet_dir)

        if session_id in self.sessions:
            # Close existing session with this ID before replacing
            self.sessions[session_id].close()

        sess = HeapSession(
            session_id=session_id,
            parquet_dir=parquet_dir,
            hprof_path=hprof_path,
        )
        sess.open()
        self.sessions[session_id] = sess
        return sess

    def get(self, session_id: str | None = None) -> HeapSession:
        """Get a session by ID, or the only active session if ID is omitted."""
        if session_id:
            if session_id not in self.sessions:
                raise KeyError(f"No session with ID '{session_id}'. "
                               f"Active sessions: {list(self.sessions.keys())}")
            sess = self.sessions[session_id]
            if not sess.is_active:
                raise ValueError(f"Session '{session_id}' is closed. "
                                 f"Use open_session to reopen it.")
            return sess

        active = [s for s in self.sessions.values() if s.is_active]
        if len(active) == 1:
            return active[0]
        if len(active) == 0:
            raise ValueError("No active sessions. Use convert_heap_dump or open_session first.")
        raise ValueError(
            f"Multiple active sessions — specify session_id. "
            f"Active: {[s.session_id for s in active]}"
        )

    def close_session(self, session_id: str) -> None:
        """Close a session's DuckDB connection, keep files."""
        if session_id not in self.sessions:
            raise KeyError(f"No session with ID '{session_id}'.")
        self.sessions[session_id].close()

    def cleanup_session(self, session_id: str) -> tuple[int, str]:
        """Close connection and delete parquet files. Returns (files_deleted, dir_path)."""
        if session_id not in self.sessions:
            raise KeyError(f"No session with ID '{session_id}'.")
        sess = self.sessions[session_id]
        sess.close()

        parquet_dir = sess.parquet_dir
        files_deleted = 0
        if parquet_dir.is_dir():
            for f in parquet_dir.glob("*.parquet"):
                f.unlink()
                files_deleted += 1
            # Remove the directory if empty
            try:
                parquet_dir.rmdir()
            except OSError:
                pass  # Not empty (has non-parquet files), leave it

        del self.sessions[session_id]
        return files_deleted, str(parquet_dir)

    def list_all(self) -> list[dict]:
        """List all sessions with their status."""
        return [
            {
                "session_id": s.session_id,
                "parquet_dir": str(s.parquet_dir),
                "hprof_path": str(s.hprof_path) if s.hprof_path else None,
                "active": s.is_active,
            }
            for s in self.sessions.values()
        ]


# Module-level singleton.
manager = SessionManager()
