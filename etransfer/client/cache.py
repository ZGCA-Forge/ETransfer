"""Local cache management for downloads."""

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from etransfer.common.constants import CACHE_DIR_NAME


class LocalCache:
    """Manage local cache for downloaded file chunks.

    Stores downloaded chunks to enable resume functionality
    and avoid re-downloading data.
    """

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        max_size_mb: int = 1024,  # 1GB default
    ) -> None:
        """Initialize local cache.

        Args:
            cache_dir: Cache directory path
            max_size_mb: Maximum cache size in MB
        """
        if cache_dir is None:
            cache_dir = Path.home() / CACHE_DIR_NAME / "cache"

        self.cache_dir = Path(cache_dir)
        self.max_size_bytes = max_size_mb * 1024 * 1024

        # Ensure directories exist
        self.chunks_dir = self.cache_dir / "chunks"
        self.meta_dir = self.cache_dir / "meta"
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)

    def _get_chunk_path(self, file_id: str, chunk_index: int) -> Path:
        """Get path for a cached chunk."""
        return self.chunks_dir / f"{file_id}_{chunk_index}.chunk"

    def _get_meta_path(self, file_id: str) -> Path:
        """Get path for file metadata."""
        return self.meta_dir / f"{file_id}.json"

    def has_chunk(self, file_id: str, chunk_index: int) -> bool:
        """Check if a chunk is cached.

        Args:
            file_id: File identifier
            chunk_index: Chunk index

        Returns:
            True if chunk is cached
        """
        return self._get_chunk_path(file_id, chunk_index).exists()

    def get_chunk(self, file_id: str, chunk_index: int) -> Optional[bytes]:
        """Get a cached chunk.

        Args:
            file_id: File identifier
            chunk_index: Chunk index

        Returns:
            Chunk data or None if not cached
        """
        path = self._get_chunk_path(file_id, chunk_index)
        if not path.exists():
            return None

        try:
            return path.read_bytes()
        except Exception:
            return None

    def put_chunk(self, file_id: str, chunk_index: int, data: bytes) -> None:
        """Cache a chunk.

        Args:
            file_id: File identifier
            chunk_index: Chunk index
            data: Chunk data
        """
        path = self._get_chunk_path(file_id, chunk_index)
        path.write_bytes(data)

        # Update metadata
        self._update_meta(file_id, chunk_index, len(data))

        # Check cache size
        self._enforce_size_limit()

    def delete_chunk(self, file_id: str, chunk_index: int) -> None:
        """Delete a cached chunk.

        Args:
            file_id: File identifier
            chunk_index: Chunk index
        """
        path = self._get_chunk_path(file_id, chunk_index)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def get_cached_chunks(self, file_id: str) -> list[int]:
        """Get list of cached chunk indices for a file.

        Args:
            file_id: File identifier

        Returns:
            List of cached chunk indices
        """
        chunks = []
        for path in self.chunks_dir.glob(f"{file_id}_*.chunk"):
            try:
                # Extract chunk index from filename
                name = path.stem
                index_str = name.split("_")[-1]
                chunks.append(int(index_str))
            except (ValueError, IndexError):
                continue

        return sorted(chunks)

    def get_download_progress(self, file_id: str) -> dict:
        """Get download progress for a file.

        Args:
            file_id: File identifier

        Returns:
            Dict with progress info
        """
        meta_path = self._get_meta_path(file_id)
        if not meta_path.exists():
            return {
                "file_id": file_id,
                "cached_chunks": [],
                "total_cached_bytes": 0,
            }

        try:
            meta = json.loads(meta_path.read_text())
            cached_chunks = self.get_cached_chunks(file_id)

            return {
                "file_id": file_id,
                "filename": meta.get("filename"),
                "total_size": meta.get("total_size"),
                "chunk_size": meta.get("chunk_size"),
                "total_chunks": meta.get("total_chunks"),
                "cached_chunks": cached_chunks,
                "total_cached_bytes": meta.get("cached_bytes", 0),
            }
        except Exception:
            return {
                "file_id": file_id,
                "cached_chunks": self.get_cached_chunks(file_id),
                "total_cached_bytes": 0,
            }

    def set_file_meta(
        self,
        file_id: str,
        filename: str,
        total_size: int,
        chunk_size: int,
    ) -> None:
        """Set metadata for a file download.

        Args:
            file_id: File identifier
            filename: Original filename
            total_size: Total file size
            chunk_size: Chunk size
        """
        total_chunks = (total_size + chunk_size - 1) // chunk_size

        meta = {
            "file_id": file_id,
            "filename": filename,
            "total_size": total_size,
            "chunk_size": chunk_size,
            "total_chunks": total_chunks,
            "cached_bytes": 0,
            "cached_chunks": [],
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }

        meta_path = self._get_meta_path(file_id)
        meta_path.write_text(json.dumps(meta, indent=2))

    def _update_meta(self, file_id: str, chunk_index: int, chunk_size: int) -> None:
        """Update metadata after caching a chunk."""
        meta_path = self._get_meta_path(file_id)
        if not meta_path.exists():
            return

        try:
            meta = json.loads(meta_path.read_text())
            cached_chunks = meta.get("cached_chunks", [])

            if chunk_index not in cached_chunks:
                cached_chunks.append(chunk_index)
                meta["cached_chunks"] = sorted(cached_chunks)
                meta["cached_bytes"] = meta.get("cached_bytes", 0) + chunk_size
                meta["updated_at"] = datetime.utcnow().isoformat()

            meta_path.write_text(json.dumps(meta, indent=2))
        except Exception:
            pass

    def clear_file(self, file_id: str) -> None:
        """Clear all cached data for a file.

        Args:
            file_id: File identifier
        """
        # Delete chunks
        for path in self.chunks_dir.glob(f"{file_id}_*.chunk"):
            try:
                path.unlink()
            except Exception:
                pass

        # Delete metadata
        meta_path = self._get_meta_path(file_id)
        try:
            meta_path.unlink()
        except FileNotFoundError:
            pass

    def clear_all(self) -> None:
        """Clear all cached data."""
        try:
            shutil.rmtree(self.chunks_dir)
            shutil.rmtree(self.meta_dir)
            self.chunks_dir.mkdir(parents=True, exist_ok=True)
            self.meta_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    def get_cache_size(self) -> int:
        """Get total cache size in bytes."""
        total = 0
        for path in self.chunks_dir.glob("*.chunk"):
            try:
                total += path.stat().st_size
            except Exception:
                pass
        return total

    def _enforce_size_limit(self) -> None:
        """Enforce cache size limit using LRU."""
        current_size = self.get_cache_size()
        if current_size <= self.max_size_bytes:
            return

        # Get all chunk files with modification times
        chunks = []
        for path in self.chunks_dir.glob("*.chunk"):
            try:
                stat = path.stat()
                chunks.append((path, stat.st_mtime, stat.st_size))
            except Exception:  # nosec B112
                continue

        # Sort by modification time (oldest first)
        chunks.sort(key=lambda x: x[1])

        # Delete oldest chunks until under limit
        for path, _, size in chunks:
            if current_size <= self.max_size_bytes:
                break

            try:
                path.unlink()
                current_size -= size
            except Exception:  # nosec B112
                continue

    def assemble_file(self, file_id: str, output_path: Path) -> bool:
        """Assemble cached chunks into complete file.

        Args:
            file_id: File identifier
            output_path: Output file path

        Returns:
            True if successful
        """
        meta_path = self._get_meta_path(file_id)
        if not meta_path.exists():
            return False

        try:
            meta = json.loads(meta_path.read_text())
            total_chunks = meta.get("total_chunks", 0)
            cached_chunks = self.get_cached_chunks(file_id)

            # Check if all chunks are cached
            if len(cached_chunks) != total_chunks:
                return False

            if set(cached_chunks) != set(range(total_chunks)):
                return False

            # Assemble file
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "wb") as f:
                for i in range(total_chunks):
                    chunk_data = self.get_chunk(file_id, i)
                    if chunk_data is None:
                        return False
                    f.write(chunk_data)

            return True
        except Exception:
            return False
