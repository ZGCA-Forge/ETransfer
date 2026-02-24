"""Abstract interface for state storage backends."""

from abc import ABC, abstractmethod
from typing import Optional


class StateBackend(ABC):
    """Abstract base class for state storage backends.

    Implementations should provide async key-value storage with:
    - Basic CRUD operations (get, set, delete)
    - Key expiration support
    - Atomic operations for distributed locking
    - Key pattern scanning

    Available implementations:
    - FileStateBackend: File-based storage (default, persistence)
    - MemoryStateBackend: In-memory storage (unit testing)
    - RedisStateBackend: Redis storage (multi-worker production)
    """

    @abstractmethod
    async def connect(self) -> None:
        """Initialize connection to the backend."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the backend."""
        pass

    @abstractmethod
    async def get(self, key: str) -> Optional[str]:
        """Get value by key.

        Args:
            key: The key to retrieve

        Returns:
            The value as string, or None if not found
        """
        pass

    @abstractmethod
    async def set(
        self,
        key: str,
        value: str,
        ex: Optional[int] = None,
        nx: bool = False,
    ) -> bool:
        """Set a key-value pair.

        Args:
            key: The key to set
            value: The value to store
            ex: Expiration time in seconds (None = no expiration)
            nx: Only set if key does not exist (for atomic operations)

        Returns:
            True if the value was set, False if nx=True and key exists
        """
        pass

    @abstractmethod
    async def delete(self, key: str) -> int:
        """Delete a key.

        Args:
            key: The key to delete

        Returns:
            Number of keys deleted (0 or 1)
        """
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if a key exists.

        Args:
            key: The key to check

        Returns:
            True if key exists
        """
        pass

    @abstractmethod
    async def scan(
        self,
        cursor: int,
        match: str = "*",
        count: int = 100,
    ) -> tuple[int, list[str]]:
        """Scan keys matching a pattern.

        Args:
            cursor: Cursor position (0 to start)
            match: Pattern to match (supports * wildcard)
            count: Hint for number of keys to return

        Returns:
            Tuple of (next_cursor, list_of_keys)
            next_cursor is 0 when scan is complete
        """
        pass

    @abstractmethod
    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment a numeric value.

        Args:
            key: The key to increment
            amount: Amount to increment by

        Returns:
            The new value after increment
        """
        pass

    @abstractmethod
    async def expire(self, key: str, seconds: int) -> bool:
        """Set expiration on a key.

        Args:
            key: The key to expire
            seconds: Seconds until expiration

        Returns:
            True if expiration was set
        """
        pass

    async def ping(self) -> bool:
        """Check if backend is available.

        Returns:
            True if backend is responding
        """
        return True

    @property
    def name(self) -> str:
        """Get backend name for logging."""
        return self.__class__.__name__
