"""Plugin discovery and registration via entry_points."""

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urlparse

from etransfer.plugins.base_sink import BaseSink, SinkContext
from etransfer.plugins.base_source import BaseSource

logger = logging.getLogger("etransfer.plugins")

_EP_GROUP_SOURCES = "etransfer.sources"
_EP_GROUP_SINKS = "etransfer.sinks"


class PluginInfo:
    """Lightweight descriptor exposed via the REST API."""

    __slots__ = ("name", "display_name", "plugin_type", "extra")

    def __init__(self, name: str, display_name: str, plugin_type: str, extra: Optional[dict] = None) -> None:
        self.name = name
        self.display_name = display_name
        self.plugin_type = plugin_type
        self.extra = extra or {}

    def to_dict(self) -> dict:
        return {"name": self.name, "display_name": self.display_name, "type": self.plugin_type, **self.extra}


class PluginRegistry:
    """Central registry that discovers and caches Source / Sink plugins."""

    def __init__(self) -> None:
        self._sources: dict[str, type[BaseSource]] = {}
        self._sinks: dict[str, type[BaseSink]] = {}
        self._source_instances: dict[str, BaseSource] = {}
        self._sink_classes: dict[str, type[BaseSink]] = {}
        self._discovered = False

    # ── Discovery ─────────────────────────────────────────────

    def discover(self) -> None:
        """Load all Source / Sink plugins from installed entry_points."""
        if self._discovered:
            return
        self._discovered = True

        import importlib.metadata as _meta
        import sys

        def _get_eps(group: str) -> list:
            if sys.version_info >= (3, 12):
                return list(_meta.entry_points(group=group))
            eps = _meta.entry_points()
            if isinstance(eps, dict):
                return list(eps.get(group, []))
            return [ep for ep in eps if ep.group == group]

        for ep in _get_eps(_EP_GROUP_SOURCES):
            try:
                cls = ep.load()
                if isinstance(cls, type) and issubclass(cls, BaseSource):
                    self._sources[ep.name] = cls
                    logger.info("Discovered source plugin: %s -> %s", ep.name, cls.__name__)
            except Exception:
                logger.exception("Failed to load source plugin: %s", ep.name)

        for ep in _get_eps(_EP_GROUP_SINKS):
            try:
                cls = ep.load()
                if isinstance(cls, type) and issubclass(cls, BaseSink):
                    self._sinks[ep.name] = cls
                    logger.info("Discovered sink plugin: %s -> %s", ep.name, cls.__name__)
            except Exception:
                logger.exception("Failed to load sink plugin: %s", ep.name)

        # Built-in sinks should work in source-tree deployments even before
        # package entry point metadata is refreshed by reinstalling.
        self._register_builtin_sink("local", "etransfer.plugins.sinks.local", "LocalSink")
        self._register_builtin_sink("tos", "etransfer.plugins.sinks.tos_multipart", "TosSink")
        self._register_builtin_sink("zos", "etransfer.plugins.sinks.zos", "ZosSink")

    def _register_builtin_sink(self, name: str, module_name: str, class_name: str) -> None:
        if name in self._sinks:
            return
        try:
            import importlib

            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            if isinstance(cls, type) and issubclass(cls, BaseSink):
                self._sinks[name] = cls
                logger.info("Registered built-in sink plugin: %s -> %s", name, cls.__name__)
        except Exception:
            logger.exception("Failed to register built-in sink plugin: %s", name)

    def register_source(self, cls: type[BaseSource]) -> None:
        """Programmatically register a Source plugin (for testing / embedded use)."""
        inst = cls()
        self._sources[inst.name] = cls
        self._source_instances[inst.name] = inst

    def register_sink(self, cls: type[BaseSink]) -> None:
        """Programmatically register a Sink plugin."""
        self._sinks[cls.name if hasattr(cls, "name") and cls.name else cls.__name__] = cls

    # ── Accessors ─────────────────────────────────────────────

    def _ensure_source_instance(self, name: str) -> BaseSource:
        if name not in self._source_instances:
            cls = self._sources.get(name)
            if cls is None:
                raise KeyError(f"Unknown source plugin: {name}")
            self._source_instances[name] = cls()
        return self._source_instances[name]

    def get_source(self, name: str) -> BaseSource:
        self.discover()
        return self._ensure_source_instance(name)

    def get_sink_class(self, name: str) -> type[BaseSink]:
        self.discover()
        cls = self._sinks.get(name)
        if cls is None:
            raise KeyError(f"Unknown sink plugin: {name}")
        return cls

    def create_sink(self, name: str, config: Optional[dict] = None) -> BaseSink:
        cls = self.get_sink_class(name)
        return cls(config=config)

    # ── Resolution ────────────────────────────────────────────

    async def resolve_source(self, url: str) -> BaseSource:
        """Match *url* to the best Source plugin by hostname + ``can_handle``.

        Sources are tried in ``priority`` descending order.
        ``DirectLinkSource`` (priority -1) acts as the fallback.
        """
        self.discover()

        parsed = urlparse(url)
        host = parsed.hostname or ""

        candidates: list[BaseSource] = []
        for name, cls in self._sources.items():
            inst = self._ensure_source_instance(name)
            candidates.append(inst)

        candidates.sort(key=lambda s: s.priority, reverse=True)

        for source in candidates:
            if source.supported_hosts and not source.matches_host(url):
                continue
            try:
                if await source.can_handle(url):
                    return source
            except Exception:
                logger.debug("Source %s.can_handle raised for %s", source.name, url, exc_info=True)

        raise ValueError(f"No source plugin can handle URL: {url}")

    def resolve_sink_config(self, sink_name: str, context: SinkContext, server_presets: Optional[dict] = None) -> dict:
        """Resolve runtime configuration for a named Sink plugin."""
        cls = self.get_sink_class(sink_name)
        return cls.resolve_config(context, server_presets or {})

    # ── Listing ───────────────────────────────────────────────

    def list_sources(self) -> list[PluginInfo]:
        self.discover()
        infos: list[PluginInfo] = []
        for name, cls in self._sources.items():
            inst = self._ensure_source_instance(name)
            infos.append(
                PluginInfo(
                    name=inst.name,
                    display_name=inst.display_name,
                    plugin_type="source",
                    extra={"supported_hosts": inst.supported_hosts, "priority": inst.priority},
                )
            )
        return infos

    def list_sinks(self) -> list[PluginInfo]:
        self.discover()
        infos: list[PluginInfo] = []
        for name, cls in self._sinks.items():
            inst_name = cls.name if hasattr(cls, "name") and cls.name else name
            display = cls.display_name if hasattr(cls, "display_name") and cls.display_name else name
            infos.append(
                PluginInfo(
                    name=inst_name,
                    display_name=display,
                    plugin_type="sink",
                    extra={"config_schema": cls.get_config_schema(), "supports_multipart": cls.supports_multipart},
                )
            )
        return infos


# Module-level singleton
plugin_registry = PluginRegistry()
