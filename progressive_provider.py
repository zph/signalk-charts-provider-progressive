"""Immutable MBTiles generations for a progressive Signal K chart provider."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote


STATE_VERSION = 1
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
VALID_PHASES = {"preview", "refined"}


class ProviderError(RuntimeError):
    """Base class for artifact and tile errors."""


class InvalidArtifact(ProviderError):
    """Raised when an MBTiles artifact is unsafe or malformed."""


class GenerationConflict(ProviderError):
    """Raised when an immutable generation would be replaced."""


class ArtifactNotFound(ProviderError):
    """Raised when a chart generation is unknown or no longer available."""


@dataclass(frozen=True)
class ArtifactGeneration:
    generation: str
    relative_path: str
    phase: str
    name: str
    description: str
    bounds: Tuple[float, float, float, float]
    minzoom: int
    maxzoom: int
    format: str
    chart_type: str
    layers: Tuple[str, ...]
    profile: Optional[str]
    size: int
    sha256: str
    registered_at: str

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["bounds"] = list(self.bounds)
        result["layers"] = list(self.layers)
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ArtifactGeneration":
        artifact = cls(
            generation=_identifier(value.get("generation"), "generation"),
            relative_path=str(value["relative_path"]),
            phase=str(value["phase"]),
            name=str(value["name"]),
            description=str(value.get("description", "")),
            bounds=_parse_bounds(value["bounds"]),
            minzoom=_parse_zoom(value["minzoom"], "minzoom"),
            maxzoom=_parse_zoom(value["maxzoom"], "maxzoom"),
            format=str(value["format"]),
            chart_type=str(value["chart_type"]),
            layers=_normalize_layers(value.get("layers", [])),
            profile=_optional_string(value.get("profile")),
            size=int(value["size"]),
            sha256=str(value["sha256"]),
            registered_at=str(value["registered_at"]),
        )
        _validate_generation(artifact)
        return artifact


@dataclass
class ChartRecord:
    identifier: str
    name: str
    description: str
    active_generation: str
    generations: Dict[str, ArtifactGeneration] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identifier": self.identifier,
            "name": self.name,
            "description": self.description,
            "active_generation": self.active_generation,
            "generations": {
                generation: artifact.to_dict()
                for generation, artifact in sorted(self.generations.items())
            },
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ChartRecord":
        raw_generations = _mapping(value.get("generations", {}))
        generations = {
            _identifier(generation, "generation"): ArtifactGeneration.from_dict(
                _mapping(raw_artifact)
            )
            for generation, raw_artifact in raw_generations.items()
        }
        if any(key != artifact.generation for key, artifact in generations.items()):
            raise InvalidArtifact("generation registry keys do not match identifiers")
        active = _identifier(value.get("active_generation"), "active generation")
        if active not in generations:
            raise InvalidArtifact("active generation is not registered")
        return cls(
            identifier=_identifier(value.get("identifier"), "chart identifier"),
            name=str(value["name"]),
            description=str(value.get("description", "")),
            active_generation=active,
            generations=generations,
        )


@dataclass(frozen=True)
class TileResponse:
    data: bytes
    headers: Dict[str, str]


class ArtifactRegistry:
    """Persist stable chart identities and immutable MBTiles generations."""

    def __init__(
        self,
        state_path: Path,
        artifact_root: Path,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.state_path = Path(state_path)
        self.artifact_root = Path(artifact_root).resolve()
        self._clock = clock
        self._lock = threading.RLock()
        self._charts: Dict[str, ChartRecord] = {}
        self._load()

    def register_mbtiles(
        self,
        chart_id: str,
        generation: object,
        artifact_path: Path,
        *,
        phase: str,
        profile: Optional[str] = None,
        layers: Optional[Sequence[str]] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        activate: bool = True,
    ) -> ArtifactGeneration:
        chart_id = _identifier(chart_id, "chart identifier")
        generation_id = _identifier(generation, "generation")
        if phase not in VALID_PHASES:
            raise InvalidArtifact("phase must be preview or refined")
        resolved, relative = self._safe_artifact_path(artifact_path)
        metadata = _inspect_mbtiles(resolved)
        available_layers = metadata.pop("layers")
        selected_layers = _normalize_layers(layers) if layers is not None else available_layers
        unknown = sorted(set(selected_layers) - set(available_layers))
        if unknown:
            raise InvalidArtifact(
                "selected layers are absent from MBTiles: %s" % ", ".join(unknown)
            )
        artifact = ArtifactGeneration(
            generation=generation_id,
            relative_path=relative,
            phase=phase,
            name=str(name or metadata["name"] or chart_id),
            description=str(
                metadata["description"] if description is None else description
            ),
            bounds=metadata["bounds"],
            minzoom=metadata["minzoom"],
            maxzoom=metadata["maxzoom"],
            format=metadata["format"],
            chart_type=metadata["chart_type"],
            layers=selected_layers,
            profile=_optional_string(profile),
            size=resolved.stat().st_size,
            sha256=_sha256_file(resolved),
            registered_at=_timestamp(self._clock()),
        )
        with self._lock:
            chart = self._charts.get(chart_id)
            if chart is not None and generation_id in chart.generations:
                existing = chart.generations[generation_id]
                if _immutable_identity(existing) == _immutable_identity(artifact):
                    return existing
                raise GenerationConflict(
                    "generation %s for chart %s is immutable" % (generation_id, chart_id)
                )
            if chart is None:
                chart = ChartRecord(
                    identifier=chart_id,
                    name=artifact.name,
                    description=artifact.description,
                    active_generation=generation_id,
                )
                self._charts[chart_id] = chart
            chart.generations[generation_id] = artifact
            if activate:
                chart.active_generation = generation_id
            self._save_locked()
            return artifact

    def chart(self, chart_id: str) -> Optional[ChartRecord]:
        with self._lock:
            chart = self._charts.get(_identifier(chart_id, "chart identifier"))
            return copy.deepcopy(chart) if chart is not None else None

    def charts(self) -> List[ChartRecord]:
        with self._lock:
            return [copy.deepcopy(self._charts[key]) for key in sorted(self._charts)]

    def delete_chart(self, chart_id: str) -> Dict[str, int]:
        chart_id = _identifier(chart_id, "chart identifier")
        with self._lock:
            chart = self._charts.pop(chart_id, None)
            if chart is None:
                return {"generations": 0, "bytes": 0}
            self._save_locked()
        removed_bytes = 0
        for artifact in chart.generations.values():
            candidate = (self.artifact_root / artifact.relative_path).resolve()
            _require_within(candidate, self.artifact_root)
            try:
                removed_bytes += candidate.stat().st_size
                candidate.unlink()
            except FileNotFoundError:
                pass
        chart_directory = self.artifact_root / chart_id
        try:
            chart_directory.rmdir()
        except (FileNotFoundError, OSError):
            pass
        return {"generations": len(chart.generations), "bytes": removed_bytes}

    def descriptor(
        self,
        chart_id: str,
        *,
        api_version: int,
        generation: Optional[object] = None,
        tile_base: str = "/signalk/v1/api/resources/charts",
    ) -> Dict[str, Any]:
        chart_id = _identifier(chart_id, "chart identifier")
        with self._lock:
            chart = self._charts.get(chart_id)
            if chart is None:
                raise ArtifactNotFound("unknown chart: %s" % chart_id)
            generation_id = (
                chart.active_generation
                if generation is None
                else _identifier(generation, "generation")
            )
            try:
                artifact = chart.generations[generation_id]
            except KeyError as error:
                raise ArtifactNotFound("unknown chart generation") from error
            tile_url = "%s/%s/generations/%s/{z}/{x}/{y}" % (
                tile_base.rstrip("/"),
                chart_id,
                generation_id,
            )
            result: Dict[str, Any] = {
                "identifier": chart.identifier,
                "name": chart.name,
                "description": chart.description,
                "bounds": list(artifact.bounds),
                "minzoom": artifact.minzoom,
                "maxzoom": artifact.maxzoom,
                "format": artifact.format,
                "type": "S-57",
                "scale": 250000,
                "generation": artifact.generation,
                "phase": artifact.phase,
            }
            if artifact.profile is not None:
                result["profile"] = artifact.profile
            if api_version == 1:
                result["tilemapUrl"] = tile_url
                result["chartLayers"] = list(artifact.layers)
            elif api_version == 2:
                result["url"] = tile_url
                result["layers"] = list(artifact.layers)
            else:
                raise ValueError("api_version must be 1 or 2")
            return result

    def resolve_generation(self, chart_id: str, generation: object) -> Path:
        chart_id = _identifier(chart_id, "chart identifier")
        generation_id = _identifier(generation, "generation")
        with self._lock:
            try:
                artifact = self._charts[chart_id].generations[generation_id]
            except KeyError as error:
                raise ArtifactNotFound("unknown chart generation") from error
            candidate = (self.artifact_root / artifact.relative_path).resolve(strict=True)
            _require_within(candidate, self.artifact_root)
            if not candidate.is_file() or candidate.suffix.lower() != ".mbtiles":
                raise ArtifactNotFound("generation artifact is unavailable")
            if candidate.stat().st_size != artifact.size:
                raise InvalidArtifact("generation artifact changed after registration")
            return candidate

    def read_tile(
        self,
        chart_id: str,
        generation: object,
        z: int,
        x: int,
        y: int,
    ) -> Optional[TileResponse]:
        path = self.resolve_generation(chart_id, generation)
        return read_xyz_tile(path, z, x, y, tile_format="pbf")

    def _safe_artifact_path(self, value: Path) -> Tuple[Path, str]:
        try:
            resolved = Path(value).resolve(strict=True)
        except FileNotFoundError as error:
            raise InvalidArtifact("MBTiles artifact does not exist") from error
        _require_within(resolved, self.artifact_root)
        if not resolved.is_file() or resolved.suffix.lower() != ".mbtiles":
            raise InvalidArtifact("artifact must be an MBTiles file")
        return resolved, resolved.relative_to(self.artifact_root).as_posix()

    def _load(self) -> None:
        with self._lock:
            if not self.state_path.exists():
                return
            try:
                raw = _mapping(json.loads(self.state_path.read_text(encoding="utf-8")))
                if raw.get("version") != STATE_VERSION:
                    raise InvalidArtifact("unsupported artifact registry version")
                raw_charts = _mapping(raw.get("charts", {}))
                charts = {
                    _identifier(chart_id, "chart identifier"): ChartRecord.from_dict(
                        _mapping(raw_chart)
                    )
                    for chart_id, raw_chart in raw_charts.items()
                }
                if any(key != chart.identifier for key, chart in charts.items()):
                    raise InvalidArtifact("chart registry keys do not match identifiers")
                self._charts = charts
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise InvalidArtifact("invalid artifact registry state") from error

    def _save_locked(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": STATE_VERSION,
            "charts": {
                chart_id: chart.to_dict()
                for chart_id, chart in sorted(self._charts.items())
            },
        }
        data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
        temporary = self.state_path.with_name(
            ".%s.%s.tmp" % (self.state_path.name, uuid.uuid4().hex)
        )
        try:
            with temporary.open("wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(str(temporary), str(self.state_path))
            try:
                directory = os.open(str(self.state_path.parent), os.O_RDONLY)
            except OSError:
                return
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def read_xyz_tile(
    path: Path,
    z: int,
    x: int,
    y: int,
    *,
    tile_format: str = "pbf",
) -> Optional[TileResponse]:
    """Read an XYZ coordinate from MBTiles, whose tile rows use TMS."""

    z, x, y = int(z), int(x), int(y)
    if z < 0 or z > 30:
        raise ValueError("zoom is outside the supported range")
    width = 1 << z
    if x < 0 or x >= width or y < 0 or y >= width:
        raise ValueError("tile coordinate is outside the zoom grid")
    tms_y = width - 1 - y
    connection = _open_readonly(path)
    try:
        row = connection.execute(
            "SELECT tile_data FROM tiles "
            "WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?",
            (z, x, tms_y),
        ).fetchone()
    except sqlite3.DatabaseError as error:
        raise InvalidArtifact("could not read MBTiles tile") from error
    finally:
        connection.close()
    if row is None:
        return None
    data = bytes(row[0])
    content_types = {
        "pbf": "application/vnd.mapbox-vector-tile",
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "webp": "image/webp",
    }
    headers = {
        "Content-Type": content_types.get(tile_format.lower(), "application/octet-stream"),
        "Content-Length": str(len(data)),
        "Cache-Control": "public, max-age=31536000, immutable",
        "ETag": '"%s"' % hashlib.sha256(data).hexdigest(),
    }
    if data.startswith(b"\x1f\x8b"):
        headers["Content-Encoding"] = "gzip"
    return TileResponse(data=data, headers=headers)


def _inspect_mbtiles(path: Path) -> Dict[str, Any]:
    connection = _open_readonly(path)
    try:
        quick_check = connection.execute("PRAGMA quick_check").fetchone()
        if quick_check is None or str(quick_check[0]).lower() != "ok":
            raise InvalidArtifact("MBTiles quick_check failed")
        rows = connection.execute("SELECT name, value FROM metadata").fetchall()
        metadata = {str(name): str(value) for name, value in rows}
        zooms = connection.execute(
            "SELECT MIN(zoom_level), MAX(zoom_level), COUNT(*) FROM tiles"
        ).fetchone()
    except sqlite3.DatabaseError as error:
        raise InvalidArtifact("invalid MBTiles schema") from error
    finally:
        connection.close()
    if not zooms or int(zooms[2]) == 0:
        raise InvalidArtifact("MBTiles contains no tiles")
    bounds = _parse_bounds(metadata.get("bounds"))
    minzoom = _parse_zoom(metadata.get("minzoom"), "minzoom")
    maxzoom = _parse_zoom(metadata.get("maxzoom"), "maxzoom")
    if minzoom > maxzoom:
        raise InvalidArtifact("minzoom exceeds maxzoom")
    if int(zooms[0]) < minzoom or int(zooms[1]) > maxzoom:
        raise InvalidArtifact("tile zooms fall outside metadata zoom range")
    tile_format = str(metadata.get("format", "")).lower()
    if tile_format != "pbf":
        raise InvalidArtifact("S-57 MBTiles format must be pbf")
    chart_type = str(metadata.get("type", ""))
    if chart_type.lower() != "s-57":
        raise InvalidArtifact("MBTiles type must be S-57")
    layers = _layers_from_metadata(metadata)
    if not layers:
        raise InvalidArtifact("MBTiles json metadata has no vector layers")
    return {
        "name": metadata.get("name", ""),
        "description": metadata.get("description", ""),
        "bounds": bounds,
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "format": tile_format,
        "chart_type": "S-57",
        "layers": layers,
    }


def _layers_from_metadata(metadata: Mapping[str, str]) -> Tuple[str, ...]:
    try:
        document = json.loads(metadata.get("json", "{}"))
    except json.JSONDecodeError as error:
        raise InvalidArtifact("MBTiles json metadata is malformed") from error
    if not isinstance(document, Mapping):
        raise InvalidArtifact("MBTiles json metadata must be an object")
    vector_layers = document.get("vector_layers", [])
    if not isinstance(vector_layers, list):
        raise InvalidArtifact("vector_layers metadata must be a list")
    values = []
    for layer in vector_layers:
        if not isinstance(layer, Mapping) or not isinstance(layer.get("id"), str):
            raise InvalidArtifact("each vector layer must have an id")
        values.append(layer["id"])
    return _normalize_layers(values)


def _open_readonly(path: Path) -> sqlite3.Connection:
    try:
        resolved = Path(path).resolve(strict=True)
    except FileNotFoundError as error:
        raise ArtifactNotFound("MBTiles artifact is unavailable") from error
    uri = "file:%s?mode=ro" % quote(resolved.as_posix(), safe="/")
    try:
        return sqlite3.connect(uri, uri=True)
    except sqlite3.DatabaseError as error:
        raise InvalidArtifact("could not open MBTiles artifact") from error


def _parse_bounds(value: object) -> Tuple[float, float, float, float]:
    raw = value.split(",") if isinstance(value, str) else value
    if not isinstance(raw, (list, tuple)) or len(raw) != 4:
        raise InvalidArtifact("bounds must contain four coordinates")
    try:
        bounds = tuple(float(item) for item in raw)
    except (TypeError, ValueError) as error:
        raise InvalidArtifact("bounds must be numeric") from error
    if not all(math.isfinite(item) for item in bounds):
        raise InvalidArtifact("bounds must be finite")
    west, south, east, north = bounds
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise InvalidArtifact("bounds are outside geographic limits")
    return bounds


def _parse_zoom(value: object, field_name: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise InvalidArtifact("%s must be an integer" % field_name) from error
    if str(value).strip() != str(result) or result < 0 or result > 30:
        raise InvalidArtifact("%s is outside the supported range" % field_name)
    return result


def _normalize_layers(value: Sequence[object]) -> Tuple[str, ...]:
    layers: List[str] = []
    seen = set()
    for item in value:
        layer = str(item).strip()
        if not layer:
            raise InvalidArtifact("layer names cannot be empty")
        if layer not in seen:
            seen.add(layer)
            layers.append(layer)
    return tuple(layers)


def _validate_generation(artifact: ArtifactGeneration) -> None:
    if artifact.phase not in VALID_PHASES:
        raise InvalidArtifact("invalid generation phase")
    if Path(artifact.relative_path).is_absolute() or ".." in Path(artifact.relative_path).parts:
        raise InvalidArtifact("artifact path must be relative and contained")
    if artifact.minzoom > artifact.maxzoom:
        raise InvalidArtifact("minzoom exceeds maxzoom")
    if artifact.format != "pbf" or artifact.chart_type != "S-57":
        raise InvalidArtifact("generation is not S-57 vector MBTiles")
    if artifact.size < 0 or not re.fullmatch(r"[0-9a-f]{64}", artifact.sha256):
        raise InvalidArtifact("generation fingerprint is invalid")


def _immutable_identity(artifact: ArtifactGeneration) -> Tuple[Any, ...]:
    return (
        artifact.relative_path,
        artifact.phase,
        artifact.bounds,
        artifact.minzoom,
        artifact.maxzoom,
        artifact.format,
        artifact.chart_type,
        artifact.layers,
        artifact.profile,
        artifact.size,
        artifact.sha256,
    )


def _require_within(path: Path, root: Path) -> None:
    try:
        path.relative_to(root)
    except ValueError as error:
        raise InvalidArtifact("artifact path escapes the configured root") from error


def _identifier(value: object, label: str) -> str:
    result = str(value) if value is not None else ""
    if not SAFE_IDENTIFIER.fullmatch(result):
        raise InvalidArtifact("%s is not URL safe" % label)
    return result


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected a JSON object")
    return value


def _optional_string(value: object) -> Optional[str]:
    return None if value is None else str(value)


def _timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
