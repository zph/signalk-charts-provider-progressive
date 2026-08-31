#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "fastapi==0.116.1",
#   "httpx==0.28.1",
#   "uvicorn[standard]==0.35.0",
# ]
# ///
"""Build and progressively serve NOAA ENC and linked chart files.

Run with: uv run chart_baker.py
The local worker is complete on its own. Optional remote workers may be added
later without changing the persisted queue or artifact formats.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import re
import signal
import shlex
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Literal
from urllib.parse import unquote, urlparse

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field, field_validator

from progressive_queue import PriorityClass, ProgressiveJob, ProgressiveQueue
from progressive_provider import ArtifactNotFound, ArtifactRegistry, InvalidArtifact, TileResponse

APP_VERSION = "0.3.12"
NOAA_CATALOG_URL = "https://www.charts.noaa.gov/InteractiveCatalog/data/enc.geojson"
NOAA_ENC_BASE_URL = "https://charts.noaa.gov/ENCs"
TOOLBOX_IMAGE = "ghcr.io/dirkwa/signalk-charts-provider-simple/charts-toolbox:1.1.0"
PMTILES_IMAGE = "ghcr.io/protomaps/go-pmtiles:v1.31.2"
MAX_CATALOG_BYTES = 256 * 1024 * 1024
MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024 * 1024
MAX_ARCHIVE_BYTES = 80 * 1024 * 1024 * 1024
MAX_ARCHIVE_FILES = 100_000
DEFAULT_DATA_DIR = Path.home() / ".local" / "share" / "signalk-chart-baker"

BAND_ZOOMS: dict[int, tuple[int, int]] = {
    1: (4, 8),
    2: (6, 10),
    3: (8, 12),
    4: (10, 14),
    5: (12, 16),
    6: (14, 18),
    7: (9, 14),
    8: (13, 16),
    9: (15, 18),
}


def enc_band_zoom_ranges(
    band_keys: Iterable[str], requested_min: int, requested_max: int
) -> dict[str, tuple[int, int]]:
    """Map available ENC bands onto every requested zoom without leaving edge gaps."""
    available: list[tuple[str, int, int, int]] = []
    for key in sorted(set(band_keys)):
        band = int(key[1:]) if key[1:].isdigit() else 0
        native_min, native_max = BAND_ZOOMS.get(
            band, (requested_min, requested_max)
        )
        available.append((key, band, native_min, native_max))
    if not available:
        return {}

    planned: dict[str, tuple[int, int]] = {}
    for key, _band, native_min, native_max in available:
        min_zoom = max(requested_min, native_min)
        max_zoom = min(requested_max, native_max)
        if min_zoom <= max_zoom:
            planned[key] = (min_zoom, max_zoom)

    lowest = min(available, key=lambda item: (item[2], item[1], item[0]))
    if requested_min < lowest[2]:
        existing = planned.get(lowest[0])
        planned[lowest[0]] = (
            requested_min,
            existing[1] if existing is not None else requested_max,
        )

    highest = max(available, key=lambda item: (item[3], item[1], item[0]))
    if requested_max > highest[3]:
        existing = planned.get(highest[0])
        planned[highest[0]] = (
            existing[0] if existing is not None else requested_min,
            requested_max,
        )
    return planned

SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")
SAFE_CHART_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
SAFE_SSH_ALIAS = re.compile(r"^[A-Za-z0-9_.@-]+$")
SUPPORTED_VECTOR = {".000", ".geojson", ".geojsonseq", ".geojsonl", ".json"}
SUPPORTED_RASTER = {".kap", ".bsb", ".tif", ".tiff", ".vrt"}
SUPPORTED_ARCHIVES = {".zip"}
SUPPORTED_READY = {".mbtiles", ".pmtiles"}
METERS_PER_FOOT = 0.3048
DEPTH_ROUNDING_THRESHOLD_METERS = 40 * METERS_PER_FOOT
S57_DEPTH_FIELDS = ("DEPTH", "DRVAL1", "DRVAL2", "VALDCO", "VALSOU")

# The initial profile is intentionally useful for navigation instead of being
# a coastline-only placeholder. This is also the extension point for a future
# layer-selection UI: artifact keys include both the profile and resolved list.
COMPATIBLE_S57_LAYERS = (
    "M_COVR",
    "DEPARE",
    "DRGARE",
    "ACHARE",
    "RESARE",
    "MIPARE",
    "CTNARE",
    "FAIRWY",
    "CANALS",
    "TSSLPT",
    "TSSBND",
    "TSEZNE",
    "UNSARE",
    "LNDARE",
    "COALNE",
    "SLCONS",
    "NAVLNE",
    "CBLSUB",
    "PIPSOL",
    "BRIDGE",
    "DEPCNT",
    "SOUNDG",
    "WRECKS",
    "UWTROC",
    "OBSTRN",
    "FOULGND",
    "BOYLAT",
    "BCNLAT",
    "BOYCAR",
    "BOYISD",
    "BOYSAW",
    "BOYSPP",
    "BCNCAR",
    "BCNISD",
    "BCNSAW",
    "BCNSPP",
    "DAYMAR",
    "LNDMRK",
    "LIGHTS",
)

BASIC_S57_LAYERS = (
    "M_COVR",
    "DEPARE",
    "LNDARE",
    "COALNE",
    "DEPCNT",
    "SOUNDG",
    "WRECKS",
    "UWTROC",
    "OBSTRN",
    "BOYLAT",
    "BCNLAT",
    "LIGHTS",
)

NOAA_STATE_REGION_NAMES = {
    "AK": "Alaska",
    "AL": "Alabama",
    "AS": "American Samoa",
    "CA": "California",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "FM": "Federated States of Micronesia",
    "GA": "Georgia",
    "GU": "Guam",
    "HI": "Hawaii",
    "IL": "Illinois",
    "IN": "Indiana",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MH": "Marshall Islands",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MP": "Northern Mariana Islands",
    "MS": "Mississippi",
    "NC": "North Carolina",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NY": "New York",
    "OH": "Ohio",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "PR": "Puerto Rico",
    "PW": "Palau",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "TX": "Texas",
    "VA": "Virginia",
    "VI": "U.S. Virgin Islands",
    "WA": "Washington",
    "WI": "Wisconsin",
}

NOAA_MAJOR_REGIONS = (
    (
        "west-coast",
        "West Coast",
        ("CA", "OR", "WA"),
        "California, Oregon, and Washington NOAA state packages",
    ),
    (
        "pacific-northwest",
        "Pacific Northwest",
        ("OR", "WA"),
        "Oregon and Washington NOAA state packages",
    ),
    (
        "gulf-coast",
        "Gulf Coast",
        ("TX", "LA", "MS", "AL", "FL"),
        "Texas through Florida NOAA state packages",
    ),
    (
        "east-coast",
        "East Coast",
        (
            "FL",
            "GA",
            "SC",
            "NC",
            "VA",
            "MD",
            "DE",
            "NJ",
            "NY",
            "CT",
            "RI",
            "MA",
            "NH",
            "ME",
        ),
        "Florida through Maine NOAA state packages",
    ),
    (
        "great-lakes",
        "Great Lakes",
        ("MN", "WI", "IL", "IN", "MI", "OH", "PA", "NY"),
        "NOAA state packages bordering the Great Lakes",
    ),
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def safe_stem(value: str, fallback: str = "chart-set") -> str:
    cleaned = SAFE_NAME.sub("-", value.strip()).strip("-._")
    return (cleaned or fallback)[:100]


def detect_runtime(preferred: str = "auto") -> str | None:
    choices = [preferred] if preferred != "auto" else ["docker", "podman"]
    return next((choice for choice in choices if shutil.which(choice)), None)


def container_subprocess_environment(source: dict[str, str] | None = None) -> dict[str, str]:
    environment = dict(os.environ if source is None else source)
    for name in ("LISTEN_FDS", "LISTEN_FDNAMES", "LISTEN_PID"):
        environment.pop(name, None)
    return environment


def mercator_coord(x: float, y: float) -> tuple[float, float]:
    if abs(x) <= 360 and abs(y) <= 90:
        return x, y
    radius = 6_378_137
    lon = x / radius * 180 / math.pi
    lat = (2 * math.atan(math.exp(y / radius)) - math.pi / 2) * 180 / math.pi
    return lon, lat


def geometry_bbox(coordinates: Any) -> list[float] | None:
    bounds = [math.inf, math.inf, -math.inf, -math.inf]

    def walk(node: Any) -> None:
        if not isinstance(node, list):
            return
        if len(node) >= 2 and isinstance(node[0], (int, float)) and isinstance(node[1], (int, float)):
            lon, lat = mercator_coord(float(node[0]), float(node[1]))
            bounds[0] = min(bounds[0], lon)
            bounds[1] = min(bounds[1], lat)
            bounds[2] = max(bounds[2], lon)
            bounds[3] = max(bounds[3], lat)
            return
        for child in node:
            walk(child)

    walk(coordinates)
    return bounds if math.isfinite(bounds[0]) else None


def overlaps(a: list[float], b: list[float]) -> bool:
    return a[0] <= b[2] and a[2] >= b[0] and a[1] <= b[3] and a[3] >= b[1]


def bounds_for(items: Iterable["Footprint"]) -> list[float]:
    bounds = [math.inf, math.inf, -math.inf, -math.inf]
    for item in items:
        bounds[0] = min(bounds[0], item.bbox[0])
        bounds[1] = min(bounds[1], item.bbox[1])
        bounds[2] = max(bounds[2], item.bbox[2])
        bounds[3] = max(bounds[3], item.bbox[3])
    if not math.isfinite(bounds[0]):
        raise ValueError("Cannot calculate bounds for an empty chart selection")
    return bounds


def expand_bounds(bounds: list[float], rings: float = 1.0) -> list[float]:
    width = max(0.01, bounds[2] - bounds[0])
    height = max(0.01, bounds[3] - bounds[1])
    return [
        max(-180.0, bounds[0] - width * rings),
        max(-85.0, bounds[1] - height * rings),
        min(180.0, bounds[2] + width * rings),
        min(85.0, bounds[3] + height * rings),
    ]


def iter_files(root: Path, suffixes: set[str]) -> Iterable[Path]:
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in suffixes and not path.name.startswith("._"):
            yield path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def conservative_depth_meters(value: Any) -> Any:
    """Floor depths above 40 ft to a whole foot while retaining meters."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value
    depth = float(value)
    if not math.isfinite(depth) or depth <= DEPTH_ROUNDING_THRESHOLD_METERS:
        return value
    return round(math.floor(depth / METERS_PER_FOOT) * METERS_PER_FOOT, 4)


def round_s57_depths(path: Path) -> int:
    """Conservatively round depth properties in a GeoJSON sequence stream."""
    output = path.with_suffix(path.suffix + ".rounding")
    changed = 0
    try:
        with path.open("r", encoding="utf-8") as source, output.open(
            "w", encoding="utf-8"
        ) as target:
            for line in source:
                if not line.strip():
                    continue
                record_separator = "\x1e" if line.startswith("\x1e") else ""
                feature = json.loads(line.removeprefix("\x1e"))
                properties = feature.get("properties")
                if isinstance(properties, dict):
                    for field_name in S57_DEPTH_FIELDS:
                        if field_name not in properties:
                            continue
                        rounded = conservative_depth_meters(properties[field_name])
                        if rounded != properties[field_name]:
                            properties[field_name] = rounded
                            changed += 1
                target.write(record_separator)
                target.write(json.dumps(feature, separators=(",", ":")))
                target.write("\n")
        output.replace(path)
    except Exception:
        output.unlink(missing_ok=True)
        raise
    return changed


@dataclass(slots=True)
class Footprint:
    chart_id: str
    version: str
    band: int
    scale: int | None
    title: str
    bbox: list[float]


@dataclass(slots=True)
class CatalogRegion:
    id: str
    name: str
    kind: str
    description: str
    chart_ids: list[str]
    bounds: list[float]


def catalog_regions(entries: Iterable[Footprint]) -> list[CatalogRegion]:
    """Build searchable regions from NOAA's band-4 state package codes."""
    by_state: dict[str, list[Footprint]] = {}
    for entry in entries:
        if entry.band != 4 or not re.fullmatch(r"US4[A-Z0-9]{5}", entry.chart_id):
            continue
        state_code = entry.chart_id[3:5]
        if state_code in NOAA_STATE_REGION_NAMES:
            by_state.setdefault(state_code, []).append(entry)

    regions: list[CatalogRegion] = []
    for region_id, name, state_codes, description in NOAA_MAJOR_REGIONS:
        members = sorted(
            (item for code in state_codes for item in by_state.get(code, [])),
            key=lambda item: item.chart_id,
        )
        if members:
            regions.append(
                CatalogRegion(
                    id=region_id,
                    name=name,
                    kind="Regional aggregate",
                    description=description,
                    chart_ids=[item.chart_id for item in members],
                    bounds=bounds_for(members),
                )
            )

    for state_code, members in sorted(
        by_state.items(), key=lambda item: NOAA_STATE_REGION_NAMES[item[0]]
    ):
        name = NOAA_STATE_REGION_NAMES[state_code]
        ordered = sorted(members, key=lambda item: item.chart_id)
        regions.append(
            CatalogRegion(
                id=f"state-{state_code.lower()}",
                name=name,
                kind="NOAA state package",
                description=f"NOAA ENCs by State coverage for {name}",
                chart_ids=[item.chart_id for item in ordered],
                bounds=bounds_for(ordered),
            )
        )
    return regions


class Catalog:
    def __init__(self, cache_path: Path) -> None:
        self.cache_path = cache_path
        self._lock = threading.Lock()
        self._footprints: list[Footprint] | None = None

    def load(self, refresh: bool = False) -> list[Footprint]:
        with self._lock:
            if self._footprints is not None and not refresh:
                return self._footprints
            stale = not self.cache_path.exists() or time.time() - self.cache_path.stat().st_mtime > 86_400
            if refresh or stale:
                try:
                    self._download()
                except Exception:
                    if not self.cache_path.exists():
                        raise
            raw = json.loads(self.cache_path.read_text(encoding="utf-8"))
            footprints: list[Footprint] = []
            for feature in raw.get("features", []):
                props = feature.get("properties") or {}
                version = props.get("enc_ed_up")
                try:
                    band = int(props.get("scale_band"))
                except (TypeError, ValueError):
                    continue
                if not isinstance(version, str) or len(version) < 8 or band not in range(1, 7):
                    continue
                bbox = geometry_bbox((feature.get("geometry") or {}).get("coordinates"))
                if bbox is None:
                    continue
                scale = props.get("scale")
                footprints.append(
                    Footprint(
                        chart_id=version[:8],
                        version=version,
                        band=band,
                        scale=int(scale) if isinstance(scale, (int, float)) else None,
                        title=str(props.get("title") or version[:8]),
                        bbox=bbox,
                    )
                )
            self._footprints = footprints
            return footprints

    def _download(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.cache_path.with_suffix(".download")
        total = 0
        with httpx.stream("GET", NOAA_CATALOG_URL, follow_redirects=True, timeout=90) as response:
            response.raise_for_status()
            with temp.open("wb") as target:
                for chunk in response.iter_bytes(1024 * 1024):
                    total += len(chunk)
                    if total > MAX_CATALOG_BYTES:
                        raise RuntimeError("NOAA catalog exceeded the 256 MiB safety limit")
                    target.write(chunk)
        temp.replace(self.cache_path)

    def band4(self) -> list[Footprint]:
        return sorted((item for item in self.load() if item.band == 4), key=lambda item: item.chart_id)

    def inclusion(self, selected: list[str]) -> list[Footprint]:
        all_items = self.load()
        by_id = {item.chart_id: item for item in all_items}
        selected_ids = {item for item in selected if item in by_id and by_id[item].band == 4}
        boxes = [by_id[item].bbox for item in selected_ids]
        if not boxes:
            raise ValueError("No current NOAA band-4 cells were selected")
        included: dict[str, Footprint] = {}
        for item in all_items:
            if item.chart_id in selected_ids or (
                item.band in {3, 5} and any(overlaps(item.bbox, box) for box in boxes)
            ):
                included[item.chart_id] = item
        return sorted(included.values(), key=lambda item: item.chart_id)


@dataclass
class Job:
    id: str
    kind: str
    name: str
    status: str = "queued"
    phase: str = "Waiting"
    progress: float = 0.0
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    inputs: int = 0
    outputs: list[str] = field(default_factory=list)
    error: str | None = None
    log: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    cancel: threading.Event = field(default_factory=threading.Event, repr=False)
    processes: list[subprocess.Popen[str]] = field(default_factory=list, repr=False)
    process_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def public(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "name": self.name,
            "status": self.status,
            "phase": self.phase,
            "progress": self.progress,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "inputs": self.inputs,
            "outputs": list(self.outputs),
            "error": self.error,
            "log": self.log[-400:],
            "config": dict(self.config),
        }


class NoaaJobRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    chart_ids: list[str] = Field(min_length=1, max_length=2_000)
    min_zoom: int = Field(default=4, ge=0, le=18)
    max_zoom: int = Field(default=16, ge=0, le=18)
    parallelism: int = Field(default=max(1, (os.cpu_count() or 4) - 1), ge=1, le=32)
    download_workers: int = Field(default=6, ge=1, le=16)

    @field_validator("chart_ids")
    @classmethod
    def valid_ids(cls, value: list[str]) -> list[str]:
        normalized = sorted(set(item.upper() for item in value))
        if any(not re.fullmatch(r"[A-Z0-9]{8}", item) for item in normalized):
            raise ValueError("NOAA chart ids must contain eight letters or digits")
        return normalized


class ProgressiveNoaaRequest(NoaaJobRequest):
    viewport_bbox: list[float] = Field(min_length=4, max_length=4)
    current_zoom: int = Field(default=12, ge=0, le=18)
    profile: Literal["compatible", "full"] = "compatible"
    layers: list[str] | None = None

    @field_validator("viewport_bbox")
    @classmethod
    def valid_viewport_bbox(cls, value: list[float]) -> list[float]:
        result = [float(item) for item in value]
        if not all(math.isfinite(item) for item in result):
            raise ValueError("viewport_bbox must contain finite coordinates")
        if result[0] >= result[2] or result[1] >= result[3]:
            raise ValueError("viewport_bbox must be west,south,east,north")
        if result[0] < -180 or result[2] > 180 or result[1] < -85 or result[3] > 85:
            raise ValueError("viewport_bbox is outside the supported world bounds")
        return result

    @field_validator("layers")
    @classmethod
    def valid_layers(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        normalized = sorted(set(item.strip().upper() for item in value if item.strip()))
        if not normalized or any(not re.fullmatch(r"[A-Z][A-Z0-9_]{1,15}", item) for item in normalized):
            raise ValueError("layers must contain S-57 object-class identifiers")
        return normalized


class ProgressiveDeleteRequest(BaseModel):
    confirm_chart_id: str = Field(min_length=1, max_length=128)
    purge_sources: bool = False


class UrlJobRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    urls: list[str] = Field(min_length=1, max_length=50)
    min_zoom: int = Field(default=4, ge=0, le=18)
    max_zoom: int = Field(default=16, ge=0, le=18)
    parallelism: int = Field(default=max(1, (os.cpu_count() or 4) - 1), ge=1, le=32)
    download_workers: int = Field(default=4, ge=1, le=12)

    @field_validator("urls")
    @classmethod
    def valid_urls(cls, value: list[str]) -> list[str]:
        result: list[str] = []
        for raw in value:
            parsed = urlparse(raw.strip())
            if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username:
                raise ValueError("Each link must be an HTTP(S) URL without embedded credentials")
            result.append(raw.strip())
        return result


class SideloadRequest(BaseModel):
    job_id: str
    target: str = "boat-pi"
    destination: Literal["auto", "freeboard", "chart-locker", "both"] = "auto"
    mbtiles_dir: str = "/home/signalk/.signalk/charts-simple"
    pmtiles_dir: str = "/home/signalk/.signalk/charts/pmtiles"
    remote_owner: str | None = "signalk:signalk"
    restart_signalk: bool = True

    @field_validator("target")
    @classmethod
    def valid_target(cls, value: str) -> str:
        if not SAFE_SSH_ALIAS.fullmatch(value):
            raise ValueError("SSH target contains unsupported characters")
        return value

    @field_validator("mbtiles_dir", "pmtiles_dir")
    @classmethod
    def valid_remote_dir(cls, value: str) -> str:
        if (
            not re.fullmatch(r"[A-Za-z0-9_./~+-]+", value)
            or ".." in Path(value).parts
            or not (value.startswith("/") or value.startswith("~/"))
        ):
            raise ValueError("Remote directory must be absolute or start with ~/")
        return value.rstrip("/")

    @field_validator("remote_owner")
    @classmethod
    def valid_remote_owner(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(r"[A-Za-z0-9_-]+:[A-Za-z0-9_-]+", value):
            raise ValueError("Remote owner must have user:group form")
        return value


class JobManager:
    def __init__(self, data_dir: Path, runtime: str | None, catalog: Catalog) -> None:
        self.data_dir = data_dir
        self.runtime = runtime
        self.catalog = catalog
        self.jobs: dict[str, Job] = {}
        self.lock = threading.Lock()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="chart-job")
        (data_dir / "jobs").mkdir(parents=True, exist_ok=True)

    def create(self, kind: str, request: NoaaJobRequest | UrlJobRequest) -> Job:
        job = Job(id=uuid.uuid4().hex[:12], kind=kind, name=request.name, config=request.model_dump())
        with self.lock:
            self.jobs[job.id] = job
        self.executor.submit(self._run, job)
        return job

    def get(self, job_id: str) -> Job:
        with self.lock:
            job = self.jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        return job

    def append(self, job: Job, message: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        with self.lock:
            job.log.extend(f"[{stamp}] {line}" for line in message.rstrip().splitlines() if line.strip())
            del job.log[:-1_000]
            job.updated_at = utc_now()

    def update(self, job: Job, phase: str, progress: float | None = None) -> None:
        with self.lock:
            job.phase = phase
            if progress is not None:
                job.progress = max(0.0, min(1.0, progress))
            job.updated_at = utc_now()

    def cancel_job(self, job_id: str) -> None:
        job = self.get(job_id)
        job.cancel.set()
        self.append(job, "Cancellation requested")
        with job.process_lock:
            processes = list(job.processes)
        for process in processes:
            if process.poll() is None:
                self._terminate_process(process)

    @staticmethod
    def _terminate_process(process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
            process.wait(timeout=5)
        except ProcessLookupError:
            return
        except subprocess.TimeoutExpired:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                return

    def _run(self, job: Job) -> None:
        job.status = "running"
        job_dir = self.data_dir / "jobs" / job.id
        try:
            job_dir.mkdir(parents=True, exist_ok=False)
            if self.runtime is None:
                raise RuntimeError("Docker or Podman is required for chart conversion")
            if job.kind == "noaa":
                self._run_noaa(job, job_dir)
            else:
                self._run_urls(job, job_dir)
            if job.cancel.is_set():
                job.status = "cancelled"
                job.phase = "Cancelled"
            else:
                job.status = "completed"
                job.phase = "Ready to sideload"
                job.progress = 1.0
                self.append(job, f"Completed with {len(job.outputs)} chart file(s)")
        except Exception as error:
            job.status = "cancelled" if job.cancel.is_set() else "failed"
            job.phase = "Cancelled" if job.cancel.is_set() else "Failed"
            job.error = str(error)
            self.append(job, f"ERROR: {error}")
        finally:
            job.updated_at = utc_now()

    def _run_noaa(self, job: Job, job_dir: Path) -> None:
        self.update(job, "Resolving NOAA coverage", 0.02)
        included = self.catalog.inclusion(job.config["chart_ids"])
        job.inputs = len(included)
        self.append(job, f"Selected {len(job.config['chart_ids'])} approach cells; resolved {len(included)} band 3/4/5 ENC cells")
        enc_root = job_dir / "input"
        download_dir = job_dir / "downloads"
        enc_root.mkdir()
        download_dir.mkdir()

        def fetch(item: Footprint) -> tuple[Footprint, Path]:
            if job.cancel.is_set():
                raise RuntimeError("cancelled")
            target = download_dir / f"{item.chart_id}.zip"
            self._download(
                f"{NOAA_ENC_BASE_URL}/{item.chart_id}.zip", target, job.cancel
            )
            return item, target

        workers = min(job.config["download_workers"], len(included))
        downloaded: list[tuple[Footprint, Path]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {pool.submit(fetch, item): item for item in included}
            for index, future in enumerate(concurrent.futures.as_completed(futures), 1):
                item = futures[future]
                try:
                    downloaded.append(future.result())
                    self.append(job, f"Downloaded {item.chart_id} ({index}/{len(included)})")
                except Exception as error:
                    self.append(job, f"WARN: skipped {item.chart_id}: {error}")
                self.update(job, "Downloading NOAA ENC cells", 0.05 + 0.25 * index / len(included))
        if job.cancel.is_set():
            raise RuntimeError("cancelled")
        if not downloaded:
            raise RuntimeError("No NOAA ENC cells downloaded successfully")
        for index, (item, archive) in enumerate(downloaded, 1):
            self._safe_extract(archive, enc_root / item.chart_id, job.cancel)
            archive.unlink(missing_ok=True)
            self.update(job, "Extracting ENC archives", 0.30 + 0.05 * index / len(downloaded))
        outputs = self._build_enc(job, enc_root, job_dir, safe_stem(job.name))
        job.outputs.extend(output.name for output in outputs)

    def _run_urls(self, job: Job, job_dir: Path) -> None:
        links: list[str] = job.config["urls"]
        input_root = job_dir / "input"
        input_root.mkdir()

        def fetch(index_url: tuple[int, str]) -> Path:
            index, url = index_url
            path_name = unquote(Path(urlparse(url).path).name) or f"download-{index}"
            target = input_root / f"{index:02d}-{safe_stem(path_name, f'download-{index}')}"
            self._download(url, target, job.cancel)
            return target

        downloaded: list[Path] = []
        workers = min(job.config["download_workers"], len(links))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            for index, result in enumerate(pool.map(fetch, enumerate(links, 1)), 1):
                downloaded.append(result)
                self.update(job, "Downloading linked files", 0.05 + 0.20 * index / len(links))
                self.append(job, f"Downloaded {result.name}")
        expanded = job_dir / "expanded"
        expanded.mkdir()
        for item in downloaded:
            if item.suffix.lower() == ".zip":
                self._safe_extract(item, expanded / safe_stem(item.stem), job.cancel)
            else:
                target = expanded / item.name
                shutil.copy2(item, target)

        job.inputs = sum(1 for path in expanded.rglob("*") if path.is_file())
        encs = list(iter_files(expanded, {".000"}))
        ready_pmtiles = list(iter_files(expanded, {".pmtiles"}))
        mbtiles = list(iter_files(expanded, {".mbtiles"}))
        vectors = list(iter_files(expanded, SUPPORTED_VECTOR - {".000"}))
        rasters = list(iter_files(expanded, SUPPORTED_RASTER))
        outputs_dir = job_dir / "outputs"
        outputs_dir.mkdir(exist_ok=True)

        for source in ready_pmtiles:
            target = self._unique_output(outputs_dir, safe_stem(source.stem) + ".pmtiles")
            shutil.copy2(source, target)
            job.outputs.append(target.name)
        if encs:
            enc_outputs = self._build_enc(job, expanded, job_dir, safe_stem(job.name))
            job.outputs.extend(output.name for output in enc_outputs)
        for source in mbtiles:
            output = self._convert_mbtiles(job, source, outputs_dir / f"{safe_stem(source.stem)}.pmtiles")
            job.outputs.append(output.name)
        for source in vectors:
            output = self._build_geojson(job, source, job_dir, safe_stem(source.stem))
            job.outputs.append(output.name)
        for source in rasters:
            output = self._build_raster(job, source, job_dir, safe_stem(source.stem))
            job.outputs.append(output.name)
        if not job.outputs:
            raise RuntimeError("No supported chart files found. Use ENC ZIP, GeoJSON, MBTiles, PMTiles, KAP, or GeoTIFF links")

    def _download(
        self, url: str, target: Path, cancel: threading.Event | None = None
    ) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(target.suffix + ".part")
        total = 0
        try:
            with httpx.stream("GET", url, follow_redirects=True, timeout=httpx.Timeout(60, read=180)) as response:
                response.raise_for_status()
                with temp.open("wb") as handle:
                    for chunk in response.iter_bytes(1024 * 1024):
                        if cancel is not None and cancel.is_set():
                            raise RuntimeError("cancelled")
                        total += len(chunk)
                        if total > MAX_DOWNLOAD_BYTES:
                            raise RuntimeError(f"Download exceeded 20 GiB: {url}")
                        handle.write(chunk)
            temp.replace(target)
        except Exception:
            temp.unlink(missing_ok=True)
            raise

    def _safe_extract(
        self,
        archive: Path,
        destination: Path,
        cancel: threading.Event | None = None,
    ) -> None:
        destination.mkdir(parents=True, exist_ok=True)
        root = destination.resolve()
        with zipfile.ZipFile(archive) as bundle:
            entries = bundle.infolist()
            if len(entries) > MAX_ARCHIVE_FILES or sum(item.file_size for item in entries) > MAX_ARCHIVE_BYTES:
                raise RuntimeError(f"Archive exceeds extraction safety limits: {archive.name}")
            for item in entries:
                if cancel is not None and cancel.is_set():
                    raise RuntimeError("cancelled")
                target = (destination / item.filename).resolve()
                if target != root and root not in target.parents:
                    raise RuntimeError(f"Unsafe archive path in {archive.name}")
                if item.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with bundle.open(item) as source, target.open("wb") as output:
                        while chunk := source.read(1024 * 1024):
                            if cancel is not None and cancel.is_set():
                                raise RuntimeError("cancelled")
                            output.write(chunk)

    def _container_base(self, name: str, mounts: list[tuple[Path, str, bool]], env: dict[str, str] | None = None) -> list[str]:
        if self.runtime is None:
            raise RuntimeError("No container runtime available")
        mount_points = {container for _, container, _ in mounts}
        workdir = "/work" if "/work" in mount_points else "/data" if "/data" in mount_points else "/"
        command = [self.runtime, "run", "--rm"]
        if self.runtime == "podman":
            command += ["--replace", "--userns=keep-id"]
        command += ["--name", name, "--network", "none", "--workdir", workdir]
        if hasattr(os, "getuid"):
            command += ["--user", f"{os.getuid()}:{os.getgid()}"]
        for host, container, readonly in mounts:
            command += ["-v", f"{host.resolve()}:{container}{':ro' if readonly else ''}"]
        for key, value in (env or {}).items():
            command += ["-e", f"{key}={value}"]
        return command

    def _exec(self, job: Job, command: list[str], label: str) -> None:
        attempts = 3 if self.runtime == "podman" else 1
        for attempt in range(1, attempts + 1):
            self.append(job, f"Starting {label}" if attempt == 1 else f"Retrying {label} ({attempt}/{attempts})")
            recent: list[str] = []
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=container_subprocess_environment(),
                start_new_session=os.name == "posix",
            )
            with job.process_lock:
                job.processes.append(process)
            try:
                assert process.stdout is not None
                for line in process.stdout:
                    recent.append(line)
                    del recent[:-30]
                    self.append(job, line)
                    if job.cancel.is_set():
                        self._terminate_process(process)
                        break
                code = process.wait()
            finally:
                with job.process_lock:
                    if process in job.processes:
                        job.processes.remove(process)
            if job.cancel.is_set():
                raise RuntimeError("cancelled")
            if code == 0:
                return
            transient_storage_error = code in (125, 126) and any(
                marker in "".join(recent).lower()
                for marker in (
                    "disk i/o error: bad file descriptor",
                    "database is locked",
                )
            )
            if transient_storage_error and attempt < attempts:
                self.append(job, "Podman storage was temporarily unavailable; retrying")
                time.sleep(attempt)
                continue
            raise RuntimeError(f"{label} exited with status {code}")

    def _build_enc(
        self,
        job: Job,
        input_root: Path,
        job_dir: Path,
        output_stem: str,
        *,
        profile: Literal["preview", "refined"] = "refined",
        layers: Iterable[str] | None = None,
        clip_bounds: Iterable[float] | None = None,
        make_pmtiles: bool = True,
    ) -> tuple[Path, ...]:
        work = job_dir / "vector-work"
        geojson = work / "geojson"
        bands = work / "bands"
        outputs = job_dir / "outputs"
        geojson.mkdir(parents=True, exist_ok=True)
        bands.mkdir(parents=True, exist_ok=True)
        outputs.mkdir(parents=True, exist_ok=True)
        parallelism = int(job.config["parallelism"])
        export_script = work / "export.sh"
        export_script.write_text(EXPORT_SCRIPT, encoding="utf-8")
        self.update(job, "Exporting S-57 layers in parallel", 0.38)
        command = self._container_base(
            f"chart-export-{job.id}",
            [(input_root, "/input", True), (work, "/work", False)],
            {
                "PARALLELISM": str(parallelism),
                "LAYER_ALLOWLIST": ",".join(layers or ()),
                "CLIP_BBOX": ",".join(str(item) for item in (clip_bounds or ())),
            },
        ) + [TOOLBOX_IMAGE, "bash", "/work/export.sh"]
        self._exec(job, command, "parallel S-57 export")
        exported = list(geojson.glob("*.geojsonseq"))
        if not exported:
            raise RuntimeError("GDAL produced no S-57 layers")
        rounded_depths = sum(round_s57_depths(path) for path in exported)
        self.append(
            job,
            f"Conservatively rounded {rounded_depths} depth values above 40 ft",
        )
        band_keys = sorted(set(path.name.split("__", 1)[0] for path in exported))
        band_zoom_ranges = enc_band_zoom_ranges(
            band_keys, int(job.config["min_zoom"]), int(job.config["max_zoom"])
        )
        if not band_zoom_ranges:
            raise RuntimeError("GDAL produced no usable S-57 scale bands")
        band_workers = min(len(band_zoom_ranges), max(1, parallelism // 2))
        threads_per_band = max(1, parallelism // band_workers)

        def tile_band(key: str) -> Path:
            band_value = int(key[1:]) if key[1:].isdigit() else 0
            min_zoom, max_zoom = band_zoom_ranges[key]
            native_min, native_max = BAND_ZOOMS.get(
                band_value, (min_zoom, max_zoom)
            )
            if min_zoom < native_min or max_zoom > native_max:
                self.append(
                    job,
                    f"Extending ENC band {band_value} from native z{native_min}-{native_max} "
                    f"to requested z{min_zoom}-{max_zoom}",
                )
            script = work / f"tippecanoe-{key}.sh"
            script.write_text(
                TIPPECANOE_PREVIEW_SCRIPT if profile == "preview" else TIPPECANOE_SCRIPT,
                encoding="utf-8",
            )
            command = self._container_base(
                f"chart-tile-{job.id}-{key}",
                [(work, "/work", False)],
                {"BAND_KEY": key, "MIN_ZOOM": str(min_zoom), "MAX_ZOOM": str(max_zoom), "TIPPECANOE_MAX_THREADS": str(threads_per_band)},
            ) + [TOOLBOX_IMAGE, "bash", f"/work/{script.name}"]
            self._exec(job, command, f"tiling ENC {key}")
            return bands / f"{key}.mbtiles"

        self.update(job, f"Building {len(band_zoom_ranges)} scale bands", 0.58)
        with concurrent.futures.ThreadPoolExecutor(max_workers=band_workers) as pool:
            band_files = list(pool.map(tile_band, band_zoom_ranges))
        mbtiles = work / f"{output_stem}.mbtiles"
        join_script = work / "join.sh"
        join_script.write_text(JOIN_SCRIPT, encoding="utf-8")
        command = self._container_base(
            f"chart-join-{job.id}",
            [(work, "/work", False)],
            {
                "CHART_NAME": job.name,
                "TIPPECANOE_MAX_THREADS": str(parallelism),
            },
        ) + [TOOLBOX_IMAGE, "bash", "/work/join.sh"]
        self.update(job, "Joining ENC scale bands", 0.84)
        self._exec(job, command, "joining scale bands")
        if not mbtiles.exists() and (work / "chart-set.mbtiles").exists():
            (work / "chart-set.mbtiles").replace(mbtiles)
        self._set_enc_metadata(mbtiles, job.name, profile)
        mbtiles_output = self._unique_output(outputs, f"{output_stem}.mbtiles")
        shutil.copy2(mbtiles, mbtiles_output)
        self.append(
            job,
            f"Created {mbtiles_output.name} for Charts Provider Simple / Freeboard "
            f"({mbtiles_output.stat().st_size / 1024 / 1024:.1f} MiB)",
        )
        if not make_pmtiles:
            return (mbtiles_output,)
        pmtiles_output = self._unique_output(outputs, f"{output_stem}.pmtiles")
        self._convert_mbtiles(job, mbtiles, pmtiles_output)
        return mbtiles_output, pmtiles_output

    def _build_geojson(self, job: Job, source: Path, job_dir: Path, stem: str) -> Path:
        work = job_dir / f"geo-{stem}"
        outputs = job_dir / "outputs"
        work.mkdir(exist_ok=True)
        outputs.mkdir(exist_ok=True)
        mbtiles = work / f"{stem}.mbtiles"
        layer = safe_stem(stem, "chart").replace("-", "_")
        command = self._container_base(
            f"chart-geo-{job.id}-{stem[:20]}",
            [(source.parent, "/input", True), (work, "/work", False)],
            {"TIPPECANOE_MAX_THREADS": str(job.config["parallelism"])},
        ) + [
            TOOLBOX_IMAGE,
            "tippecanoe", "-o", f"/work/{mbtiles.name}", "-l", layer,
            "-Z", str(job.config["min_zoom"]), "-z", str(job.config["max_zoom"]),
            "--no-tile-size-limit", "--no-feature-limit", "--force", f"/input/{source.name}",
        ]
        self._exec(job, command, f"tiling {source.name}")
        return self._convert_mbtiles(job, mbtiles, self._unique_output(outputs, f"{stem}.pmtiles"))

    def _build_raster(self, job: Job, source: Path, job_dir: Path, stem: str) -> Path:
        work = job_dir / f"raster-{stem}"
        outputs = job_dir / "outputs"
        work.mkdir(exist_ok=True)
        outputs.mkdir(exist_ok=True)
        mbtiles = work / f"{stem}.mbtiles"
        script = "set -e; gdal_translate -of MBTILES -co TILE_FORMAT=PNG -co ZOOM_LEVEL_STRATEGY=UPPER \"/input/$SOURCE_NAME\" \"/work/$OUTPUT_NAME\"; gdaladdo -r average \"/work/$OUTPUT_NAME\" 2 4 8 16"
        command = self._container_base(
            f"chart-raster-{job.id}-{stem[:20]}",
            [(source.parent, "/input", True), (work, "/work", False)],
            {"SOURCE_NAME": source.name, "OUTPUT_NAME": mbtiles.name},
        ) + [TOOLBOX_IMAGE, "bash", "-c", script]
        self._exec(job, command, f"tiling raster {source.name}")
        return self._convert_mbtiles(job, mbtiles, self._unique_output(outputs, f"{stem}.pmtiles"))

    def _convert_mbtiles(self, job: Job, source: Path, output: Path) -> Path:
        if not source.exists():
            raise RuntimeError(f"Missing intermediate archive: {source.name}")
        self._ensure_mbtiles_metadata(source)
        output.parent.mkdir(parents=True, exist_ok=True)
        packed = source.parent / f".{safe_stem(output.stem)}-{uuid.uuid4().hex[:8]}.pmtiles"
        command = self._container_base(
            f"chart-pmtiles-{job.id}-{uuid.uuid4().hex[:6]}",
            [(source.parent, "/data", False)],
        ) + [PMTILES_IMAGE, "convert", f"/data/{source.name}", f"/data/{packed.name}"]
        self.update(job, "Packing PMTiles", max(job.progress, 0.90))
        self._exec(job, command, f"packing {output.name}")
        if not packed.exists() or packed.stat().st_size < 127:
            raise RuntimeError(f"PMTiles conversion produced an invalid file: {output.name}")
        packed.replace(output)
        self.append(job, f"Created {output.name} ({output.stat().st_size / 1024 / 1024:.1f} MiB)")
        return output

    def _ensure_mbtiles_metadata(self, path: Path) -> None:
        with sqlite3.connect(path) as database:
            rows = dict(database.execute("SELECT name, value FROM metadata").fetchall())
            if "format" not in rows:
                sample = database.execute("SELECT tile_data FROM tiles LIMIT 1").fetchone()
                if not sample:
                    raise RuntimeError(f"MBTiles has no tiles: {path.name}")
                data = sample[0]
                tile_format = "pbf" if data[:2] == b"\x1f\x8b" else "png" if data[:8] == b"\x89PNG\r\n\x1a\n" else "jpg" if data[:2] == b"\xff\xd8" else None
                if tile_format is None:
                    raise RuntimeError(f"Cannot infer MBTiles format: {path.name}")
                database.execute("INSERT INTO metadata(name,value) VALUES('format',?)", (tile_format,))
                database.commit()

    @staticmethod
    def _set_enc_metadata(path: Path, chart_name: str, phase: str = "refined") -> None:
        """Mark raw S-57 MVT so an S-57-aware Signal K provider applies portrayal."""
        values = {
            "name": chart_name,
            "description": "NOAA S-57 ENC vector tiles",
            "format": "pbf",
            "type": "S-57",
            "progressive_phase": phase,
        }
        with sqlite3.connect(path) as database:
            for key, value in values.items():
                database.execute("DELETE FROM metadata WHERE name = ?", (key,))
                database.execute("INSERT INTO metadata(name, value) VALUES(?, ?)", (key, value))
            database.commit()

    @staticmethod
    def _unique_output(directory: Path, name: str) -> Path:
        candidate = directory / name
        index = 2
        while candidate.exists():
            candidate = directory / f"{Path(name).stem}-{index}{Path(name).suffix}"
            index += 1
        return candidate


class ProgressiveController:
    """Run a durable, local-first preview and refinement queue."""

    def __init__(
        self, data_dir: Path, catalog: Catalog, builder: JobManager, task_workspace_ttl_days: int = 7
    ) -> None:
        self.data_dir = data_dir / "progressive"
        self.catalog = catalog
        self.builder = builder
        self.task_workspace_ttl_seconds = task_workspace_ttl_days * 24 * 60 * 60
        self.last_task_cleanup = 0.0
        self.queue = ProgressiveQueue(self.data_dir / "queue.json", lease_seconds=180)
        self.queue.reclaim_worker_kind("local")
        self.registry = ArtifactRegistry(
            self.data_dir / "provider.json", self.data_dir / "artifacts"
        )
        self.stop_event = threading.Event()
        self.source_lock = threading.Lock()
        self.active_condition = threading.Condition()
        self.active_builds: dict[str, tuple[str, Job]] = {}
        self.worker = threading.Thread(
            target=self._worker_loop,
            name="progressive-chart-local-worker",
            daemon=True,
        )

    def start(self) -> None:
        self.cleanup_expired_task_workspaces()
        self.worker.start()

    def cleanup_expired_task_workspaces(self, now: float | None = None) -> list[str]:
        """Remove only terminal task workspaces, preserving durable queue history."""
        if self.task_workspace_ttl_seconds <= 0:
            return []
        now = time.time() if now is None else now
        self.last_task_cleanup = now
        cutoff = now - self.task_workspace_ttl_seconds
        removed: list[str] = []
        terminal = {"complete", "failed", "cancelled"}
        for job in self.queue.jobs():
            if job.status not in terminal:
                continue
            try:
                updated = datetime.fromisoformat(job.updated_at.replace("Z", "+00:00")).timestamp()
            except (TypeError, ValueError):
                continue
            if updated >= cutoff:
                continue
            task_dir = self.data_dir / "tasks" / job.id
            if task_dir.exists():
                shutil.rmtree(task_dir, ignore_errors=True)
                removed.append(job.id)
        return removed

    def stop(self) -> None:
        self.stop_event.set()
        with self.active_condition:
            builds = [build for _, build in self.active_builds.values()]
        self._cancel_builds(builds)
        self.worker.join(timeout=20)

    def create(self, request: ProgressiveNoaaRequest) -> dict[str, Any]:
        included = self.catalog.inclusion(request.chart_ids)
        visible = [item for item in included if overlaps(item.bbox, request.viewport_bbox)]
        if not visible:
            selected = set(request.chart_ids)
            visible = [item for item in included if item.chart_id in selected] or included[:1]
        halo_bounds = expand_bounds(request.viewport_bbox, 1.0)
        halo = [item for item in included if overlaps(item.bbox, halo_bounds)]
        layers = tuple(
            request.layers
            or (() if request.profile == "full" else COMPATIBLE_S57_LAYERS)
        )
        source_generation = hashlib.sha256(
            json.dumps(
                {
                    "cells": [(item.chart_id, item.version) for item in included],
                    "profile": request.profile,
                    "layers": layers,
                    "profile_version": 1,
                    "pipeline_version": APP_VERSION,
                    "toolbox": TOOLBOX_IMAGE,
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:16]
        chart_id = safe_stem(request.name).lower()
        view_plan = hashlib.sha256(
            json.dumps(
                {"bbox": request.viewport_bbox, "zoom": request.current_zoom},
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:10]
        base = {
            "chart_id": chart_id,
            "chart_name": request.name,
            "source_generation": source_generation,
            "profile": request.profile,
            "requested_layers": list(layers),
            "parallelism": max(1, min(request.parallelism, os.cpu_count() or 1)),
        }

        def enqueue(
            packet_name: str,
            cells: list[Footprint],
            min_zoom: int,
            max_zoom: int,
            priority: PriorityClass,
            *,
            zoom_distance: int = 0,
            ring: int = 0,
            refine: bool = False,
            clip_bounds: list[float] | None = None,
        ) -> ProgressiveJob:
            metadata = {
                **base,
                "packet_name": packet_name,
                "cells": [asdict(item) for item in cells],
                "bounds": bounds_for(cells),
                "clip_bounds": clip_bounds,
                "min_zoom": min_zoom,
                "max_zoom": max_zoom,
                "refine": refine,
            }
            packet_id = (
                f"{chart_id}-{packet_name}"
                if refine
                else f"{chart_id}-{packet_name}-{view_plan}"
            )
            return self.queue.enqueue_packet(
                packet_id,
                request.profile,
                source_generation,
                priority_class=priority,
                zoom_distance=zoom_distance,
                ring=ring,
                layers=layers,
                metadata=metadata,
            )

        current_zoom = max(request.min_zoom, min(request.current_zoom, request.max_zoom))
        viewport_clip = expand_bounds(request.viewport_bbox, 0.15)
        scheduled: list[ProgressiveJob] = [
            enqueue(
                "viewport-current",
                visible,
                current_zoom,
                current_zoom,
                PriorityClass.VIEWPORT,
                clip_bounds=viewport_clip,
            )
        ]
        if request.min_zoom != current_zoom or request.max_zoom != current_zoom:
            scheduled.append(
                enqueue(
                    "viewport-all-zooms",
                    visible,
                    request.min_zoom,
                    request.max_zoom,
                    PriorityClass.ADJACENT_ZOOM,
                    zoom_distance=1,
                    clip_bounds=viewport_clip,
                )
            )
        if {item.chart_id for item in halo} != {item.chart_id for item in visible}:
            scheduled.append(
                enqueue(
                    "surrounding-ring-1",
                    halo,
                    request.min_zoom,
                    request.max_zoom,
                    PriorityClass.SURROUNDING_RING,
                    ring=1,
                    clip_bounds=halo_bounds,
                )
            )
        scheduled.append(
            enqueue(
                "selected-region",
                included,
                request.min_zoom,
                request.max_zoom,
                PriorityClass.BACKGROUND,
                refine=True,
            )
        )
        return {
            "chart_id": chart_id,
            "name": request.name,
            "source_generation": source_generation,
            "selected_cells": len(request.chart_ids),
            "resolved_cells": len(included),
            "viewport_cells": len(visible),
            "scheduled": [job.to_dict() for job in scheduled],
        }

    def status(self) -> dict[str, Any]:
        return {
            "local_worker": self.builder.runtime is not None,
            "tasks": [job.to_dict() for job in self.queue.jobs()],
            "chart_sets": self.chart_sets(),
            "charts": [
                self.registry.descriptor(chart.identifier, api_version=2)
                for chart in self.registry.charts()
            ],
        }

    def chart_sets(self) -> list[dict[str, Any]]:
        jobs = self.queue.jobs()
        records = {chart.identifier: chart for chart in self.registry.charts()}
        chart_ids = set(records)
        chart_ids.update(
            str(job.metadata["chart_id"])
            for job in jobs
            if isinstance(job.metadata.get("chart_id"), str)
        )
        result: list[dict[str, Any]] = []
        for chart_id in sorted(chart_ids):
            chart_jobs = [job for job in jobs if job.metadata.get("chart_id") == chart_id]
            counts = {
                status: sum(job.status == status for job in chart_jobs)
                for status in ("queued", "leased", "paused", "complete", "failed", "cancelled")
            }
            paused = self.queue.is_chart_paused(chart_id)
            record = records.get(chart_id)
            active_artifact = (
                record.generations[record.active_generation] if record is not None else None
            )
            if paused:
                state = "paused"
            elif counts["leased"]:
                state = "refining" if active_artifact is not None else "building"
            elif counts["queued"]:
                state = "refining" if active_artifact is not None else "queued"
            elif counts["failed"]:
                state = "failed"
            elif counts["cancelled"] and active_artifact is None:
                state = "cancelled"
            elif active_artifact is not None:
                state = "ready" if active_artifact.phase == "refined" else "preview"
            else:
                state = "idle"
            latest_job = max(chart_jobs, key=lambda job: job.sequence, default=None)
            name = (
                record.name
                if record is not None
                else str(latest_job.metadata.get("chart_name", chart_id))
                if latest_job is not None
                else chart_id
            )
            unfinished = counts["queued"] + counts["leased"]
            result.append(
                {
                    "chart_id": chart_id,
                    "name": name,
                    "state": state,
                    "paused": paused,
                    "phase": active_artifact.phase if active_artifact is not None else None,
                    "generation": record.active_generation if record is not None else None,
                    "task_counts": counts,
                    "actions": {
                        "pause": not paused and unfinished > 0,
                        "resume": paused,
                        "cancel": unfinished > 0 or paused,
                        "retry": counts["failed"] + counts["cancelled"] > 0,
                        "clear_history": counts["failed"] + counts["cancelled"] > 0,
                        "delete": True,
                    },
                }
            )
        return result

    def pause_chart(self, chart_id: str) -> dict[str, Any]:
        chart_id = self._require_chart(chart_id)
        changed = self.queue.pause_chart(chart_id)
        return {"chart_id": chart_id, "paused": True, "changed": changed}

    def resume_chart(self, chart_id: str) -> dict[str, Any]:
        chart_id = self._require_chart(chart_id)
        changed = self.queue.resume_chart(chart_id)
        return {"chart_id": chart_id, "paused": False, "changed": changed}

    def cancel_chart(self, chart_id: str) -> dict[str, Any]:
        chart_id = self._require_chart(chart_id)
        with self.active_condition:
            cancelled = self.queue.cancel_chart(chart_id)
            self.queue.resume_chart(chart_id)
            builds = [
                build for active_chart, build in self.active_builds.values()
                if active_chart == chart_id
            ]
        self._cancel_builds(builds)
        return {"chart_id": chart_id, "cancelled_tasks": len(cancelled)}

    def retry_chart(self, chart_id: str) -> dict[str, Any]:
        chart_id = self._require_chart(chart_id)
        retryable = [
            job
            for job in self.queue.jobs()
            if job.metadata.get("chart_id") == chart_id
            and job.status in {"failed", "cancelled"}
        ]
        for job in retryable:
            shutil.rmtree(self.data_dir / "tasks" / job.id, ignore_errors=True)
        retried = self.queue.retry_chart(chart_id)
        self.queue.resume_chart(chart_id)
        return {"chart_id": chart_id, "retried_tasks": len(retried)}

    def clear_chart_history(self, chart_id: str) -> dict[str, Any]:
        chart_id = self._require_chart(chart_id)
        removed = self.queue.clear_chart_history(chart_id)
        for job_id in removed:
            shutil.rmtree(self.data_dir / "tasks" / job_id, ignore_errors=True)
        self._remove_build_history(removed)
        return {"chart_id": chart_id, "removed_tasks": len(removed)}

    def delete_chart(self, chart_id: str, *, purge_sources: bool = False) -> dict[str, Any]:
        chart_id = self._require_chart(chart_id)
        chart_jobs = [
            job for job in self.queue.jobs() if job.metadata.get("chart_id") == chart_id
        ]
        cell_versions = {
            (str(cell["chart_id"]), str(cell["version"]))
            for job in chart_jobs
            for cell in job.metadata.get("cells", [])
            if isinstance(cell, dict)
            and isinstance(cell.get("chart_id"), str)
            and isinstance(cell.get("version"), str)
        }
        with self.active_condition:
            self.queue.cancel_chart(chart_id)
            builds = [
                build for active_chart, build in self.active_builds.values()
                if active_chart == chart_id
            ]
        self._cancel_builds(builds)
        if not self._wait_until_inactive(chart_id):
            raise RuntimeError("Chart conversion is still stopping; delete it again in a few seconds")
        removed_job_ids = self.queue.remove_chart(chart_id)
        for job_id in removed_job_ids:
            shutil.rmtree(self.data_dir / "tasks" / job_id, ignore_errors=True)
        self._remove_build_history(removed_job_ids)
        artifacts = self.registry.delete_chart(chart_id)
        source_bytes = 0
        purged_cells = 0
        if purge_sources:
            referenced_versions = {
                (str(cell["chart_id"]), str(cell["version"]))
                for job in self.queue.jobs()
                for cell in job.metadata.get("cells", [])
                if isinstance(cell, dict)
                and isinstance(cell.get("chart_id"), str)
                and isinstance(cell.get("version"), str)
            }
            with self.source_lock:
                for cell_id, version in sorted(cell_versions - referenced_versions):
                    source = self.data_dir / "sources" / cell_id / safe_stem(version)
                    source_bytes += self._directory_size(source)
                    if source.exists():
                        shutil.rmtree(source)
                        purged_cells += 1
                    try:
                        source.parent.rmdir()
                    except (FileNotFoundError, OSError):
                        pass
        return {
            "chart_id": chart_id,
            "removed_tasks": len(removed_job_ids),
            "removed_generations": artifacts["generations"],
            "removed_artifact_bytes": artifacts["bytes"],
            "purged_source_cells": purged_cells,
            "purged_source_bytes": source_bytes,
        }

    def _require_chart(self, chart_id: str) -> str:
        if not SAFE_CHART_ID.fullmatch(chart_id):
            raise ValueError("Invalid chart identifier")
        known = self.registry.chart(chart_id) is not None or any(
            job.metadata.get("chart_id") == chart_id for job in self.queue.jobs()
        )
        if not known:
            raise KeyError(chart_id)
        return chart_id

    def _cancel_active_builds(self, chart_id: str) -> None:
        with self.active_condition:
            builds = [
                build for active_chart, build in self.active_builds.values()
                if active_chart == chart_id
            ]
        self._cancel_builds(builds)

    def _cancel_builds(self, builds: Iterable[Job]) -> None:
        for build in builds:
            try:
                self.builder.cancel_job(build.id)
            except KeyError:
                pass

    def _wait_until_inactive(self, chart_id: str, timeout: float = 20.0) -> bool:
        deadline = time.monotonic() + timeout
        with self.active_condition:
            while any(active_chart == chart_id for active_chart, _ in self.active_builds.values()):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.active_condition.wait(min(remaining, 1.0))
        return True

    def _remove_build_history(self, job_ids: Iterable[str]) -> None:
        prefixes = tuple(f"p-{job_id[:10]}-" for job_id in job_ids)
        if not prefixes:
            return
        with self.builder.lock:
            for build_id in list(self.builder.jobs):
                if build_id.startswith(prefixes):
                    self.builder.jobs.pop(build_id, None)

    @staticmethod
    def _directory_size(path: Path) -> int:
        if not path.exists():
            return 0
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())

    def _worker_loop(self) -> None:
        worker_id = f"local-{os.getpid()}"
        while not self.stop_event.is_set():
            if time.time() - self.last_task_cleanup >= 24 * 60 * 60:
                self.cleanup_expired_task_workspaces()
            if self.builder.runtime is None:
                self.stop_event.wait(5)
                continue
            work = self.queue.lease_next(worker_id, worker_kind="local")
            if work is None:
                self.stop_event.wait(1)
                continue
            assert work.lease is not None
            token = work.lease.token
            heartbeat_stop = threading.Event()

            def heartbeat() -> None:
                while not heartbeat_stop.wait(60):
                    try:
                        self.queue.renew(work.id, token)
                    except Exception:
                        self._cancel_active_builds(
                            str(work.metadata.get("chart_id", ""))
                        )
                        return

            heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
            heartbeat_thread.start()
            try:
                self._execute(work)
                self.queue.complete(work.id, token)
            except Exception as error:
                try:
                    if self.stop_event.is_set():
                        self.queue.release(work.id, token)
                    else:
                        self.queue.fail(
                            work.id, token, str(error), retry=work.attempts < 2
                        )
                except Exception:
                    pass
            finally:
                heartbeat_stop.set()
                heartbeat_thread.join(timeout=2)

    def _execute(self, work: ProgressiveJob) -> None:
        metadata = work.metadata
        cells = [Footprint(**item) for item in metadata["cells"]]
        phase: Literal["preview", "refined"] = (
            "preview" if work.target_state == "preview" else "refined"
        )
        lease_token = work.lease.token if work.lease is not None else ""
        task_dir = self.data_dir / "tasks" / work.id / phase
        task_dir.mkdir(parents=True, exist_ok=True)
        build_job = Job(
            id=f"p-{work.id[:10]}-{phase[0]}",
            kind="progressive",
            name=str(metadata["chart_name"]),
            status="running",
            phase=f"Building {phase} chart",
            config={
                "min_zoom": int(metadata["min_zoom"]),
                "max_zoom": int(metadata["max_zoom"]),
                "parallelism": int(metadata["parallelism"]),
                "download_workers": 1,
                "chart_id": str(metadata["chart_id"]),
            },
        )
        with self.active_condition:
            current = self.queue.get(work.id)
            if current is None or current.status != "leased":
                raise RuntimeError("Chart task was cancelled before it started")
            self.active_builds[work.id] = (str(metadata["chart_id"]), build_job)
        with self.builder.lock:
            self.builder.jobs[build_job.id] = build_job
        requested_layers = tuple(str(item) for item in metadata["requested_layers"])
        layers = (
            tuple(item for item in BASIC_S57_LAYERS if not requested_layers or item in requested_layers)
            if phase == "preview"
            else requested_layers
            if metadata["profile"] == "compatible"
            else None
        )
        try:
            input_root = self._assemble_inputs(work, cells, build_job.cancel)
            if build_job.cancel.is_set():
                raise RuntimeError("cancelled")
            outputs = self.builder._build_enc(
                build_job,
                input_root,
                task_dir,
                f"{metadata['chart_id']}-{metadata['packet_name']}-{phase}",
                profile=phase,
                layers=layers,
                clip_bounds=metadata.get("clip_bounds"),
                make_pmtiles=False,
            )
            source = outputs[0]
            generation_number = work.sequence * 2 + (1 if phase == "refined" else 0)
            generation = f"g{generation_number:06d}"
            artifact_dir = self.data_dir / "artifacts" / str(metadata["chart_id"])
            artifact_dir.mkdir(parents=True, exist_ok=True)
            artifact = artifact_dir / f"{generation}.mbtiles"
            partial = artifact.with_suffix(".part.mbtiles")
            with self.active_condition:
                current = self.queue.get(work.id)
                if (
                    current is None
                    or current.status != "leased"
                    or current.lease is None
                    or current.lease.token != lease_token
                    or build_job.cancel.is_set()
                ):
                    raise RuntimeError("Chart task was cancelled before publication")
                existing = self.registry.chart(str(metadata["chart_id"]))
                active_is_refined = (
                    existing is not None
                    and existing.generations[existing.active_generation].phase == "refined"
                )
                shutil.copy2(source, partial)
                partial.replace(artifact)
                self.registry.register_mbtiles(
                    str(metadata["chart_id"]),
                    generation,
                    artifact,
                    phase=phase,
                    profile=str(metadata["profile"]),
                    name=str(metadata["chart_name"]),
                    description=(
                        "NOAA S-57 ENC progressive preview"
                        if phase == "preview"
                        else "NOAA S-57 ENC refined vector chart"
                    ),
                    activate=not (phase == "preview" and active_is_refined),
                )
            build_job.outputs = [source.name]
            build_job.status = "completed"
            build_job.phase = f"Published {phase} generation {generation}"
            build_job.progress = 1.0
        except Exception as error:
            build_job.status = "cancelled" if build_job.cancel.is_set() else "failed"
            build_job.phase = "Cancelled" if build_job.cancel.is_set() else "Failed"
            build_job.error = str(error)
            raise
        finally:
            build_job.updated_at = utc_now()
            with self.active_condition:
                self.active_builds.pop(work.id, None)
                self.active_condition.notify_all()

    def _assemble_inputs(
        self,
        work: ProgressiveJob,
        cells: list[Footprint],
        cancel: threading.Event | None = None,
    ) -> Path:
        packet_root = self.data_dir / "tasks" / work.id / "input"
        packet_root.mkdir(parents=True, exist_ok=True)
        for cell in cells:
            if cancel is not None and cancel.is_set():
                raise RuntimeError("cancelled")
            source = self._ensure_cell(cell, cancel)
            if cancel is not None and cancel.is_set():
                raise RuntimeError("cancelled")
            target = packet_root / cell.chart_id
            if target.exists():
                continue

            def link_or_copy(src: str, dst: str) -> str:
                try:
                    os.link(src, dst)
                except OSError:
                    shutil.copy2(src, dst)
                return dst

            shutil.copytree(source, target, copy_function=link_or_copy)
        return packet_root

    def _ensure_cell(
        self, cell: Footprint, cancel: threading.Event | None = None
    ) -> Path:
        cell_root = self.data_dir / "sources" / cell.chart_id / safe_stem(cell.version)
        extracted = cell_root / "enc"
        marker = cell_root / ".complete"
        if marker.exists() and extracted.exists():
            return extracted
        with self.source_lock:
            if cancel is not None and cancel.is_set():
                raise RuntimeError("cancelled")
            if marker.exists() and extracted.exists():
                return extracted
            cell_root.mkdir(parents=True, exist_ok=True)
            archive = cell_root / f"{cell.chart_id}.zip"
            if not archive.exists():
                self.builder._download(
                    f"{NOAA_ENC_BASE_URL}/{cell.chart_id}.zip", archive, cancel
                )
            staging = cell_root / f".extract-{uuid.uuid4().hex[:8]}"
            try:
                self.builder._safe_extract(archive, staging, cancel)
                if extracted.exists():
                    shutil.rmtree(staging, ignore_errors=True)
                else:
                    staging.replace(extracted)
            except Exception:
                shutil.rmtree(staging, ignore_errors=True)
                raise
            marker.write_text(
                json.dumps(
                    {
                        "chart_id": cell.chart_id,
                        "version": cell.version,
                        "sha256": sha256_file(archive),
                    },
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
        return extracted


EXPORT_SCRIPT = r'''#!/usr/bin/env bash
set -euo pipefail
mkdir -p /work/geojson
export CLIP_BBOX
export_one() {
  enc="$1"
  name="$(basename "$enc" .000)"
  band="${name:2:1}"
  [[ "$band" =~ ^[1-9]$ ]] || band="u"
  mapfile -t layers < <(ogrinfo -ro -so "$enc" 2>/dev/null | awk -F': ' '/^[0-9]+:/{print $2}' | awk '{print $1}')
  for layer in "${layers[@]}"; do
    case "$layer" in DSID|C_AGGR|C_ASSO|Generic) continue ;; esac
    if [[ -n "${LAYER_ALLOWLIST:-}" && ",${LAYER_ALLOWLIST}," != *",${layer},"* ]]; then
      continue
    fi
    output="/work/geojson/b${band}__${layer}__${name}.geojsonseq"
    local layer_clip=()
    if [[ -n "${CLIP_BBOX:-}" ]]; then
      IFS=',' read -r west south east north <<< "$CLIP_BBOX"
      layer_clip=(-spat "$west" "$south" "$east" "$north" -clipsrc "$west" "$south" "$east" "$north")
    fi
    if [[ "$layer" == "SOUNDG" ]]; then
      ogr2ogr -f GeoJSONSeq -skipfailures -mapFieldType DateTime=String -lco COORDINATE_PRECISION=6 -oo LIST_AS_STRING=YES -oo SPLIT_MULTIPOINT=YES -oo ADD_SOUNDG_DEPTH=YES "${layer_clip[@]}" "$output" "$enc" "$layer"
    else
      ogr2ogr -f GeoJSONSeq -skipfailures -mapFieldType DateTime=String -lco COORDINATE_PRECISION=6 -oo LIST_AS_STRING=YES "${layer_clip[@]}" "$output" "$enc" "$layer"
    fi
    [[ -s "$output" ]] || rm -f "$output"
  done
  echo "Exported $name"
}
export -f export_one
find /input -iname '*.000' ! -name '._*' -type f -print0 | xargs -0 -n 1 -P "${PARALLELISM:-4}" bash -c 'export_one "$1"' _
'''

TIPPECANOE_SCRIPT = r'''#!/usr/bin/env bash
set -euo pipefail
layers=()
while IFS= read -r -d '' file; do
  base="$(basename "$file")"
  rest="${base#*__}"
  layer="${rest%%__*}"
  layers+=(-L "$layer:$file")
done < <(find /work/geojson -name "${BAND_KEY}__*.geojsonseq" -type f -print0)
if [[ ${#layers[@]} -eq 0 ]]; then echo "No layers for $BAND_KEY" >&2; exit 2; fi
tippecanoe -o "/work/bands/${BAND_KEY}.mbtiles" -Z "$MIN_ZOOM" -z "$MAX_ZOOM" --no-tile-size-limit --no-feature-limit --detect-shared-borders --no-simplification --no-tiny-polygon-reduction --buffer=80 --force "${layers[@]}"
'''

TIPPECANOE_PREVIEW_SCRIPT = r'''#!/usr/bin/env bash
set -euo pipefail
layers=()
while IFS= read -r -d '' file; do
  base="$(basename "$file")"
  rest="${base#*__}"
  layer="${rest%%__*}"
  layers+=(-L "$layer:$file")
done < <(find /work/geojson -name "${BAND_KEY}__*.geojsonseq" -type f -print0)
if [[ ${#layers[@]} -eq 0 ]]; then echo "No layers for $BAND_KEY" >&2; exit 2; fi
tippecanoe -o "/work/bands/${BAND_KEY}.mbtiles" -Z "$MIN_ZOOM" -z "$MAX_ZOOM" --no-tile-size-limit --no-feature-limit --detect-shared-borders --no-simplification --no-tiny-polygon-reduction --buffer=80 --force "${layers[@]}"
'''

JOIN_SCRIPT = r'''#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob
inputs=(/work/bands/*.mbtiles)
if [[ ${#inputs[@]} -eq 0 ]]; then echo 'No band archives' >&2; exit 2; fi
tile-join -o /work/chart-set.mbtiles -n "$CHART_NAME" --no-tile-size-limit --force "${inputs[@]}"
'''


def run_checked(command: list[str], timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, timeout=timeout, check=True)


def expand_remote_dir(target: str, remote_dir: str) -> str:
    if remote_dir.startswith("~/"):
        home = run_checked(["ssh", "-o", "BatchMode=yes", target, "pwd"], timeout=20).stdout.strip()
        return f"{home}/{remote_dir[2:]}"
    return remote_dir


def sideload(manager: JobManager, request: SideloadRequest) -> dict[str, Any]:
    if not shutil.which("ssh") or not shutil.which("scp"):
        raise RuntimeError("ssh and scp must be installed for sideloading")
    job = manager.get(request.job_id)
    if job.status != "completed" or not job.outputs:
        raise RuntimeError("Only completed jobs can be sideloaded")
    mbtiles = [name for name in job.outputs if name.lower().endswith(".mbtiles")]
    pmtiles = [name for name in job.outputs if name.lower().endswith(".pmtiles")]
    if request.destination == "auto":
        selected = mbtiles if mbtiles else pmtiles
    elif request.destination == "freeboard":
        selected = mbtiles
    elif request.destination == "chart-locker":
        selected = pmtiles
    else:
        selected = mbtiles + pmtiles
    if not selected:
        raise RuntimeError(f"This job has no output for destination {request.destination}")

    remote_dirs = {
        ".mbtiles": expand_remote_dir(request.target, request.mbtiles_dir),
        ".pmtiles": expand_remote_dir(request.target, request.pmtiles_dir),
    }
    for remote_dir in sorted({remote_dirs[Path(name).suffix.lower()] for name in selected}):
        run_checked(
            ["ssh", "-o", "BatchMode=yes", request.target, f"mkdir -p -- {shlex.quote(remote_dir)}"],
            timeout=30,
        )
    transferred: list[dict[str, Any]] = []
    for name in selected:
        local = manager.data_dir / "jobs" / job.id / "outputs" / name
        if not local.exists():
            raise RuntimeError(f"Output disappeared: {name}")
        remote_dir = remote_dirs[local.suffix.lower()]
        partial = f"{remote_dir}/.{name}.part"
        final = f"{remote_dir}/{name}"
        run_checked(["scp", "-q", "-o", "BatchMode=yes", str(local), f"{request.target}:{partial}"], timeout=None)
        remote_hash = run_checked(["ssh", "-o", "BatchMode=yes", request.target, f"sha256sum -- {shlex.quote(partial)}"], timeout=120).stdout.split()[0]
        local_hash = sha256_file(local)
        if remote_hash != local_hash:
            raise RuntimeError(f"Checksum mismatch while sending {name}")
        owner_command = f"chown {shlex.quote(request.remote_owner)} -- {shlex.quote(partial)} && " if request.remote_owner else ""
        run_checked(["ssh", "-o", "BatchMode=yes", request.target, f"{owner_command}chmod 0644 -- {shlex.quote(partial)} && mv -f -- {shlex.quote(partial)} {shlex.quote(final)}"], timeout=30)
        transferred.append({"name": name, "remote_dir": remote_dir, "bytes": local.stat().st_size, "sha256": local_hash})
    if request.restart_signalk:
        run_checked(
            ["ssh", "-o", "BatchMode=yes", request.target, "systemctl restart signalk.service"],
            timeout=60,
        )
    return {
        "target": request.target,
        "destination": request.destination,
        "restarted_signalk": request.restart_signalk,
        "files": transferred,
    }


def create_app(
    data_dir: Path = DEFAULT_DATA_DIR, runtime: str = "auto", task_workspace_ttl_days: int = 7
) -> FastAPI:
    data_dir = data_dir.expanduser().resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    selected_runtime = detect_runtime(runtime)
    catalog = Catalog(data_dir / "cache" / "enc.geojson")
    manager = JobManager(data_dir, selected_runtime, catalog)
    progressive = ProgressiveController(data_dir, catalog, manager, task_workspace_ttl_days)
    app = FastAPI(title="Charts Provider Progressive", version=APP_VERSION)
    app.state.manager = manager
    app.state.catalog = catalog
    app.state.progressive = progressive
    progressive.start()

    @app.on_event("shutdown")
    def shutdown_progressive_worker() -> None:
        progressive.stop()

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return UI_HTML

    @app.get("/api/status")
    def status() -> dict[str, Any]:
        runtime_version = None
        if selected_runtime:
            try:
                runtime_version = run_checked([selected_runtime, "--version"], timeout=5).stdout.strip()
            except Exception:
                runtime_version = f"{selected_runtime} installed but unavailable"
        return {
            "version": APP_VERSION,
            "runtime": selected_runtime,
            "runtime_version": runtime_version,
            "data_dir": str(data_dir),
            "progressive": True,
            "local_worker": selected_runtime is not None,
        }

    @app.get("/api/noaa/catalog")
    def noaa_catalog(refresh: bool = False) -> dict[str, Any]:
        try:
            entries = catalog.band4() if not refresh else sorted((x for x in catalog.load(True) if x.band == 4), key=lambda x: x.chart_id)
            regions = catalog_regions(entries)
            return {
                "entries": [asdict(item) for item in entries],
                "presets": {"california": [item.chart_id for item in entries if item.chart_id.startswith("US4CA")]},
                "regions": [asdict(region) for region in regions],
            }
        except Exception as error:
            raise HTTPException(502, str(error)) from error

    @app.post("/api/jobs/noaa", status_code=202)
    def create_noaa_job(request: NoaaJobRequest) -> dict[str, Any]:
        if request.min_zoom > request.max_zoom:
            raise HTTPException(422, "min_zoom must not exceed max_zoom")
        return manager.create("noaa", request).public()

    @app.post("/api/progressive/noaa", status_code=202)
    def create_progressive_noaa(request: ProgressiveNoaaRequest) -> dict[str, Any]:
        if request.min_zoom > request.max_zoom:
            raise HTTPException(422, "min_zoom must not exceed max_zoom")
        try:
            return progressive.create(request)
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    @app.get("/api/progressive/status")
    def progressive_status() -> dict[str, Any]:
        return progressive.status()

    def chart_action(action: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        try:
            return action()
        except KeyError as error:
            raise HTTPException(404, "Unknown progressive chart") from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except RuntimeError as error:
            raise HTTPException(409, str(error)) from error

    @app.post("/api/progressive/charts/{chart_id}/pause", status_code=202)
    def pause_progressive_chart(chart_id: str) -> dict[str, Any]:
        return chart_action(lambda: progressive.pause_chart(chart_id))

    @app.post("/api/progressive/charts/{chart_id}/resume", status_code=202)
    def resume_progressive_chart(chart_id: str) -> dict[str, Any]:
        return chart_action(lambda: progressive.resume_chart(chart_id))

    @app.post("/api/progressive/charts/{chart_id}/cancel", status_code=202)
    def cancel_progressive_chart(chart_id: str) -> dict[str, Any]:
        return chart_action(lambda: progressive.cancel_chart(chart_id))

    @app.post("/api/progressive/charts/{chart_id}/retry", status_code=202)
    def retry_progressive_chart(chart_id: str) -> dict[str, Any]:
        return chart_action(lambda: progressive.retry_chart(chart_id))

    @app.post("/api/progressive/charts/{chart_id}/history/clear")
    def clear_progressive_chart_history(chart_id: str) -> dict[str, Any]:
        return chart_action(lambda: progressive.clear_chart_history(chart_id))

    @app.post("/api/progressive/charts/{chart_id}/delete")
    def delete_progressive_chart(
        chart_id: str, request: ProgressiveDeleteRequest
    ) -> dict[str, Any]:
        if request.confirm_chart_id != chart_id:
            raise HTTPException(422, "confirm_chart_id must match the chart identifier")
        return chart_action(
            lambda: progressive.delete_chart(
                chart_id, purge_sources=request.purge_sources
            )
        )

    @app.get("/api/provider/charts")
    def provider_charts() -> dict[str, Any]:
        return {
            "charts": [
                progressive.registry.descriptor(chart.identifier, api_version=2)
                for chart in progressive.registry.charts()
            ]
        }

    @app.get("/api/provider/charts/{chart_id}/{generation}/{z}/{x}/{y}")
    def provider_tile(
        chart_id: str, generation: str, z: int, x: int, y: int
    ) -> Response:
        try:
            tile: TileResponse | None = progressive.registry.read_tile(
                chart_id, generation, z, x, y
            )
        except ArtifactNotFound as error:
            raise HTTPException(404, str(error)) from error
        except (InvalidArtifact, ValueError) as error:
            raise HTTPException(409, str(error)) from error
        if tile is None:
            raise HTTPException(404, "Tile is outside this chart generation")
        return Response(content=tile.data, headers=tile.headers)

    @app.post("/api/jobs/url", status_code=202)
    def create_url_job(request: UrlJobRequest) -> dict[str, Any]:
        if request.min_zoom > request.max_zoom:
            raise HTTPException(422, "min_zoom must not exceed max_zoom")
        return manager.create("url", request).public()

    @app.get("/api/jobs")
    def list_jobs() -> list[dict[str, Any]]:
        with manager.lock:
            jobs = sorted(manager.jobs.values(), key=lambda item: item.created_at, reverse=True)
        return [job.public() for job in jobs]

    @app.post("/api/jobs/{job_id}/cancel", status_code=202)
    def cancel_job(job_id: str) -> dict[str, bool]:
        try:
            manager.cancel_job(job_id)
            return {"cancelled": True}
        except KeyError as error:
            raise HTTPException(404, "Unknown job") from error

    @app.get("/api/jobs/{job_id}/files/{file_name}")
    def download_output(job_id: str, file_name: str) -> FileResponse:
        try:
            job = manager.get(job_id)
        except KeyError as error:
            raise HTTPException(404, "Unknown job") from error
        if file_name not in job.outputs or Path(file_name).name != file_name:
            raise HTTPException(404, "Unknown output")
        path = data_dir / "jobs" / job.id / "outputs" / file_name
        if not path.exists():
            raise HTTPException(404, "Output is missing")
        media_type = "application/vnd.pmtiles" if path.suffix.lower() == ".pmtiles" else "application/vnd.mapbox-vector-tile"
        return FileResponse(path, filename=file_name, media_type=media_type)

    @app.post("/api/sideload")
    def sideload_files(request: SideloadRequest) -> dict[str, Any]:
        try:
            return sideload(manager, request)
        except KeyError as error:
            raise HTTPException(404, "Unknown job") from error
        except (RuntimeError, subprocess.SubprocessError) as error:
            detail = error.stderr.strip() if isinstance(error, subprocess.CalledProcessError) and error.stderr else str(error)
            raise HTTPException(502, detail) from error

    return app


UI_HTML = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Charts Provider Progressive</title>
<style>
:root{color-scheme:dark;--bg:#07131a;--panel:#0d2029;--line:#22414d;--ink:#ecf7f5;--muted:#91aab1;--sea:#0a2734;--cyan:#50d6ca;--lime:#c5ed72;--amber:#ffc56e;--red:#ff766f}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 65% -10%,#123949 0,transparent 38%),var(--bg);color:var(--ink);font:15px/1.45 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}button,input,textarea,select{font:inherit}button{cursor:pointer}header{padding:28px clamp(18px,4vw,54px) 20px;display:flex;gap:20px;align-items:end;justify-content:space-between}h1{margin:0;font-size:clamp(28px,4vw,48px);letter-spacing:-.04em}.eyebrow{color:var(--cyan);text-transform:uppercase;letter-spacing:.14em;font-size:12px;font-weight:800}.lede{margin:7px 0 0;color:var(--muted);max-width:720px}.status{border:1px solid var(--line);background:#0a1a21;padding:9px 13px;border-radius:999px;white-space:nowrap}.status.good{color:var(--lime)}main{padding:0 clamp(18px,4vw,54px) 48px;display:grid;grid-template-columns:minmax(0,1.7fr) minmax(320px,.8fr);gap:20px}.panel{background:color-mix(in srgb,var(--panel) 94%,transparent);border:1px solid var(--line);border-radius:18px;overflow:hidden;box-shadow:0 18px 50px #0005}.panel-head{padding:18px 20px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;gap:12px}.panel-head h2,.form h3{margin:0;font-size:17px}.count{color:var(--muted);font-variant-numeric:tabular-nums}.map-wrap{position:relative;height:min(58vh,620px);min-height:420px;background:var(--sea)}canvas{width:100%;height:100%;display:block;touch-action:none}.map-tools{position:absolute;top:12px;left:12px;right:12px;display:flex;gap:8px;pointer-events:none}.map-tools>*{pointer-events:auto}.map-attribution{position:absolute;left:9px;bottom:8px;padding:3px 7px;border-radius:5px;background:#07131acc;color:#d8e5e3;font-size:10px;line-height:1.25}.map-attribution a{color:inherit}.search-shell{position:relative;flex:1;min-width:0}.search{width:100%}.search-results{position:absolute;z-index:5;top:calc(100% + 6px);left:0;right:0;max-height:min(390px,55vh);overflow:auto;border:1px solid #3a6573;background:#07171ecc;border-radius:12px;padding:6px;box-shadow:0 18px 45px #0009;backdrop-filter:blur(12px)}.search-results[hidden]{display:none}.search-heading{padding:7px 9px 4px;color:var(--cyan);font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase}.search-option{display:block;width:100%;border:0;background:transparent;color:var(--ink);padding:9px;border-radius:8px;text-align:left}.search-option:hover,.search-option:focus{background:#173642;outline:none}.search-option strong,.search-option span{display:block}.search-option span{color:var(--muted);font-size:12px}.field,textarea,select{width:100%;border:1px solid var(--line);background:#081920;color:var(--ink);border-radius:10px;padding:10px 12px;outline:none}.field:focus,textarea:focus,select:focus{border-color:var(--cyan);box-shadow:0 0 0 3px #50d6ca22}.btn{border:1px solid var(--line);background:#112b35;color:var(--ink);padding:10px 13px;border-radius:10px;font-weight:750}.btn:hover{border-color:#3a6573}.btn.primary{background:var(--cyan);border-color:var(--cyan);color:#041315}.btn.accent{background:var(--lime);border-color:var(--lime);color:#101904}.btn.danger{color:var(--red)}.forms{display:grid;grid-template-columns:1fr 1fr;border-top:1px solid var(--line)}.form{padding:18px 20px}.form+.form{border-left:1px solid var(--line)}label{display:block;margin:12px 0 5px;color:var(--muted);font-size:12px;font-weight:750}.row{display:grid;grid-template-columns:1fr 1fr;gap:10px}.hint{color:var(--muted);font-size:12px;margin:8px 0}.aside{display:flex;flex-direction:column;min-height:660px}.jobs{padding:12px;display:flex;flex-direction:column;gap:10px;overflow:auto;max-height:calc(100vh - 150px)}.job{border:1px solid var(--line);border-radius:13px;background:#091a21;padding:13px}.job-top{display:flex;justify-content:space-between;gap:12px}.job-title{font-weight:800}.job-phase{color:var(--muted);font-size:12px;margin:3px 0 9px}.badge{font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:var(--amber)}.badge.completed,.badge.ready,.badge.preview{color:var(--lime)}.badge.failed,.badge.cancelled{color:var(--red)}.badge.paused{color:var(--cyan)}progress{width:100%;height:7px;accent-color:var(--cyan)}details{margin-top:9px}summary{color:var(--muted);font-size:12px;cursor:pointer}pre{white-space:pre-wrap;word-break:break-word;background:#041016;border-radius:8px;padding:9px;max-height:180px;overflow:auto;font:11px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace}.actions{display:flex;gap:7px;flex-wrap:wrap;margin-top:9px}.actions .btn{padding:6px 9px;font-size:12px}.empty{padding:30px 18px;color:var(--muted);text-align:center}.note{padding:12px 20px;border-top:1px solid var(--line);color:var(--muted);font-size:12px}dialog{border:1px solid var(--line);background:var(--panel);color:var(--ink);border-radius:16px;width:min(460px,calc(100vw - 30px));padding:20px}dialog::backdrop{background:#000a}.dialog-summary{font-size:18px;font-weight:800}.error{color:var(--red)}@media(max-width:920px){main{grid-template-columns:1fr}.aside{min-height:400px}.jobs{max-height:600px}}@media(max-width:650px){header{align-items:start;flex-direction:column}.forms{grid-template-columns:1fr}.form+.form{border-left:0;border-top:1px solid var(--line)}.map-wrap{min-height:360px}.map-tools{flex-wrap:wrap}.search-shell{flex-basis:100%}}
</style></head><body>
<header><div><div class="eyebrow">Local-first progressive charts</div><h1>Charts Provider Progressive</h1><p class="lede">Select NOAA ENC coverage, publish the current view quickly, then improve zoom levels, nearby coverage, and full fidelity in the background.</p></div><div id="runtime" class="status">Checking runtime…</div></header>
<main><section class="panel"><div class="panel-head"><h2>NOAA ENC coverage</h2><span id="selectionCount" class="count">Loading catalog…</span></div><div class="map-wrap"><canvas id="map" aria-label="Interactive NOAA ENC coverage map"></canvas><div class="map-tools"><div class="search-shell"><input id="search" class="field search" placeholder="Find a NOAA region, chart id, or place" role="combobox" aria-autocomplete="list" aria-controls="searchResults" aria-expanded="false" autocomplete="off" disabled><div id="searchResults" class="search-results" role="listbox" hidden></div></div><button class="btn" id="browseRegions" disabled>Browse regions</button><button class="btn" id="clear">Clear</button></div><div class="map-attribution"><a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">© OpenStreetMap contributors</a><span id="mapStatus"> · NOAA ENC Online</span></div></div><div class="forms"><form class="form" id="noaaForm"><h3>Start progressive chart</h3><label for="noaaName">Chart set name</label><input class="field" id="noaaName" value="NOAA ENC California" required><div class="row"><div><label for="minZoom">Minimum zoom</label><input class="field" id="minZoom" type="number" min="0" max="18" value="4"></div><div><label for="maxZoom">Maximum zoom</label><input class="field" id="maxZoom" type="number" min="0" max="18" value="16"></div></div><div class="row"><div><label for="parallelism">Local CPU workers</label><input class="field" id="parallelism" type="number" min="1" max="32" value="1"></div><div><label for="profile">Layer profile</label><select id="profile"><option value="compatible">Compatible portrayal</option><option value="full">Full S-57</option></select></div></div><p class="hint">Search for a NOAA state package or major region, confirm its coverage, and the required approach cells are selected automatically. The visible map view is built first; adjacent zooms and refined coverage follow.</p><button class="btn primary" type="submit">Start progressive chart</button></form><form class="form" id="urlForm"><h3>Build from links</h3><label for="urlName">Chart set name</label><input class="field" id="urlName" value="Imported charts" required><label for="urls">Direct chart links, one per line</label><textarea id="urls" rows="5" placeholder="https://…/enc.zip&#10;https://…/chart.mbtiles"></textarea><p class="hint">Linked-file conversion remains available as a batch operation. Progressive NOAA charts are always built by the local worker and published as soon as each generation is ready.</p><button class="btn" type="submit">Build linked files</button></form></div><div class="note">Navigation warning: generated charts are supplemental and must not be your sole means of navigation.</div></section><aside class="panel aside"><div class="panel-head"><h2>Build queue</h2><button class="btn" id="refreshJobs">Refresh</button></div><div id="jobs" class="jobs"><div class="empty">No builds yet.</div></div></aside></main>
<dialog id="regionDialog" aria-labelledby="regionTitle"><form method="dialog"><h2 id="regionTitle">Select NOAA region</h2><p id="regionDescription" class="hint"></p><p id="regionCount" class="dialog-summary"></p><p class="hint">This replaces the current map selection. NOAA coastal and harbor cells needed for the selected approach coverage are resolved automatically when the chart build starts.</p><div class="actions"><button class="btn accent" id="confirmRegion" value="confirm">Confirm region</button><button class="btn" value="cancel">Cancel</button></div></form></dialog>
<dialog id="sideloadDialog"><form method="dialog" id="sideloadForm"><h2>Send to Signal K</h2><input type="hidden" id="sideloadJob"><label for="sshTarget">SSH target</label><input class="field" id="sshTarget" value="boat-pi"><label for="destination">Chart consumer</label><select id="destination"><option value="auto">Auto: S-57 MBTiles (recommended)</option><option value="freeboard">Freeboard and Binnacle S-57 via Charts Provider Simple</option><option value="chart-locker">Chart Locker PMTiles (generic vector or raster)</option><option value="both">Both destinations</option></select><label for="mbtilesDir">Charts Provider Simple folder</label><input class="field" id="mbtilesDir" value="/home/signalk/.signalk/charts-simple"><label for="pmtilesDir">Chart Locker PMTiles folder</label><input class="field" id="pmtilesDir" value="/home/signalk/.signalk/charts/pmtiles"><label for="remoteOwner">Remote owner</label><input class="field" id="remoteOwner" value="signalk:signalk"><label><input id="restartSignalk" type="checkbox" checked> Restart Signal K after transfer</label><p class="hint">Auto sends S-57 MBTiles through the provider that advertises <code>type: S-57</code>. Current Freeboard and Binnacle releases portray this vector format. Files are checksum-verified and moved into place atomically.</p><p id="sideloadError" class="error"></p><div class="actions"><button class="btn accent" value="send">Send files</button><button class="btn" value="cancel">Cancel</button></div></form></dialog>
<script>
const $=s=>document.querySelector(s),NOAA_WMS_URL='https://gis.charttools.noaa.gov/arcgis/rest/services/MCS/ENCOnline/MapServer/exts/MaritimeChartService/WMSServer',NOAA_WMS_LAYERS='0,1,2,3,4,5,6,7,8,9,10,11,12',OPEN_MAP_TILE_URL='https://tile.openstreetmap.org',WEB_MERCATOR_RADIUS=6378137,state={entries:[],regions:[],selected:new Set(),selectionLabel:'',pendingRegion:null,view:{lon:-119,lat:37,scale:10},drag:null,openMapTiles:[],openMapRequest:0,basemap:null,basemapRequest:0,basemapTimer:null};
const canvas=$('#map'),ctx=canvas.getContext('2d');
function mercatorY(lat){const radians=Math.max(-85,Math.min(85,lat))*Math.PI/180;return Math.log(Math.tan(Math.PI/4+radians/2))*180/Math.PI}
function latitudeFromMercator(y){return Math.max(-85,Math.min(85,(2*Math.atan(Math.exp(y*Math.PI/180))-Math.PI/2)*180/Math.PI))}
function webMercatorX(lon){return WEB_MERCATOR_RADIUS*lon*Math.PI/180}
function webMercatorY(lat){return WEB_MERCATOR_RADIUS*mercatorY(lat)*Math.PI/180}
function resize(){const d=devicePixelRatio||1,r=canvas.getBoundingClientRect();canvas.width=r.width*d;canvas.height=r.height*d;ctx.setTransform(d,0,0,d,0,0);draw();scheduleBasemap()} addEventListener('resize',resize);
function project(lon,lat){const r=canvas.getBoundingClientRect(),s=state.view.scale;return [r.width/2+(lon-state.view.lon)*s,r.height/2-(mercatorY(lat)-mercatorY(state.view.lat))*s]}
function unproject(x,y){const r=canvas.getBoundingClientRect(),s=state.view.scale;return [state.view.lon+(x-r.width/2)/s,latitudeFromMercator(mercatorY(state.view.lat)-(y-r.height/2)/s)]}
function drawGrid(r){ctx.fillStyle='#0a2734';ctx.fillRect(0,0,r.width,r.height);ctx.strokeStyle='#163b48';ctx.lineWidth=1;for(let lon=-180;lon<=180;lon+=10){let a=project(lon,-80),b=project(lon,80);ctx.beginPath();ctx.moveTo(...a);ctx.lineTo(...b);ctx.stroke()}for(let lat=-80;lat<=80;lat+=10){let a=project(-180,lat),b=project(180,lat);ctx.beginPath();ctx.moveTo(...a);ctx.lineTo(...b);ctx.stroke()}}
function draw(){const r=canvas.getBoundingClientRect();ctx.clearRect(0,0,r.width,r.height);drawGrid(r);for(const tile of state.openMapTiles){const a=project(tile.bounds[0],tile.bounds[3]),b=project(tile.bounds[2],tile.bounds[1]);ctx.drawImage(tile.image,a[0],a[1],b[0]-a[0],b[1]-a[1])}if(state.basemap){const a=project(state.basemap.bounds[0],state.basemap.bounds[3]),b=project(state.basemap.bounds[2],state.basemap.bounds[1]);ctx.drawImage(state.basemap.image,a[0],a[1],b[0]-a[0],b[1]-a[1])}for(const e of state.entries){const a=project(e.bbox[0],e.bbox[3]),b=project(e.bbox[2],e.bbox[1]);if(b[0]<0||a[0]>r.width||b[1]<0||a[1]>r.height)continue;const on=state.selected.has(e.chart_id);ctx.fillStyle=on?'#c5ed724d':'#005fb829';ctx.strokeStyle=on?'#2b4900':'#00519bcc';ctx.lineWidth=on?2:.8;ctx.beginPath();ctx.rect(a[0],a[1],Math.max(2,b[0]-a[0]),Math.max(2,b[1]-a[1]));ctx.fill();ctx.stroke()}}
function tileLongitude(x,z){return x/2**z*360-180}
function tileLatitude(y,z){return Math.atan(Math.sinh(Math.PI*(1-2*y/2**z)))*180/Math.PI}
function tileX(lon,z){return Math.floor((lon+180)/360*2**z)}
function tileY(lat,z){const radians=Math.max(-85,Math.min(85,lat))*Math.PI/180;return Math.floor((1-Math.asinh(Math.tan(radians))/Math.PI)/2*2**z)}
function scheduleBasemap(){clearTimeout(state.basemapTimer);state.basemapTimer=setTimeout(()=>{loadOpenMap();loadBasemap()},220)}
function loadOpenMap(){const bounds=viewportBounds(),z=Math.max(2,Math.min(15,currentZoom())),n=2**z,minX=Math.max(0,tileX(bounds[0],z)),maxX=Math.min(n-1,tileX(bounds[2],z)),minY=Math.max(0,tileY(bounds[3],z)),maxY=Math.min(n-1,tileY(bounds[1],z)),request=++state.openMapRequest,tiles=[];state.openMapTiles=[];for(let x=minX;x<=maxX;x++)for(let y=minY;y<=maxY;y++){const image=new Image(),tile={image,bounds:[tileLongitude(x,z),tileLatitude(y+1,z),tileLongitude(x+1,z),tileLatitude(y,z)]};image.crossOrigin='anonymous';image.onload=()=>{if(request!==state.openMapRequest)return;tiles.push(tile);state.openMapTiles=tiles;draw()};image.src=`${OPEN_MAP_TILE_URL}/${z}/${x}/${y}.png`}}
function loadBasemap(){const bounds=viewportBounds(),r=canvas.getBoundingClientRect(),width=Math.max(256,Math.min(1600,Math.round(r.width))),height=Math.max(256,Math.min(1200,Math.round(r.height))),params=new URLSearchParams({SERVICE:'WMS',VERSION:'1.3.0',REQUEST:'GetMap',LAYERS:NOAA_WMS_LAYERS,STYLES:'',CRS:'EPSG:102100',BBOX:[webMercatorX(bounds[0]),webMercatorY(bounds[1]),webMercatorX(bounds[2]),webMercatorY(bounds[3])].join(','),WIDTH:String(width),HEIGHT:String(height),FORMAT:'image/png',TRANSPARENT:'FALSE'}),image=new Image(),request=++state.basemapRequest;$('#mapStatus').textContent=' · NOAA ENC Online loading';image.onload=()=>{if(request!==state.basemapRequest)return;state.basemap={image,bounds};$('#mapStatus').textContent=' · NOAA ENC Online';draw()};image.onerror=()=>{if(request!==state.basemapRequest)return;state.basemap=null;$('#mapStatus').textContent=' · NOAA unavailable, OpenStreetMap background';draw()};image.src=NOAA_WMS_URL+'?'+params}
function updateCount(){const n=state.selected.size;$('#selectionCount').textContent=`${n} approach cell${n===1?'':'s'} selected${state.selectionLabel?` · ${state.selectionLabel}`:''}`}
function fit(ids){const es=state.entries.filter(e=>ids.has(e.chart_id));if(!es.length)return;let b=[180,90,-180,-90];for(const e of es){b[0]=Math.min(b[0],e.bbox[0]);b[1]=Math.min(b[1],e.bbox[1]);b[2]=Math.max(b[2],e.bbox[2]);b[3]=Math.max(b[3],e.bbox[3])}const r=canvas.getBoundingClientRect(),south=mercatorY(b[1]),north=mercatorY(b[3]);state.view.lon=(b[0]+b[2])/2;state.view.lat=latitudeFromMercator((south+north)/2);state.view.scale=Math.min(r.width/Math.max(1,(b[2]-b[0])*1.15),r.height/Math.max(1,(north-south)*1.15));draw();scheduleBasemap()}
canvas.addEventListener('pointerdown',e=>{canvas.setPointerCapture(e.pointerId);state.drag={x:e.offsetX,y:e.offsetY,lon:state.view.lon,mercatorLat:mercatorY(state.view.lat),moved:false}});canvas.addEventListener('pointermove',e=>{if(!state.drag)return;const dx=e.offsetX-state.drag.x,dy=e.offsetY-state.drag.y;if(Math.abs(dx)+Math.abs(dy)>4)state.drag.moved=true;state.view.lon=state.drag.lon-dx/state.view.scale;state.view.lat=latitudeFromMercator(state.drag.mercatorLat+dy/state.view.scale);draw()});canvas.addEventListener('pointerup',e=>{if(!state.drag?.moved){const [lon,lat]=unproject(e.offsetX,e.offsetY);const hits=state.entries.filter(x=>lon>=x.bbox[0]&&lon<=x.bbox[2]&&lat>=x.bbox[1]&&lat<=x.bbox[3]);const hit=hits.sort((a,b)=>(a.bbox[2]-a.bbox[0])-(b.bbox[2]-b.bbox[0]))[0];if(hit){state.selected.has(hit.chart_id)?state.selected.delete(hit.chart_id):state.selected.add(hit.chart_id);state.selectionLabel='';updateCount();draw()}}else scheduleBasemap();state.drag=null});canvas.addEventListener('wheel',e=>{e.preventDefault();const before=unproject(e.offsetX,e.offsetY);state.view.scale=Math.max(1,Math.min(900,state.view.scale*Math.exp(-e.deltaY*.0015)));const after=unproject(e.offsetX,e.offsetY);state.view.lon+=before[0]-after[0];const adjusted=mercatorY(state.view.lat)+mercatorY(before[1])-mercatorY(after[1]);state.view.lat=latitudeFromMercator(adjusted);draw();scheduleBasemap()},{passive:false});
function closeSearch(){const results=$('#searchResults');results.hidden=true;$('#search').setAttribute('aria-expanded','false')}
function regionScore(region,q){const name=region.name.toLowerCase();if(name===q)return 0;if(name.startsWith(q))return 1;if(name.includes(q))return 2;if(region.kind.toLowerCase().includes(q))return 3;if(region.description.toLowerCase().includes(q))return 4;return 99}
function renderSearch(showAll=false){const q=$('#search').value.trim().toLowerCase(),regions=state.regions.map((region,index)=>({region,index,score:showAll||!q?0:regionScore(region,q)})).filter(item=>item.score<99).sort((a,b)=>a.score-b.score||a.index-b.index).slice(0,12).map(item=>item.region),cells=q?state.entries.filter(e=>e.chart_id.toLowerCase().includes(q)||e.title.toLowerCase().includes(q)).slice(0,8):[],parts=[];if(regions.length){parts.push('<div class="search-heading">NOAA regions</div>',...regions.map(r=>`<button class="search-option" type="button" role="option" data-region="${esc(r.id)}"><strong>${esc(r.name)}</strong><span>${esc(r.kind)} · ${r.chart_ids.length} approach cells</span></button>`))}if(cells.length){parts.push('<div class="search-heading">Individual ENC cells</div>',...cells.map(e=>`<button class="search-option" type="button" role="option" data-cell="${esc(e.chart_id)}"><strong>${esc(e.chart_id)}</strong><span>${esc(e.title)}</span></button>`))}const results=$('#searchResults');results.innerHTML=parts.length?parts.join(''):'<div class="empty">No matching NOAA regions or cells.</div>';results.hidden=false;$('#search').setAttribute('aria-expanded','true')}
function openRegion(region){state.pendingRegion=region;$('#regionTitle').textContent=region.name;$('#regionDescription').textContent=`${region.kind} — ${region.description}`;$('#regionCount').textContent=`Select ${region.chart_ids.length} NOAA approach cells?`;closeSearch();$('#regionDialog').showModal()}
$('#searchResults').onclick=e=>{const option=e.target.closest('.search-option');if(!option)return;if(option.dataset.region){const region=state.regions.find(r=>r.id===option.dataset.region);if(region)openRegion(region);return}const cell=state.entries.find(item=>item.chart_id===option.dataset.cell);if(cell){state.selected.add(cell.chart_id);state.selectionLabel='';$('#search').value=`${cell.chart_id} — ${cell.title}`;updateCount();fit(new Set([cell.chart_id]));closeSearch()}};
$('#confirmRegion').onclick=()=>{const region=state.pendingRegion;if(!region)return;state.selected=new Set(region.chart_ids);state.selectionLabel=region.name;$('#search').value=region.name;$('#noaaName').value=`NOAA ENC ${region.name}`;updateCount();fit(state.selected);state.pendingRegion=null;setTimeout(()=>{$('#search').blur();closeSearch()},0)};$('#regionDialog').addEventListener('close',()=>{state.pendingRegion=null});
$('#search').addEventListener('input',()=>renderSearch(false));$('#search').addEventListener('focus',()=>{if($('#search').value.trim())renderSearch(false)});$('#search').addEventListener('keydown',e=>{if(e.key==='Escape')closeSearch();if(e.key==='ArrowDown'){e.preventDefault();$('#searchResults').querySelector('.search-option')?.focus()}if(e.key==='Enter'){const first=$('#searchResults').querySelector('.search-option');if(first&&!$('#searchResults').hidden){e.preventDefault();first.click()}}});$('#search').addEventListener('blur',()=>setTimeout(()=>{if(!$('#searchResults').contains(document.activeElement))closeSearch()},120));
$('#browseRegions').onclick=()=>{$('#search').value='';renderSearch(true);$('#search').focus()};$('#clear').onclick=()=>{state.selected.clear();state.selectionLabel='';$('#search').value='';closeSearch();updateCount();draw()};
const API_BASE=globalThis.CHART_PROVIDER_API_BASE||(location.pathname.startsWith('/plugins/signalk-charts-provider-progressive')?'/plugins/signalk-charts-provider-progressive':'');
async function api(url,options){const r=await fetch(API_BASE+url,{headers:{'content-type':'application/json'},...options});if(!r.ok){let m=await r.text();try{m=JSON.parse(m).detail}catch{}throw Error(m)}return r.json()}
async function boot(){const s=await api('/api/status');const rt=$('#runtime');rt.textContent=s.runtime_version||'Docker/Podman unavailable';rt.classList.toggle('good',!!s.runtime);const c=await api('/api/noaa/catalog');state.entries=c.entries;state.regions=c.regions||[];$('#search').disabled=false;$('#browseRegions').disabled=false;updateCount();resize();await jobs()}boot().catch(e=>{$('#selectionCount').textContent=e.message});
function viewportBounds(){const r=canvas.getBoundingClientRect(),sw=unproject(0,r.height),ne=unproject(r.width,0);return [Math.max(-180,sw[0]),Math.max(-85,sw[1]),Math.min(180,ne[0]),Math.min(85,ne[1])]}
function currentZoom(){return Math.max(0,Math.min(18,Math.round(Math.log2(state.view.scale*360/256))))}
$('#noaaForm').onsubmit=async e=>{e.preventDefault();if(!state.selected.size)return alert('Select at least one NOAA approach cell.');try{await api('/api/progressive/noaa',{method:'POST',body:JSON.stringify({name:$('#noaaName').value,chart_ids:[...state.selected],viewport_bbox:viewportBounds(),current_zoom:currentZoom(),min_zoom:+$('#minZoom').value,max_zoom:+$('#maxZoom').value,parallelism:+$('#parallelism').value,download_workers:2,profile:$('#profile').value})});await jobs()}catch(x){alert(x.message)}};
$('#urlForm').onsubmit=async e=>{e.preventDefault();const urls=$('#urls').value.split(/\n/).map(x=>x.trim()).filter(Boolean);if(!urls.length)return alert('Add at least one direct chart link.');try{await api('/api/jobs/url',{method:'POST',body:JSON.stringify({name:$('#urlName').value,urls,min_zoom:+$('#minZoom').value,max_zoom:+$('#maxZoom').value,parallelism:+$('#parallelism').value,download_workers:4})});await jobs()}catch(x){alert(x.message)}};
function esc(x){return String(x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function taskSummary(counts){return Object.entries(counts).filter(([,n])=>n).map(([status,n])=>`${n} ${status}`).join(' · ')||'No queued work'}
function chartButtons(chart){const a=chart.actions,id=chart.chart_id,buttons=[];if(a.pause)buttons.push(`<button class="btn" onclick="chartAction('${id}','pause')">Pause</button>`);if(a.resume)buttons.push(`<button class="btn accent" onclick="chartAction('${id}','resume')">Resume</button>`);if(a.cancel)buttons.push(`<button class="btn danger" onclick="chartAction('${id}','cancel',true)">Stop build</button>`);if(a.retry)buttons.push(`<button class="btn" onclick="chartAction('${id}','retry')">Retry failed</button>`);if(a.clear_history)buttons.push(`<button class="btn" onclick="chartAction('${id}','history/clear',true)">Clear history</button>`);if(a.delete)buttons.push(`<button class="btn danger" onclick="deleteChart('${id}')">Delete chart</button>`);return buttons.join('')}
async function jobs(){const [list,p]=await Promise.all([api('/api/jobs'),api('/api/progressive/status')]);const chartSets=p.chart_sets.map(c=>`<article class="job"><div class="job-top"><div><div class="job-title">${esc(c.name)}</div><div class="job-phase">${c.generation?`Published ${esc(c.phase)} generation ${esc(c.generation)}`:'No published generation'}</div></div><span class="badge ${esc(c.state)}">${esc(c.state)}</span></div><div class="hint">${esc(taskSummary(c.task_counts))}</div><div class="actions">${chartButtons(c)}</div></article>`);const batches=list.filter(j=>j.kind!=='progressive').map(j=>`<article class="job"><div class="job-top"><div><div class="job-title">${esc(j.name)}</div><div class="job-phase">${esc(j.phase)} · ${Math.round(j.progress*100)}%</div></div><span class="badge ${j.status}">${esc(j.status)}</span></div><progress max="1" value="${j.progress}"></progress>${j.error?`<p class="error">${esc(j.error)}</p>`:''}<div class="actions">${j.outputs.map(f=>`<a class="btn" href="/api/jobs/${j.id}/files/${encodeURIComponent(f)}">Download ${esc(f)}</a>`).join('')}${j.status==='completed'?`<button class="btn accent" onclick="openSideload('${j.id}')">Send to Pi</button>`:''}${j.status==='running'?`<button class="btn danger" onclick="cancelJob('${j.id}')">Cancel</button>`:''}</div><details><summary>Build log</summary><pre>${esc(j.log.join('\n'))}</pre></details></article>`);const cards=[...chartSets,...batches];$('#jobs').innerHTML=cards.length?cards.join(''):'<div class="empty">No builds yet.</div>'}
async function chartAction(id,action,confirmAction=false){if(confirmAction&&!confirm(`${action==='cancel'?'Stop all unfinished work for':'Clear failed and cancelled history for'} ${id}?`))return;try{await api(`/api/progressive/charts/${encodeURIComponent(id)}/${action}`,{method:'POST',body:'{}'});await jobs()}catch(x){alert(x.message)}}
async function deleteChart(id){if(!confirm(`Delete ${id}, its build history, and all published generations?`))return;const purge=confirm('Also remove downloaded NOAA source cells that no other chart set uses? Select Cancel to retain the shared source cache.');try{await api(`/api/progressive/charts/${encodeURIComponent(id)}/delete`,{method:'POST',body:JSON.stringify({confirm_chart_id:id,purge_sources:purge})});await jobs()}catch(x){alert(x.message)}}
$('#refreshJobs').onclick=jobs;async function cancelJob(id){await api(`/api/jobs/${id}/cancel`,{method:'POST'});jobs()}function openSideload(id){$('#sideloadJob').value=id;$('#sideloadError').textContent='';$('#sideloadDialog').showModal()}$('#sideloadForm').addEventListener('submit',async e=>{if(e.submitter?.value!=='send')return;e.preventDefault();$('#sideloadError').textContent='Sending…';try{const r=await api('/api/sideload',{method:'POST',body:JSON.stringify({job_id:$('#sideloadJob').value,target:$('#sshTarget').value,destination:$('#destination').value,mbtiles_dir:$('#mbtilesDir').value,pmtiles_dir:$('#pmtilesDir').value,remote_owner:$('#remoteOwner').value||null,restart_signalk:$('#restartSignalk').checked})});$('#sideloadError').textContent=`Sent ${r.files.length} file(s) to ${r.target}${r.restarted_signalk?' and restarted Signal K':''}`;setTimeout(()=>$('#sideloadDialog').close(),2200)}catch(x){$('#sideloadError').textContent=x.message}});setInterval(jobs,2000);
</script></body></html>'''


def self_test() -> int:
    assert safe_stem(" NOAA ENC / California ") == "NOAA-ENC-California"
    assert overlaps([-2, -2, 1, 1], [1, 1, 3, 3])
    assert not overlaps([-2, -2, 0, 0], [1, 1, 3, 3])
    lon, lat = mercator_coord(-13_358_338.9, 4_430_000)
    assert -121 < lon < -119 and 36 < lat < 38
    def feature(chart_id: str, band: int, west: float, title: str) -> dict[str, Any]:
        return {
            "properties": {
                "enc_ed_up": f"{chart_id}_ED001_UP000",
                "scale_band": band,
                "scale": 22000,
                "title": title,
            },
            "geometry": {
                "coordinates": [
                    [[west, 34], [west + 1, 34], [west + 1, 35], [west, 35], [west, 34]]
                ]
            },
        }

    sample = {
        "features": [
            feature("US4CA123", 4, -120, "California"),
            feature("US4CA124", 4, -119.5, "Neighbor approach"),
            feature("US4OR123", 4, -130, "Oregon approach"),
            feature("US4WA123", 4, -135, "Washington approach"),
            feature("US3CA12M", 3, -120, "Coastal"),
            feature("US5CA12M", 5, -120, "Harbor"),
        ]
    }
    temp = Path("/tmp") / f"chart-baker-test-{uuid.uuid4().hex}"
    try:
        path = temp / "enc.geojson"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(sample))
        catalog = Catalog(path)
        assert catalog.band4()[0].chart_id == "US4CA123"
        included_ids = {item.chart_id for item in catalog.inclusion(["US4CA123"])}
        assert included_ids == {"US3CA12M", "US4CA123", "US5CA12M"}
        regions = {region.id: region for region in catalog_regions(catalog.band4())}
        assert regions["state-ca"].chart_ids == ["US4CA123", "US4CA124"]
        assert regions["state-ca"].kind == "NOAA state package"
        assert regions["west-coast"].chart_ids == [
            "US4CA123",
            "US4CA124",
            "US4OR123",
            "US4WA123",
        ]
        assert regions["pacific-northwest"].chart_ids == [
            "US4OR123",
            "US4WA123",
        ]
        manager = JobManager(temp, None, catalog)
        progressive = ProgressiveController(temp, catalog, manager)
        planned = progressive.create(
            ProgressiveNoaaRequest(
                name="Test progressive ENC",
                chart_ids=["US4CA123"],
                viewport_bbox=[-120, 34, -119, 35],
                current_zoom=12,
                min_zoom=8,
                max_zoom=16,
                parallelism=1,
            )
        )
        assert planned["resolved_cells"] == 3
        assert planned["scheduled"][0]["metadata"]["packet_name"] == "viewport-current"
        assert planned["scheduled"][0]["metadata"]["clip_bounds"] == [
            -120.15,
            33.85,
            -118.85,
            35.15,
        ]
        assert planned["scheduled"][-1]["metadata"]["clip_bounds"] is None
        assert planned["scheduled"][-1]["metadata"]["refine"] is True
        assert "-clipsrc" in EXPORT_SCRIPT
        assert "|| true" not in EXPORT_SCRIPT
        manager.runtime = "podman"
        podman_command = manager._container_base("test", [(temp, "/work", False)])
        assert "--userns=keep-id" in podman_command
        assert "--replace" in podman_command
        manager.runtime = "docker"
        docker_command = manager._container_base("test", [(temp, "/work", False)])
        assert "--userns=keep-id" not in docker_command
        assert container_subprocess_environment(
            {
                "HOME": "/home/signalk",
                "LISTEN_FDS": "1",
                "LISTEN_FDNAMES": "signalk.socket",
                "LISTEN_PID": "123",
            }
        ) == {"HOME": "/home/signalk"}
        assert conservative_depth_meters(12.192) == 12.192
        assert conservative_depth_meters(12.3) == 12.192
        assert conservative_depth_meters(16.4) == 16.1544
        assert enc_band_zoom_ranges(["B03", "B04", "B05"], 4, 16) == {
            "B03": (4, 12),
            "B04": (10, 14),
            "B05": (12, 16),
        }
        assert enc_band_zoom_ranges(["B03", "B04", "B05"], 6, 6) == {
            "B03": (6, 6)
        }
        assert enc_band_zoom_ranges(["B03", "B05"], 18, 18) == {"B05": (18, 18)}
        depth_stream = temp / "depths.geojsonseq"
        depth_stream.write_text(
            '\x1e{"type":"Feature","properties":{"DEPTH":12.3,"VALSOU":16.4,"OTHER":99}}\n',
            encoding="utf-8",
        )
        assert round_s57_depths(depth_stream) == 2
        rounded_feature = json.loads(
            depth_stream.read_text(encoding="utf-8").removeprefix("\x1e")
        )
        assert rounded_feature["properties"] == {
            "DEPTH": 12.192,
            "VALSOU": 16.1544,
            "OTHER": 99,
        }
        assert "<title>Charts Provider Progressive</title>" in UI_HTML
        assert "NOAA_WMS_URL" in UI_HTML
        assert "EPSG:102100" in UI_HTML
        assert "tile.openstreetmap.org" in UI_HTML
        assert 'role="combobox"' in UI_HTML
        assert 'id="regionDialog"' in UI_HTML
        assert "Confirm region" in UI_HTML
        archive = temp / "enc.mbtiles"
        with sqlite3.connect(archive) as database:
            database.execute("CREATE TABLE metadata (name TEXT, value TEXT)")
            database.execute("INSERT INTO metadata VALUES ('type', 'overlay')")
        JobManager._set_enc_metadata(archive, "Test ENC")
        with sqlite3.connect(archive) as database:
            metadata = dict(database.execute("SELECT name, value FROM metadata"))
        assert metadata["name"] == "Test ENC"
        assert metadata["type"] == "S-57"
        assert metadata["format"] == "pbf"
        request = SideloadRequest(job_id="abc")
        assert request.destination == "auto"
        assert request.mbtiles_dir.endswith("/charts-simple")
    finally:
        shutil.rmtree(temp, ignore_errors=True)
    print("Self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--runtime", choices=["auto", "docker", "podman"], default="auto")
    parser.add_argument("--task-workspace-ttl-days", type=int, default=7)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.task_workspace_ttl_days < 0 or args.task_workspace_ttl_days > 365:
        parser.error("--task-workspace-ttl-days must be from 0 through 365")
    uvicorn.run(
        create_app(args.data_dir, args.runtime, args.task_workspace_ttl_days),
        host=args.host,
        port=args.port,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
