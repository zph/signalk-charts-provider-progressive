"""Persistent scheduling primitives for progressive chart conversion.

The queue has no process, network, or chart-format dependencies.  A local worker
can lease work today, while the serialized lease protocol leaves room for a
remote worker transport later.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


STATE_VERSION = 1


class QueueError(RuntimeError):
    """Base class for queue errors."""


class InvalidJob(QueueError):
    """Raised when a job identity or scheduling hint is invalid."""


class LeaseConflict(QueueError):
    """Raised when a worker tries to update a job using a stale lease."""


class PriorityClass(IntEnum):
    """The user-visible order in which chart areas should be improved."""

    VIEWPORT = 0
    ADJACENT_ZOOM = 1
    SURROUNDING_RING = 2
    BACKGROUND = 3


@dataclass(frozen=True)
class JobKey:
    """Stable identity for either a source cell or a generated packet."""

    kind: str
    dataset: Optional[str] = None
    cell: Optional[str] = None
    packet: Optional[str] = None
    profile: Optional[str] = None
    generation: Optional[str] = None

    @classmethod
    def for_cell(cls, dataset: str, cell: str) -> "JobKey":
        return cls(kind="dataset-cell", dataset=dataset, cell=cell).validated()

    @classmethod
    def for_packet(
        cls, packet: str, profile: str, generation: object
    ) -> "JobKey":
        return cls(
            kind="packet-profile-generation",
            packet=packet,
            profile=profile,
            generation=str(generation),
        ).validated()

    def validated(self) -> "JobKey":
        if self.kind == "dataset-cell":
            if not _present(self.dataset) or not _present(self.cell):
                raise InvalidJob("dataset-cell jobs require dataset and cell")
            if any(value is not None for value in (self.packet, self.profile, self.generation)):
                raise InvalidJob("dataset-cell jobs cannot include packet fields")
        elif self.kind == "packet-profile-generation":
            if not all(_present(value) for value in (self.packet, self.profile, self.generation)):
                raise InvalidJob(
                    "packet-profile-generation jobs require packet, profile, and generation"
                )
            if any(value is not None for value in (self.dataset, self.cell)):
                raise InvalidJob("packet jobs cannot include dataset fields")
        else:
            raise InvalidJob("unsupported job key kind: %s" % self.kind)
        return self

    @property
    def canonical(self) -> str:
        self.validated()
        if self.kind == "dataset-cell":
            values = [self.kind, self.dataset, self.cell]
        else:
            values = [self.kind, self.packet, self.profile, self.generation]
        return json.dumps(values, ensure_ascii=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "JobKey":
        return cls(
            kind=str(value.get("kind", "")),
            dataset=_optional_string(value.get("dataset")),
            cell=_optional_string(value.get("cell")),
            packet=_optional_string(value.get("packet")),
            profile=_optional_string(value.get("profile")),
            generation=_optional_string(value.get("generation")),
        ).validated()


@dataclass
class Lease:
    token: str
    worker_id: str
    worker_kind: str
    acquired_at: str
    expires_at: float

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "Lease":
        return cls(
            token=str(value["token"]),
            worker_id=str(value["worker_id"]),
            worker_kind=str(value.get("worker_kind", "local")),
            acquired_at=str(value["acquired_at"]),
            expires_at=float(value["expires_at"]),
        )


@dataclass
class ProgressiveJob:
    id: str
    key: JobKey
    priority_class: int
    zoom_distance: int
    ring: int
    sequence: int
    layers: Tuple[str, ...] = field(default_factory=tuple)
    profile: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: str = "queued"
    chart_state: str = "pending"
    target_state: str = "preview"
    attempts: int = 0
    created_at: str = ""
    updated_at: str = ""
    lease: Optional[Lease] = None
    error: Optional[str] = None

    @property
    def priority(self) -> Tuple[int, int, int, int, int]:
        stage = 0 if self.target_state == "preview" else 1
        return (
            stage,
            self.priority_class,
            self.zoom_distance,
            self.ring,
            self.sequence,
        )

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["layers"] = list(self.layers)
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ProgressiveJob":
        raw_lease = value.get("lease")
        job = cls(
            id=str(value["id"]),
            key=JobKey.from_dict(_mapping(value["key"])),
            priority_class=int(value["priority_class"]),
            zoom_distance=int(value.get("zoom_distance", 0)),
            ring=int(value.get("ring", 0)),
            sequence=int(value["sequence"]),
            layers=_normalize_layers(value.get("layers")),
            profile=_optional_string(value.get("profile")),
            metadata=dict(_mapping(value.get("metadata", {}))),
            status=str(value.get("status", "queued")),
            chart_state=str(value.get("chart_state", "pending")),
            target_state=str(value.get("target_state", "preview")),
            attempts=int(value.get("attempts", 0)),
            created_at=str(value.get("created_at", "")),
            updated_at=str(value.get("updated_at", "")),
            lease=Lease.from_dict(_mapping(raw_lease)) if raw_lease else None,
            error=_optional_string(value.get("error")),
        )
        _validate_job(job)
        return job


class ProgressiveQueue:
    """A small atomic JSON queue suitable for a Raspberry Pi chart provider."""

    def __init__(
        self,
        state_path: Path,
        lease_seconds: float = 300.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.state_path = Path(state_path)
        self.lease_seconds = float(lease_seconds)
        self._clock = clock
        self._lock = threading.RLock()
        self._jobs: Dict[str, ProgressiveJob] = {}
        self._by_key: Dict[str, str] = {}
        self._next_sequence = 0
        self._load()

    def enqueue_cell(
        self,
        dataset: str,
        cell: str,
        *,
        priority_class: PriorityClass = PriorityClass.BACKGROUND,
        zoom_distance: int = 0,
        ring: int = 0,
        layers: Optional[Sequence[str]] = None,
        profile: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> ProgressiveJob:
        return self.enqueue(
            JobKey.for_cell(dataset, cell),
            priority_class=priority_class,
            zoom_distance=zoom_distance,
            ring=ring,
            layers=layers,
            profile=profile,
            metadata=metadata,
        )

    def enqueue_packet(
        self,
        packet: str,
        profile: str,
        generation: object,
        *,
        priority_class: PriorityClass = PriorityClass.BACKGROUND,
        zoom_distance: int = 0,
        ring: int = 0,
        layers: Optional[Sequence[str]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> ProgressiveJob:
        return self.enqueue(
            JobKey.for_packet(packet, profile, generation),
            priority_class=priority_class,
            zoom_distance=zoom_distance,
            ring=ring,
            layers=layers,
            profile=profile,
            metadata=metadata,
        )

    def enqueue(
        self,
        key: JobKey,
        *,
        priority_class: PriorityClass = PriorityClass.BACKGROUND,
        zoom_distance: int = 0,
        ring: int = 0,
        layers: Optional[Sequence[str]] = None,
        profile: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> ProgressiveJob:
        key.validated()
        priority = _validate_priority(priority_class, zoom_distance, ring)
        normalized_layers = _normalize_layers(layers)
        normalized_metadata = dict(metadata or {})
        _json_bytes(normalized_metadata)
        with self._lock:
            existing_id = self._by_key.get(key.canonical)
            if existing_id is not None:
                existing = self._jobs[existing_id]
                changed = False
                if priority < _spatial_priority(existing):
                    existing.priority_class, existing.zoom_distance, existing.ring = priority
                    changed = True
                if normalized_layers and not existing.layers:
                    existing.layers = normalized_layers
                    changed = True
                if profile is not None and existing.profile is None:
                    existing.profile = str(profile)
                    changed = True
                if normalized_metadata:
                    additions = {
                        name: value
                        for name, value in normalized_metadata.items()
                        if name not in existing.metadata
                    }
                    if additions:
                        existing.metadata.update(additions)
                        changed = True
                if changed:
                    existing.updated_at = self._timestamp()
                    self._save_locked()
                return existing

            now = self._timestamp()
            job = ProgressiveJob(
                id=uuid.uuid4().hex,
                key=key,
                priority_class=priority[0],
                zoom_distance=priority[1],
                ring=priority[2],
                sequence=self._next_sequence,
                layers=normalized_layers,
                profile=str(profile) if profile is not None else key.profile,
                metadata=normalized_metadata,
                created_at=now,
                updated_at=now,
            )
            self._next_sequence += 1
            self._jobs[job.id] = job
            self._by_key[key.canonical] = job.id
            self._save_locked()
            return job

    def lease_next(
        self,
        worker_id: str,
        *,
        worker_kind: str = "local",
        lease_seconds: Optional[float] = None,
    ) -> Optional[ProgressiveJob]:
        if not _present(worker_id) or not _present(worker_kind):
            raise ValueError("worker_id and worker_kind are required")
        duration = self.lease_seconds if lease_seconds is None else float(lease_seconds)
        if duration <= 0:
            raise ValueError("lease_seconds must be positive")
        with self._lock:
            changed = self._requeue_expired_locked()
            queued = [job for job in self._jobs.values() if job.status == "queued"]
            if not queued:
                if changed:
                    self._save_locked()
                return None
            job = min(queued, key=lambda candidate: candidate.priority)
            now = self._clock()
            job.status = "leased"
            job.lease = Lease(
                token=uuid.uuid4().hex,
                worker_id=worker_id,
                worker_kind=worker_kind,
                acquired_at=_timestamp(now),
                expires_at=now + duration,
            )
            job.updated_at = _timestamp(now)
            job.error = None
            self._save_locked()
            return job

    def renew(
        self,
        job_id: str,
        lease_token: str,
        lease_seconds: Optional[float] = None,
    ) -> ProgressiveJob:
        duration = self.lease_seconds if lease_seconds is None else float(lease_seconds)
        if duration <= 0:
            raise ValueError("lease_seconds must be positive")
        with self._lock:
            job = self._leased_job(job_id, lease_token)
            now = self._clock()
            if job.lease is None or job.lease.expires_at <= now:
                self._requeue_one(job, now)
                self._save_locked()
                raise LeaseConflict("lease has expired")
            job.lease.expires_at = now + duration
            job.updated_at = _timestamp(now)
            self._save_locked()
            return job

    def complete(self, job_id: str, lease_token: str) -> ProgressiveJob:
        """Finish one pass, automatically queueing refinement after preview."""

        with self._lock:
            job = self._leased_job(job_id, lease_token)
            now = self._clock()
            if job.lease is None or job.lease.expires_at <= now:
                self._requeue_one(job, now)
                self._save_locked()
                raise LeaseConflict("lease has expired")
            job.lease = None
            job.error = None
            if job.target_state == "preview" and job.metadata.get("refine", True):
                job.chart_state = "preview"
                job.target_state = "refined"
                job.status = "queued"
            elif job.target_state == "preview":
                job.chart_state = "preview"
                job.status = "complete"
            else:
                job.chart_state = "refined"
                job.status = "complete"
            job.updated_at = _timestamp(now)
            self._save_locked()
            return job

    def fail(
        self,
        job_id: str,
        lease_token: str,
        error: str,
        *,
        retry: bool = True,
    ) -> ProgressiveJob:
        with self._lock:
            job = self._leased_job(job_id, lease_token)
            now = self._clock()
            if job.lease is None or job.lease.expires_at <= now:
                self._requeue_one(job, now)
                self._save_locked()
                raise LeaseConflict("lease has expired")
            job.lease = None
            job.attempts += 1
            job.error = str(error)
            job.status = "queued" if retry else "failed"
            job.updated_at = _timestamp(now)
            self._save_locked()
            return job

    def requeue_expired(self) -> List[ProgressiveJob]:
        with self._lock:
            expired = self._requeue_expired_locked()
            if expired:
                self._save_locked()
            return expired

    def get(self, job_id: str) -> Optional[ProgressiveJob]:
        with self._lock:
            return self._jobs.get(job_id)

    def get_by_key(self, key: JobKey) -> Optional[ProgressiveJob]:
        with self._lock:
            job_id = self._by_key.get(key.validated().canonical)
            return self._jobs.get(job_id) if job_id else None

    def jobs(self) -> List[ProgressiveJob]:
        with self._lock:
            return sorted(self._jobs.values(), key=lambda job: job.sequence)

    def queued(self) -> List[ProgressiveJob]:
        with self._lock:
            return sorted(
                (job for job in self._jobs.values() if job.status == "queued"),
                key=lambda job: job.priority,
            )

    def _leased_job(self, job_id: str, lease_token: str) -> ProgressiveJob:
        try:
            job = self._jobs[job_id]
        except KeyError as error:
            raise LeaseConflict("unknown job") from error
        if job.status != "leased" or job.lease is None or job.lease.token != lease_token:
            raise LeaseConflict("job is not held by this lease")
        return job

    def _requeue_expired_locked(self) -> List[ProgressiveJob]:
        now = self._clock()
        expired = [
            job
            for job in self._jobs.values()
            if job.status == "leased" and job.lease is not None and job.lease.expires_at <= now
        ]
        for job in expired:
            self._requeue_one(job, now)
        return expired

    @staticmethod
    def _requeue_one(job: ProgressiveJob, now: float) -> None:
        job.status = "queued"
        job.lease = None
        job.attempts += 1
        job.updated_at = _timestamp(now)

    def _load(self) -> None:
        with self._lock:
            if not self.state_path.exists():
                return
            try:
                raw = _mapping(json.loads(self.state_path.read_text(encoding="utf-8")))
                if raw.get("version") != STATE_VERSION:
                    raise QueueError("unsupported queue state version")
                loaded = [ProgressiveJob.from_dict(_mapping(item)) for item in raw.get("jobs", [])]
                by_id = {job.id: job for job in loaded}
                if len(by_id) != len(loaded):
                    raise QueueError("queue state contains duplicate job ids")
                by_key = {job.key.canonical: job.id for job in loaded}
                if len(by_key) != len(loaded):
                    raise QueueError("queue state contains duplicate job keys")
                self._jobs = by_id
                self._by_key = by_key
                stored_next = int(raw.get("next_sequence", 0))
                minimum_next = max((job.sequence for job in loaded), default=-1) + 1
                self._next_sequence = max(stored_next, minimum_next)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                raise QueueError("invalid queue state in %s" % self.state_path) from error

    def _save_locked(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": STATE_VERSION,
            "next_sequence": self._next_sequence,
            "jobs": [job.to_dict() for job in self.jobs()],
        }
        data = _json_bytes(payload)
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

    def _timestamp(self) -> str:
        return _timestamp(self._clock())


def _validate_priority(
    priority_class: PriorityClass, zoom_distance: int, ring: int
) -> Tuple[int, int, int]:
    try:
        priority = PriorityClass(priority_class)
    except ValueError as error:
        raise InvalidJob("unknown priority class") from error
    zoom_distance = int(zoom_distance)
    ring = int(ring)
    if zoom_distance < 0 or ring < 0:
        raise InvalidJob("zoom distance and ring must be nonnegative")
    if priority == PriorityClass.VIEWPORT:
        return int(priority), 0, 0
    if priority == PriorityClass.ADJACENT_ZOOM:
        return int(priority), zoom_distance, 0
    if priority == PriorityClass.SURROUNDING_RING:
        return int(priority), 0, ring
    return int(priority), zoom_distance, ring


def _spatial_priority(job: ProgressiveJob) -> Tuple[int, int, int]:
    return job.priority_class, job.zoom_distance, job.ring


def _validate_job(job: ProgressiveJob) -> None:
    _validate_priority(PriorityClass(job.priority_class), job.zoom_distance, job.ring)
    if job.status not in {"queued", "leased", "complete", "failed"}:
        raise ValueError("invalid job status")
    if job.chart_state not in {"pending", "preview", "refined"}:
        raise ValueError("invalid chart state")
    if job.target_state not in {"preview", "refined"}:
        raise ValueError("invalid target state")
    if (job.status == "leased") != (job.lease is not None):
        raise ValueError("lease and status disagree")


def _normalize_layers(value: Optional[Iterable[object]]) -> Tuple[str, ...]:
    if value is None:
        return ()
    result: List[str] = []
    seen = set()
    for item in value:
        layer = str(item).strip()
        if not layer:
            raise InvalidJob("layer names cannot be empty")
        if layer not in seen:
            seen.add(layer)
            result.append(layer)
    return tuple(result)


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected a JSON object")
    return value


def _present(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _optional_string(value: object) -> Optional[str]:
    return None if value is None else str(value)


def _timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def _json_bytes(value: object) -> bytes:
    try:
        text = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        return text.encode("utf-8")
    except (TypeError, ValueError) as error:
        raise InvalidJob("queue metadata must be JSON serializable") from error
