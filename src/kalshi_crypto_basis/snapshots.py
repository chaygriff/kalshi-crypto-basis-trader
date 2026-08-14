"""Immutable, deterministic point-in-time snapshot envelopes."""

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from types import MappingProxyType

type JsonValue = bool | int | str | list[JsonValue] | dict[str, JsonValue] | None

_ENVELOPE_FIELDS = {
    "schema_version",
    "source",
    "request_fingerprint",
    "observed_at",
    "ingested_at",
    "parser_version",
    "raw_sha256",
    "normalized",
    "snapshot_id",
    "idempotency_key",
}
_HTTP_METHOD = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")


class SnapshotError(ValueError):
    """Raised when snapshot input cannot be represented deterministically."""


@dataclass(frozen=True, slots=True)
class SnapshotEnvelope:
    """Content-addressed raw evidence and its normalized interpretation."""

    source: str
    request_fingerprint: str
    observed_at: datetime
    ingested_at: datetime
    parser_version: str
    raw_sha256: str
    normalized: object
    snapshot_id: str
    idempotency_key: str

    def __post_init__(self) -> None:
        source = _required_text(self.source, "source")
        request_fingerprint = _required_sha256_id(self.request_fingerprint, "request_fingerprint")
        parser_version = _required_text(self.parser_version, "parser_version")
        raw_sha256 = _required_hex_sha256(self.raw_sha256, "raw_sha256")
        observed_text = _utc_text(self.observed_at)
        ingested_text = _utc_text(self.ingested_at)
        if self.ingested_at < self.observed_at:
            raise SnapshotError("ingested_at precedes observed_at")
        frozen_normalized = _freeze_value(self.normalized)
        expected_snapshot_id, expected_idempotency_key = _identities(
            source=source,
            request_fingerprint=request_fingerprint,
            observed_text=observed_text,
            ingested_text=ingested_text,
            parser_version=parser_version,
            raw_sha256=raw_sha256,
            normalized=frozen_normalized,
        )
        if self.snapshot_id != expected_snapshot_id:
            raise SnapshotError("snapshot identity mismatch")
        if self.idempotency_key != expected_idempotency_key:
            raise SnapshotError("idempotency key mismatch")
        object.__setattr__(self, "normalized", frozen_normalized)

    @classmethod
    def create(
        cls,
        *,
        source: str,
        request_fingerprint: str,
        observed_at: datetime,
        ingested_at: datetime,
        parser_version: str,
        raw_payload: bytes,
        normalized: object,
    ) -> "SnapshotEnvelope":
        source = _required_text(source, "source")
        request_fingerprint = _required_text(request_fingerprint, "request_fingerprint")
        parser_version = _required_text(parser_version, "parser_version")
        observed_text = _utc_text(observed_at)
        ingested_text = _utc_text(ingested_at)
        if ingested_at < observed_at:
            raise SnapshotError("ingested_at precedes observed_at")
        raw_sha256 = hashlib.sha256(raw_payload).hexdigest()
        frozen_normalized = _freeze_value(normalized)
        snapshot_id, idempotency_key = _identities(
            source=source,
            request_fingerprint=request_fingerprint,
            observed_text=observed_text,
            ingested_text=ingested_text,
            parser_version=parser_version,
            raw_sha256=raw_sha256,
            normalized=frozen_normalized,
        )
        return cls(
            source=source,
            request_fingerprint=request_fingerprint,
            observed_at=observed_at,
            ingested_at=ingested_at,
            parser_version=parser_version,
            raw_sha256=raw_sha256,
            normalized=frozen_normalized,
            snapshot_id=snapshot_id,
            idempotency_key=idempotency_key,
        )

    @classmethod
    def from_canonical_json(cls, payload: bytes, *, raw_payload: bytes) -> "SnapshotEnvelope":
        """Validate and replay an envelope against its exact raw evidence."""
        parsed = _parse_json(payload)
        if not isinstance(parsed, dict):
            raise SnapshotError("snapshot envelope must be a JSON object")
        schema_version = parsed.get("schema_version")
        if type(schema_version) is not int or schema_version != 1:
            raise SnapshotError(f"unsupported schema version: {schema_version}")
        if set(parsed) != _ENVELOPE_FIELDS:
            raise SnapshotError("snapshot envelope fields do not match schema version 1")
        raw_sha256 = hashlib.sha256(raw_payload).hexdigest()
        if parsed["raw_sha256"] != raw_sha256:
            raise SnapshotError("raw payload hash mismatch")
        try:
            observed_at = _parse_utc_text(parsed["observed_at"])
            ingested_at = _parse_utc_text(parsed["ingested_at"])
            source = _required_text(parsed["source"], "source")
            request_fingerprint = _required_text(
                parsed["request_fingerprint"], "request_fingerprint"
            )
            parser_version = _required_text(parsed["parser_version"], "parser_version")
        except (TypeError, ValueError) as error:
            raise SnapshotError("snapshot envelope contains invalid field types") from error
        replayed = cls(
            source=source,
            request_fingerprint=request_fingerprint,
            observed_at=observed_at,
            ingested_at=ingested_at,
            parser_version=parser_version,
            raw_sha256=raw_sha256,
            normalized=_decode_canonical_value(parsed["normalized"]),
            snapshot_id=_required_text(parsed["snapshot_id"], "snapshot_id"),
            idempotency_key=_required_text(parsed["idempotency_key"], "idempotency_key"),
        )
        if replayed.to_canonical_json() != payload:
            raise SnapshotError("snapshot envelope is not canonical")
        return replayed

    def to_canonical_json(self) -> bytes:
        """Serialize the complete envelope to deterministic UTF-8 JSON."""
        return _canonical_bytes(
            {
                "schema_version": 1,
                "source": self.source,
                "request_fingerprint": self.request_fingerprint,
                "observed_at": _utc_text(self.observed_at),
                "ingested_at": _utc_text(self.ingested_at),
                "parser_version": self.parser_version,
                "raw_sha256": self.raw_sha256,
                "normalized": self.normalized,
                "snapshot_id": self.snapshot_id,
                "idempotency_key": self.idempotency_key,
            }
        )


class InMemorySnapshotStore:
    """Reference idempotency semantics for tests and replay tooling."""

    def __init__(self) -> None:
        self._by_id: dict[str, SnapshotEnvelope] = {}
        self._by_idempotency_key: dict[str, SnapshotEnvelope] = {}
        self._raw_by_sha256: dict[str, bytes] = {}

    def put(self, snapshot: SnapshotEnvelope, *, raw_payload: bytes) -> SnapshotEnvelope:
        try:
            validated = SnapshotEnvelope(
                source=snapshot.source,
                request_fingerprint=snapshot.request_fingerprint,
                observed_at=snapshot.observed_at,
                ingested_at=snapshot.ingested_at,
                parser_version=snapshot.parser_version,
                raw_sha256=snapshot.raw_sha256,
                normalized=snapshot.normalized,
                snapshot_id=snapshot.snapshot_id,
                idempotency_key=snapshot.idempotency_key,
            )
        except AttributeError as error:
            raise SnapshotError("snapshot envelope is missing required attributes") from error
        raw_sha256 = hashlib.sha256(raw_payload).hexdigest()
        if raw_sha256 != validated.raw_sha256:
            raise SnapshotError("raw payload hash mismatch")
        existing = self._by_idempotency_key.get(validated.idempotency_key)
        if existing is not None:
            if _canonical_bytes(existing.normalized) != _canonical_bytes(validated.normalized):
                raise SnapshotError("idempotency conflict: parser output changed")
            return existing
        self._by_id[validated.snapshot_id] = validated
        self._by_idempotency_key[validated.idempotency_key] = validated
        self._raw_by_sha256[validated.raw_sha256] = bytes(raw_payload)
        return validated

    def get(self, snapshot_id: str) -> SnapshotEnvelope | None:
        return self._by_id.get(snapshot_id)

    def get_raw(self, raw_sha256: str) -> bytes | None:
        return self._raw_by_sha256.get(raw_sha256)


def migrate_canonical_json(payload: bytes, *, target_version: int) -> bytes:
    """Return a canonical envelope only when an explicit migration exists."""
    if type(target_version) is not int or target_version != 1:
        raise SnapshotError(f"unsupported target schema version: {target_version}")
    parsed = _parse_json(payload)
    if not isinstance(parsed, dict):
        raise SnapshotError("snapshot envelope must be a JSON object")
    source_version = parsed.get("schema_version")
    if type(source_version) is not int or source_version != 1:
        raise SnapshotError(f"no migration path from schema version {source_version}")
    if set(parsed) != _ENVELOPE_FIELDS:
        raise SnapshotError("snapshot envelope fields do not match schema version 1")
    try:
        envelope = SnapshotEnvelope(
            source=_required_text(parsed["source"], "source"),
            request_fingerprint=_required_text(
                parsed["request_fingerprint"], "request_fingerprint"
            ),
            observed_at=_parse_utc_text(parsed["observed_at"]),
            ingested_at=_parse_utc_text(parsed["ingested_at"]),
            parser_version=_required_text(parsed["parser_version"], "parser_version"),
            raw_sha256=_required_text(parsed["raw_sha256"], "raw_sha256"),
            normalized=_decode_canonical_value(parsed["normalized"]),
            snapshot_id=_required_text(parsed["snapshot_id"], "snapshot_id"),
            idempotency_key=_required_text(parsed["idempotency_key"], "idempotency_key"),
        )
    except (TypeError, ValueError) as error:
        raise SnapshotError("snapshot envelope contains invalid field types") from error
    if envelope.to_canonical_json() != payload:
        raise SnapshotError("snapshot envelope is not canonical")
    return payload


def canonical_request_fingerprint(method: str, path: str, parameters: object) -> str:
    """Hash a canonical method, path, and parameter object."""
    if not isinstance(method, str) or not method or _HTTP_METHOD.fullmatch(method) is None:
        raise SnapshotError("method must be a non-empty unpadded HTTP token")
    if (
        not isinstance(path, str)
        or not path.startswith("/")
        or path != path.strip()
        or "?" in path
        or "#" in path
    ):
        raise SnapshotError("path must be a query-free absolute path")
    if not isinstance(parameters, Mapping):
        raise SnapshotError("parameters must be a mapping")
    material = _canonical_bytes({"method": method.upper(), "path": path, "parameters": parameters})
    return f"sha256:{hashlib.sha256(material).hexdigest()}"


def _canonical_bytes(value: object) -> bytes:
    canonical = _canonical_value(value)
    return json.dumps(
        canonical,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_value(value: object) -> JsonValue:
    if isinstance(value, float):
        raise SnapshotError("float values are not supported; use Decimal")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise SnapshotError("non-finite Decimal values are not supported")
        return {"$decimal": str(value)}
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise SnapshotError("mapping keys must be strings")
        if "$decimal" in value:
            raise SnapshotError("reserved canonical key: $decimal")
        return {key: _canonical_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_canonical_value(item) for item in value]
    raise SnapshotError(f"unsupported canonical value type: {type(value).__name__}")


def _utc_text(value: datetime) -> str:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise SnapshotError("timestamps must be timezone-aware UTC")
    if offset.total_seconds() != 0:
        raise SnapshotError("timestamps must be normalized to UTC")
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc_text(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise SnapshotError("timestamp must use canonical UTC format")
    parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    if parsed.tzinfo != UTC or _utc_text(parsed) != value:
        raise SnapshotError("timestamp must use canonical UTC format")
    return parsed


def _parse_json(payload: bytes) -> object:
    def reject_duplicate(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise SnapshotError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(payload, object_pairs_hook=reject_duplicate)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SnapshotError("invalid snapshot JSON") from error


def _freeze_value(value: object) -> object:
    if isinstance(value, float):
        raise SnapshotError("float values are not supported; use Decimal")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise SnapshotError("non-finite Decimal values are not supported")
        return value
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise SnapshotError("mapping keys must be strings")
        if "$decimal" in value:
            raise SnapshotError("reserved canonical key: $decimal")
        return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_value(item) for item in value)
    raise SnapshotError(f"unsupported canonical value type: {type(value).__name__}")


def _decode_canonical_value(value: object) -> object:
    if isinstance(value, dict):
        if "$decimal" in value:
            if set(value) != {"$decimal"} or not isinstance(value["$decimal"], str):
                raise SnapshotError("invalid canonical Decimal marker")
            try:
                decimal = Decimal(value["$decimal"])
            except (InvalidOperation, ValueError) as error:
                raise SnapshotError("invalid canonical Decimal value") from error
            if not decimal.is_finite() or str(decimal) != value["$decimal"]:
                raise SnapshotError("invalid canonical Decimal value")
            return decimal
        return {key: _decode_canonical_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode_canonical_value(item) for item in value]
    return value


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise SnapshotError(f"{field} must be a non-empty string")
    return value


def _required_hex_sha256(value: object, field: str) -> str:
    text = _required_text(value, field)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise SnapshotError(f"{field} must be a lowercase SHA-256 digest")
    return text


def _required_sha256_id(value: object, field: str) -> str:
    text = _required_text(value, field)
    prefix = "sha256:"
    if not text.startswith(prefix):
        raise SnapshotError(f"{field} must be a SHA-256 identity")
    _required_hex_sha256(text.removeprefix(prefix), field)
    return text


def _identities(
    *,
    source: str,
    request_fingerprint: str,
    observed_text: str,
    ingested_text: str,
    parser_version: str,
    raw_sha256: str,
    normalized: object,
) -> tuple[str, str]:
    snapshot_material = _canonical_bytes(
        {
            "schema_version": 1,
            "source": source,
            "request_fingerprint": request_fingerprint,
            "observed_at": observed_text,
            "ingested_at": ingested_text,
            "parser_version": parser_version,
            "raw_sha256": raw_sha256,
            "normalized": normalized,
        }
    )
    idempotency_material = _canonical_bytes(
        {
            "source": source,
            "request_fingerprint": request_fingerprint,
            "observed_at": observed_text,
            "parser_version": parser_version,
            "raw_sha256": raw_sha256,
        }
    )
    return (
        f"sha256:{hashlib.sha256(snapshot_material).hexdigest()}",
        f"sha256:{hashlib.sha256(idempotency_material).hexdigest()}",
    )
