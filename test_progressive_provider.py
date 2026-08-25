import gzip
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from progressive_provider import (
    ArtifactRegistry,
    GenerationConflict,
    InvalidArtifact,
    read_xyz_tile,
)


def create_mbtiles(path, *, phase_layer="DEPARE", tile=b"vector tile"):
    connection = sqlite3.connect(str(path))
    try:
        connection.execute("CREATE TABLE metadata (name TEXT, value TEXT)")
        connection.execute(
            "CREATE TABLE tiles ("
            "zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)"
        )
        metadata = {
            "name": "NOAA West Coast",
            "description": "Progressive local ENC",
            "bounds": "-125,32,-117,49",
            "minzoom": "2",
            "maxzoom": "4",
            "format": "pbf",
            "type": "S-57",
            "json": json.dumps(
                {"vector_layers": [{"id": phase_layer}, {"id": "SOUNDG"}]}
            ),
        }
        connection.executemany(
            "INSERT INTO metadata (name, value) VALUES (?, ?)", metadata.items()
        )
        connection.execute(
            "INSERT INTO tiles "
            "(zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
            (2, 1, 2, tile),
        )
        connection.commit()
    finally:
        connection.close()


class ArtifactRegistryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "artifacts"
        self.root.mkdir()
        self.state = Path(self.temporary.name) / "registry.json"
        self.preview = self.root / "preview.mbtiles"
        create_mbtiles(self.preview)
        self.registry = ArtifactRegistry(self.state, self.root, clock=lambda: 1_700_000_000)

    def test_registers_and_atomically_reloads_generation(self):
        registered = self.registry.register_mbtiles(
            "noaa-west",
            "generation-1",
            self.preview,
            phase="preview",
            profile="day-s52",
            layers=["DEPARE"],
        )

        state = json.loads(self.state.read_text(encoding="utf-8"))
        self.assertEqual(1, state["version"])
        self.assertEqual("preview.mbtiles", registered.relative_path)
        self.assertEqual(("DEPARE",), registered.layers)
        self.assertEqual([], list(self.state.parent.glob(".*.tmp")))

        reloaded = ArtifactRegistry(self.state, self.root)
        restored = reloaded.chart("noaa-west")
        self.assertEqual("generation-1", restored.active_generation)
        self.assertEqual("day-s52", restored.generations["generation-1"].profile)

    def test_descriptors_keep_chart_id_and_include_generation_url(self):
        self.registry.register_mbtiles(
            "noaa-west", "preview-1", self.preview, phase="preview"
        )
        refined_path = self.root / "refined.mbtiles"
        create_mbtiles(refined_path, phase_layer="LNDARE")
        self.registry.register_mbtiles(
            "noaa-west", "refined-1", refined_path, phase="refined"
        )

        v1 = self.registry.descriptor("noaa-west", api_version=1)
        v2 = self.registry.descriptor(
            "noaa-west", api_version=2, generation="preview-1"
        )

        self.assertEqual("noaa-west", v1["identifier"])
        self.assertEqual("refined", v1["phase"])
        self.assertIn("/noaa-west/generations/refined-1/{z}/{x}/{y}", v1["tilemapUrl"])
        self.assertEqual(["LNDARE", "SOUNDG"], v1["chartLayers"])
        self.assertEqual("preview", v2["phase"])
        self.assertIn("/generations/preview-1/", v2["url"])
        self.assertEqual(["DEPARE", "SOUNDG"], v2["layers"])
        self.assertEqual("S-57", v2["type"])

    def test_generation_is_immutable_but_identical_registration_is_idempotent(self):
        first = self.registry.register_mbtiles(
            "noaa-west", "g1", self.preview, phase="preview"
        )
        again = self.registry.register_mbtiles(
            "noaa-west", "g1", self.preview, phase="preview"
        )
        self.assertEqual(first, again)

        replacement = self.root / "replacement.mbtiles"
        create_mbtiles(replacement, tile=b"different")
        with self.assertRaises(GenerationConflict):
            self.registry.register_mbtiles(
                "noaa-west", "g1", replacement, phase="preview"
            )

    def test_rejects_invalid_metadata_and_selected_layers(self):
        with self.assertRaises(InvalidArtifact):
            self.registry.register_mbtiles(
                "noaa-west",
                "g1",
                self.preview,
                phase="preview",
                layers=["LIGHTS"],
            )
        connection = sqlite3.connect(str(self.preview))
        connection.execute("UPDATE metadata SET value = 'png' WHERE name = 'format'")
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(InvalidArtifact, "format must be pbf"):
            self.registry.register_mbtiles(
                "noaa-west", "g2", self.preview, phase="refined"
            )

    def test_rejects_artifact_outside_configured_root(self):
        outside = Path(self.temporary.name) / "outside.mbtiles"
        create_mbtiles(outside)
        with self.assertRaisesRegex(InvalidArtifact, "escapes"):
            self.registry.register_mbtiles(
                "noaa-west", "g1", outside, phase="preview"
            )

    def test_resolve_rejects_an_artifact_changed_after_registration(self):
        self.registry.register_mbtiles(
            "noaa-west", "g1", self.preview, phase="preview"
        )
        self.assertEqual(
            self.preview.resolve(), self.registry.resolve_generation("noaa-west", "g1")
        )
        with self.preview.open("ab") as handle:
            handle.write(b"changed")

        with self.assertRaisesRegex(InvalidArtifact, "changed after registration"):
            self.registry.resolve_generation("noaa-west", "g1")

    def test_reads_xyz_using_tms_flip_with_gzip_and_etag(self):
        compressed = gzip.compress(b"vector tile")
        tile_path = self.root / "tile.mbtiles"
        create_mbtiles(tile_path, tile=compressed)

        tile = read_xyz_tile(tile_path, 2, 1, 1)

        self.assertEqual(compressed, tile.data)
        self.assertEqual("gzip", tile.headers["Content-Encoding"])
        self.assertEqual(
            '"%s"' % hashlib.sha256(compressed).hexdigest(), tile.headers["ETag"]
        )
        self.assertEqual("application/vnd.mapbox-vector-tile", tile.headers["Content-Type"])
        self.assertIsNone(read_xyz_tile(tile_path, 2, 1, 2))


if __name__ == "__main__":
    unittest.main()
