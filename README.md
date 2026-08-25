# Charts Provider Progressive for Signal K

Charts Provider Progressive is a local-first Signal K chart provider and conversion service. It makes a useful S-57 vector chart available for the current view first, then fills adjacent zooms, nearby coverage, the selected region, and the refined chart in the background.

A workstation is never required. The Raspberry Pi can complete every queued task using one local worker. The queue already uses expiring worker leases so an optional workstation runner can be added later as an accelerator without becoming a dependency.

## Progressive behavior

1. Open the plugin and search or select NOAA approach coverage.
2. The provider resolves the required coastal, selected approach, and harbor ENC cells.
3. It downloads and retains NOAA's original S-57 ZIPs by cell edition.
4. It builds the visible viewport at the current zoom with a small but useful navigation layer profile.
5. It builds the same viewport across the requested zoom range.
6. It adds a surrounding viewport ring for quick nearby panning.
7. It builds a provisional version of the full selected region.
8. It builds and publishes the refined selected region.

Each publication is an immutable MBTiles generation. Signal K chart URLs include the generation, preventing Freeboard, Binnacle, or the browser cache from retaining provisional tiles after refinement.

The coverage selector draws NOAA ENC Online's rendered chart beneath the selectable ENC cell boundaries. OpenStreetMap remains underneath as a geographic fallback while NOAA is loading or unavailable. If both online map sources are unavailable, the selector retains a coordinate grid so downloaded catalog coverage can still be selected.

The live path remains vector throughout:

```text
NOAA S-57 -> GeoJSONSeq -> MVT MBTiles -> Signal K charts API
```

Binnacle and Freeboard apply their S-57 portrayal at display time. PMTiles packaging is not part of the progressive critical path.

Depth properties above 40 ft are conservatively floored to the nearest whole foot before tiling, then retained in meters. This applies to soundings, depth-area limits, contours, wrecks, rocks, and obstructions.

## Components

- `chart_baker.py`: local catalog UI, NOAA downloads, conversion worker, progressive APIs, and MBTiles tile service.
- `progressive_queue.py`: atomic persistent queue, priority ordering, deduplication, preview/refined state, retries, and worker leases.
- `progressive_provider.py`: immutable artifact registry, MBTiles validation, chart descriptors, and XYZ tile reads.
- `index.js`: thin Signal K plugin bridge. It starts the bundled local backend, registers chart resources, proxies tiles, emits chart deltas, and exposes the management UI under the plugin path.

The Python backend binds to loopback when the Signal K bridge starts it. Management routes are proxied through the Signal K plugin, while tile responses use the normal read-access chart API.

## Requirements

- Signal K with Node 22.5 or newer
- `uv` available to the Signal K service user
- Docker or Podman available locally for GDAL and Tippecanoe conversion
- Enough local storage for source ENCs, immutable generations, and conversion scratch

The pinned conversion images are:

- `ghcr.io/dirkwa/signalk-charts-provider-simple/charts-toolbox:1.1.0`
- `ghcr.io/protomaps/go-pmtiles:v1.31.2`

The second image is used only by legacy manual packaging operations.

## Run the backend directly

```sh
uv run chart_baker.py
```

Open <http://127.0.0.1:8787>.

To force a runtime or choose another data directory:

```sh
uv run chart_baker.py --runtime podman --data-dir "$HOME/Charts/progressive-noaa"
```

The direct service binds only to localhost by default. The Signal K plugin also keeps it on loopback.

## Install as a Signal K plugin

Build the local package:

```sh
npm pack
```

Install the resulting archive through Signal K or use the boat repository's native sideload helper. The plugin is enabled by default for local trial installs. Open its web interface from the Signal K Apps or plugin page.

The plugin starts the bundled backend by default. Its settings allow changing the loopback port or disabling managed startup when the backend is already supervised locally.

## Priority and local resource use

The persistent queue orders work as:

1. Current viewport and zoom
2. Other zooms for the same viewport
3. Surrounding viewport rings
4. Remaining selected coverage
5. Refined conversion

Preview work across all regions completes before refinement begins. The default UI uses one conversion worker so Signal K remains responsive on a Raspberry Pi. GDAL export, Tippecanoe, and tile-join share that configured local CPU budget.

Downloaded ENC ZIPs are content checked and retained. Completed chart generations survive restarts. Expired local leases return to the queue automatically.

## Layer profiles

The first release exposes two profiles:

- **Compatible portrayal** retains the S-57 object classes currently portrayed by Binnacle and Freeboard.
- **Full S-57** retains all usable object classes in the refined generation.

The initial viewport uses a smaller essential profile containing land, depth areas, contours, soundings, hazards, lateral aids, and lights. Artifact identities already include the resolved profile and layer list. A future improvement can add individual layer selection without invalidating downloaded source cells or changing the queue protocol.

Navigation-critical exclusions must remain explicit. A custom layer profile can omit hazards, soundings, aids, or regulatory areas and must never be presented as equivalent to the complete ENC.

## Existing batch tools

Direct URL conversion and manual SSH sideloading remain available for compatibility. They are not required by the progressive provider and do not participate in its local chart publication path.

Supported linked inputs include S-57 ZIPs, GeoJSON, MBTiles, PMTiles, KAP/BSB, and georeferenced TIFF.

## Verification

```sh
uv run chart_baker.py --self-test
python3 -m unittest -v test_progressive_queue.py test_progressive_provider.py
npm test
node --check index.js
```

## Data and safety

Build data defaults to `~/.local/share/signalk-chart-baker`. When the Signal K plugin manages the backend, data is stored beneath the plugin data directory.

Downloads, archive extraction, management proxy responses, and tile responses have explicit size and path limits. MBTiles generations must pass SQLite integrity, metadata, bounds, zoom, S-57 type, layer, and tile-count checks before publication.

Depth display disclaimer: generated vector charts conservatively floor depth values above 40 ft to the nearest whole foot. For example, 40.7 ft is published as 40 ft. This is a display-space policy, not a correction to NOAA source data. Always consult current official chart information and account for datum, tide, vessel draft, squat, and safety margin.

Generated charts are supplemental aids and are not a replacement for official carriage requirements, current source data, prudent seamanship, or independent navigation information.
