import json
import tempfile
import unittest
from pathlib import Path

from progressive_queue import JobKey, LeaseConflict, PriorityClass, ProgressiveQueue


class FakeClock:
    def __init__(self, value=1_700_000_000.0):
        self.value = value

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class ProgressiveQueueTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "queue.json"
        self.clock = FakeClock()
        self.queue = ProgressiveQueue(self.path, lease_seconds=30, clock=self.clock)

    def test_state_is_atomic_json_and_reloads_all_identity_fields(self):
        job = self.queue.enqueue_packet(
            "west-coast",
            "day-s52",
            7,
            priority_class=PriorityClass.ADJACENT_ZOOM,
            zoom_distance=2,
            layers=["DEPARE", "SOUNDG", "DEPARE"],
            metadata={"source": "NOAA"},
        )

        state = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(1, state["version"])
        self.assertEqual(["DEPARE", "SOUNDG"], state["jobs"][0]["layers"])
        self.assertEqual([], list(self.path.parent.glob(".*.tmp")))

        reloaded = ProgressiveQueue(self.path, lease_seconds=30, clock=self.clock)
        restored = reloaded.get(job.id)
        self.assertIsNotNone(restored)
        self.assertEqual(
            '["packet-profile-generation","west-coast","day-s52","7"]',
            restored.key.canonical,
        )
        self.assertEqual("day-s52", restored.profile)
        self.assertEqual({"source": "NOAA"}, restored.metadata)

    def test_deduplicates_by_stable_key_and_promotes_priority(self):
        first = self.queue.enqueue_cell(
            "noaa-enc",
            "US5CA52M",
            priority_class=PriorityClass.SURROUNDING_RING,
            ring=4,
        )
        duplicate = self.queue.enqueue_cell(
            "noaa-enc",
            "US5CA52M",
            priority_class=PriorityClass.VIEWPORT,
            layers=["DEPARE"],
            profile="safety",
        )

        self.assertEqual(first.id, duplicate.id)
        self.assertEqual(1, len(self.queue.jobs()))
        self.assertEqual(PriorityClass.VIEWPORT, duplicate.priority_class)
        self.assertEqual(("DEPARE",), duplicate.layers)
        self.assertEqual("safety", duplicate.profile)

    def test_resubmitting_a_failed_job_requeues_it(self):
        original = self.queue.enqueue_cell("noaa", "US5CA52M")
        leased = self.queue.lease_next("pi-local")
        self.queue.fail(original.id, leased.lease.token, "container failed", retry=False)

        retried = self.queue.enqueue_cell("noaa", "US5CA52M")

        self.assertEqual(original.id, retried.id)
        self.assertEqual("queued", retried.status)
        self.assertEqual(0, retried.attempts)
        self.assertIsNone(retried.error)

    def test_orders_viewport_then_adjacent_zooms_then_rings(self):
        ring_two = self.queue.enqueue_cell(
            "noaa", "ring-2", priority_class=PriorityClass.SURROUNDING_RING, ring=2
        )
        adjacent_two = self.queue.enqueue_cell(
            "noaa", "zoom-2", priority_class=PriorityClass.ADJACENT_ZOOM, zoom_distance=2
        )
        viewport = self.queue.enqueue_cell(
            "noaa", "visible", priority_class=PriorityClass.VIEWPORT
        )
        ring_one = self.queue.enqueue_cell(
            "noaa", "ring-1", priority_class=PriorityClass.SURROUNDING_RING, ring=1
        )
        adjacent_one = self.queue.enqueue_cell(
            "noaa", "zoom-1", priority_class=PriorityClass.ADJACENT_ZOOM, zoom_distance=1
        )

        expected = [viewport.id, adjacent_one.id, adjacent_two.id, ring_one.id, ring_two.id]
        leased = []
        for index in range(5):
            job = self.queue.lease_next("pi-%d" % index)
            leased.append(job.id)
        self.assertEqual(expected, leased)

    def test_preview_completion_requeues_refinement(self):
        original = self.queue.enqueue_cell(
            "noaa", "US4CA11M", priority_class=PriorityClass.VIEWPORT
        )
        preview_work = self.queue.lease_next("pi-local")
        self.assertEqual("preview", preview_work.target_state)
        self.assertEqual("local", preview_work.lease.worker_kind)

        preview = self.queue.complete(original.id, preview_work.lease.token)
        self.assertEqual("preview", preview.chart_state)
        self.assertEqual("refined", preview.target_state)
        self.assertEqual("queued", preview.status)

        refined_work = self.queue.lease_next("pi-local")
        refined = self.queue.complete(original.id, refined_work.lease.token)
        self.assertEqual("refined", refined.chart_state)
        self.assertEqual("complete", refined.status)
        self.assertIsNone(self.queue.lease_next("pi-local"))

    def test_preview_only_work_completes_without_refinement(self):
        job = self.queue.enqueue_cell(
            "noaa", "US4CA11M", metadata={"refine": False}
        )
        work = self.queue.lease_next("pi-local")

        completed = self.queue.complete(job.id, work.lease.token)

        self.assertEqual("preview", completed.chart_state)
        self.assertEqual("complete", completed.status)
        self.assertIsNone(self.queue.lease_next("pi-local"))

    def test_all_visible_previews_are_built_before_visible_refinement(self):
        first = self.queue.enqueue_cell(
            "noaa", "US4CA11M", priority_class=PriorityClass.VIEWPORT
        )
        second = self.queue.enqueue_cell(
            "noaa", "US4CA12M", priority_class=PriorityClass.VIEWPORT
        )
        first_preview = self.queue.lease_next("pi-local")
        self.queue.complete(first.id, first_preview.lease.token)

        next_work = self.queue.lease_next("pi-local")

        self.assertEqual(second.id, next_work.id)
        self.assertEqual("preview", next_work.target_state)

    def test_all_preview_expansion_precedes_refinement(self):
        viewport = self.queue.enqueue_cell(
            "noaa", "visible", priority_class=PriorityClass.VIEWPORT
        )
        adjacent = self.queue.enqueue_cell(
            "noaa",
            "adjacent",
            priority_class=PriorityClass.ADJACENT_ZOOM,
            zoom_distance=1,
        )
        visible_preview = self.queue.lease_next("pi-local")
        self.assertEqual(viewport.id, visible_preview.id)
        self.queue.complete(viewport.id, visible_preview.lease.token)

        next_work = self.queue.lease_next("pi-local")
        self.assertEqual(adjacent.id, next_work.id)
        self.assertEqual("preview", next_work.target_state)

    def test_expired_lease_is_requeued_and_stale_token_is_rejected(self):
        job = self.queue.enqueue_cell("noaa", "US3AK50M")
        first_lease = self.queue.lease_next("pi-local")
        first_token = first_lease.lease.token
        self.clock.advance(31)

        second_lease = self.queue.lease_next("future-workstation", worker_kind="remote")
        self.assertEqual(job.id, second_lease.id)
        self.assertEqual(1, second_lease.attempts)
        self.assertEqual("remote", second_lease.lease.worker_kind)
        self.assertNotEqual(first_token, second_lease.lease.token)
        with self.assertRaises(LeaseConflict):
            self.queue.complete(job.id, first_token)

    def test_lease_survives_reload_and_can_be_reclaimed(self):
        job = self.queue.enqueue_cell("noaa", "US5CA62M")
        leased = self.queue.lease_next("pi-local")

        self.clock.advance(31)
        reloaded = ProgressiveQueue(self.path, lease_seconds=30, clock=self.clock)
        expired = reloaded.requeue_expired()

        self.assertEqual([job.id], [item.id for item in expired])
        self.assertEqual("queued", reloaded.get(job.id).status)
        with self.assertRaises(LeaseConflict):
            reloaded.fail(job.id, leased.lease.token, "late result")

    def test_cell_and_packet_namespaces_do_not_collide(self):
        cell = self.queue.enqueue(JobKey.for_cell("packet", "profile"))
        packet = self.queue.enqueue(JobKey.for_packet("packet", "profile", "1"))
        self.assertNotEqual(cell.id, packet.id)


if __name__ == "__main__":
    unittest.main()
