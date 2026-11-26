const { test } = require("node:test");
const assert = require("node:assert/strict");
const approvedHead = require("./functional-head.cjs");

const sha = "a".repeat(40);
const moved = "b".repeat(40);
const created = "2026-01-02T12:00:00Z";
const context = {
  eventName: "issue_comment",
  repo: { owner: "owner", repo: "repo" },
  issue: { number: 49 },
  payload: { comment: { id: 7, created_at: created } },
};
function api({ timeline, head = sha, updated = created, final = head } = {}) {
  let reads = 0;
  return {
    rest: {
      pulls: { get: async () => ({ data: {
        head: { sha: reads++ ? final : head }, updated_at: updated,
      } }) },
      issues: { listEventsForTimeline: () => {} },
    },
    paginate: async () => timeline ?? [
      { event: "committed", sha },
      { event: "commented", id: 7 },
    ],
  };
}
test("pins the immutable head present at the approval comment", async () => {
  assert.equal(await approvedHead(api(), context), sha);
});
test("refuses a queued job whose head moved after its comment", async () => {
  await assert.rejects(approvedHead(api({
    head: moved,
    timeline: [
      { event: "committed", sha }, { event: "commented", id: 7 },
      { event: "committed", sha: moved },
    ],
  }), context), /moved after/);
});
test("refuses a force push even when the head moves back", async () => {
  await assert.rejects(approvedHead(api({ timeline: [
    { event: "committed", sha }, { event: "commented", id: 7 },
    { event: "head_ref_force_pushed" },
  ] }), context), /moved after/);
});
test("refuses a head movement during approval resolution", async () => {
  await assert.rejects(approvedHead(api({ final: moved }), context), /while resolving/);
});
test("refuses missing timeline evidence and a stale pinned SHA", async () => {
  await assert.rejects(approvedHead(api({ timeline: [] }), context), /not in the PR timeline/);
  await assert.rejects(approvedHead(api(), context, moved), /no longer current/);
});
test("dispatch and tag certification pin their triggering SHA", async () => {
  assert.equal(await approvedHead(null, { eventName: "push", sha }), sha);
  assert.equal(await approvedHead(null, { eventName: "workflow_dispatch", sha }), sha);
});
