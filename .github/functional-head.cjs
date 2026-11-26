const headChanges = new Set([
  "committed", "head_ref_force_pushed", "head_ref_deleted", "head_ref_restored",
]);

module.exports = async function approvedHead(github, context, expected) {
  let sha = context.sha;
  if (context.eventName === "issue_comment") {
    const comment = context.payload.comment;
    const parameters = { ...context.repo, pull_number: context.issue.number };
    const { data: pull } = await github.rest.pulls.get(parameters);
    const timeline = await github.paginate(github.rest.issues.listEventsForTimeline, {
      ...context.repo, issue_number: context.issue.number, per_page: 100,
    });
    const index = timeline.findIndex(
      event => event.event === "commented" && event.id === comment.id,
    );
    if (index < 0) throw new Error("The approval comment is not in the PR timeline");
    if (timeline.slice(index + 1).some(event => headChanges.has(event.event))) {
      throw new Error("The PR head moved after the approval comment");
    }
    const commits = timeline.slice(0, index).filter(event => event.event === "committed");
    sha = commits.at(-1)?.sha;
    if (!sha || pull.head.sha !== sha || pull.updated_at > comment.created_at) {
      throw new Error("The PR head at comment time is no longer current");
    }
    const { data: current } = await github.rest.pulls.get(parameters);
    if (current.head.sha !== sha) throw new Error("The PR head moved while resolving approval");
  }
  if (expected && sha !== expected) throw new Error("The approved commit is no longer current");
  return sha;
};
