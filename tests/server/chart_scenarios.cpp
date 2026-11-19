void add(long id, const string& symbol, bool allocated = true) {
    charts[id] = {symbol, allocated ? "ticks" : "", ""};
    next_chart = std::max(next_chart, id + 1);
    SymbolSelect(symbol, true);
    if (allocated) RememberOpened(id, ROLE_PUBLISHER, symbol);
}
void pass() {
    close_calls = deselect_calls = 0;
    now_ms += 16;
    OnTimer();
    require(close_calls <= 1, "more than one ChartClose in a pass");
    require(deselect_calls <= 1, "more than one SymbolSelect(false) in a pass");
}
void disappear() {
    for (long id : closing) charts.erase(id);
    closing.clear();
}
bool has(const string& event) {
    return std::find(events.begin(), events.end(), event) != events.end();
}
int main(int argc, char** argv) {
    try {
        charts[1] = {"OWNER", "ticks", ""};
        string scenario = argv[1];
        if (scenario == "opens") {
            for (int i = 0; i < 15; ++i) {
                string symbol = "OLD" + std::to_string(i);
                add(i + 2, symbol);
                QueueChartFrame(HUB_FRAME_CLOSE_CHART, ROLE_PUBLISHER, symbol);
            }
            QueueChartFrame(HUB_FRAME_OPEN_CHART, ROLE_PUBLISHER, "NEW");
            pass();
            require(has("open:NEW"), "a fresh open waited behind the close backlog");
            require(events.front() == "open:NEW", "a close ran before the fresh open");
            for (int i = 1; i < 15; ++i) { disappear(); pass(); }
            require(std::count_if(events.begin(), events.end(), [](const string& e) {
                return e.rfind("close:", 0) == 0;
            }) == 15, "queued closes did not drain in fifteen passes");
        } else if (scenario == "budgets") {
            add(2, "A"); add(3, "B"); add(4, "C");
            QueueChartFrame(HUB_FRAME_CLOSE_CHART, ROLE_PUBLISHER, "A");
            pass(); disappear();
            QueueChartFrame(HUB_FRAME_CLOSE_CHART, ROLE_PUBLISHER, "B");
            QueueChartFrame(HUB_FRAME_CLOSE_CHART, ROLE_PUBLISHER, "C");
            pass();
            require(has("deselect:A"), "a gone chart's unused symbol waited behind queued closes");
            require(has("close:B"), "deselection delayed the oldest queued close");
            require(!has("close:C"), "two queued closes ran in one pass");
        } else if (scenario == "symbols") {
            selected = {"HIDDEN", "IDLE", "HELD", "REQUESTED"};
            hidden.insert("HIDDEN");
            positions.push_back("HELD");
            QueueChartFrame(HUB_FRAME_OPEN_CHART, ROLE_PUBLISHER, "REQUESTED");
            for (int i = 0; i < 100; ++i) pass();
            require(has("deselect:IDLE"), "the selected-symbol cursor did not advance");
            require(!has("deselect:HIDDEN"), "a hidden symbol was deselected");
            require(!has("deselect:HELD"), "a held symbol was deselected");
            require(!has("deselect:REQUESTED"), "a requested symbol was deselected");
        } else if (scenario == "teardown") {
            add(2, "A");
            QueueChartFrame(HUB_FRAME_CLOSE_CHART, ROLE_PUBLISHER, "A");
            pass();
            QueueChartFrame(HUB_FRAME_OPEN_CHART, ROLE_PUBLISHER, "A");
            QueueChartFrame(HUB_FRAME_OPEN_CHART, ROLE_PUBLISHER, "B");
            pass();
            require(has("open:B"), "one symbol's teardown blocked another symbol's open");
            require(!has("open:A"), "an open ignored its own symbol's teardown");
        } else if (scenario == "capacity") {
            for (int i = 2; i <= 100; ++i) add(i, "OLD" + std::to_string(i));
            QueueChartFrame(HUB_FRAME_OPEN_CHART, ROLE_PUBLISHER, "NEW1");
            QueueChartFrame(HUB_FRAME_OPEN_CHART, ROLE_PUBLISHER, "NEW2");
            QueueChartFrame(HUB_FRAME_CLOSE_CHART, ROLE_PUBLISHER, "OLD2");
            QueueChartFrame(HUB_FRAME_CLOSE_CHART, ROLE_PUBLISHER, "OLD3");
            for (int i = 0; i < 4; ++i) { pass(); disappear(); }
            require(has("open:NEW1") && has("open:NEW2"), "waiting closes did not free capacity");
            require(!has("failed:NEW1") && !has("failed:NEW2"), "temporary capacity was reported failed");
        } else if (scenario == "sightings") {
            add(2, "STRAY", false);
            pass(); pass();
            require(!has("close:STRAY"), "the first unallocated sighting closed a chart");
            for (int i = 0; i < 20; ++i) pass();
            require(has("close:STRAY"), "the second unallocated round did not close the chart");
        } else if (scenario == "rollback") {
            template_failures = {"A", "B"};
            QueueChartFrame(HUB_FRAME_OPEN_CHART, ROLE_PUBLISHER, "A");
            QueueChartFrame(HUB_FRAME_OPEN_CHART, ROLE_PUBLISHER, "B");
            pass(); disappear(); pass();
            require(has("failed:A") && has("failed:B"), "template errors lost their answers");
            require(has("close:A") && has("close:B"), "failed templates were not cleaned up");
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n'; return 1;
    }
}
