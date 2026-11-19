#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

using string = std::string;
using datetime = long;
enum ENUM_CHART_ROLE { ROLE_SPAWNER, ROLE_PUBLISHER, ROLE_HISTORY, ROLE_ACCOUNT };
enum ENUM_HUB_FRAME { HUB_FRAME_OPEN_CHART, HUB_FRAME_CLOSE_CHART };
enum ENUM_TIMEFRAMES { PERIOD_M1 };
enum { MQL_PROGRAM_NAME, CHART_COMMENT, CHART_EXPERT_NAME, SYMBOL_VISIBLE };
constexpr int CHART_CLOSE_GONE_SECONDS = 10;
constexpr int ERR_MARKET_UNKNOWN_SYMBOL = 4301;
constexpr int ORDER_SYMBOL = 1;
const string ChartTemplate = "ticks.tpl";
const string HistoryTemplate = "ticks_history.tpl";
const string AccountTemplate = "ticks_account.tpl";
const int RelaySeconds = 5;
const int ReconnectIntervalSec = 3;
const ENUM_CHART_ROLE Role = ROLE_SPAWNER;
bool g_refused = false;
bool g_closing = false;
datetime g_lastReconnect = 0;
unsigned long g_lastRelay = 0;
unsigned long now_ms = 0;
struct Socket { bool isConnected() { return true; } } wss;

template<class T> int ArraySize(const std::vector<T>& a) { return a.size(); }
template<class T> int ArrayResize(std::vector<T>& a, int n, int reserve = 0) {
    a.resize(n); return n;
}
template<class T> void ArrayRemove(std::vector<T>& a, int i, int n) {
    a.erase(a.begin() + i, a.begin() + i + n);
}
template<class T> void ArraySwap(std::vector<T>& a, std::vector<T>& b) { a.swap(b); }
void Push(std::vector<long>& a, long n) { a.push_back(n); }
struct Chart { string symbol; string expert; string comment; };
std::map<long, Chart> charts;
std::set<long> closing;
std::vector<string> selected;
std::set<string> hidden;
std::set<string> template_failures;
std::vector<string> positions;
std::vector<string> orders;
std::vector<string> events;
std::vector<string> logs;
int close_calls = 0;
int deselect_calls = 0;
long next_chart = 2;

void require(bool value, const char* reason) {
    if (!value) throw std::runtime_error(reason);
}
long ChartID() { return 1; }
long ChartFirst() { return charts.empty() ? -1 : charts.begin()->first; }
long ChartNext(long id) {
    auto p = charts.upper_bound(id); return p == charts.end() ? -1 : p->first;
}
string ChartSymbol(long id) { return charts.count(id) ? charts[id].symbol : ""; }
string ChartGetString(long id, int property) {
    require(!closing.count(id), "a synchronous property read reached a closing chart");
    return property == CHART_COMMENT ? charts[id].comment : charts[id].expert;
}
bool ChartGetString(long id, int property, string& value) {
    value = ChartGetString(id, property); return true;
}
ENUM_TIMEFRAMES ChartPeriod(long id) {
    require(!closing.count(id), "ChartPeriod reached a closing chart"); return PERIOD_M1;
}
bool ChartClose(long id) {
    ++close_calls; closing.insert(id); events.push_back("close:" + charts[id].symbol); return true;
}
bool SymbolExist(const string& symbol, bool& custom) { return true; }
bool SymbolSelect(const string& symbol, bool select) {
    if (select) {
        if (std::find(selected.begin(), selected.end(), symbol) == selected.end())
            selected.push_back(symbol);
    } else {
        ++deselect_calls;
        for (const auto& [id, chart] : charts)
            if (chart.symbol == symbol) return false;
        selected.erase(std::remove(selected.begin(), selected.end(), symbol), selected.end());
        events.push_back("deselect:" + symbol);
    }
    return true;
}
long ChartOpen(const string& symbol, ENUM_TIMEFRAMES period) {
    if (charts.size() == 100) return 0;
    long id = next_chart++;
    charts[id] = {symbol, "", ""};
    return id;
}
bool ChartApplyTemplate(long id, const string& path) {
    if (template_failures.count(charts[id].symbol)) return false;
    charts[id].expert = "ticks"; return true;
}
int SymbolsTotal(bool only_selected) { return selected.size(); }
string SymbolName(int i, bool only_selected) {
    return i < (int)selected.size() ? selected[i] : "";
}
long SymbolInfoInteger(const string& symbol, int property) {
    return !hidden.count(symbol) && std::find(selected.begin(), selected.end(), symbol) != selected.end();
}
int PositionsTotal() { return positions.size(); }
string PositionGetSymbol(int i) { return positions[i]; }
int OrdersTotal() { return orders.size(); }
int current_order = 0;
long OrderGetTicket(int i) { current_order = i; return i + 1; }
string OrderGetString(int property) { return orders[current_order]; }
string MQLInfoString(int property) { return "ticks"; }
string ChartRoleName(ENUM_CHART_ROLE role) { return std::to_string(role); }
string EnumToString(ENUM_TIMEFRAMES period) { return "M1"; }
unsigned long GetTickCount64() { return now_ms; }
long TimeLocal() { return now_ms / 1000; }
void ResetLastError() {}
int GetLastError() { return 1; }
template<class... T> void PrintFormat(const char* format, T... args) { logs.push_back(format); }
void SendChartOpened(ENUM_CHART_ROLE role, const string& symbol) { events.push_back("open:" + symbol); }
void SendChartFailed(ENUM_CHART_ROLE role, const string& symbol, int error) {
    events.push_back("failed:" + symbol);
}
void SendChartKept(ENUM_CHART_ROLE role, const string& symbol, int p, int o) {
    events.push_back("kept:" + symbol);
}
void ReadHub() {}
void Reconnect() {}
void PublishTicks() {}
void PublishBars() {}
void ServeHistory() {}
void Relay() { g_lastRelay = now_ms; }
