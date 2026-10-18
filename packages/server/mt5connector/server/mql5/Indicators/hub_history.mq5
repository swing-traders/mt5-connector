// The history indicator a publisher attaches to its own chart: it syncs each window of rates the
// publisher asks for by chart event, never waiting, and signals it by chart event once it settles.
// A read in an indicator answers at once and starts the download it lacks, so a window still
// loading is read again on every calculation and every second; one loading past the history
// deadline is signalled with ERR_HISTORY_TIMEOUT and forgotten. Ticks are not its to sync: a tick
// window without ticks never settles in an indicator.
#property indicator_chart_window
#property indicator_buffers 0
#property indicator_plots 0

#include <hub_wire.mqh>

// An ask: the hub request it serves, the event it came as, and its window — a timeframe and the
// range's bounds or its start and count, in broker seconds.
struct HistoryAsk
{
    long requestId;
    ENUM_HUB_EVENT kind;
    int timeframe;
    long from;
    long to;
    ulong askedAt;
};

HistoryAsk g_asks[];

// The rows the terminal has for the ask's window, its error in `error` and its first row's time in
// `first`, 0 for none.
int ReadWindow(const HistoryAsk &ask, int &error, long &first)
{
    MqlRates rates[];
    int copied = -1;
    ResetLastError();
    if (ask.kind == HUB_EVENT_ASK_RATES)
        copied = CopyRates(_Symbol, (ENUM_TIMEFRAMES)ask.timeframe, (datetime)ask.from,
                           (datetime)ask.to, rates);
    else
        copied = CopyRates(_Symbol, (ENUM_TIMEFRAMES)ask.timeframe, (datetime)ask.from,
                           (int)ask.to, rates);
    error = GetLastError();
    first = 0;
    if (copied > 0)
        first = (long)rates[0].time;
    return copied;
}

// The ready: its lparam the request, its dparam the rows, its sparam "<error>,<first row's time>".
void Signal(const HistoryAsk &ask, const int rows, const int error, const long first)
{
    const string answer = StringFormat("%d,%I64d", error, first);
    if (!EventChartCustom(0, (ushort)HUB_EVENT_RATES_READY, ask.requestId, (double)rows, answer))
        PrintFormat("hub_history: the ready of request %I64d was not sent: %d", ask.requestId,
                    GetLastError());
}

// Reads the ask's window and signals it once settled: rows, none with no error, or an error that is
// not a loading one; or still loading past the history deadline. False while it loads.
bool Answer(const HistoryAsk &ask)
{
    int error = 0;
    long first = 0;
    const int copied = ReadWindow(ask, error, first);
    const bool loading = error == ERR_HISTORY_NOT_FOUND || error == ERR_HISTORY_TIMEOUT
                         || (copied < 0 && error == 0);
    bool settled = true;
    if (copied > 0 || !loading)
    {
        Signal(ask, copied, error, first);
    }
    else if (GetTickCount64() - ask.askedAt >= (ulong)HUB_HISTORY_DEADLINE_S * 1000)
    {
        PrintFormat("hub_history: request %I64d's window did not sync within %d s", ask.requestId,
                    HUB_HISTORY_DEADLINE_S);
        Signal(ask, copied, ERR_HISTORY_TIMEOUT, 0);
    }
    else
    {
        settled = false;
    }
    return settled;
}

void AnswerPending()
{
    for (int i = ArraySize(g_asks) - 1; i >= 0; i--)
        if (Answer(g_asks[i]))
            ArrayRemove(g_asks, i, 1);
}

// An ask's window is "timeframe,from,to" or "timeframe,from,count". It is read at once and kept
// until it settles.
void OnAsk(const ENUM_HUB_EVENT kind, const long requestId, const string window)
{
    string fields[];
    HistoryAsk ask;
    ask.requestId = requestId;
    ask.kind = kind;
    ask.timeframe = 0;
    ask.from = 0;
    ask.to = 0;
    ask.askedAt = GetTickCount64();
    if (StringSplit(window, ',', fields) == 3)
    {
        ask.timeframe = (int)StringToInteger(fields[0]);
        ask.from = StringToInteger(fields[1]);
        ask.to = StringToInteger(fields[2]);
        if (!Answer(ask))
        {
            const int n = ArraySize(g_asks);
            ArrayResize(g_asks, n + 1);
            g_asks[n] = ask;
        }
    }
    else
    {
        PrintFormat("hub_history: request %I64d's window is out of its layout: '%s'", requestId,
                    window);
        Signal(ask, -1, ERR_INVALID_PARAMETER, 0);
    }
}

void OnForget(const long requestId)
{
    for (int i = ArraySize(g_asks) - 1; i >= 0; i--)
        if (g_asks[i].requestId == requestId)
            ArrayRemove(g_asks, i, 1);
}

int OnInit()
{
    IndicatorSetString(INDICATOR_SHORTNAME, HUB_HISTORY_INDICATOR);
    // A quiet market calculates nothing, so the timer reads the pending windows too.
    if (!EventSetTimer(1))
    {
        PrintFormat("hub_history: EventSetTimer failed: %d", GetLastError());
        return INIT_FAILED;
    }
    return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
    EventKillTimer();
}

void OnTimer()
{
    AnswerPending();
}

int OnCalculate(const int rates_total, const int prev_calculated, const int begin,
                const double &price[])
{
    AnswerPending();
    return rates_total;
}

// The publisher's asks and forgets; every other event — this indicator's own signals and the
// chart's among them — is not the indicator's.
void OnChartEvent(const int id, const long &lparam, const double &dparam, const string &sparam)
{
    switch (id - CHARTEVENT_CUSTOM)
    {
    case HUB_EVENT_ASK_RATES:
    case HUB_EVENT_ASK_RATES_FROM:
        OnAsk((ENUM_HUB_EVENT)(id - CHARTEVENT_CUSTOM), lparam, sparam);
        break;
    case HUB_EVENT_FORGET:
        OnForget(lparam);
        break;
    }
}
