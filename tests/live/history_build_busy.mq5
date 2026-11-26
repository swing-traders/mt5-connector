#property strict

ulong probeClock = 0;
int probeFile = INVALID_HANDLE;
string probePhase = "";

ulong BuildClock()
{
    return probeClock;
}

bool BuildStep()
{
    if (probeFile != INVALID_HANDLE)
    {
        probeClock += 20000;
        FileWrite(probeFile, probePhase, "work", probeClock, 0);
    }
    return true;
}

template<typename T>
int BuildSize(const T &values[])
{
    BuildStep();
    return ArraySize(values);
}

void SendBusy(const ulong requestId, const uint boundSeconds);

#define GetTickCount64 BuildClock
#define OnInit UnusedOnInit
#define OnDeinit UnusedOnDeinit
#define OnTimer UnusedOnTimer
#define OnTick UnusedOnTick
#define OnTradeTransaction UnusedOnTradeTransaction
#include "ticks_under_test.mqh"
#undef GetTickCount64
#undef OnInit
#undef OnDeinit
#undef OnTimer
#undef OnTick
#undef OnTradeTransaction

void SendBusy(const ulong requestId, const uint boundSeconds)
{
    FileWrite(probeFile, probePhase, "busy", probeClock, boundSeconds);
}

void BeginBuild(const string phase, HistoryRequest *r)
{
    probePhase = phase;
    probeClock = 0;
    r.busyAt = 0;
    FileWrite(probeFile, probePhase, "busy", probeClock, HUB_HISTORY_BUSY_S);
}

void EndBuild()
{
    FileWrite(probeFile, probePhase, "end", probeClock, 0);
}

void OnStart()
{
    Print("history build probe starting");
    FolderCreate("tickgen-raw");
    probeFile = FileOpen("tickgen-raw/history-build-busy.csv",
                         FILE_WRITE | FILE_CSV | FILE_ANSI, ',');
    if (probeFile == INVALID_HANDLE)
        return;
    HistoryRequest *r = new HistoryRequest(1, HUB_CALL_COPY_TICKS_RANGE);
    ArrayResize(g_history, 1);
    g_history[0] = r;
    MqlRates bars[];
    MqlTick a[];
    MqlTick b[];
    bool tickless[];
    ArrayResize(bars, 128);
    ArrayResize(a, 128);
    ArrayResize(b, 128);
    ArrayResize(tickless, 128);
    for (int i = 0; i < 128; i++)
    {
        ZeroMemory(bars[i]);
        bars[i].time = (datetime)(60000 + i * 60);
        bars[i].open = 100;
        bars[i].high = 102;
        bars[i].low = 99;
        bars[i].close = 101;
        bars[i].tick_volume = 10;
        bars[i].spread = 2;
        tickless[i] = true;
        ZeroMemory(a[i]);
        ZeroMemory(b[i]);
        a[i].time_msc = 60000000 + i * 60000;
        b[i].time_msc = a[i].time_msc + 1;
    }
    HistoryRun run;
    MqlTick ticks[];
    BeginBuild("append", r);
    AppendTickless(r, run, bars, tickless, 0, ticks);
    EndBuild();
    BeginBuild("merge", r);
    Merge(a, b, ticks);
    EndBuild();
    BeginBuild("keep_ticks", r);
    KeepTicksWithin(ticks, 0, 100000000);
    EndBuild();
    BeginBuild("keep_rates", r);
    KeepRatesWithin(bars, 0, 100000000);
    EndBuild();
    BeginBuild("off_grid", r);
    FirstOffGrid(bars, 60);
    EndBuild();
    BeginBuild("decrease", r);
    FirstDecrease(ticks);
    EndBuild();
    long prices[];
    BeginBuild("bids", r);
    BarBids(100, 102, 99, 101, 256, 1, prices);
    EndBuild();
    BeginBuild("offsets", r);
    TickOffsets(60000, 256, 256, prices);
    EndBuild();
    ArrayResize(g_history, 0);
    delete r;
    FileWrite(probeFile, "done", "end", probeClock, 0);
    FileClose(probeFile);
    probeFile = INVALID_HANDLE;
    const int outcome = FileOpen("tickgen-raw/outcome.txt", FILE_WRITE | FILE_TXT | FILE_ANSI);
    if (outcome != INVALID_HANDLE)
    {
        FileWrite(outcome, "done");
        FileClose(outcome);
    }
}
