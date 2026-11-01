// The tick-generator acceptance's raw collector, a startup script: the venue's own ticks of every
// kind and M1 bars of each window MQL5/Files/tickgen_raw.txt names, read with the terminal's native
// calls alone, written under MQL5/Files/tickgen-raw/ with outcome.txt last, which names M1's first
// date as the terminal held it before any read. The file's first line is the symbol, each further
// line a window's label, first and last broker second.

#define PARAMETERS "tickgen_raw.txt"
#define OUTPUT "tickgen-raw\\"
// How long a read may go on answering loading, or a window's answers keep changing, in seconds.
#define READ_BOUND_S 1800

bool Connected()
{
    return TerminalInfoInteger(TERMINAL_CONNECTED) != 0;
}

void Log(const string text)
{
    PrintFormat("tickgen_raw: %s", text);
}

// The window's ticks of every kind, read until two consecutive reads answer the same count, first
// and last stamp; false past the bound.
bool ReadTicks(const string symbol, const long from, const long to, MqlTick &ticks[])
{
    const ulong started = GetTickCount64();
    int held = -2;
    long heldFirst = 0;
    long heldLast = 0;
    bool settled = false;
    while (!settled && !IsStopped() && GetTickCount64() - started < READ_BOUND_S * 1000)
    {
        ResetLastError();
        const int copied = CopyTicksRange(symbol, ticks, COPY_TICKS_ALL, (ulong)from * 1000,
                                          (ulong)to * 1000 + 999);
        const int error = GetLastError();
        long first = 0;
        long last = 0;
        if (copied > 0)
        {
            first = ticks[0].time_msc;
            last = ticks[copied - 1].time_msc;
        }
        if (copied >= 0 && error == 0 && copied == held && first == heldFirst && last == heldLast)
        {
            settled = true;
        }
        else if (copied >= 0 && error == 0)
        {
            held = copied;
            heldFirst = first;
            heldLast = last;
        }
        else
        {
            held = -2;
            Sleep(1000);
        }
    }
    return settled;
}

// The window's M1 bars, read until two consecutive reads answer the same count, first and last
// open; false past the bound.
bool ReadBars(const string symbol, const long from, const long to, MqlRates &bars[])
{
    const ulong started = GetTickCount64();
    int held = -2;
    long heldFirst = 0;
    long heldLast = 0;
    bool settled = false;
    while (!settled && !IsStopped() && GetTickCount64() - started < READ_BOUND_S * 1000)
    {
        ResetLastError();
        const int copied = CopyRates(symbol, PERIOD_M1, (datetime)from, (datetime)to, bars);
        const int error = GetLastError();
        long first = 0;
        long last = 0;
        if (copied > 0)
        {
            first = (long)bars[0].time;
            last = (long)bars[copied - 1].time;
        }
        if (copied >= 0 && error == 0 && copied == held && first == heldFirst && last == heldLast)
        {
            settled = true;
        }
        else if (copied >= 0 && error == 0)
        {
            held = copied;
            heldFirst = first;
            heldLast = last;
        }
        else
        {
            held = -2;
            Sleep(1000);
        }
    }
    return settled;
}

string Exact(const double value)
{
    return StringFormat("%.17g", value);
}

bool WriteTicks(const string name, const MqlTick &ticks[])
{
    const int file = FileOpen(OUTPUT + name, FILE_WRITE | FILE_TXT | FILE_ANSI);
    bool written = file != INVALID_HANDLE;
    if (written)
    {
        FileWriteString(file, "time,time_msc,bid,ask,last,volume,flags,volume_real\n");
        for (int i = 0; i < ArraySize(ticks); i++)
            FileWriteString(file, StringFormat("%I64d,%I64d,%s,%s,%s,%I64u,%u,%s\n",
                                               (long)ticks[i].time, ticks[i].time_msc,
                                               Exact(ticks[i].bid), Exact(ticks[i].ask),
                                               Exact(ticks[i].last), ticks[i].volume,
                                               ticks[i].flags, Exact(ticks[i].volume_real)));
        FileClose(file);
    }
    return written;
}

bool WriteBars(const string name, const MqlRates &bars[])
{
    const int file = FileOpen(OUTPUT + name, FILE_WRITE | FILE_TXT | FILE_ANSI);
    bool written = file != INVALID_HANDLE;
    if (written)
    {
        FileWriteString(file, "time,open,high,low,close,tick_volume,spread,real_volume\n");
        for (int i = 0; i < ArraySize(bars); i++)
            FileWriteString(file, StringFormat("%I64d,%s,%s,%s,%s,%I64d,%d,%I64d\n",
                                               (long)bars[i].time, Exact(bars[i].open),
                                               Exact(bars[i].high), Exact(bars[i].low),
                                               Exact(bars[i].close), bars[i].tick_volume,
                                               bars[i].spread, bars[i].real_volume));
        FileClose(file);
    }
    return written;
}

void OnStart()
{
    const int parameters = FileOpen(PARAMETERS, FILE_READ | FILE_TXT | FILE_ANSI);
    string outcome = "";
    if (parameters == INVALID_HANDLE)
        outcome = StringFormat("failed: %s could not be opened, error %d", PARAMETERS,
                               GetLastError());
    string symbol = "";
    string labels[];
    long froms[];
    long tos[];
    if (outcome == "")
    {
        symbol = FileReadString(parameters);
        while (!FileIsEnding(parameters))
        {
            string fields[];
            const string line = FileReadString(parameters);
            if (StringSplit(line, ' ', fields) == 3)
            {
                const int n = ArraySize(labels);
                ArrayResize(labels, n + 1);
                ArrayResize(froms, n + 1);
                ArrayResize(tos, n + 1);
                labels[n] = fields[0];
                froms[n] = StringToInteger(fields[1]);
                tos[n] = StringToInteger(fields[2]);
            }
            else if (line != "")
            {
                outcome = StringFormat("failed: the window line '%s' is not three fields", line);
            }
        }
        FileClose(parameters);
    }
    const ulong started = GetTickCount64();
    while (outcome == "" && !Connected() && !IsStopped()
           && GetTickCount64() - started < READ_BOUND_S * 1000)
        Sleep(1000);
    if (outcome == "" && !Connected())
        outcome = "failed: the terminal did not connect";
    if (outcome == "" && !SymbolSelect(symbol, true))
        outcome = StringFormat("failed: SymbolSelect '%s', error %d", symbol, GetLastError());
    long m1First = 0;
    const ulong asked = GetTickCount64();
    while (outcome == ""
           && !(SeriesInfoInteger(symbol, PERIOD_M1, SERIES_FIRSTDATE, m1First) && m1First > 0)
           && !IsStopped() && GetTickCount64() - asked < READ_BOUND_S * 1000)
        Sleep(1000);
    if (outcome == "" && m1First <= 0)
        outcome = "failed: M1's first date is not known";
    string summary = StringFormat("m1_first %I64d\n", m1First);
    for (int w = 0; w < ArraySize(labels) && outcome == ""; w++)
    {
        MqlTick ticks[];
        MqlRates bars[];
        if (!ReadTicks(symbol, froms[w], tos[w], ticks))
            outcome = StringFormat("failed: %s ticks did not settle", labels[w]);
        else if (!ReadBars(symbol, froms[w], tos[w], bars))
            outcome = StringFormat("failed: %s bars did not settle", labels[w]);
        else if (!WriteTicks(labels[w] + "-ticks.csv", ticks)
                 || !WriteBars(labels[w] + "-bars.csv", bars))
            outcome = StringFormat("failed: %s could not be written, error %d", labels[w],
                                   GetLastError());
        else
            summary += StringFormat("%s %d %d\n", labels[w], ArraySize(ticks), ArraySize(bars));
        Log(StringFormat("%s: %d ticks, %d bars", labels[w], ArraySize(ticks), ArraySize(bars)));
    }
    if (outcome == "")
        outcome = "done\n" + summary;
    const int done = FileOpen(OUTPUT + "outcome.txt", FILE_WRITE | FILE_TXT | FILE_ANSI);
    if (done != INVALID_HANDLE)
    {
        FileWriteString(done, outcome);
        FileClose(done);
    }
    Log(outcome);
}
