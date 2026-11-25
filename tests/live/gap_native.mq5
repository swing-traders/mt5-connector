#property strict

#define CUSTOM "BTCUSD.a.GAPTEST"
#define OUTPUT "reference-floor\\"

bool LoadTicks(MqlTick &rows[])
{
    const int file = FileOpen("native-ticks.csv", FILE_READ | FILE_CSV | FILE_ANSI, ',');
    if (file == INVALID_HANDLE)
        return false;
    for (int i = 0; i < 8; i++)
        FileReadString(file);
    while (!FileIsEnding(file))
    {
        const string time = FileReadString(file);
        if (time == "" && FileIsEnding(file))
            break;
        const int n = ArraySize(rows);
        ArrayResize(rows, n + 1, 4096);
        rows[n].time = (datetime)StringToInteger(time);
        rows[n].time_msc = StringToInteger(FileReadString(file));
        rows[n].bid = StringToDouble(FileReadString(file));
        rows[n].ask = StringToDouble(FileReadString(file));
        rows[n].last = StringToDouble(FileReadString(file));
        rows[n].volume = (ulong)StringToInteger(FileReadString(file));
        rows[n].flags = (uint)StringToInteger(FileReadString(file));
        rows[n].volume_real = StringToDouble(FileReadString(file));
    }
    FileClose(file);
    return ArraySize(rows) > 0;
}

bool LoadBars(MqlRates &rows[])
{
    const int file = FileOpen("native-bars.csv", FILE_READ | FILE_CSV | FILE_ANSI, ',');
    if (file == INVALID_HANDLE)
        return false;
    for (int i = 0; i < 8; i++)
        FileReadString(file);
    while (!FileIsEnding(file))
    {
        const string time = FileReadString(file);
        if (time == "" && FileIsEnding(file))
            break;
        const int n = ArraySize(rows);
        ArrayResize(rows, n + 1, 4096);
        rows[n].time = (datetime)StringToInteger(time);
        rows[n].open = StringToDouble(FileReadString(file));
        rows[n].high = StringToDouble(FileReadString(file));
        rows[n].low = StringToDouble(FileReadString(file));
        rows[n].close = StringToDouble(FileReadString(file));
        rows[n].tick_volume = StringToInteger(FileReadString(file));
        rows[n].spread = (int)StringToInteger(FileReadString(file));
        rows[n].real_volume = StringToInteger(FileReadString(file));
    }
    FileClose(file);
    return ArraySize(rows) >= 3;
}

void TicksFile(const string name, const MqlTick &rows[])
{
    const int file = FileOpen(OUTPUT + name, FILE_WRITE | FILE_TXT | FILE_ANSI);
    FileWriteString(file, "time,time_msc,bid,ask,last,volume,flags,volume_real\n");
    for (int i = 0; i < ArraySize(rows); i++)
        FileWriteString(file, StringFormat("%I64d,%I64d,%.17g,%.17g,%.17g,%I64u,%u,%.17g\n",
                                         (long)rows[i].time, rows[i].time_msc, rows[i].bid,
                                         rows[i].ask, rows[i].last, rows[i].volume,
                                         rows[i].flags, rows[i].volume_real));
    FileClose(file);
}

void Fact(const int file, const string name, const long value)
{
    FileWriteString(file, StringFormat("%s,%I64d\n", name, value));
}

void OnStart()
{
    MqlTick original[];
    MqlRates bars[];
    if (!LoadTicks(original) || !LoadBars(bars))
    {
        Print("CUSTOM_PROBE_INPUT_FAILED");
        return;
    }
    const long lo = (long)bars[0].time;
    const long hi = (long)bars[ArraySize(bars) - 1].time + 59;
    const long gap = 1795140120;
    ResetLastError();
    const bool readOnly = FileIsExist("read-only.marker");
    const bool reload = FileIsExist("reload.marker");
    if (!readOnly && !reload && !CustomSymbolCreate(CUSTOM, "NativeGapProof", "BTCUSD.a"))
    {
        PrintFormat("CUSTOM_PROBE_CREATE_FAILED %d", GetLastError());
        return;
    }
    if (!SymbolSelect(CUSTOM, true))
        return;
    int written = 0;
    int deleted = 0;
    int rates = 0;
    if (!readOnly)
    {
        written = CustomTicksReplace(CUSTOM, lo * 1000, hi * 1000 + 999, original);
        deleted = CustomTicksDelete(CUSTOM, gap * 1000, (gap + 60) * 1000 - 1);
        rates = CustomRatesReplace(CUSTOM, (datetime)lo, (datetime)hi, bars);
    }
    MqlTick missing[];
    MqlTick preceding[];
    MqlTick all[];
    MqlRates active[];
    ResetLastError();
    const int empty = CopyTicksRange(CUSTOM, missing, COPY_TICKS_ALL, gap * 1000,
                                    (gap + 60) * 1000 - 1);
    const int emptyError = GetLastError();
    ResetLastError();
    const int before = CopyTicksRange(CUSTOM, preceding, COPY_TICKS_ALL, lo * 1000,
                                     gap * 1000 - 1);
    const int beforeError = GetLastError();
    ResetLastError();
    const int held = CopyRates(CUSTOM, PERIOD_M1, (datetime)gap, (datetime)(gap + 59), active);
    const int barError = GetLastError();
    ResetLastError();
    const int total = CopyTicks(CUSTOM, all, COPY_TICKS_ALL, 1, UINT_MAX);
    const int totalError = GetLastError();
    long serverFirst = 0;
    long terminalFirst = 0;
    long frameFirst = 0;
    ResetLastError();
    const bool serverKnown = SeriesInfoInteger(CUSTOM, PERIOD_M1, SERIES_SERVER_FIRSTDATE,
                                               serverFirst);
    const int serverError = GetLastError();
    const bool terminalKnown = SeriesInfoInteger(CUSTOM, PERIOD_M1, SERIES_TERMINAL_FIRSTDATE,
                                                 terminalFirst);
    SeriesInfoInteger(CUSTOM, PERIOD_M1, SERIES_FIRSTDATE, frameFirst);
    MqlRates deep[];
    const int deepCount = CopyRates(CUSTOM, PERIOD_M1, (datetime)1, (datetime)61, deep);
    long afterServer = 0;
    SeriesInfoInteger(CUSTOM, PERIOD_M1, SERIES_SERVER_FIRSTDATE, afterServer);
    const int meta = FileOpen(OUTPUT + "metadata.csv", FILE_WRITE | FILE_TXT | FILE_ANSI);
    FileWriteString(meta, "key,value\n");
    Fact(meta, "terminal_build", TerminalInfoInteger(TERMINAL_BUILD));
    Fact(meta, "first_imported_bar", lo);
    Fact(meta, "last_imported_bar", hi - 59);
    Fact(meta, "read_only", readOnly);
    Fact(meta, "reload", reload);
    Fact(meta, "custom", SymbolInfoInteger(CUSTOM, SYMBOL_CUSTOM));
    Fact(meta, "written_ticks", written);
    Fact(meta, "deleted_ticks", deleted);
    Fact(meta, "written_bars", rates);
    Fact(meta, "gap_minute", gap);
    Fact(meta, "gap_ticks", empty);
    Fact(meta, "gap_ticks_error", emptyError);
    Fact(meta, "preceding_ticks", before);
    Fact(meta, "preceding_ticks_error", beforeError);
    Fact(meta, "gap_bars", held);
    Fact(meta, "gap_bars_error", barError);
    if (held == 1)
    {
        Fact(meta, "gap_bar_time", (long)active[0].time);
        Fact(meta, "gap_bar_volume", active[0].tick_volume);
        Fact(meta, "gap_bar_spread", active[0].spread);
    }
    Fact(meta, "full_ticks", total);
    Fact(meta, "full_ticks_error", totalError);
    Fact(meta, "server_first_known", serverKnown);
    Fact(meta, "server_first", serverFirst);
    Fact(meta, "server_first_error", serverError);
    Fact(meta, "terminal_first_known", terminalKnown);
    Fact(meta, "terminal_first", terminalFirst);
    Fact(meta, "frame_first", frameFirst);
    Fact(meta, "deep_read_rows", deepCount);
    Fact(meta, "server_first_after_deep_read", afterServer);
    FileClose(meta);
    TicksFile("preceding.csv", preceding);
    TicksFile("all.csv", all);
    const int barFile = FileOpen(OUTPUT + "gap-bar.csv", FILE_WRITE | FILE_TXT | FILE_ANSI);
    FileWriteString(barFile, "time,open,high,low,close,tick_volume,spread,real_volume\n");
    for (int i = 0; i < ArraySize(active); i++)
        FileWriteString(barFile, StringFormat("%I64d,%.17g,%.17g,%.17g,%.17g,%I64d,%d,%I64d\n",
                                            (long)active[i].time, active[i].open, active[i].high,
                                            active[i].low, active[i].close, active[i].tick_volume,
                                            active[i].spread, active[i].real_volume));
    FileClose(barFile);
    const int outcome = FileOpen(OUTPUT + "outcome.txt", FILE_WRITE | FILE_TXT | FILE_ANSI);
    FileWriteString(outcome, "done");
    FileClose(outcome);
    Print("CUSTOM_PROBE_INPUT_RECORDED");
}
