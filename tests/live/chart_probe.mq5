#property script_show_inputs
#property strict

input string OutputFile = "connector-chart-probe.tsv";
input string CommandsFile = "connector-chart-probe.commands.tsv";
input uint SampleMilliseconds = 10;

enum ProbeEvent
{
    START, CHART_ADDED, CHART_GONE, WATCH_SELECTED, WATCH_GONE, WATCH_HELD,
    CONNECTED, DISCONNECTED, ACK, FAILED, TRADE_OPEN, TRADE_CLOSED
};
enum ProbeCommand
{
    OPEN_STRAY = 1, CLOSE_SYMBOL = 2, SELECT_SYMBOL = 3, FIND_PUBLISHER = 4,
    CREATE_TICKS = 5, BURST_TICKS = 6, TRADE_SESSION = 7, LOG_BOUNDARY = 8
};
struct ChartState { long id; string symbol; };
struct WatchState { string symbol; bool visible; int positions; int orders; };

int output = INVALID_HANDLE;
bool failed = false;
ChartState previousCharts[];
WatchState previousWatch[];

void Record(const ProbeEvent event, const string symbol = "", const long chart = 0,
            const bool selected = false, const bool visible = false,
            const int positions = 0, const int orders = 0, const string token = "")
{
    if (FileWrite(output, GetMicrosecondCount(), EnumToString(event), symbol, chart,
                  (int)selected, (int)visible, positions, orders, token) == 0)
    {
        PrintFormat("chart probe: FileWrite failed: %d", GetLastError());
        failed = true;
    }
}

int ChartAt(const ChartState &rows[], const long id)
{
    for (int i = 0; i < ArraySize(rows); i++)
        if (rows[i].id == id) return i;
    return -1;
}

int WatchAt(const WatchState &rows[], const string symbol)
{
    for (int i = 0; i < ArraySize(rows); i++)
        if (rows[i].symbol == symbol) return i;
    return -1;
}

void Charts()
{
    ChartState current[];
    for (long chart = ChartFirst(); chart != -1; chart = ChartNext(chart))
    {
        const int index = ArraySize(current);
        ArrayResize(current, index + 1);
        current[index].id = chart;
        current[index].symbol = ChartSymbol(chart);
        if (ChartAt(previousCharts, chart) == -1)
            Record(CHART_ADDED, current[index].symbol, chart);
    }
    for (int i = 0; i < ArraySize(previousCharts); i++)
        if (ChartAt(current, previousCharts[i].id) == -1)
            Record(CHART_GONE, previousCharts[i].symbol, previousCharts[i].id);
    ArraySwap(current, previousCharts);
}

void MarketWatch()
{
    WatchState current[];
    for (int i = 0; i < SymbolsTotal(true); i++)
    {
        const string symbol = SymbolName(i, true);
        const int index = ArraySize(current);
        ArrayResize(current, index + 1);
        current[index].symbol = symbol;
        current[index].visible = (bool)SymbolInfoInteger(symbol, SYMBOL_VISIBLE);
        current[index].positions = 0;
        current[index].orders = 0;
        for (int p = 0; p < PositionsTotal(); p++)
            if (PositionGetSymbol(p) == symbol) current[index].positions++;
        for (int o = 0; o < OrdersTotal(); o++)
        {
            const ulong ticket = OrderGetTicket(o);
            if (ticket != 0 && OrderGetString(ORDER_SYMBOL) == symbol) current[index].orders++;
        }
        const int prior = WatchAt(previousWatch, symbol);
        if (prior == -1 || previousWatch[prior].visible != current[index].visible)
            Record(WATCH_SELECTED, symbol, 0, true, current[index].visible,
                   current[index].positions, current[index].orders);
        else if (previousWatch[prior].positions != current[index].positions
                 || previousWatch[prior].orders != current[index].orders)
            Record(WATCH_HELD, symbol, 0, true, current[index].visible,
                   current[index].positions, current[index].orders);
    }
    for (int i = 0; i < ArraySize(previousWatch); i++)
        if (WatchAt(current, previousWatch[i].symbol) == -1)
            Record(WATCH_GONE, previousWatch[i].symbol, 0, false, previousWatch[i].visible,
                   previousWatch[i].positions, previousWatch[i].orders);
    ArraySwap(current, previousWatch);
}

bool ReadTradeSession(const string symbol, bool &open)
{
    open = false;
    MqlDateTime clock;
    if (!TimeToStruct(TimeTradeServer(), clock))
        return false;
    const long seconds = clock.hour * 3600 + clock.min * 60 + clock.sec;
    for (int previous = 0; previous <= 1; previous++)
    {
        const ENUM_DAY_OF_WEEK day = (ENUM_DAY_OF_WEEK)((clock.day_of_week + 7 - previous) % 7);
        datetime from, to;
        for (uint index = 0; SymbolInfoSessionTrade(symbol, day, index, from, to); index++)
        {
            const long start = (long)from % 86400;
            const long end = (long)to % 86400;
            if (previous == 0
                && ((start == end) || (start < end && seconds >= start && seconds < end)
                    || (start > end && seconds >= start)))
            {
                open = true;
                return true;
            }
            if (previous == 1 && start > end && seconds < end)
            {
                open = true;
                return true;
            }
        }
    }
    return true;
}

void Commands()
{
    if (!FileIsExist(CommandsFile, FILE_COMMON)) return;
    const int handle = FileOpen(CommandsFile, FILE_COMMON | FILE_READ | FILE_CSV | FILE_ANSI,
                               '\t', CP_UTF8);
    if (handle == INVALID_HANDLE)
    {
        Record(FAILED);
        failed = true;
        return;
    }
    const string token = FileReadString(handle);
    const int operation = (int)StringToInteger(FileReadString(handle));
    const string symbol = FileReadString(handle);
    FileClose(handle);
    if (!FileDelete(CommandsFile, FILE_COMMON))
    {
        Record(FAILED, symbol, 0, false, false, 0, 0, token);
        failed = true;
        return;
    }
    bool ok = true;
    long opened = 0;
    if (operation == OPEN_STRAY)
    {
        opened = ChartOpen(symbol, PERIOD_M1);
        ok = opened != 0;
    }
    else if (operation == CLOSE_SYMBOL)
    {
        for (long chart = ChartFirst(); chart != -1;)
        {
            const long next = ChartNext(chart);
            if (chart != ChartID() && ChartSymbol(chart) == symbol)
                ok = ChartClose(chart) && ok;
            chart = next;
        }
    }
    else if (operation == SELECT_SYMBOL)
        ok = SymbolSelect(symbol, true);
    else if (operation == FIND_PUBLISHER)
    {
        int count = 0;
        for (long chart = ChartFirst(); chart != -1; chart = ChartNext(chart))
        {
            string expert;
            if (ChartSymbol(chart) == symbol
                && ChartGetString(chart, CHART_EXPERT_NAME, expert) && expert != ""
                && ChartGetString(chart, CHART_COMMENT) == expert + " publisher")
            {
                opened = chart;
                count++;
            }
        }
        ok = count <= 1;
    }
    else if (operation == CREATE_TICKS)
    {
        ok = CustomSymbolCreate(symbol, "ConnectorProbe", _Symbol)
             && CustomSymbolSetInteger(symbol, SYMBOL_TRADE_MODE, SYMBOL_TRADE_MODE_DISABLED)
             && SymbolSelect(symbol, true);
        if (ok)
        {
            MqlTick seed[];
            ArrayResize(seed, 1);
            ZeroMemory(seed[0]);
            seed[0].time = TimeTradeServer() - 10;
            seed[0].time_msc = (long)seed[0].time * 1000;
            seed[0].bid = 1;
            seed[0].ask = 1.0001;
            seed[0].flags = TICK_FLAG_BID | TICK_FLAG_ASK;
            ok = CustomTicksAdd(symbol, seed) == 1;
        }
    }
    else if (operation == BURST_TICKS)
    {
        ok = (bool)SymbolInfoInteger(symbol, SYMBOL_CUSTOM);
        if (ok)
        {
            MqlTick ticks[];
            ArrayResize(ticks, 5000);
            const long first = (long)TimeTradeServer() * 1000 - 5000;
            for (int i = 0; i < ArraySize(ticks); i++)
            {
                ZeroMemory(ticks[i]);
                ticks[i].time_msc = first + i;
                ticks[i].time = (datetime)(ticks[i].time_msc / 1000);
                ticks[i].bid = 1 + i * 0.00001;
                ticks[i].ask = ticks[i].bid + 0.0001;
                ticks[i].flags = TICK_FLAG_BID | TICK_FLAG_ASK;
            }
            ok = CustomTicksAdd(symbol, ticks) == ArraySize(ticks);
        }
    }
    else if (operation == TRADE_SESSION)
    {
        bool custom = false;
        ok = SymbolExist(symbol, custom) && (bool)TerminalInfoInteger(TERMINAL_CONNECTED);
        bool open = false;
        if (ok)
            ok = ReadTradeSession(symbol, open);
        if (ok)
            Record(open ? TRADE_OPEN : TRADE_CLOSED,
                   symbol, 0, false, false, 0, 0, token);
    }
    else if (operation == LOG_BOUNDARY)
        PrintFormat("chart probe: log boundary %s", token);
    else
        ok = false;
    if (ok) Record(ACK, symbol, opened, false, false, 0, 0, token);
    else
    {
        Record(FAILED, symbol, 0, false, false, 0, 0, token);
        failed = true;
    }
}

void OnStart()
{
    if (SampleMilliseconds == 0)
    {
        Print("chart probe: SampleMilliseconds must be positive");
        return;
    }
    output = FileOpen(OutputFile, FILE_COMMON | FILE_WRITE | FILE_CSV | FILE_SHARE_READ
                      | FILE_ANSI, '\t', CP_UTF8);
    if (output == INVALID_HANDLE)
    {
        PrintFormat("chart probe: FileOpen failed: %d", GetLastError());
        return;
    }
    FileWrite(output, "microseconds", "event", "symbol", "chart", "selected", "visible",
              "positions", "orders", "token");
    Record(START);
    bool connected = (bool)TerminalInfoInteger(TERMINAL_CONNECTED);
    Record(connected ? CONNECTED : DISCONNECTED);
    while (!IsStopped() && !failed)
    {
        Commands();
        Charts();
        MarketWatch();
        const bool nowConnected = (bool)TerminalInfoInteger(TERMINAL_CONNECTED);
        if (nowConnected != connected)
        {
            connected = nowConnected;
            Record(connected ? CONNECTED : DISCONNECTED);
        }
        FileFlush(output);
        Sleep(SampleMilliseconds);
    }
    FileFlush(output);
    FileClose(output);
}
