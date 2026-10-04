//+------------------------------------------------------------------+
//|                                                       ticks.mq5  |
//|  Publishes its chart symbol's trade transactions, the ticks and  |
//|  closed bars the hub wants of it, its commission schedule and    |
//|  the trade server's time to the WS hub; the spawner also opens   |
//|  and closes the charts the hub asks it to                        |
//+------------------------------------------------------------------+
#property script_show_inputs

//+------------------------------------------------------------------+
//| I N P U T S                                                      |
//+------------------------------------------------------------------+
input string Server = "ws://127.0.0.1:9000/";
input int ReconnectIntervalSec = 3;
input int RelaySeconds = 5;
input int PollMilliseconds = 250;
input bool Spawner = false;
input string ChartTemplate = "ticks.tpl";
// The longest the hub takes to drop a dead connection: its ping interval, ping timeout and close
// timeout together.
input int HubPingTimeoutSec = 50;

#include <MQL5Book/AutoPtr.mqh>
#include <MQL5Book/ws/wsclient.mqh>
#include <JAson.mqh>

#define PROTOCOL_VERSION 1
// The most new ticks one read takes; the next read resumes where it stopped.
#define TICK_BATCH 1000
// How often the spawner tries again to take a symbol whose chart it closed out of Market Watch
// while open positions or pending orders hold it.
#define HELD_DESELECT_SECONDS 900

// The kinds of the frames the EA sends and reads, and the role it declares.
enum ENUM_FRAME_KIND
{
    FRAME_HELLO,
    FRAME_WANTED,
    FRAME_OPEN_CHART,
    FRAME_CLOSE_CHART,
    FRAME_CHART_OPENED,
    FRAME_CHART_FAILED,
    FRAME_CHART_KEPT,
    FRAME_DUPLICATE,
    FRAME_TICK,
    FRAME_BAR,
    FRAME_TRADE_TRANSACTION,
    FRAME_SERVER_TIME,
    FRAME_COMMISSIONS
};

enum ENUM_ROLE
{
    ROLE_EA
};

// Whether the tick cursor knows how many ticks of its millisecond came before it.
enum ENUM_TICK_CURSOR_STATE
{
    CURSOR_COUNT_PENDING,
    CURSOR_READY
};
WebSocketClient<Hybi> *wss = NULL;
bool g_helloSent = false;
datetime g_lastReconnect = 0;
ulong g_lastRelay = 0;
// Set once the EA has asked for its chart to close: it reads, publishes and reconnects no more.
bool g_closing = false;
// When the hub first refused the spawner's hello as a duplicate since it last took one, by
// GetTickCount64; zero once the hub takes its hello.
ulong g_firstRefusal = 0;

// Whether the hub wants the symbol's ticks, with the time_msc of the last tick consumed, how many
// consumed ticks share that millisecond, and whether that count is complete: until it is, the EA
// publishes no tick.
bool g_wantTicks = false;
long g_tickCursorMsc = 0;
int g_tickCursorCount = 0;
ENUM_TICK_CURSOR_STATE g_tickCursorState = CURSOR_COUNT_PENDING;

// The timeframes whose closed bars the hub wants, each with the open of the bar forming when last
// read; zero until one is read.
ENUM_TIMEFRAMES g_barTimeframes[];
datetime g_barOpens[];

// The charts the spawner opened, each with the symbol it was opened for.
long g_openedCharts[];
string g_openedSymbols[];

// The symbols whose charts the spawner closed that are still in Market Watch, each with when to try
// the deselect again, by GetTickCount64, and whether positions or orders were found holding it.
string g_deselectSymbols[];
ulong g_deselectAt[];
bool g_deselectHeld[];

//+------------------------------------------------------------------+
//| JSON values                                                      |
//+------------------------------------------------------------------+
string Quote(const string value)
{
    string quoted = "\"";
    const int length = StringLen(value);
    for (int i = 0; i < length; i++)
    {
        const ushort c = StringGetCharacter(value, i);
        if (c == '"')
            quoted += "\\\"";
        else if (c == '\\')
            quoted += "\\\\";
        else if (c < 0x20)
            quoted += StringFormat("\\u%04x", c);
        else
            quoted += ShortToString(c);
    }
    return quoted + "\"";
}

string Number(const double value)
{
    // Seventeen significant digits read back as the same double; JSON has no NaN or infinity.
    if (!MathIsValidNumber(value))
        return "null";
    return StringFormat("%.17g", value);
}

string Unsigned(const ulong value)
{
    return StringFormat("%I64u", value);
}

string Boolean(const bool value)
{
    if (value)
        return "true";
    else
        return "false";
}

//+------------------------------------------------------------------+
//| The protocol's names                                             |
//+------------------------------------------------------------------+
string FrameName(const ENUM_FRAME_KIND kind)
{
    string name = "";
    switch (kind)
    {
    case FRAME_HELLO:
        name = "hello";
        break;
    case FRAME_WANTED:
        name = "wanted";
        break;
    case FRAME_OPEN_CHART:
        name = "open_chart";
        break;
    case FRAME_CLOSE_CHART:
        name = "close_chart";
        break;
    case FRAME_CHART_OPENED:
        name = "chart_opened";
        break;
    case FRAME_CHART_FAILED:
        name = "chart_failed";
        break;
    case FRAME_CHART_KEPT:
        name = "chart_kept";
        break;
    case FRAME_DUPLICATE:
        name = "duplicate";
        break;
    case FRAME_TICK:
        name = "tick";
        break;
    case FRAME_BAR:
        name = "bar";
        break;
    case FRAME_TRADE_TRANSACTION:
        name = "trade_transaction";
        break;
    case FRAME_SERVER_TIME:
        name = "server_time";
        break;
    case FRAME_COMMISSIONS:
        name = "commissions";
        break;
    }
    return name;
}

string RoleName(const ENUM_ROLE role)
{
    string name = "";
    switch (role)
    {
    case ROLE_EA:
        name = "ea";
        break;
    }
    return name;
}

string Envelope(const ENUM_FRAME_KIND kind)
{
    return "\"v\":" + IntegerToString(PROTOCOL_VERSION) + ",\"type\":" + Quote(FrameName(kind));
}

void Send(const string frame)
{
    // The library waits on every send to a lost connection; the first lost send tells isConnected.
    if (wss.isConnected())
        wss.send(frame);
}

//+------------------------------------------------------------------+
//| Announce this EA, its symbol and whether it is the spawner, to   |
//| the hub (once per connect)                                       |
//+------------------------------------------------------------------+
void SendHello()
{
    if (g_helloSent || wss == NULL || !wss.isConnected())
        return;
    Send("{" + Envelope(FRAME_HELLO)
             + ",\"role\":" + Quote(RoleName(ROLE_EA))
             + ",\"symbol\":" + Quote(_Symbol)
             + ",\"spawner\":" + Boolean(Spawner)
             + "}");
    g_helloSent = true;
    Print("Hello Send");
}

//+------------------------------------------------------------------+
//| Relay the trade server's time to the server through the hub      |
//+------------------------------------------------------------------+
void SendServerTime()
{
    Send("{" + Envelope(FRAME_SERVER_TIME)
             + ",\"symbol\":" + Quote(_Symbol)
             + ",\"trade_server\":" + IntegerToString((long)TimeTradeServer())
             + ",\"current\":" + IntegerToString((long)TimeCurrent())
             + ",\"gmt\":" + IntegerToString((long)TimeGMT())
             + ",\"connected\":" + IntegerToString(TerminalInfoInteger(TERMINAL_CONNECTED))
             + "}");
}

//+------------------------------------------------------------------+
//| Relay the symbol's commission rules, their enums by name         |
//+------------------------------------------------------------------+
void SendCommissions()
{
    MqlCommission rules[];
    ResetLastError();
    const int ret = SymbolInfoCommissions(_Symbol, rules);
    const int error = GetLastError();
    string frame = "{" + Envelope(FRAME_COMMISSIONS)
                   + ",\"symbol\":" + Quote(_Symbol)
                   + ",\"ret\":" + IntegerToString(ret)
                   + ",\"last_error\":" + IntegerToString(error)
                   + ",\"rules\":[";
    for (int i = 0; i < ArraySize(rules); i++)
    {
        if (i > 0)
            frame += ",";
        frame += "{\"currency\":" + Quote(rules[i].currency)
                 + ",\"mode_range\":" + Quote(EnumToString(rules[i].mode_range))
                 + ",\"mode_charge\":" + Quote(EnumToString(rules[i].mode_charge))
                 + ",\"mode_entry\":" + Quote(EnumToString(rules[i].mode_entry))
                 + ",\"mode_direction\":" + Quote(EnumToString(rules[i].mode_direction))
                 + ",\"mode_profit\":" + Quote(EnumToString(rules[i].mode_profit))
                 + ",\"tiers\":[";
        for (int j = 0; j < ArraySize(rules[i].tiers); j++)
        {
            if (j > 0)
                frame += ",";
            frame += "{\"mode\":" + Quote(EnumToString(rules[i].tiers[j].mode))
                     + ",\"volume_type\":" + Quote(EnumToString(rules[i].tiers[j].volume_type))
                     + ",\"value\":" + Number(rules[i].tiers[j].value)
                     + ",\"min_value\":" + Number(rules[i].tiers[j].min_value)
                     + ",\"max_value\":" + Number(rules[i].tiers[j].max_value)
                     + ",\"range_from\":" + Number(rules[i].tiers[j].range_from)
                     + ",\"range_to\":" + Number(rules[i].tiers[j].range_to)
                     + ",\"currency\":" + Quote(rules[i].tiers[j].currency)
                     + "}";
        }
        frame += "]}";
    }
    Send(frame + "]}");
}

//+------------------------------------------------------------------+
//| Publish one tick, the MqlTick fields verbatim                    |
//+------------------------------------------------------------------+
void SendTick(const MqlTick &tick)
{
    Send("{" + Envelope(FRAME_TICK)
             + ",\"symbol\":" + Quote(_Symbol)
             + ",\"time\":" + IntegerToString((long)tick.time)
             + ",\"bid\":" + Number(tick.bid)
             + ",\"ask\":" + Number(tick.ask)
             + ",\"last\":" + Number(tick.last)
             + ",\"volume\":" + Unsigned(tick.volume)
             + ",\"time_msc\":" + IntegerToString(tick.time_msc)
             + ",\"flags\":" + IntegerToString(tick.flags)
             + ",\"volume_real\":" + Number(tick.volume_real)
             + "}");
}

//+------------------------------------------------------------------+
//| Publish one closed bar, the MqlRates fields verbatim             |
//+------------------------------------------------------------------+
void SendBar(const ENUM_TIMEFRAMES timeframe, const MqlRates &rates)
{
    Send("{" + Envelope(FRAME_BAR)
             + ",\"symbol\":" + Quote(_Symbol)
             + ",\"timeframe\":" + IntegerToString((int)timeframe)
             + ",\"time\":" + IntegerToString((long)rates.time)
             + ",\"open\":" + Number(rates.open)
             + ",\"high\":" + Number(rates.high)
             + ",\"low\":" + Number(rates.low)
             + ",\"close\":" + Number(rates.close)
             + ",\"tick_volume\":" + IntegerToString(rates.tick_volume)
             + ",\"spread\":" + IntegerToString(rates.spread)
             + ",\"real_volume\":" + IntegerToString(rates.real_volume)
             + "}");
}

//+------------------------------------------------------------------+
//| Publish one trade transaction, its three structs verbatim and    |
//| their enums as integers                                          |
//+------------------------------------------------------------------+
void SendTransaction(const MqlTradeTransaction &trans,
                     const MqlTradeRequest &request,
                     const MqlTradeResult &result)
{
    string frame = "{" + Envelope(FRAME_TRADE_TRANSACTION);
    frame += ",\"transaction\":{\"deal\":" + Unsigned(trans.deal)
             + ",\"order\":" + Unsigned(trans.order)
             + ",\"symbol\":" + Quote(trans.symbol)
             + ",\"type\":" + IntegerToString((int)trans.type)
             + ",\"order_type\":" + IntegerToString((int)trans.order_type)
             + ",\"order_state\":" + IntegerToString((int)trans.order_state)
             + ",\"deal_type\":" + IntegerToString((int)trans.deal_type)
             + ",\"time_type\":" + IntegerToString((int)trans.time_type)
             + ",\"time_expiration\":" + IntegerToString((long)trans.time_expiration)
             + ",\"price\":" + Number(trans.price)
             + ",\"price_trigger\":" + Number(trans.price_trigger)
             + ",\"price_sl\":" + Number(trans.price_sl)
             + ",\"price_tp\":" + Number(trans.price_tp)
             + ",\"volume\":" + Number(trans.volume)
             + ",\"position\":" + Unsigned(trans.position)
             + ",\"position_by\":" + Unsigned(trans.position_by)
             + "}";
    frame += ",\"request\":{\"action\":" + IntegerToString((int)request.action)
             + ",\"magic\":" + Unsigned(request.magic)
             + ",\"order\":" + Unsigned(request.order)
             + ",\"symbol\":" + Quote(request.symbol)
             + ",\"volume\":" + Number(request.volume)
             + ",\"price\":" + Number(request.price)
             + ",\"stoplimit\":" + Number(request.stoplimit)
             + ",\"sl\":" + Number(request.sl)
             + ",\"tp\":" + Number(request.tp)
             + ",\"deviation\":" + Unsigned(request.deviation)
             + ",\"type\":" + IntegerToString((int)request.type)
             + ",\"type_filling\":" + IntegerToString((int)request.type_filling)
             + ",\"type_time\":" + IntegerToString((int)request.type_time)
             + ",\"expiration\":" + IntegerToString((long)request.expiration)
             + ",\"comment\":" + Quote(request.comment)
             + ",\"position\":" + Unsigned(request.position)
             + ",\"position_by\":" + Unsigned(request.position_by)
             + "}";
    frame += ",\"result\":{\"retcode\":" + IntegerToString(result.retcode)
             + ",\"deal\":" + Unsigned(result.deal)
             + ",\"order\":" + Unsigned(result.order)
             + ",\"volume\":" + Number(result.volume)
             + ",\"price\":" + Number(result.price)
             + ",\"bid\":" + Number(result.bid)
             + ",\"ask\":" + Number(result.ask)
             + ",\"comment\":" + Quote(result.comment)
             + ",\"request_id\":" + IntegerToString(result.request_id)
             + ",\"retcode_external\":" + IntegerToString(result.retcode_external)
             + "}}";
    Send(frame);
}

//+------------------------------------------------------------------+
//| The symbol a transaction is of: its request's for a request      |
//| transaction, whose own symbol MQL5 leaves unfilled               |
//+------------------------------------------------------------------+
string TransactionSymbol(const MqlTradeTransaction &trans, const MqlTradeRequest &request)
{
    if (trans.type == TRADE_TRANSACTION_REQUEST)
        return request.symbol;
    else
        return trans.symbol;
}

//+------------------------------------------------------------------+
//| The wanted streams                                               |
//+------------------------------------------------------------------+
int BarIndex(const ENUM_TIMEFRAMES timeframe)
{
    for (int i = 0; i < ArraySize(g_barTimeframes); i++)
        if (g_barTimeframes[i] == timeframe)
            return i;
    return -1;
}

void ClearWanted()
{
    g_wantTicks = false;
    ArrayFree(g_barTimeframes);
    ArrayFree(g_barOpens);
}

//+------------------------------------------------------------------+
//| Count the ticks of the cursor's millisecond: ready only when the |
//| read answered every one of them                                  |
//+------------------------------------------------------------------+
ENUM_TICK_CURSOR_STATE CountCursor(const long cursorMsc, int &cursorCount)
{
    MqlTick ticks[];
    ResetLastError();
    const int copied = CopyTicksRange(_Symbol, ticks, COPY_TICKS_INFO, (ulong)cursorMsc,
                                      (ulong)cursorMsc);
    const int error = GetLastError();
    // A timeout can return a partial count, which cannot establish the subscription boundary.
    if (copied >= 0 && error == ERR_SUCCESS)
    {
        cursorCount = copied;
        return CURSOR_READY;
    }
    else
    {
        return CURSOR_COUNT_PENDING;
    }
}

//+------------------------------------------------------------------+
//| The tick cursor of newly wanted ticks: the symbol's latest tick, |
//| so ticks from before the hub wanted them are never published     |
//+------------------------------------------------------------------+
void StartCursor()
{
    MqlTick last;
    if (SymbolInfoTick(_Symbol, last) && last.time_msc > 0)
        g_tickCursorMsc = last.time_msc;
    else
        g_tickCursorMsc = (long)TimeTradeServer() * 1000;
    g_tickCursorCount = 0;
    g_tickCursorState = CountCursor(g_tickCursorMsc, g_tickCursorCount);
    if (g_tickCursorState == CURSOR_COUNT_PENDING)
        PrintFormat("ticks: '%s' count pending at %I64d: error %d; publication paused", _Symbol,
                    g_tickCursorMsc, GetLastError());
}

//+------------------------------------------------------------------+
//| Take the hub's wanted frame for the symbol: start the tick       |
//| cursor when its ticks become wanted, keep the opens of the       |
//| timeframes that stay wanted, start the new ones                  |
//+------------------------------------------------------------------+
void ApplyWanted(CJAVal &frame)
{
    const bool wantTicks = frame["ticks"].ToBool();
    if (wantTicks && !g_wantTicks)
        StartCursor();
    g_wantTicks = wantTicks;

    ENUM_TIMEFRAMES barTimeframes[];
    datetime barOpens[];
    for (int i = 0; i < frame["timeframes"].Size(); i++)
    {
        const ENUM_TIMEFRAMES timeframe = (ENUM_TIMEFRAMES)frame["timeframes"][i].ToInt();
        const int known = BarIndex(timeframe);
        const int n = ArraySize(barTimeframes);
        ArrayResize(barTimeframes, n + 1);
        ArrayResize(barOpens, n + 1);
        barTimeframes[n] = timeframe;
        if (known >= 0)
            barOpens[n] = g_barOpens[known];
        else
            barOpens[n] = iTime(_Symbol, timeframe, 0);
    }
    ArraySwap(g_barTimeframes, barTimeframes);
    ArraySwap(g_barOpens, barOpens);
    PrintFormat("ticks: '%s' wanted: ticks %s, %d bar streams", _Symbol, Boolean(g_wantTicks),
                ArraySize(g_barTimeframes));
}

//+------------------------------------------------------------------+
//| The spawner's charts                                             |
//+------------------------------------------------------------------+
void SendChartOpened(const string name)
{
    Send("{" + Envelope(FRAME_CHART_OPENED) + ",\"symbol\":" + Quote(name) + "}");
}

// A chart_failed or chart_kept frame, which carries the spawner's reason.
void SendChartReason(const ENUM_FRAME_KIND kind, const string name, const string reason)
{
    Send("{" + Envelope(kind)
             + ",\"symbol\":" + Quote(name)
             + ",\"reason\":" + Quote(reason)
             + "}");
}

// The open positions and pending orders the account holds on the symbol; empty when it holds none.
string HoldingReason(const string name)
{
    int positions = 0;
    for (int i = PositionsTotal() - 1; i >= 0; i--)
        if (PositionGetSymbol(i) == name)
            positions++;
    int orders = 0;
    for (int i = OrdersTotal() - 1; i >= 0; i--)
        if (OrderGetTicket(i) > 0 && OrderGetString(ORDER_SYMBOL) == name)
            orders++;
    if (positions == 0 && orders == 0)
        return "";
    else
        return StringFormat("open positions: %d, pending orders: %d", positions, orders);
}

int DeselectIndex(const string name)
{
    for (int i = 0; i < ArraySize(g_deselectSymbols); i++)
        if (g_deselectSymbols[i] == name)
            return i;
    return -1;
}

void KeepDeselecting(const string name)
{
    const int n = ArraySize(g_deselectSymbols);
    ArrayResize(g_deselectSymbols, n + 1);
    ArrayResize(g_deselectAt, n + 1);
    ArrayResize(g_deselectHeld, n + 1);
    g_deselectSymbols[n] = name;
    g_deselectAt[n] = 0;
    g_deselectHeld[n] = false;
}

void ForgetDeselect(const string name)
{
    const int index = DeselectIndex(name);
    if (index >= 0)
    {
        ArrayRemove(g_deselectSymbols, index, 1);
        ArrayRemove(g_deselectAt, index, 1);
        ArrayRemove(g_deselectHeld, index, 1);
    }
}

//+------------------------------------------------------------------+
//| Take out of Market Watch each symbol whose chart the spawner     |
//| closed, once the terminal lets it; while positions or orders     |
//| hold one, it is tried again only every HELD_DESELECT_SECONDS     |
//+------------------------------------------------------------------+
void RetryDeselects()
{
    const ulong now = GetTickCount64();
    for (int i = ArraySize(g_deselectSymbols) - 1; i >= 0; i--)
    {
        if (now >= g_deselectAt[i])
        {
            const string name = g_deselectSymbols[i];
            const string holding = HoldingReason(name);
            if (holding != "")
            {
                if (!g_deselectHeld[i])
                    PrintFormat("ticks: '%s' stays in Market Watch: %s", name, holding);
                g_deselectHeld[i] = true;
                g_deselectAt[i] = now + (ulong)HELD_DESELECT_SECONDS * 1000;
            }
            else if (SymbolSelect(name, false))
            {
                PrintFormat("ticks: took '%s' out of Market Watch", name);
                ForgetDeselect(name);
            }
        }
    }
}

// Forgets the charts the spawner opened that are closed: a closed chart's symbol reads empty.
void ForgetClosedCharts()
{
    long charts[];
    string symbols[];
    for (int i = 0; i < ArraySize(g_openedCharts); i++)
    {
        if (ChartSymbol(g_openedCharts[i]) == g_openedSymbols[i])
        {
            const int n = ArraySize(charts);
            ArrayResize(charts, n + 1);
            ArrayResize(symbols, n + 1);
            charts[n] = g_openedCharts[i];
            symbols[n] = g_openedSymbols[i];
        }
    }
    ArraySwap(g_openedCharts, charts);
    ArraySwap(g_openedSymbols, symbols);
}

// Whether a chart of the symbol runs this EA, or one the spawner opened for it is open: the
// template's EA loads only once the chart has processed the queued template.
bool ChartServes(const string name)
{
    for (int i = 0; i < ArraySize(g_openedSymbols); i++)
        if (g_openedSymbols[i] == name)
            return true;
    long chart = ChartFirst();
    while (chart != -1)
    {
        string expert;
        if (ChartSymbol(chart) == name
            && ChartGetString(chart, CHART_EXPERT_NAME, expert)
            && expert == MQLInfoString(MQL_PROGRAM_NAME))
            return true;
        chart = ChartNext(chart);
    }
    return false;
}

//+------------------------------------------------------------------+
//| Open an M1 chart of the symbol with the EA's template, unless a  |
//| chart already serves it, and tell the hub whether one does       |
//+------------------------------------------------------------------+
void OpenChart(const string name)
{
    ForgetClosedCharts();
    // Empty while a chart serves the symbol.
    string failure = "";
    if (ChartServes(name))
    {
        PrintFormat("ticks: a '%s' chart already serves it", name);
    }
    else if (!SymbolSelect(name, true))
    {
        failure = StringFormat("SymbolSelect failed with error %d", GetLastError());
    }
    else
    {
        const long chart = ChartOpen(name, PERIOD_M1);
        if (chart == 0)
        {
            failure = StringFormat("ChartOpen returned 0 with error %d", GetLastError());
        }
        else if (!ChartApplyTemplate(chart, ChartTemplate))
        {
            failure = StringFormat("ChartApplyTemplate %s failed with error %d", ChartTemplate,
                                   GetLastError());
            ChartClose(chart);
        }
        else
        {
            const int n = ArraySize(g_openedCharts);
            ArrayResize(g_openedCharts, n + 1);
            ArrayResize(g_openedSymbols, n + 1);
            g_openedCharts[n] = chart;
            g_openedSymbols[n] = name;
            PrintFormat("ticks: opened a '%s' chart", name);
        }
    }
    if (failure == "")
    {
        ForgetDeselect(name);
        SendChartOpened(name);
    }
    else
    {
        PrintFormat("ticks: cannot open a '%s' chart: %s", name, failure);
        SendChartReason(FRAME_CHART_FAILED, name, failure);
    }
}

//+------------------------------------------------------------------+
//| Close the chart the spawner opened for the symbol, then take the |
//| symbol out of Market Watch, which the terminal refuses while a   |
//| chart of it is open or it has open positions; a symbol with open |
//| positions or pending orders keeps its chart, and the hub is told |
//+------------------------------------------------------------------+
void CloseOpenedChart(const string name)
{
    ForgetClosedCharts();
    int index = -1;
    for (int i = 0; i < ArraySize(g_openedSymbols); i++)
        if (g_openedSymbols[i] == name)
            index = i;
    const string holding = HoldingReason(name);
    if (holding != "")
    {
        PrintFormat("ticks: keeping the '%s' chart open: %s", name, holding);
        SendChartReason(FRAME_CHART_KEPT, name, holding);
    }
    else if (index < 0)
    {
        PrintFormat("ticks: no open chart was opened for '%s'", name);
    }
    else if (!ChartClose(g_openedCharts[index]))
    {
        PrintFormat("ticks: ChartClose of the '%s' chart failed: %d", name, GetLastError());
    }
    else
    {
        ArrayRemove(g_openedCharts, index, 1);
        ArrayRemove(g_openedSymbols, index, 1);
        // The terminal may still count the chart open, so a refused deselect is tried again.
        if (SymbolSelect(name, false))
        {
            PrintFormat("ticks: closed the '%s' chart and took it out of Market Watch", name);
        }
        else
        {
            PrintFormat("ticks: closed the '%s' chart; it leaves Market Watch once allowed", name);
            KeepDeselecting(name);
        }
    }
}

//+------------------------------------------------------------------+
//| Close this EA's chart, which unloads the EA                      |
//+------------------------------------------------------------------+
void CloseOwnChart()
{
    PrintFormat("ticks: another EA publishes '%s'; closing this chart", _Symbol);
    if (ChartClose(0))
        g_closing = true;
    else
        PrintFormat("ticks: ChartClose failed: %d", GetLastError());
}

//+------------------------------------------------------------------+
//| The hub refused this EA's hello: another EA publishes its symbol |
//+------------------------------------------------------------------+
void OnDuplicate()
{
    const ulong now = GetTickCount64();
    if (!Spawner)
    {
        CloseOwnChart();
    }
    else if (g_firstRefusal == 0)
    {
        // The publisher may be this terminal's own earlier connection, which the hub drops at most
        // HubPingTimeoutSec after its last pong; the reconnect then says hello again.
        g_firstRefusal = now;
        PrintFormat("ticks: another EA publishes '%s'; saying hello again every %d s for %d s",
                    _Symbol, ReconnectIntervalSec, HubPingTimeoutSec);
    }
    else if (now - g_firstRefusal >= (ulong)HubPingTimeoutSec * 1000)
    {
        CloseOwnChart();
    }
}

//+------------------------------------------------------------------+
//| Handle one frame from the hub                                    |
//+------------------------------------------------------------------+
void OnHubFrame(const string text)
{
    CJAVal frame;
    if (!frame.Deserialize(text) || frame["v"].ToInt() != PROTOCOL_VERSION)
    {
        Print("ticks: a hub frame of another version: ", text);
    }
    else
    {
        const string kind = frame["type"].ToStr();
        const string target = frame["symbol"].ToStr();
        // The hub sends these to the spawner, naming the chart's symbol.
        const bool spawnerFrame = kind == FrameName(FRAME_OPEN_CHART)
                                  || kind == FrameName(FRAME_CLOSE_CHART);
        // The hub addresses these to the EA of the symbol they name.
        const bool addressed = kind == FrameName(FRAME_WANTED)
                               || kind == FrameName(FRAME_DUPLICATE);
        if (spawnerFrame && !Spawner)
            Print("ticks: a spawner's frame to an EA that is not the spawner: ", text);
        else if (kind == FrameName(FRAME_OPEN_CHART))
            OpenChart(target);
        else if (kind == FrameName(FRAME_CLOSE_CHART))
            CloseOpenedChart(target);
        else if (addressed && target != _Symbol)
            Print("ticks: a hub frame for another symbol: ", text);
        else if (kind == FrameName(FRAME_WANTED))
        {
            // The hub tells an EA its wanted frame once it takes its hello.
            g_firstRefusal = 0;
            ApplyWanted(frame);
        }
        else if (kind == FrameName(FRAME_DUPLICATE))
            OnDuplicate();
        else
            Print("ticks: a hub frame of an unknown kind: ", text);
    }
}

//+------------------------------------------------------------------+
//| Read what the hub sent                                           |
//+------------------------------------------------------------------+
void ReadHub()
{
    // The library answers a ping and acknowledges a close only while it reads, and it reads nothing
    // while a message it holds is unread.
    IWebSocketMessage *msg = wss.readMessage(false);
    while (msg != NULL)
    {
        OnHubFrame(msg.getString());
        delete msg;
        msg = wss.readMessage(false);
    }
}

//+------------------------------------------------------------------+
//| Publish the symbol's ticks since its cursor                      |
//+------------------------------------------------------------------+
void PublishCursorTicks()
{
    // The read starts at the cursor's millisecond, so it takes the ticks of it already consumed on
    // top of a batch of new ones.
    MqlTick ticks[];
    const int copied = CopyTicks(_Symbol, ticks, COPY_TICKS_INFO, (ulong)g_tickCursorMsc,
                                 (uint)(g_tickCursorCount + TICK_BATCH));
    if (copied < 0)
        PrintFormat("ticks: CopyTicks '%s' failed: %d", _Symbol, GetLastError());
    int atCursor = 0;
    for (int i = 0; i < copied; i++)
    {
        if (ticks[i].time_msc != g_tickCursorMsc)
        {
            SendTick(ticks[i]);
            g_tickCursorMsc = ticks[i].time_msc;
            g_tickCursorCount = 1;
            atCursor = 1;
        }
        else
        {
            atCursor++;
            if (atCursor > g_tickCursorCount)
            {
                SendTick(ticks[i]);
                g_tickCursorCount = atCursor;
            }
        }
    }
}

//+------------------------------------------------------------------+
//| Publish the symbol's ticks since its cursor while they are       |
//| wanted, once the cursor's count is complete                      |
//+------------------------------------------------------------------+
void PublishTicks()
{
    if (g_wantTicks && g_tickCursorState == CURSOR_COUNT_PENDING)
    {
        g_tickCursorState = CountCursor(g_tickCursorMsc, g_tickCursorCount);
        if (g_tickCursorState == CURSOR_READY)
            PrintFormat("ticks: '%s' count complete at %I64d; publication enabled", _Symbol,
                        g_tickCursorMsc);
    }
    if (g_wantTicks && g_tickCursorState == CURSOR_READY)
        PublishCursorTicks();
}

//+------------------------------------------------------------------+
//| Publish the bar that closed for every wanted timeframe whose     |
//| forming bar moved on                                             |
//+------------------------------------------------------------------+
void PublishBars()
{
    for (int b = 0; b < ArraySize(g_barTimeframes); b++)
    {
        const datetime open = iTime(_Symbol, g_barTimeframes[b], 0);
        if (open != 0 && open != g_barOpens[b])
        {
            if (g_barOpens[b] == 0)
            {
                g_barOpens[b] = open;
            }
            else
            {
                MqlRates rates[];
                if (CopyRates(_Symbol, g_barTimeframes[b], 1, 1, rates) == 1)
                {
                    SendBar(g_barTimeframes[b], rates[0]);
                    g_barOpens[b] = open;
                }
                else
                {
                    PrintFormat("ticks: CopyRates '%s' %s failed: %d", _Symbol,
                                EnumToString(g_barTimeframes[b]), GetLastError());
                }
            }
        }
    }
}

//+------------------------------------------------------------------+
//| The relay cadence: the server time and the symbol's commission   |
//| schedule                                                         |
//+------------------------------------------------------------------+
void Relay()
{
    g_lastRelay = GetTickCount64();
    SendServerTime();
    SendCommissions();
}

//+------------------------------------------------------------------+
//| Connect to the hub again; it tells a reconnected EA what it      |
//| wants, so nothing missed while away is published                 |
//+------------------------------------------------------------------+
void Reconnect()
{
    g_lastReconnect = TimeLocal();
    Print("Reconnecting");
    g_helloSent = false;
    ClearWanted();
    if (!wss.open())
        Print("Failed to reconnect to server");
    else
        SendHello();
}

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
    Print("ticks EA initializing");
    if (RelaySeconds < 1)
    {
        Print("RelaySeconds ", RelaySeconds, " is not positive");
        return INIT_PARAMETERS_INCORRECT;
    }
    if (PollMilliseconds < 1)
    {
        Print("PollMilliseconds ", PollMilliseconds, " is not positive");
        return INIT_PARAMETERS_INCORRECT;
    }
    if (HubPingTimeoutSec < 1)
    {
        Print("HubPingTimeoutSec ", HubPingTimeoutSec, " is not positive");
        return INIT_PARAMETERS_INCORRECT;
    }
    // The timer fires whether or not the market ticks, so the EA reconnects, reads the hub and
    // relays the server time while the market is closed too.
    if (!EventSetMillisecondTimer(PollMilliseconds))
    {
        Print("EventSetMillisecondTimer failed: ", GetLastError());
        return INIT_FAILED;
    }

    wss = new WebSocketClient<Hybi>(Server);
    wss.setTimeOut(10000);
    if (wss.open())
    {
        SendHello();
        Print("ticks EA initialization successful");
    }
    else
    {
        Print("Failed to connect to server");
    }
    return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
    EventKillTimer();
    if (wss != NULL)
    {
        wss.close();
        delete wss;
        wss = NULL;
        Print("WebSocket client closed");
    }
    Print("Deinitialization");
}

//+------------------------------------------------------------------+
//| Timer event handler                                              |
//+------------------------------------------------------------------+
void OnTimer()
{
    if (g_closing)
        return;
    ReadHub();
    RetryDeselects();
    // TimeLocal, not TimeCurrent: the time of the last quote stands still while the market is
    // closed.
    if (!g_closing && !wss.isConnected() && TimeLocal() - g_lastReconnect >= ReconnectIntervalSec)
        Reconnect();
    if (!g_closing && wss.isConnected())
    {
        // The terminal queues no tick event while one is queued or handled, so a tick that arrives
        // then is read from the cursor here, at the latest one poll interval on.
        PublishTicks();
        PublishBars();
        if (GetTickCount64() - g_lastRelay >= (ulong)RelaySeconds * 1000)
            Relay();
    }
}

//+------------------------------------------------------------------+
//| onTick method is called every tick                               |
//+------------------------------------------------------------------+
void OnTick()
{
    if (g_closing)
        return;
    ReadHub();
    if (!g_closing && wss.isConnected())
    {
        PublishTicks();
        PublishBars();
    }
}

//+------------------------------------------------------------------+
//| Every trade transaction of the symbol, as it arrives; the        |
//| spawner's also every one that names no symbol                    |
//+------------------------------------------------------------------+
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result)
{
    // Every EA receives the account's whole stream; the symbol filter sends each transaction once.
    const string name = TransactionSymbol(trans, request);
    if (!g_closing && wss.isConnected() && (name == _Symbol || (name == "" && Spawner)))
        SendTransaction(trans, request, result);
}
