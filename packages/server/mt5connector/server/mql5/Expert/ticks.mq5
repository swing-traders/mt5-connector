// The EA inside the terminal, in the role its chart's template sets: the spawner serves the
// account, relays the trade server's time and opens and closes the charts the hub asks for; a
// publisher serves its symbol's ticks, closed bars, transactions, reads and trade commands. Every
// frame it sends and reads is the hub protocol's binary one.
#property script_show_inputs

#include <MQL5Book/ws/wsclient.mqh>
#include <hub_wire.mqh>

input ENUM_CHART_ROLE Role = ROLE_PUBLISHER;
input string Server = "ws://127.0.0.1:9000/";
input int ReconnectIntervalSec = 3;
input int RelaySeconds = 5;
input int PollMilliseconds = 16;
// The template the spawner applies to a chart it opens for a publisher.
input string ChartTemplate = "ticks.tpl";
// The whole time from a dead connection's last pong to the hub dropping it, while the hub's writes
// to it drain: the hub's ping interval, ping timeout and close timeout together.
input int HubPingTimeoutSec = 50;

// The most new ticks one read takes; the next read resumes where it stopped.
#define TICK_BATCH 1000
// How often the spawner tries again to take a symbol whose chart it closed out of Market Watch
// while open positions or pending orders hold it.
#define HELD_DESELECT_SECONDS 900
// The REQUEST transactions a publisher keeps for the outcomes call, the latest first.
#define OUTCOME_RING 1000
#define DAY_SECONDS 86400

// The two session schedules a symbol states per weekday.
enum ENUM_SESSION_KIND
{
    SESSION_QUOTE,
    SESSION_TRADE
};

// Whether the tick cursor knows how many ticks of its millisecond came before it.
enum ENUM_TICK_CURSOR_STATE
{
    CURSOR_COUNT_PENDING,
    CURSOR_READY
};

// A REQUEST transaction a publisher published: its request id, and its three structs as the
// trade_transaction frame and an outcome carry them.
struct Outcome
{
    uint requestId;
    uchar record[];
};

WebSocketClient<Hybi> *wss = NULL;
bool g_helloSent = false;
datetime g_lastReconnect = 0;
ulong g_lastRelay = 0;
// Set once the EA has asked for its chart to close: it reads, publishes and reconnects no more.
bool g_closing = false;
// When the hub first refused the spawner's hello as a duplicate, by GetTickCount64; zero once a
// connection that said hello ends unrefused. A refused connection sets g_refused.
ulong g_firstRefusal = 0;
bool g_refused = false;

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

// The charts the spawner opened, each with the role and symbol it was opened for.
long g_openedCharts[];
ENUM_CHART_ROLE g_openedRoles[];
string g_openedSymbols[];

// The symbols whose charts the spawner closed that are still in Market Watch, each with when to try
// the deselect again, by GetTickCount64, and whether positions or orders were found holding it.
string g_deselectSymbols[];
ulong g_deselectAt[];
bool g_deselectHeld[];

// The publisher's outcome log, a ring: g_outcomeNext is the slot the next transaction takes.
Outcome g_outcomes[OUTCOME_RING];
int g_outcomesHeld = 0;
int g_outcomeNext = 0;

// ── Frames to the hub ────────────────────────────────────────────────────────────────────────────

// Sends a frame while connected; a frame holding a string too long for its length prefix is not
// sent.
void Send(BinaryWriter &frame)
{
    if (frame.failed)
    {
        Print("ticks: a frame holding a string too long for the protocol is not sent");
    }
    else if (wss.isConnected())
    {
        // The library waits on every send to a lost connection; the first lost send tells
        // isConnected.
        uchar bytes[];
        frame.Take(bytes);
        wss.send(bytes);
    }
}

void SendHello()
{
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_HELLO, 0, 0);
    frame.String(HUB_ROLE_EA);
    frame.String(ChartRoleName(Role));
    frame.String(_Symbol);
    frame.Seal(0);
    Send(frame);
    g_helloSent = true;
    PrintFormat("ticks: hello sent as the %s of '%s'", ChartRoleName(Role), _Symbol);
}

void SendServerTime()
{
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_SERVER_TIME, 0, 0);
    frame.String(_Symbol);
    frame.I64((long)TimeTradeServer());
    frame.I64((long)TimeCurrent());
    frame.I64((long)TimeGMT());
    frame.U8((uchar)TerminalInfoInteger(TERMINAL_CONNECTED));
    frame.Seal(0);
    Send(frame);
}

// The symbol's commission rules, their enums by name
void SendCommissions()
{
    MqlCommission rules[];
    ResetLastError();
    const int ret = SymbolInfoCommissions(_Symbol, rules);
    const int error = GetLastError();
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_COMMISSIONS, 0, 0);
    frame.String(_Symbol);
    frame.I32(ret);
    frame.I32(error);
    frame.U16((ushort)ArraySize(rules));
    for (int i = 0; i < ArraySize(rules); i++)
    {
        frame.String(rules[i].currency);
        frame.String(EnumToString(rules[i].mode_range));
        frame.String(EnumToString(rules[i].mode_charge));
        frame.String(EnumToString(rules[i].mode_entry));
        frame.String(EnumToString(rules[i].mode_direction));
        frame.String(EnumToString(rules[i].mode_profit));
        frame.U16((ushort)ArraySize(rules[i].tiers));
        for (int j = 0; j < ArraySize(rules[i].tiers); j++)
        {
            frame.String(EnumToString(rules[i].tiers[j].mode));
            frame.String(EnumToString(rules[i].tiers[j].volume_type));
            frame.F64(rules[i].tiers[j].value);
            frame.F64(rules[i].tiers[j].min_value);
            frame.F64(rules[i].tiers[j].max_value);
            frame.F64(rules[i].tiers[j].range_from);
            frame.F64(rules[i].tiers[j].range_to);
            frame.String(rules[i].tiers[j].currency);
        }
    }
    frame.Seal(0);
    Send(frame);
}

// ── The symbol's quote and trade sessions ────────────────────────────────────────────────────────

bool ReadSession(const ENUM_SESSION_KIND kind, const ENUM_DAY_OF_WEEK day, const uint index,
                 datetime &opens, datetime &closes)
{
    bool found = false;
    switch (kind)
    {
    case SESSION_QUOTE:
        found = SymbolInfoSessionQuote(_Symbol, day, index, opens, closes);
        break;
    case SESSION_TRADE:
        found = SymbolInfoSessionTrade(_Symbol, day, index, opens, closes);
        break;
    }
    return found;
}

// The schedule's sessions, Sunday first: each weekday's count, and every session's [from, to] as
// the terminal gives them, seconds from 00:00, all the week's in order.
void ReadWeek(const ENUM_SESSION_KIND kind, int &counts[], datetime &froms[], datetime &tos[])
{
    ArrayResize(counts, 7);
    for (int day = SUNDAY; day <= SATURDAY; day++)
    {
        datetime opens = 0;
        datetime closes = 0;
        uint index = 0;
        while (ReadSession(kind, (ENUM_DAY_OF_WEEK)day, index, opens, closes))
        {
            const int n = ArraySize(froms);
            ArrayResize(froms, n + 1);
            ArrayResize(tos, n + 1);
            froms[n] = opens;
            tos[n] = closes;
            index++;
        }
        counts[day - SUNDAY] = (int)index;
    }
}

void WriteWeek(BinaryWriter &frame, const int &counts[], const datetime &froms[],
               const datetime &tos[])
{
    int next = 0;
    for (int day = 0; day < ArraySize(counts); day++)
    {
        frame.U16((ushort)counts[day]);
        for (int k = 0; k < counts[day]; k++)
        {
            frame.I32((int)froms[next]);
            frame.I32((int)tos[next]);
            next++;
        }
    }
}

// Relays the symbol's quote and trade sessions per weekday, and the error their reads left.
void SendSessions()
{
    int quoteCounts[];
    datetime quoteFroms[];
    datetime quoteTos[];
    int tradeCounts[];
    datetime tradeFroms[];
    datetime tradeTos[];
    ResetLastError();
    ReadWeek(SESSION_QUOTE, quoteCounts, quoteFroms, quoteTos);
    ReadWeek(SESSION_TRADE, tradeCounts, tradeFroms, tradeTos);
    const int error = GetLastError();
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_SESSIONS, 0, 0);
    frame.String(_Symbol);
    WriteWeek(frame, quoteCounts, quoteFroms, quoteTos);
    WriteWeek(frame, tradeCounts, tradeFroms, tradeTos);
    frame.I32(error);
    frame.Seal(0);
    Send(frame);
}

// The relay cadence, by role
void Relay()
{
    g_lastRelay = GetTickCount64();
    switch (Role)
    {
    case ROLE_SPAWNER:
        SendServerTime();
        break;
    case ROLE_PUBLISHER:
        SendCommissions();
        SendSessions();
        break;
    }
}

void SendBar(const ENUM_TIMEFRAMES timeframe, const MqlRates &rates)
{
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_BAR, 0, 0);
    frame.String(_Symbol);
    frame.I32((int)timeframe);
    frame.Rates(rates);
    frame.Seal(0);
    Send(frame);
}

// ── The spawner's answers to the hub's chart frames ──────────────────────────────────────────────

void SendChartOpened(const ENUM_CHART_ROLE role, const string symbol)
{
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_CHART_OPENED, 0, 0);
    frame.String(ChartRoleName(role));
    frame.String(symbol);
    frame.Seal(0);
    Send(frame);
}

void SendChartFailed(const ENUM_CHART_ROLE role, const string symbol, const int error)
{
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_CHART_FAILED, 0, 0);
    frame.String(ChartRoleName(role));
    frame.String(symbol);
    frame.I32(error);
    frame.Seal(0);
    Send(frame);
}

void SendChartKept(const ENUM_CHART_ROLE role, const string symbol, const int positions,
                   const int orders)
{
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_CHART_KEPT, 0, 0);
    frame.String(ChartRoleName(role));
    frame.String(symbol);
    frame.I32(positions);
    frame.I32(orders);
    frame.Seal(0);
    Send(frame);
}

// Tells the hub this EA is about to wait on a request for at most `boundSeconds`
void SendBusy(const ulong requestId, const uint boundSeconds)
{
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_BUSY, 0, requestId);
    frame.U32(boundSeconds);
    frame.Seal(0);
    Send(frame);
}

// ── Requests: each answered once, by its result in parts or an error ─────────────────────────────

// A request's arguments by their index in its call, as the hub sent them.
class RequestArguments
{
private:
    bool m_has[HUB_ARGUMENTS_MAX];
    long m_integers[HUB_ARGUMENTS_MAX];
    string m_strings[HUB_ARGUMENTS_MAX];

    // A u32 count, then each request id as a u32; a count past the bytes left does not fit.
    bool ReadIds(BinaryReader &reader)
    {
        const uint count = reader.U32();
        const bool fits = !reader.failed && count <= (uint)reader.Remaining() / 4;
        if (fits)
        {
            ArrayResize(ids, (int)count);
            for (uint i = 0; i < count; i++)
                ids[i] = reader.U32();
        }
        return fits;
    }

    bool ReadValue(const ENUM_HUB_ARGUMENT type, const int index, BinaryReader &reader)
    {
        bool fits = true;
        switch (type)
        {
        case HUB_ARGUMENT_INTEGER:
        case HUB_ARGUMENT_EPOCH:
            m_integers[index] = reader.I64();
            break;
        case HUB_ARGUMENT_STRING:
            m_strings[index] = reader.String();
            break;
        case HUB_ARGUMENT_BOOL:
            m_integers[index] = reader.U8();
            fits = m_integers[index] <= 1;
            break;
        case HUB_ARGUMENT_TRADE_REQUEST:
            ReadTradeRequest(reader, request);
            break;
        case HUB_ARGUMENT_REQUEST_IDS:
            fits = ReadIds(reader);
            break;
        default:
            // A type the EA reads no argument of.
            fits = false;
            break;
        }
        return fits && !reader.failed;
    }

public:
    MqlTradeRequest request;
    uint ids[];

    RequestArguments()
    {
        for (int i = 0; i < HUB_ARGUMENTS_MAX; i++)
        {
            m_has[i] = false;
            m_integers[i] = 0;
            m_strings[i] = "";
        }
        ZeroMemory(request);
    }

    // Reads `count` arguments of the call, each its index, ascending, and its value; false for
    // arguments out of the call's layout.
    bool Read(const ENUM_HUB_CALL call, const uint count, BinaryReader &reader)
    {
        bool fits = true;
        int last = -1;
        for (uint k = 0; k < count && fits; k++)
        {
            const int index = reader.U8();
            ENUM_HUB_ARGUMENT type = HUB_ARGUMENT_INTEGER;
            if (reader.failed || index <= last || !HubArgumentType(call, index, type))
            {
                fits = false;
            }
            else
            {
                last = index;
                m_has[index] = true;
                fits = ReadValue(type, index, reader);
            }
        }
        return fits && reader.Done();
    }

    bool Has(const int index) const { return m_has[index]; }
    long Integer(const int index) const { return m_integers[index]; }
    string String(const int index) const { return m_strings[index]; }
    bool Bool(const int index) const { return m_integers[index] != 0; }
};

// A request's answer, built by its call's handler: its result's records back to back in rows, each
// ending at its offset in ends; or, once failed, why and the MQL5 error the call met.
class Answer
{
public:
    BinaryWriter rows;
    int ends[];
    bool failed;
    string message;
    int error;

    Answer() : failed(false), message(""), error(0) {}

    // Marks the end of the record just written to rows.
    void Record()
    {
        const int n = ArraySize(ends);
        ArrayResize(ends, n + 1, 64);
        ends[n] = rows.Size();
    }

    void Fail(const string why, const int mql5Error)
    {
        failed = true;
        message = why;
        error = mql5Error;
    }
};

void SendPart(const ulong requestId, const uint part, const bool last, const int lastError,
              BinaryWriter &rows, const int start, const int end, const int records)
{
    ushort flags = HUB_FLAG_OK;
    if (last)
        flags = HUB_FLAG_OK | HUB_FLAG_LAST;
    uchar bytes[];
    rows.Slice(start, end - start, bytes);
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_ANSWER, flags, requestId);
    frame.U32(part);
    frame.I32(lastError);
    frame.Bytes(bytes, 0, ArraySize(bytes));
    frame.Seal((uint)records);
    Send(frame);
}

// Answers a request once: its records in parts of at most HUB_PART_BYTES result bytes, numbered
// from 0 and the last flagged, each carrying the MQL5 error the call left; or the error it met.
void SendAnswer(const ulong requestId, Answer &answer, const int lastError)
{
    if (answer.rows.failed)
        answer.Fail("a record holds a string too long for the protocol", 0);
    if (answer.failed)
    {
        BinaryWriter frame;
        frame.Begin(HUB_FRAME_ANSWER, HUB_FLAG_LAST, requestId);
        frame.U32(0);
        frame.I32(answer.error);
        frame.U8((uchar)HUB_ERROR_EA_ERROR);
        frame.String(answer.message);
        frame.Seal(0);
        Send(frame);
    }
    else
    {
        const int records = ArraySize(answer.ends);
        uint part = 0;
        // The first record of the part being gathered, and its first byte.
        int first = 0;
        int start = 0;
        for (int i = 0; i < records; i++)
        {
            if (i > first && answer.ends[i] - start > HUB_PART_BYTES)
            {
                SendPart(requestId, part, false, lastError, answer.rows, start,
                         answer.ends[i - 1], i - first);
                part++;
                start = answer.ends[i - 1];
                first = i;
            }
        }
        int end = 0;
        if (records > 0)
            end = answer.ends[records - 1];
        SendPart(requestId, part, true, lastError, answer.rows, start, end, records - first);
    }
}

// ── The spawner's calls: the account ─────────────────────────────────────────────────────────────

void AnswerSymbols(RequestArguments &arguments, Answer &answer)
{
    if (arguments.String(HUB_ARG_SYMBOLS_GET_GROUP) != "")
    {
        answer.Fail("a group filter is not served", ERR_FUNCTION_NOT_ALLOWED);
    }
    else
    {
        // Every symbol the terminal knows, in Market Watch or not.
        const int total = SymbolsTotal(false);
        for (int i = 0; i < total; i++)
        {
            WriteSymbolInfo(answer.rows, SymbolName(i, false));
            answer.Record();
        }
    }
}

void AnswerPositions(RequestArguments &arguments, Answer &answer)
{
    if (arguments.String(HUB_ARG_POSITIONS_GET_GROUP) != "")
    {
        answer.Fail("a group filter is not served", ERR_FUNCTION_NOT_ALLOWED);
    }
    else
    {
        const bool bySymbol = arguments.Has(HUB_ARG_POSITIONS_GET_SYMBOL);
        const bool byTicket = arguments.Has(HUB_ARG_POSITIONS_GET_TICKET);
        const string symbol = arguments.String(HUB_ARG_POSITIONS_GET_SYMBOL);
        const ulong ticket = (ulong)arguments.Integer(HUB_ARG_POSITIONS_GET_TICKET);
        const int total = PositionsTotal();
        for (int i = 0; i < total; i++)
        {
            // Each selects the position for the writer.
            const string held = PositionGetSymbol(i);
            const ulong heldTicket = PositionGetTicket(i);
            if (heldTicket > 0 && (!bySymbol || held == symbol)
                && (!byTicket || heldTicket == ticket))
            {
                WritePosition(answer.rows);
                answer.Record();
            }
        }
    }
}

void AnswerOrders(RequestArguments &arguments, Answer &answer)
{
    if (arguments.String(HUB_ARG_ORDERS_GET_GROUP) != "")
    {
        answer.Fail("a group filter is not served", ERR_FUNCTION_NOT_ALLOWED);
    }
    else
    {
        const bool bySymbol = arguments.Has(HUB_ARG_ORDERS_GET_SYMBOL);
        const bool byTicket = arguments.Has(HUB_ARG_ORDERS_GET_TICKET);
        const string symbol = arguments.String(HUB_ARG_ORDERS_GET_SYMBOL);
        const ulong ticket = (ulong)arguments.Integer(HUB_ARG_ORDERS_GET_TICKET);
        const int total = OrdersTotal();
        for (int i = 0; i < total; i++)
        {
            // Selects the order for the writer.
            const ulong held = OrderGetTicket(i);
            if (held > 0 && (!bySymbol || OrderGetString(ORDER_SYMBOL) == symbol)
                && (!byTicket || held == ticket))
            {
                WriteOrder(answer.rows);
                answer.Record();
            }
        }
    }
}

// The orders of the history a query names: one by its ticket, every one of a position, or those of
// a window on the broker's clock.
void AnswerHistoryOrders(RequestArguments &arguments, Answer &answer)
{
    if (arguments.String(HUB_ARG_HISTORY_ORDERS_GET_GROUP) != "")
    {
        answer.Fail("a group filter is not served", ERR_FUNCTION_NOT_ALLOWED);
    }
    else if (arguments.Has(HUB_ARG_HISTORY_ORDERS_GET_TICKET))
    {
        // A ticket the history does not hold answers no record, its error the answer's.
        const ulong ticket = (ulong)arguments.Integer(HUB_ARG_HISTORY_ORDERS_GET_TICKET);
        if (HistoryOrderSelect(ticket))
        {
            WriteHistoryOrder(answer.rows, ticket);
            answer.Record();
        }
    }
    else if (arguments.Has(HUB_ARG_HISTORY_ORDERS_GET_POSITION))
    {
        if (HistorySelectByPosition(arguments.Integer(HUB_ARG_HISTORY_ORDERS_GET_POSITION)))
            AnswerSelectedOrders(answer);
        else
            answer.Fail("HistorySelectByPosition failed", GetLastError());
    }
    else
    {
        if (HistorySelect((datetime)arguments.Integer(HUB_ARG_HISTORY_ORDERS_GET_DATE_FROM),
                          (datetime)arguments.Integer(HUB_ARG_HISTORY_ORDERS_GET_DATE_TO)))
            AnswerSelectedOrders(answer);
        else
            answer.Fail("HistorySelect failed", GetLastError());
    }
}

void AnswerSelectedOrders(Answer &answer)
{
    const int total = HistoryOrdersTotal();
    for (int i = 0; i < total; i++)
    {
        const ulong ticket = HistoryOrderGetTicket(i);
        if (ticket > 0)
        {
            WriteHistoryOrder(answer.rows, ticket);
            answer.Record();
        }
    }
}

// The deals of the history a query names: those of the order its ticket names, every one of a
// position, or those of a window on the broker's clock.
void AnswerHistoryDeals(RequestArguments &arguments, Answer &answer)
{
    if (arguments.String(HUB_ARG_HISTORY_DEALS_GET_GROUP) != "")
    {
        answer.Fail("a group filter is not served", ERR_FUNCTION_NOT_ALLOWED);
    }
    else if (arguments.Has(HUB_ARG_HISTORY_DEALS_GET_TICKET))
    {
        // The package's ticket here is an order's, which its deals carry as DEAL_ORDER; the whole
        // history is selected to find them.
        const ulong order = (ulong)arguments.Integer(HUB_ARG_HISTORY_DEALS_GET_TICKET);
        if (HistorySelect(0, TimeTradeServer() + DAY_SECONDS))
            AnswerSelectedDeals(answer, true, order);
        else
            answer.Fail("HistorySelect failed", GetLastError());
    }
    else if (arguments.Has(HUB_ARG_HISTORY_DEALS_GET_POSITION))
    {
        if (HistorySelectByPosition(arguments.Integer(HUB_ARG_HISTORY_DEALS_GET_POSITION)))
            AnswerSelectedDeals(answer, false, 0);
        else
            answer.Fail("HistorySelectByPosition failed", GetLastError());
    }
    else
    {
        if (HistorySelect((datetime)arguments.Integer(HUB_ARG_HISTORY_DEALS_GET_DATE_FROM),
                          (datetime)arguments.Integer(HUB_ARG_HISTORY_DEALS_GET_DATE_TO)))
            AnswerSelectedDeals(answer, false, 0);
        else
            answer.Fail("HistorySelect failed", GetLastError());
    }
}

// The deals the history selected, those of the order `order` alone when `byOrder`.
void AnswerSelectedDeals(Answer &answer, const bool byOrder, const ulong order)
{
    const int total = HistoryDealsTotal();
    for (int i = 0; i < total; i++)
    {
        const ulong ticket = HistoryDealGetTicket(i);
        if (ticket > 0 && (!byOrder || (ulong)HistoryDealGetInteger(ticket, DEAL_ORDER) == order))
        {
            WriteDeal(answer.rows, ticket);
            answer.Record();
        }
    }
}

void ServeAccount(const ENUM_HUB_CALL call, RequestArguments &arguments, Answer &answer)
{
    switch (call)
    {
    case HUB_CALL_ACCOUNT_INFO:
        WriteAccountInfo(answer.rows);
        answer.Record();
        break;
    case HUB_CALL_TERMINAL_INFO:
        WriteTerminalInfo(answer.rows);
        answer.Record();
        break;
    case HUB_CALL_SYMBOLS_TOTAL:
        answer.rows.I64(SymbolsTotal(false));
        answer.Record();
        break;
    case HUB_CALL_SYMBOLS_GET:
        AnswerSymbols(arguments, answer);
        break;
    case HUB_CALL_POSITIONS_GET:
        AnswerPositions(arguments, answer);
        break;
    case HUB_CALL_ORDERS_GET:
        AnswerOrders(arguments, answer);
        break;
    case HUB_CALL_HISTORY_ORDERS_GET:
        AnswerHistoryOrders(arguments, answer);
        break;
    case HUB_CALL_HISTORY_DEALS_GET:
        AnswerHistoryDeals(arguments, answer);
        break;
    default:
        answer.Fail("the call is not served by this EA's role", ERR_FUNCTION_NOT_ALLOWED);
        break;
    }
}

// ── The publisher's calls: its symbol ────────────────────────────────────────────────────────────

void AnswerSymbolSelect(RequestArguments &arguments, Answer &answer)
{
    // The package's enable defaults to true.
    bool enable = true;
    if (arguments.Has(HUB_ARG_SYMBOL_SELECT_ENABLE))
        enable = arguments.Bool(HUB_ARG_SYMBOL_SELECT_ENABLE);
    if (SymbolSelect(arguments.String(HUB_ARG_SYMBOL_SELECT_SYMBOL), enable))
        answer.rows.U8(1);
    else
        answer.rows.U8(0);
    answer.Record();
}

void AnswerSymbolInfo(RequestArguments &arguments, Answer &answer)
{
    const string symbol = arguments.String(HUB_ARG_SYMBOL_INFO_SYMBOL);
    bool custom = false;
    if (SymbolExist(symbol, custom))
    {
        WriteSymbolInfo(answer.rows, symbol);
        answer.Record();
    }
    else
    {
        answer.Fail("SymbolExist found no such symbol", GetLastError());
    }
}

void AnswerSymbolInfoTick(RequestArguments &arguments, Answer &answer)
{
    MqlTick tick;
    if (SymbolInfoTick(arguments.String(HUB_ARG_SYMBOL_INFO_TICK_SYMBOL), tick))
    {
        answer.rows.Tick(tick);
        answer.Record();
    }
    else
    {
        answer.Fail("SymbolInfoTick failed", GetLastError());
    }
}

// Sends the request as it came; the answer means sent, and the REQUEST transaction carrying its
// request_id is the venue's verdict. A request the terminal refuses itself is answered with the
// result that says why.
void AnswerOrderSend(RequestArguments &arguments, Answer &answer)
{
    MqlTradeResult result;
    ZeroMemory(result);
    if (!OrderSendAsync(arguments.request, result))
        PrintFormat("ticks: '%s' the terminal refused a trade request: error %d", _Symbol,
                    GetLastError());
    WriteTradeResult(answer.rows, result);
    answer.Record();
}

// The outcome log's slot holding the request id's transaction, or -1.
int OutcomeSlot(const uint requestId)
{
    for (int age = 0; age < g_outcomesHeld; age++)
    {
        const int slot = (g_outcomeNext - 1 - age + OUTCOME_RING) % OUTCOME_RING;
        if (g_outcomes[slot].requestId == requestId)
            return slot;
    }
    return -1;
}

// Each request id's transaction the outcome log holds, in the order asked; an id it no longer
// holds answers nothing.
void AnswerOutcomes(RequestArguments &arguments, Answer &answer)
{
    for (int i = 0; i < ArraySize(arguments.ids); i++)
    {
        const int slot = OutcomeSlot(arguments.ids[i]);
        if (slot >= 0)
        {
            answer.rows.Bytes(g_outcomes[slot].record, 0, ArraySize(g_outcomes[slot].record));
            answer.Record();
        }
    }
}

void ServeSymbol(const ENUM_HUB_CALL call, RequestArguments &arguments, Answer &answer)
{
    switch (call)
    {
    case HUB_CALL_SYMBOL_SELECT:
        AnswerSymbolSelect(arguments, answer);
        break;
    case HUB_CALL_SYMBOL_INFO:
        AnswerSymbolInfo(arguments, answer);
        break;
    case HUB_CALL_SYMBOL_INFO_TICK:
        AnswerSymbolInfoTick(arguments, answer);
        break;
    case HUB_CALL_ORDER_SEND:
        AnswerOrderSend(arguments, answer);
        break;
    case HUB_CALL_OUTCOMES:
        AnswerOutcomes(arguments, answer);
        break;
    case HUB_CALL_COPY_RATES_RANGE:
    case HUB_CALL_COPY_RATES_FROM:
    case HUB_CALL_COPY_TICKS_RANGE:
    case HUB_CALL_COPY_TICKS_FROM:
        answer.Fail("history reads are not served", ERR_FUNCTION_NOT_ALLOWED);
        break;
    default:
        answer.Fail("the call is not served by this EA's role", ERR_FUNCTION_NOT_ALLOWED);
        break;
    }
}

// Serve one request and answer it once
void OnRequest(const ulong requestId, const uint count, BinaryReader &reader)
{
    Answer answer;
    int lastError = 0;
    const ushort code = reader.U16();
    if (reader.failed)
    {
        answer.Fail("a request out of its layout", ERR_INVALID_PARAMETER);
    }
    else if (!HubServes(Role, (ENUM_HUB_CALL)code))
    {
        answer.Fail("the call is not served by this EA's role", ERR_FUNCTION_NOT_ALLOWED);
    }
    else
    {
        RequestArguments arguments;
        if (!arguments.Read((ENUM_HUB_CALL)code, count, reader))
        {
            answer.Fail("arguments out of the call's layout", ERR_INVALID_PARAMETER);
        }
        else
        {
            ResetLastError();
            switch (Role)
            {
            case ROLE_SPAWNER:
                ServeAccount((ENUM_HUB_CALL)code, arguments, answer);
                break;
            case ROLE_PUBLISHER:
                ServeSymbol((ENUM_HUB_CALL)code, arguments, answer);
                break;
            }
            lastError = GetLastError();
        }
    }
    SendAnswer(requestId, answer, lastError);
}

// ── Transactions, as the trade_transaction frame and an outcome carry them ───────────────────────

// The symbol a transaction is of: its request's for a request transaction, whose own symbol MQL5
// leaves unfilled.
string TransactionSymbol(const MqlTradeTransaction &trans, const MqlTradeRequest &request)
{
    if (trans.type == TRADE_TRANSACTION_REQUEST)
        return request.symbol;
    else
        return trans.symbol;
}

void RecordOutcome(const uint requestId, BinaryWriter &frame)
{
    g_outcomes[g_outcomeNext].requestId = requestId;
    frame.Slice(HUB_HEADER_SIZE, frame.Size() - HUB_HEADER_SIZE, g_outcomes[g_outcomeNext].record);
    g_outcomeNext = (g_outcomeNext + 1) % OUTCOME_RING;
    if (g_outcomesHeld < OUTCOME_RING)
        g_outcomesHeld++;
}

// ── The wanted streams ───────────────────────────────────────────────────────────────────────────

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

// Count the ticks of the cursor's millisecond: ready only when the read answered every one of them
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

// The tick cursor of newly wanted ticks: the symbol's latest tick, so ticks from before the hub
// wanted them are never published
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

// Take the hub's wanted streams for the symbol: start the tick cursor when its ticks become wanted,
// keep the opens of the timeframes that stay wanted, start the new ones
void ApplyWanted(const bool wantTicks, const ENUM_TIMEFRAMES &timeframes[])
{
    if (wantTicks && !g_wantTicks)
        StartCursor();
    g_wantTicks = wantTicks;

    ENUM_TIMEFRAMES barTimeframes[];
    datetime barOpens[];
    for (int i = 0; i < ArraySize(timeframes); i++)
    {
        const int known = BarIndex(timeframes[i]);
        const int n = ArraySize(barTimeframes);
        ArrayResize(barTimeframes, n + 1);
        ArrayResize(barOpens, n + 1);
        barTimeframes[n] = timeframes[i];
        if (known >= 0)
            barOpens[n] = g_barOpens[known];
        else
            barOpens[n] = iTime(_Symbol, timeframes[i], 0);
    }
    ArraySwap(g_barTimeframes, barTimeframes);
    ArraySwap(g_barOpens, barOpens);
    PrintFormat("ticks: '%s' wanted: ticks %s, %d bar streams", _Symbol, (string)g_wantTicks,
                ArraySize(g_barTimeframes));
}

// ── The spawner's charts ─────────────────────────────────────────────────────────────────────────

// The comment an EA sets on its chart, by which the spawner finds the chart of a role and symbol.
string Marker(const ENUM_CHART_ROLE role)
{
    return MQLInfoString(MQL_PROGRAM_NAME) + " " + ChartRoleName(role);
}

// The open positions and pending orders the account holds on the symbol.
void CountHoldings(const string symbol, int &positions, int &orders)
{
    positions = 0;
    orders = 0;
    for (int i = PositionsTotal() - 1; i >= 0; i--)
        if (PositionGetSymbol(i) == symbol)
            positions++;
    for (int i = OrdersTotal() - 1; i >= 0; i--)
        if (OrderGetTicket(i) > 0 && OrderGetString(ORDER_SYMBOL) == symbol)
            orders++;
}

// The chart of the symbol whose comment marks the role, its EA running or gone; -1 when none.
long MarkedChart(const ENUM_CHART_ROLE role, const string symbol)
{
    long chart = ChartFirst();
    while (chart != -1)
    {
        if (ChartSymbol(chart) == symbol && ChartGetString(chart, CHART_COMMENT) == Marker(role))
            return chart;
        chart = ChartNext(chart);
    }
    return -1;
}

bool RunsThisEa(const long chart)
{
    string expert;
    return ChartGetString(chart, CHART_EXPERT_NAME, expert)
           && expert == MQLInfoString(MQL_PROGRAM_NAME);
}

// A chart of the symbol other than `closed` that is open, or -1 when none is.
long OtherChart(const string symbol, const long closed)
{
    long chart = ChartFirst();
    while (chart != -1)
    {
        if (chart != closed && ChartSymbol(chart) == symbol)
            return chart;
        chart = ChartNext(chart);
    }
    return -1;
}

// The index of the chart the spawner opened for the role and symbol, or -1.
int OpenedIndex(const ENUM_CHART_ROLE role, const string symbol)
{
    for (int i = 0; i < ArraySize(g_openedCharts); i++)
        if (g_openedRoles[i] == role && g_openedSymbols[i] == symbol)
            return i;
    return -1;
}

void RememberOpened(const long chart, const ENUM_CHART_ROLE role, const string symbol)
{
    const int n = ArraySize(g_openedCharts);
    ArrayResize(g_openedCharts, n + 1);
    ArrayResize(g_openedRoles, n + 1);
    ArrayResize(g_openedSymbols, n + 1);
    g_openedCharts[n] = chart;
    g_openedRoles[n] = role;
    g_openedSymbols[n] = symbol;
}

void ForgetOpened(const long chart)
{
    for (int i = ArraySize(g_openedCharts) - 1; i >= 0; i--)
    {
        if (g_openedCharts[i] == chart)
        {
            ArrayRemove(g_openedCharts, i, 1);
            ArrayRemove(g_openedRoles, i, 1);
            ArrayRemove(g_openedSymbols, i, 1);
        }
    }
}

// Forgets the charts the spawner opened that are closed: a closed chart's symbol reads empty.
void ForgetClosedCharts()
{
    for (int i = ArraySize(g_openedCharts) - 1; i >= 0; i--)
    {
        if (ChartSymbol(g_openedCharts[i]) != g_openedSymbols[i])
        {
            ArrayRemove(g_openedCharts, i, 1);
            ArrayRemove(g_openedRoles, i, 1);
            ArrayRemove(g_openedSymbols, i, 1);
        }
    }
}

int DeselectIndex(const string symbol)
{
    for (int i = 0; i < ArraySize(g_deselectSymbols); i++)
        if (g_deselectSymbols[i] == symbol)
            return i;
    return -1;
}

void KeepDeselecting(const string symbol)
{
    if (DeselectIndex(symbol) < 0)
    {
        const int n = ArraySize(g_deselectSymbols);
        ArrayResize(g_deselectSymbols, n + 1);
        ArrayResize(g_deselectAt, n + 1);
        ArrayResize(g_deselectHeld, n + 1);
        g_deselectSymbols[n] = symbol;
        g_deselectAt[n] = 0;
        g_deselectHeld[n] = false;
    }
}

void ForgetDeselect(const string symbol)
{
    const int index = DeselectIndex(symbol);
    if (index >= 0)
    {
        ArrayRemove(g_deselectSymbols, index, 1);
        ArrayRemove(g_deselectAt, index, 1);
        ArrayRemove(g_deselectHeld, index, 1);
    }
}

// Take out of Market Watch each symbol whose chart the spawner closed, once the terminal lets it;
// while positions or orders hold one, it is tried again only every HELD_DESELECT_SECONDS
void RetryDeselects()
{
    const ulong now = GetTickCount64();
    for (int i = ArraySize(g_deselectSymbols) - 1; i >= 0; i--)
    {
        if (now >= g_deselectAt[i])
        {
            const string symbol = g_deselectSymbols[i];
            int positions = 0;
            int orders = 0;
            CountHoldings(symbol, positions, orders);
            if (positions > 0 || orders > 0)
            {
                if (!g_deselectHeld[i])
                    PrintFormat("ticks: '%s' stays in Market Watch: %d open positions, %d "
                                "pending orders", symbol, positions, orders);
                g_deselectHeld[i] = true;
                g_deselectAt[i] = now + (ulong)HELD_DESELECT_SECONDS * 1000;
            }
            else if (SymbolSelect(symbol, false))
            {
                PrintFormat("ticks: took '%s' out of Market Watch", symbol);
                ForgetDeselect(symbol);
            }
        }
    }
}

// Takes the symbol out of Market Watch once no chart of it but `closed` is open; the terminal may
// still count a chart just closed open, so a refused deselect is tried again.
void Deselect(const string symbol, const long closed)
{
    if (OtherChart(symbol, closed) >= 0)
    {
        PrintFormat("ticks: '%s' stays in Market Watch for its other chart", symbol);
    }
    else if (SymbolSelect(symbol, false))
    {
        PrintFormat("ticks: took '%s' out of Market Watch", symbol);
    }
    else
    {
        PrintFormat("ticks: '%s' leaves Market Watch once allowed", symbol);
        KeepDeselecting(symbol);
    }
}

// Have a chart of the role and symbol open, and tell the hub whether one is: a chart whose EA is
// gone takes its template again, and a chart the spawner opened is loading its EA, so the open is
// idempotent
void OpenChart(const ENUM_CHART_ROLE role, const string symbol)
{
    ForgetClosedCharts();
    const long marked = MarkedChart(role, symbol);
    // The MQL5 call that failed, empty while a chart of the role and symbol is open, and its error.
    string failing = "";
    int error = 0;
    if (marked >= 0 && RunsThisEa(marked))
    {
        PrintFormat("ticks: the %s chart of '%s' already runs", ChartRoleName(role), symbol);
    }
    else if (marked >= 0)
    {
        ResetLastError();
        if (ChartApplyTemplate(marked, ChartTemplate))
        {
            RememberOpened(marked, role, symbol);
            PrintFormat("ticks: the %s chart of '%s' takes its template again", ChartRoleName(role),
                        symbol);
        }
        else
        {
            failing = "ChartApplyTemplate";
            error = GetLastError();
        }
    }
    else if (OpenedIndex(role, symbol) >= 0)
    {
        PrintFormat("ticks: the %s chart of '%s' is loading its EA", ChartRoleName(role), symbol);
    }
    else
    {
        ResetLastError();
        if (!SymbolSelect(symbol, true))
        {
            failing = "SymbolSelect";
            error = GetLastError();
        }
        else
        {
            const long chart = ChartOpen(symbol, PERIOD_M1);
            if (chart == 0)
            {
                failing = "ChartOpen";
                error = GetLastError();
            }
            else if (!ChartApplyTemplate(chart, ChartTemplate))
            {
                failing = "ChartApplyTemplate";
                error = GetLastError();
                ChartClose(chart);
            }
            else
            {
                RememberOpened(chart, role, symbol);
                PrintFormat("ticks: opened the %s chart of '%s'", ChartRoleName(role), symbol);
            }
        }
    }
    if (failing == "")
    {
        ForgetDeselect(symbol);
        SendChartOpened(role, symbol);
    }
    else
    {
        PrintFormat("ticks: cannot open the %s chart of '%s': %s failed with error %d",
                    ChartRoleName(role), symbol, failing, error);
        SendChartFailed(role, symbol, error);
    }
}

// Close the chart of the role and symbol, then take the symbol out of Market Watch once no chart of
// it is open; while the account holds positions or pending orders on the symbol the chart stays,
// and the hub is told
void CloseChart(const ENUM_CHART_ROLE role, const string symbol)
{
    ForgetClosedCharts();
    int positions = 0;
    int orders = 0;
    CountHoldings(symbol, positions, orders);
    // The chart the spawner opened for the role and symbol, or one that predates it.
    long chart = MarkedChart(role, symbol);
    const int opened = OpenedIndex(role, symbol);
    if (opened >= 0)
        chart = g_openedCharts[opened];
    if (positions > 0 || orders > 0)
    {
        PrintFormat("ticks: keeping the %s chart of '%s' open: %d open positions, %d pending "
                    "orders", ChartRoleName(role), symbol, positions, orders);
        SendChartKept(role, symbol, positions, orders);
    }
    else if (chart < 0)
    {
        PrintFormat("ticks: no %s chart of '%s' is open", ChartRoleName(role), symbol);
        Deselect(symbol, -1);
    }
    else if (!ChartClose(chart))
    {
        PrintFormat("ticks: ChartClose of the %s chart of '%s' failed: %d", ChartRoleName(role),
                    symbol, GetLastError());
    }
    else
    {
        ForgetOpened(chart);
        PrintFormat("ticks: closed the %s chart of '%s'", ChartRoleName(role), symbol);
        Deselect(symbol, chart);
    }
}

// Close this EA's chart, which unloads the EA
void CloseOwnChart()
{
    PrintFormat("ticks: another EA serves the %s of '%s'; closing this chart", ChartRoleName(Role),
                _Symbol);
    if (ChartClose(0))
        g_closing = true;
    else
        PrintFormat("ticks: ChartClose failed: %d", GetLastError());
}

// The hub refused this EA's hello: another EA serves its role and symbol
void OnDuplicate()
{
    const ulong now = GetTickCount64();
    g_refused = true;
    if (Role != ROLE_SPAWNER)
    {
        CloseOwnChart();
    }
    else if (g_firstRefusal == 0)
    {
        // The spawner may be this terminal's own earlier connection, which the hub drops at most
        // HubPingTimeoutSec after its last pong while its writes to it drain; the reconnect then
        // says hello again.
        g_firstRefusal = now;
        PrintFormat("ticks: another EA serves the spawner; saying hello again every %d s for %d s",
                    ReconnectIntervalSec, HubPingTimeoutSec);
    }
    else if (now - g_firstRefusal >= (ulong)HubPingTimeoutSec * 1000)
    {
        CloseOwnChart();
    }
}

// Frames from the hub
void OnWanted(const uint count, BinaryReader &reader)
{
    const string symbol = reader.String();
    const uchar ticks = reader.U8();
    ENUM_TIMEFRAMES timeframes[];
    if (!reader.failed && count <= (uint)reader.Remaining() / 4)
    {
        ArrayResize(timeframes, (int)count);
        for (uint i = 0; i < count; i++)
            timeframes[i] = (ENUM_TIMEFRAMES)reader.I32();
    }
    if (!reader.Done() || ArraySize(timeframes) != (int)count || ticks > 1)
        Print("ticks: a wanted frame out of its layout");
    else if (Role != ROLE_PUBLISHER)
        Print("ticks: a wanted frame to an EA that publishes no stream");
    else if (symbol != _Symbol)
        PrintFormat("ticks: a wanted frame for '%s', not '%s'", symbol, _Symbol);
    else
        ApplyWanted(ticks == 1, timeframes);
}

// An open_chart or close_chart frame, which the hub sends the spawner.
void OnChartFrame(const ENUM_HUB_FRAME kind, BinaryReader &reader)
{
    const string roleName = reader.String();
    const string symbol = reader.String();
    ENUM_CHART_ROLE role = ROLE_PUBLISHER;
    if (!reader.Done() || !ChartRoleNamed(roleName, role) || symbol == "")
        Print("ticks: a chart frame out of its layout");
    else if (Role != ROLE_SPAWNER)
        Print("ticks: a chart frame to an EA that is not the spawner");
    else if (role == ROLE_SPAWNER)
        PrintFormat("ticks: a chart frame for the spawner's chart of '%s', which the startup "
                    "script alone opens", symbol);
    else if (kind == HUB_FRAME_OPEN_CHART)
        OpenChart(role, symbol);
    else
        CloseChart(role, symbol);
}

void OnDuplicateFrame(BinaryReader &reader)
{
    const string roleName = reader.String();
    const string symbol = reader.String();
    ENUM_CHART_ROLE role = ROLE_PUBLISHER;
    if (!reader.Done() || !ChartRoleNamed(roleName, role))
        Print("ticks: a duplicate frame out of its layout");
    else if (role != Role || symbol != _Symbol)
        PrintFormat("ticks: a duplicate frame for the %s of '%s'", roleName, symbol);
    else
        OnDuplicate();
}

void OnHubFrame(const uchar &data[])
{
    BinaryReader reader;
    reader.Load(data);
    HubHeader header;
    if (!reader.Header(header) || header.flags != 0)
    {
        PrintFormat("ticks: a hub frame out of the protocol, %d bytes", ArraySize(data));
    }
    else
    {
        switch (header.kind)
        {
        case HUB_FRAME_REQUEST:
            OnRequest(header.correlation, header.count, reader);
            break;
        case HUB_FRAME_WANTED:
            OnWanted(header.count, reader);
            break;
        case HUB_FRAME_OPEN_CHART:
        case HUB_FRAME_CLOSE_CHART:
            OnChartFrame((ENUM_HUB_FRAME)header.kind, reader);
            break;
        case HUB_FRAME_DUPLICATE:
            OnDuplicateFrame(reader);
            break;
        case HUB_FRAME_ERROR:
            PrintFormat("ticks: the hub refused a frame: %s", reader.String());
            break;
        default:
            PrintFormat("ticks: a hub frame of kind %d, which no EA reads", header.kind);
            break;
        }
    }
}

// Read what the hub sent
void ReadHub()
{
    // The library answers a ping and acknowledges a close only while it reads, and it reads nothing
    // while a message it holds is unread.
    IWebSocketMessage *msg = wss.readMessage(false);
    while (msg != NULL)
    {
        uchar data[];
        msg.getData(data);
        delete msg;
        OnHubFrame(data);
        msg = wss.readMessage(false);
    }
}

// Publish the symbol's ticks since its cursor, a batch per frame
void PublishCursorTicks()
{
    // The read starts at the cursor's millisecond, so it takes the ticks of it already consumed on
    // top of a batch of new ones.
    MqlTick ticks[];
    const int copied = CopyTicks(_Symbol, ticks, COPY_TICKS_INFO, (ulong)g_tickCursorMsc,
                                 (uint)(g_tickCursorCount + TICK_BATCH));
    if (copied < 0)
        PrintFormat("ticks: CopyTicks '%s' failed: %d", _Symbol, GetLastError());
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_TICK, 0, 0);
    frame.String(_Symbol);
    uint published = 0;
    int atCursor = 0;
    for (int i = 0; i < copied; i++)
    {
        if (ticks[i].time_msc != g_tickCursorMsc)
        {
            frame.Tick(ticks[i]);
            published++;
            g_tickCursorMsc = ticks[i].time_msc;
            g_tickCursorCount = 1;
            atCursor = 1;
        }
        else
        {
            atCursor++;
            if (atCursor > g_tickCursorCount)
            {
                frame.Tick(ticks[i]);
                published++;
                g_tickCursorCount = atCursor;
            }
        }
    }
    if (published > 0)
    {
        frame.Seal(published);
        Send(frame);
    }
}

// Publish the symbol's ticks since its cursor while they are wanted, once the cursor's count is
// complete
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

// Publish the bar that closed for every wanted timeframe whose forming bar moved on
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

// Connect to the hub again; it tells a reconnected publisher what it wants, so nothing missed while
// away is published
void Reconnect()
{
    g_lastReconnect = TimeLocal();
    // A connection that said hello and ended unrefused was taken: the spawner's next refusal is a
    // first one.
    if (g_helloSent && !g_refused)
        g_firstRefusal = 0;
    g_helloSent = false;
    g_refused = false;
    ClearWanted();
    Print("Reconnecting");
    if (!wss.open())
        Print("Failed to reconnect to server");
    else
        SendHello();
}

// Whether MqlTick and MqlRates pack to the sizes the protocol carries them at.
bool PodsPackToTheProtocol()
{
    MqlTick tick;
    MqlRates rates;
    ZeroMemory(tick);
    ZeroMemory(rates);
    uchar tickBytes[];
    uchar ratesBytes[];
    StructToCharArray(tick, tickBytes);
    StructToCharArray(rates, ratesBytes);
    const bool packed = ArraySize(tickBytes) == HUB_TICK_SIZE
                        && ArraySize(ratesBytes) == HUB_RATES_SIZE;
    if (!packed)
        PrintFormat("ticks: MqlTick packs to %d bytes and MqlRates to %d, not %d and %d",
                    ArraySize(tickBytes), ArraySize(ratesBytes), HUB_TICK_SIZE, HUB_RATES_SIZE);
    return packed;
}

int OnInit()
{
    PrintFormat("ticks EA initializing as the %s of '%s'", ChartRoleName(Role), _Symbol);
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
    if (!PodsPackToTheProtocol())
        return INIT_FAILED;
    if (!ChartSetString(0, CHART_COMMENT, Marker(Role)))
        PrintFormat("ticks: the chart's role marker is not set: %d", GetLastError());
    // The timer fires whether or not the market ticks, so the EA reconnects, reads the hub and
    // relays while the market is closed too.
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
        if (Role == ROLE_PUBLISHER)
        {
            PublishTicks();
            PublishBars();
        }
        if (GetTickCount64() - g_lastRelay >= (ulong)RelaySeconds * 1000)
            Relay();
    }
}

void OnTick()
{
    if (g_closing)
        return;
    ReadHub();
    if (!g_closing && wss.isConnected() && Role == ROLE_PUBLISHER)
    {
        PublishTicks();
        PublishBars();
    }
}

// Every trade transaction of the publisher's symbol, as it arrives, a REQUEST transaction also into
// the outcome log; the spawner's every one that names no symbol
void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result)
{
    // Every EA receives the account's whole stream; the symbol filter publishes each transaction
    // once.
    const string symbol = TransactionSymbol(trans, request);
    const bool publisher = Role == ROLE_PUBLISHER && symbol == _Symbol;
    const bool spawner = Role == ROLE_SPAWNER && symbol == "";
    if (!g_closing && (publisher || spawner))
    {
        BinaryWriter frame;
        frame.Begin(HUB_FRAME_TRADE_TRANSACTION, 0, 0);
        WriteTradeTransaction(frame, trans);
        WriteTradeRequest(frame, request);
        WriteTradeResult(frame, result);
        frame.Seal(0);
        if (publisher && trans.type == TRADE_TRANSACTION_REQUEST)
            RecordOutcome(result.request_id, frame);
        Send(frame);
    }
}
