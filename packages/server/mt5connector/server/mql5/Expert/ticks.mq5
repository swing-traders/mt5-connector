// The EA inside the terminal, in the role its chart's template sets: the spawner serves the
// account, relays the trade server's time and opens and closes the charts the hub asks for; a
// publisher serves its symbol's ticks, closed bars, transactions, reads, trade commands and bar
// history, which the indicator it attaches beside itself syncs; a history EA serves its symbol's
// tick history, read in its own thread. Every frame it sends and reads is the hub protocol's binary
// one.
#property script_show_inputs

#include <MQL5Book/ws/wsclient.mqh>
#include <hub_wire.mqh>

input ENUM_CHART_ROLE Role = ROLE_PUBLISHER;
input string Server = "ws://127.0.0.1:9000/";
input int ReconnectIntervalSec = 3;
input int RelaySeconds = 5;
input int PollMilliseconds = 16;
// The templates the spawner applies to a chart it opens for a publisher and for a history EA.
input string ChartTemplate = "ticks.tpl";
input string HistoryTemplate = "ticks_history.tpl";
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

    // Fails the answer naming every property of `record` a generated writer could not read, each
    // with its error; the first one's error is the answer's.
    void FailRead(const string record, const HubPropertyFailures &failures)
    {
        int listed = failures.count;
        if (listed > HUB_PROPERTY_FAILURES_MAX)
            listed = HUB_PROPERTY_FAILURES_MAX;
        string named = "";
        for (int i = 0; i < listed; i++)
        {
            if (i > 0)
                named += ", ";
            named += StringFormat("%s (%d)", failures.properties[i], failures.errors[i]);
        }
        if (failures.count > listed)
            named += StringFormat(" and %d more", failures.count - listed);
        Fail(StringFormat("%s: %s could not be read", record, named), failures.errors[0]);
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

// Answers a request with an error, the MQL5 error its last_error; a deferring kind carries the
// delay before the request is asked again.
void SendError(const ulong requestId, const ENUM_HUB_ERROR kind, const string message,
               const int error)
{
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_ANSWER, HUB_FLAG_LAST, requestId);
    frame.U32(0);
    frame.I32(error);
    frame.U8((uchar)kind);
    frame.String(message);
    if (HubDefers(kind))
        frame.U32(HUB_RETRY_AFTER_MS);
    frame.Seal(0);
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
        SendError(requestId, HUB_ERROR_EA_ERROR, answer.message, answer.error);
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
        // Every symbol the terminal knows, in Market Watch or not, by its definition; one property
        // that cannot be read fails the listing.
        const int total = SymbolsTotal(false);
        HubPropertyFailures failures;
        for (int i = 0; i < total && !answer.failed; i++)
        {
            const string name = SymbolName(i, false);
            if (WriteSymbolListing(answer.rows, name, failures))
                answer.Record();
            else
                answer.FailRead(name, failures);
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
        HubPropertyFailures failures;
        for (int i = 0; i < total && !answer.failed; i++)
        {
            // Each selects the position for the writer.
            const string held = PositionGetSymbol(i);
            const ulong heldTicket = PositionGetTicket(i);
            if (heldTicket > 0 && (!bySymbol || held == symbol)
                && (!byTicket || heldTicket == ticket))
            {
                if (WritePosition(answer.rows, failures))
                    answer.Record();
                else
                    answer.FailRead(StringFormat("position %I64u", heldTicket), failures);
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
        HubPropertyFailures failures;
        for (int i = 0; i < total && !answer.failed; i++)
        {
            // Selects the order for the writer.
            const ulong held = OrderGetTicket(i);
            if (held > 0 && (!bySymbol || OrderGetString(ORDER_SYMBOL) == symbol)
                && (!byTicket || held == ticket))
            {
                if (WriteOrder(answer.rows, failures))
                    answer.Record();
                else
                    answer.FailRead(StringFormat("order %I64u", held), failures);
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
            AnswerHistoryOrder(answer, ticket);
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

void AnswerHistoryOrder(Answer &answer, const ulong ticket)
{
    HubPropertyFailures failures;
    if (WriteHistoryOrder(answer.rows, ticket, failures))
        answer.Record();
    else
        answer.FailRead(StringFormat("order %I64u", ticket), failures);
}

void AnswerSelectedOrders(Answer &answer)
{
    const int total = HistoryOrdersTotal();
    for (int i = 0; i < total && !answer.failed; i++)
    {
        const ulong ticket = HistoryOrderGetTicket(i);
        if (ticket > 0)
            AnswerHistoryOrder(answer, ticket);
    }
}

// The deals of the history a query names: the one its ticket names, every one of a position, or
// those of a window on the broker's clock.
void AnswerHistoryDeals(RequestArguments &arguments, Answer &answer)
{
    if (arguments.String(HUB_ARG_HISTORY_DEALS_GET_GROUP) != "")
    {
        answer.Fail("a group filter is not served", ERR_FUNCTION_NOT_ALLOWED);
    }
    else if (arguments.Has(HUB_ARG_HISTORY_DEALS_GET_TICKET))
    {
        // A ticket the history does not hold answers no record, its error the answer's.
        const ulong ticket = (ulong)arguments.Integer(HUB_ARG_HISTORY_DEALS_GET_TICKET);
        if (HistoryDealSelect(ticket))
            AnswerDeal(answer, ticket);
    }
    else if (arguments.Has(HUB_ARG_HISTORY_DEALS_GET_POSITION))
    {
        if (HistorySelectByPosition(arguments.Integer(HUB_ARG_HISTORY_DEALS_GET_POSITION)))
            AnswerSelectedDeals(answer);
        else
            answer.Fail("HistorySelectByPosition failed", GetLastError());
    }
    else
    {
        if (HistorySelect((datetime)arguments.Integer(HUB_ARG_HISTORY_DEALS_GET_DATE_FROM),
                          (datetime)arguments.Integer(HUB_ARG_HISTORY_DEALS_GET_DATE_TO)))
            AnswerSelectedDeals(answer);
        else
            answer.Fail("HistorySelect failed", GetLastError());
    }
}

void AnswerDeal(Answer &answer, const ulong ticket)
{
    HubPropertyFailures failures;
    if (WriteDeal(answer.rows, ticket, failures))
        answer.Record();
    else
        answer.FailRead(StringFormat("deal %I64u", ticket), failures);
}

void AnswerSelectedDeals(Answer &answer)
{
    const int total = HistoryDealsTotal();
    for (int i = 0; i < total && !answer.failed; i++)
    {
        const ulong ticket = HistoryDealGetTicket(i);
        if (ticket > 0)
            AnswerDeal(answer, ticket);
    }
}

void ServeAccount(const ENUM_HUB_CALL call, RequestArguments &arguments, Answer &answer)
{
    HubPropertyFailures failures;
    switch (call)
    {
    case HUB_CALL_ACCOUNT_INFO:
        if (WriteAccountInfo(answer.rows, failures))
            answer.Record();
        else
            answer.FailRead("the account", failures);
        break;
    case HUB_CALL_TERMINAL_INFO:
        if (WriteTerminalInfo(answer.rows, failures))
            answer.Record();
        else
            answer.FailRead("the terminal", failures);
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

void AnswerSymbolInfo(RequestArguments &arguments, Answer &answer)
{
    const string symbol = arguments.String(HUB_ARG_SYMBOL_INFO_SYMBOL);
    bool custom = false;
    HubPropertyFailures failures;
    if (!SymbolExist(symbol, custom))
        answer.Fail("SymbolExist found no such symbol", ERR_MARKET_UNKNOWN_SYMBOL);
    else if (WriteSymbolInfo(answer.rows, symbol, failures))
        answer.Record();
    else
        answer.FailRead(symbol, failures);
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

// Each request id's transaction the outcome log holds, in the order asked; an id it no longer holds
// answers nothing.
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
    default:
        answer.Fail("the call is not served by this EA's role", ERR_FUNCTION_NOT_ALLOWED);
        break;
    }
}

// Serves a call that answers at once, by its role.
void Serve(const ulong requestId, const ENUM_HUB_CALL call, RequestArguments &arguments)
{
    Answer answer;
    ResetLastError();
    switch (Role)
    {
    case ROLE_SPAWNER:
        ServeAccount(call, arguments, answer);
        break;
    case ROLE_PUBLISHER:
        ServeSymbol(call, arguments, answer);
        break;
    }
    SendAnswer(requestId, answer, GetLastError());
}

// Serve one request and answer it once: a history read once its windows are synced, any other call
// at once
void OnRequest(const ulong requestId, const uint count, BinaryReader &reader)
{
    const ENUM_HUB_CALL call = (ENUM_HUB_CALL)reader.U16();
    RequestArguments arguments;
    if (reader.failed)
        SendError(requestId, HUB_ERROR_EA_ERROR, "a request out of its layout",
                  ERR_INVALID_PARAMETER);
    else if (!HubServes(Role, call))
        SendError(requestId, HUB_ERROR_EA_ERROR, "the call is not served by this EA's role",
                  ERR_FUNCTION_NOT_ALLOWED);
    else if (!arguments.Read(call, count, reader))
        SendError(requestId, HUB_ERROR_EA_ERROR, "arguments out of the call's layout",
                  ERR_INVALID_PARAMETER);
    else if (call == HUB_CALL_COPY_RATES_RANGE || call == HUB_CALL_COPY_TICKS_RANGE)
        StartHistory(requestId, call, arguments);
    else
        Serve(requestId, call, arguments);
}

// ── History: windows synced, the rules over them, answers in parts ───────────────────────────────

// How long a series' floor is used before it is measured again.
#define FLOOR_TTL_SECONDS 900
// 1970-01-02 on the broker's clock, before any plausible history: where the floor stubs are read.
#define BEFORE_HISTORY_SECONDS 86400
#define WEEK_SECONDS (7 * DAY_SECONDS)
// Broker weeks start on Monday, and 1970-01-05 was the first.
#define FIRST_MONDAY_SECONDS (4 * DAY_SECONDS)
// The ticks among the series a floor is kept for: no timeframe is 0.
#define TICK_SERIES 0
// How often a tick request's busy is renewed while it is in flight, well within its bound.
#define TICK_BUSY_RENEWAL_S (HUB_TICK_BUSY_S / 3)
// The least time between two calls of one tick read, so a call the terminal answers at once with a
// loading error is not made again in a spin.
#define TICK_CALL_SPACING_MS 1000

// Where a series' rows begin on the broker's clock — seconds for bars, milliseconds for ticks —
// when it was measured, by GetTickCount64, what set it, and whether an answer has told the hub of
// it on this connection.
struct Floor
{
    bool held;
    long value;
    ulong measuredAt;
    ENUM_FLOOR_ORIGIN origin;
    bool published;
};

// A window of rates the indicator syncs: the ask that carries it, a timeframe, and a range's bounds
// or a start and count, in broker seconds. Synced in a stage of its own, it holds the rows and the
// first row's time the indicator's read of it answered.
struct HistoryWindow
{
    ENUM_HUB_EVENT ask;
    int timeframe;
    long from;
    long to;
    bool answered;
    int rows;
    long first;
};

// What a tick read is: a range, or a count from a time.
enum ENUM_TICK_READ
{
    TICK_READ_RANGE,
    TICK_READ_FROM
};

// How the terminal's answers stand on a gap: proven empty, or what keeps them from proving it.
enum ENUM_GAP_PROOF
{
    GAP_PROVEN,
    GAP_HOLDS_LAST_QUOTE,
    GAP_NO_RECORD_AFTER,
    GAP_NO_ROW_BEFORE
};

// A stretch of a window no read answered a row for, in broker seconds, and whether rows were read
// before and after it.
struct Gap
{
    long lo;
    long hi;
    bool rowsBefore;
    bool rowsAfter;
};

// How a run of a request's rules ended, or that it goes on.
enum ENUM_RUN_STATE
{
    RUN_GOING,
    RUN_SYNCING,
    RUN_UNPROVEN,
    RUN_FAILED
};

// A history request, from its arrival to its answer's last part.
class HistoryRequest
{
public:
    ulong id;
    ENUM_HUB_CALL call;
    string symbol;
    // A bar read's timeframe and its series' period.
    int timeframe;
    int period;
    // The window's bounds in broker seconds.
    long from;
    long to;
    uint flags;
    // The symbol's last quote and the terminal's time at arrival, which every run of the rules
    // reads: whether SymbolInfoTick answered, its time, zero for the zero tick, and its error.
    bool quoteRead;
    long quoteTime;
    int quoteError;
    long present;
    HistoryWindow synced[];
    HistoryWindow stage[];
    // The readies heard of the stage, the first error one carried, and when it was asked.
    int readies;
    int stageError;
    ulong askedAt;
    bool answering;
    // When the hub last heard the request is busy, and when a tick request arrived, which bounds
    // how long its reads wait.
    ulong busyAt;
    ulong arrivedAt;
    MqlRates rates[];
    MqlTick ticks[];
    int sent;
    uint part;
    int lastError;

    HistoryRequest(const ulong requestId, const ENUM_HUB_CALL requested)
        : id(requestId), call(requested), symbol(""), timeframe(0), period(0),
          from(0), to(0), flags(0), quoteRead(false), quoteTime(0), quoteError(0), present(0),
          readies(0), stageError(0), askedAt(0), answering(false), busyAt(0), arrivedAt(0), sent(0),
          part(0), lastError(0)
    {
    }
};

HistoryRequest *g_history[];
// The floors of this EA's series, by timeframe or TICK_SERIES.
int g_floorSeries[];
Floor g_floors[];

// One run of a request's rules over the windows synced for it. Its floor writes reach the store
// only once it ends, so a run stopped to sync leaves the store as it found it.
class HistoryRun
{
public:
    ENUM_RUN_STATE state;
    string why;
    int error;
    // The error the latest read left.
    int lastError;
    HistoryWindow needed[];
    // The request's series, and its floor as the store holds it with this run's writes.
    int series;
    Floor floor;
    bool wrote;
    int writtenSeries[];
    long writtenValues[];
    ENUM_FLOOR_ORIGIN writtenOrigins[];

    HistoryRun() : state(RUN_GOING), why(""), error(0), lastError(0), series(0), wrote(false)
    {
        ZeroMemory(floor);
    }

    bool Going() const { return state == RUN_GOING; }

    void Stop(const ENUM_RUN_STATE stopped, const string reason, const int mql5Error)
    {
        if (state == RUN_GOING)
        {
            state = stopped;
            why = reason;
            error = mql5Error;
        }
    }
};

bool IsBars(const HistoryRequest *r)
{
    return r.call == HUB_CALL_COPY_RATES_RANGE;
}

int RowCount(const HistoryRequest *r)
{
    if (IsBars(r))
        return ArraySize(r.rates);
    else
        return ArraySize(r.ticks);
}

string SeriesName(const HistoryRequest *r)
{
    string name = HUB_TICK_SERIES;
    int period = 0;
    if (IsBars(r))
        HubSeries(r.timeframe, name, period);
    return name;
}

string Described(const HistoryRequest *r)
{
    return StringFormat("%s %s broker %I64d..%I64d", r.symbol, SeriesName(r), r.from, r.to);
}

// ── Floors ───────────────────────────────────────────────────────────────────────────────────────

int FloorIndex(const int series)
{
    for (int i = 0; i < ArraySize(g_floorSeries); i++)
        if (g_floorSeries[i] == series)
            return i;
    return -1;
}

void StoredFloor(const int series, Floor &floor)
{
    const int index = FloorIndex(series);
    if (index >= 0)
        floor = g_floors[index];
    else
        ZeroMemory(floor);
}

bool IsFresh(const Floor &floor)
{
    return floor.held && GetTickCount64() - floor.measuredAt < (ulong)FLOOR_TTL_SECONDS * 1000;
}

// The floor a measurement leaves in force: the stub of a series served with a coarse prefix is that
// prefix's first row, so a stub never moves a coarse floor back.
void Measured(Floor &floor, const long value, const ENUM_FLOOR_ORIGIN origin)
{
    const bool coarseStands = floor.held && floor.origin == FLOOR_ORIGIN_COARSE
                              && origin == FLOOR_ORIGIN_STUB && value <= floor.value;
    if (!coarseStands)
    {
        floor.value = value;
        floor.origin = origin;
    }
    floor.held = true;
    floor.measuredAt = GetTickCount64();
    floor.published = false;
}

// Records a measurement of a series in the run, and answers the floor then in force in `floor`.
void WriteFloor(HistoryRun &run, const int series, const long value,
                const ENUM_FLOOR_ORIGIN origin, Floor &floor)
{
    if (series == run.series)
    {
        Measured(run.floor, value, origin);
        floor = run.floor;
    }
    else
    {
        StoredFloor(series, floor);
        Measured(floor, value, origin);
    }
    const int n = ArraySize(run.writtenValues);
    ArrayResize(run.writtenSeries, n + 1);
    ArrayResize(run.writtenValues, n + 1);
    ArrayResize(run.writtenOrigins, n + 1);
    run.writtenSeries[n] = series;
    run.writtenValues[n] = value;
    run.writtenOrigins[n] = origin;
    run.wrote = true;
}

// The run's measurements into the store, each by the store's rule.
void CommitFloors(const HistoryRun &run)
{
    for (int i = 0; i < ArraySize(run.writtenValues); i++)
    {
        int index = FloorIndex(run.writtenSeries[i]);
        if (index < 0)
        {
            index = ArraySize(g_floorSeries);
            ArrayResize(g_floorSeries, index + 1);
            ArrayResize(g_floors, index + 1);
            g_floorSeries[index] = run.writtenSeries[i];
            ZeroMemory(g_floors[index]);
        }
        Measured(g_floors[index], run.writtenValues[i], run.writtenOrigins[i]);
    }
}

int SeriesOf(const HistoryRequest *r)
{
    if (IsBars(r))
        return r.timeframe;
    else
        return TICK_SERIES;
}

// A hub that reconnects may be a new one, which holds no floor.
void UnpublishFloors()
{
    for (int i = 0; i < ArraySize(g_floors); i++)
        g_floors[i].published = false;
}

// ── Windows synced and needed ────────────────────────────────────────────────────────────────────

void SetWindow(HistoryWindow &window, const ENUM_HUB_EVENT ask, const int timeframe,
               const long from, const long to)
{
    window.ask = ask;
    window.timeframe = timeframe;
    window.from = from;
    window.to = to;
    window.answered = false;
    window.rows = 0;
    window.first = 0;
}

bool SameWindow(const HistoryWindow &a, const HistoryWindow &b)
{
    return a.ask == b.ask && a.timeframe == b.timeframe && a.from == b.from && a.to == b.to;
}

// Whether the windows synced for the request hold the window: a range within their union, a start
// and count by the same ask.
bool Covered(const HistoryRequest *r, const HistoryWindow &window)
{
    bool covered = false;
    if (window.ask == HUB_EVENT_ASK_RATES_FROM)
    {
        for (int i = 0; i < ArraySize(r.synced) && !covered; i++)
            covered = SameWindow(r.synced[i], window);
    }
    else
    {
        // The first second of the window no synced range holds yet.
        long cursor = window.from;
        bool moved = true;
        while (cursor <= window.to && moved)
        {
            moved = false;
            for (int i = 0; i < ArraySize(r.synced); i++)
            {
                const HistoryWindow synced = r.synced[i];
                if (synced.ask == window.ask && synced.timeframe == window.timeframe
                    && synced.from <= cursor && cursor <= synced.to)
                {
                    cursor = synced.to + 1;
                    moved = true;
                }
            }
        }
        covered = cursor > window.to;
    }
    return covered;
}

// Whether the window is synced for the request; one that is not is added to the run's needs, which
// stops the run until the indicator has synced them.
bool Need(const HistoryRequest *r, HistoryRun &run, const HistoryWindow &window)
{
    const bool covered = Covered(r, window);
    if (!covered)
        AskNext(run, window);
    return covered;
}

// Adds the window to the run's needs, once, which stops the run until the indicator has synced
// them.
void AskNext(HistoryRun &run, const HistoryWindow &window)
{
    bool known = false;
    for (int i = 0; i < ArraySize(run.needed) && !known; i++)
        known = SameWindow(run.needed[i], window);
    if (!known)
    {
        const int n = ArraySize(run.needed);
        ArrayResize(run.needed, n + 1);
        run.needed[n] = window;
    }
    run.Stop(RUN_SYNCING, "", 0);
}

// ── Reads of synced windows ──────────────────────────────────────────────────────────────────────

// The rates opened in [from, to]; false, the run stopped, for a window not synced or a read that
// does not succeed.
bool ReadRates(const HistoryRequest *r, HistoryRun &run, const long from, const long to,
               MqlRates &rows[])
{
    HistoryWindow window;
    SetWindow(window, HUB_EVENT_ASK_RATES, r.timeframe, from, to);
    bool read = false;
    if (run.Going() && Need(r, run, window))
    {
        ResetLastError();
        const int copied = CopyRates(r.symbol, (ENUM_TIMEFRAMES)r.timeframe, (datetime)from,
                                     (datetime)to, rows);
        run.lastError = GetLastError();
        read = copied >= 0 && run.lastError == 0;
        if (!read)
            run.Stop(RUN_UNPROVEN, StringFormat("%s %s broker %I64d..%I64d: %d rows, last_error %d",
                                                r.symbol, SeriesName(r), from, to, copied,
                                                run.lastError),
                     run.lastError);
    }
    return read;
}

// Whether the indicator's read of the window has answered in a stage of its own, with its rows and
// its first row's time; one not answered yet is asked alone, which stops the run. The stubs and the
// probes' single rows are taken from it: the publisher's own read of a window wholly before a
// series answers -1 with 4401 even once the indicator's read answers it.
bool NeedAnswer(const HistoryRequest *r, HistoryRun &run, const HistoryWindow &window,
                     int &rows, long &first)
{
    bool answered = false;
    for (int i = 0; i < ArraySize(r.synced) && !answered && run.Going(); i++)
    {
        if (SameWindow(r.synced[i], window) && r.synced[i].answered)
        {
            rows = r.synced[i].rows;
            first = r.synced[i].first;
            answered = true;
        }
    }
    if (!answered && run.Going())
        AskNext(run, window);
    return answered;
}

void KeepRatesWithin(MqlRates &rows[], const long from, const long to)
{
    int kept = 0;
    for (int i = 0; i < ArraySize(rows); i++)
    {
        if ((long)rows[i].time >= from && (long)rows[i].time <= to)
        {
            rows[kept] = rows[i];
            kept++;
        }
    }
    ArrayResize(rows, kept);
}

void KeepTicksWithin(MqlTick &rows[], const long fromMsc, const long toMsc)
{
    int kept = 0;
    for (int i = 0; i < ArraySize(rows); i++)
    {
        if (rows[i].time_msc >= fromMsc && rows[i].time_msc <= toMsc)
        {
            rows[kept] = rows[i];
            kept++;
        }
    }
    ArrayResize(rows, kept);
}

// ── The rules' facts ─────────────────────────────────────────────────────────────────────────────

bool SelectSymbol(const HistoryRequest *r, HistoryRun &run)
{
    if (!SymbolSelect(r.symbol, true))
        run.Stop(RUN_FAILED, StringFormat("SymbolSelect '%s' failed", r.symbol), GetLastError());
    return run.Going();
}

// The widest window one read of the series spans: the terminal answers a wider one with nothing.
bool SpanSeconds(const HistoryRequest *r, HistoryRun &run, long &span)
{
    ResetLastError();
    const long maxbars = TerminalInfoInteger(TERMINAL_MAXBARS);
    const int error = GetLastError();
    if (error != 0)
        run.Stop(RUN_UNPROVEN, StringFormat("TERMINAL_MAXBARS: last_error %d", error), error);
    else if (maxbars <= HUB_SPAN_MARGIN)
        run.Stop(RUN_FAILED, StringFormat("the terminal's maxbars %I64d spans nothing", maxbars),
                 0);
    else
        span = (maxbars - HUB_SPAN_MARGIN) * r.period;
    return run.Going();
}

// The broker second of the symbol's last quote at the request's arrival, `quoted` false for the
// zero tick, which dates nothing.
void LastQuote(const HistoryRequest *r, HistoryRun &run, bool &quoted, long &lastQuote)
{
    if (!r.quoteRead)
        run.Stop(RUN_UNPROVEN, StringFormat("SymbolInfoTick: last_error %d", r.quoteError),
                 r.quoteError);
    quoted = r.quoteRead && r.quoteTime != 0;
    lastQuote = r.quoteTime;
}

// The time no bar opens after: the symbol's last quote, or the terminal's time at the request's
// arrival when it has none.
long Present(const HistoryRequest *r)
{
    if (r.quoteRead && r.quoteTime != 0)
        return r.quoteTime;
    else
        return r.present;
}

long Earliest(const long a, const long b)
{
    if (a < b)
        return a;
    else
        return b;
}

long Latest(const long a, const long b)
{
    if (a > b)
        return a;
    else
        return b;
}

// [lo, hi] split oldest-first into the fewest equal ranges spanning at most `span` each, so no
// range is a sliver.
void Chunks(const long lo, const long hi, const long span, long &los[], long &his[])
{
    const long count = Latest(1, (hi - lo + span - 1) / span);
    ArrayResize(los, (int)count);
    ArrayResize(his, (int)count);
    for (long i = 0; i < count; i++)
    {
        los[(int)i] = lo + (hi - lo) * i / count;
        his[(int)i] = lo + (hi - lo) * (i + 1) / count - 1;
    }
    his[(int)count - 1] = hi;
}

// The first row off the grid the first row sets, or not after the row before it; -1 for none.
int FirstOffGrid(const MqlRates &rows[], const int period)
{
    for (int i = 0; i < ArraySize(rows); i++)
        if (((long)rows[i].time - (long)rows[0].time) % period != 0
            || (i > 0 && rows[i].time <= rows[i - 1].time))
            return i;
    return -1;
}

int FirstDecrease(const MqlTick &rows[])
{
    for (int i = 1; i < ArraySize(rows); i++)
        if (rows[i].time_msc < rows[i - 1].time_msc)
            return i;
    return -1;
}

// How many leading rows an intraday series serves a whole number of days apart, ahead of its first
// two rows one period apart.
int CoarsePrefix(const MqlRates &rows[], const int period)
{
    int first = -1;
    for (int i = 0; i + 1 < ArraySize(rows) && first < 0; i++)
        if ((long)rows[i + 1].time - (long)rows[i].time == period)
            first = i;
    bool daily = true;
    for (int i = 0; i < first && daily; i++)
        daily = ((long)rows[i + 1].time - (long)rows[i].time) % DAY_SECONDS == 0;
    if (period < DAY_SECONDS && first > 0 && daily)
        return first;
    else
        return 0;
}

// The whole units, counted from `origin`, that [from, to] lies in.
void Containing(const long from, const long to, const long unit, const long origin, long &lo,
                long &hi)
{
    lo = from - FloorMod(from - origin, unit);
    hi = to - FloorMod(to - origin, unit) + unit - 1;
}

long FloorMod(const long value, const long unit)
{
    const long remainder = value % unit;
    if (remainder < 0)
        return remainder + unit;
    else
        return remainder;
}

// ── Bars ─────────────────────────────────────────────────────────────────────────────────────────

// The series' stub — the one row the terminal answers for a window before any plausible history —
// recorded, and the floor then in force in `floor`; none when it answers none, which proves
// nothing.
void MeasureBarFloor(const HistoryRequest *r, HistoryRun &run, Floor &floor)
{
    HistoryWindow window;
    SetWindow(window, HUB_EVENT_ASK_RATES, r.timeframe, BEFORE_HISTORY_SECONDS,
              BEFORE_HISTORY_SECONDS + r.period);
    int rows = 0;
    long first = 0;
    if (NeedAnswer(r, run, window, rows, first))
    {
        if (rows == 0)
            ZeroMemory(floor);
        else
            WriteFloor(run, run.series, first, FLOOR_ORIGIN_STUB, floor);
    }
}

// Whether the last bar the terminal answers as opened at or before the gap's start opened before
// it.
bool BarBefore(const HistoryRequest *r, HistoryRun &run, const Gap &gap)
{
    // CopyRates reads `count` bars back from its date: this one is the last at or before it.
    HistoryWindow window;
    SetWindow(window, HUB_EVENT_ASK_RATES_FROM, r.timeframe, gap.lo, 1);
    int rows = 0;
    long first = 0;
    return NeedAnswer(r, run, window, rows, first) && rows > 0 && first < gap.lo;
}

// Whether the first bar of the widest read from the gap's end, up to the present, lies after the
// gap.
bool BarAfter(const HistoryRequest *r, HistoryRun &run, const Gap &gap, const long span)
{
    const long hi = Earliest(gap.hi + span, Present(r));
    MqlRates rows[];
    return hi > gap.hi && ReadRates(r, run, gap.hi, hi, rows) && ArraySize(rows) > 0
           && (long)rows[0].time > gap.hi;
}

// Every chunk of the window from `lo` on, asked as one stage.
void NeedChunks(const HistoryRequest *r, HistoryRun &run, const long lo, const long span)
{
    HistoryWindow window;
    long los[];
    long his[];
    Chunks(lo, r.to, span, los, his);
    for (int i = 0; i < ArraySize(los); i++)
    {
        SetWindow(window, HUB_EVENT_ASK_RATES, r.timeframe, los[i], his[i]);
        Need(r, run, window);
    }
}

// Seeks the terminal's proof that the gap is empty — reading, or asking the indicator for, the
// records around it — and answers how it stands: proven when it lies after the second of the
// symbol's last quote, or rows lie before it and a record after it. The last quote is itself a
// record, so a gap holding its second is unproven.
ENUM_GAP_PROOF ProveGap(const HistoryRequest *r, HistoryRun &run, const Gap &gap, const long span)
{
    bool quoted = false;
    long lastQuote = 0;
    if (!gap.rowsAfter)
        LastQuote(r, run, quoted, lastQuote);
    ENUM_GAP_PROOF proof = GAP_PROVEN;
    // A run its last quote stopped judges nothing more; the live edge proves the gap.
    if (!run.Going() || (quoted && lastQuote < gap.lo))
        proof = GAP_PROVEN;
    else if (quoted && lastQuote <= gap.hi)
        proof = GAP_HOLDS_LAST_QUOTE;
    else if (!gap.rowsAfter && !RecordAfter(r, run, gap, span))
        proof = GAP_NO_RECORD_AFTER;
    else if (!gap.rowsBefore && !RowBefore(r, run, gap))
        proof = GAP_NO_ROW_BEFORE;
    return proof;
}

string Unproven(const HistoryRequest *r, const ENUM_GAP_PROOF proof)
{
    string reason = "";
    switch (proof)
    {
    case GAP_HOLDS_LAST_QUOTE:
        reason = StringFormat("the last quote, broker %I64d, lies in it", r.quoteTime);
        break;
    case GAP_NO_RECORD_AFTER:
        reason = "no record follows it";
        break;
    case GAP_NO_ROW_BEFORE:
        reason = "no row precedes it";
        break;
    }
    return reason;
}

bool RecordAfter(const HistoryRequest *r, HistoryRun &run, const Gap &gap, const long span)
{
    if (IsBars(r))
        return BarAfter(r, run, gap, span);
    else
        return TickAfter(r, run, gap);
}

bool RowBefore(const HistoryRequest *r, HistoryRun &run, const Gap &gap)
{
    if (IsBars(r))
        return BarBefore(r, run, gap);
    else
        return TicksBefore(r, run, gap);
}

// Stops the run unproven unless no bar lies in the gap, and leaves the floor then in force in
// `floor`: a kept floor is measured again for a gap no row precedes, since the terminal answers a
// window between a stale floor and its own with its own first bar alone.
void ProveNoBars(const HistoryRequest *r, HistoryRun &run, const Gap &gap, const long span,
                 Floor &floor, const bool kept)
{
    if (kept && !gap.rowsBefore && gap.hi >= floor.value)
    {
        Floor measured;
        ZeroMemory(measured);
        MeasureBarFloor(r, run, measured);
        if (run.Going() && measured.held)
            floor = measured;
    }
    if (run.Going() && !(floor.held && gap.hi < floor.value))
    {
        const ENUM_GAP_PROOF proof = ProveGap(r, run, gap, span);
        if (run.Going() && proof != GAP_PROVEN)
            run.Stop(RUN_UNPROVEN, StringFormat("no %s bar for broker %I64d..%I64d: %s",
                                                SeriesName(r), gap.lo, gap.hi, Unproven(r, proof)),
                     run.lastError);
    }
}

// The bars opened in [lo, the request's end], read chunk by chunk, on the series' grid and from its
// floor on, into the request's rates; stops the run unproven for a stretch no chunk answered a bar
// for that nothing proves empty.
void ReadBars(HistoryRequest *r, HistoryRun &run, const long lo, const long span, Floor &floor,
              const bool kept)
{
    NeedChunks(r, run, lo, span);
    long los[];
    long his[];
    Chunks(lo, r.to, span, los, his);
    const int chunks = ArraySize(los);
    int counts[];
    ArrayResize(counts, chunks);
    ArrayInitialize(counts, 0);
    MqlRates rows[];
    for (int i = 0; i < chunks && run.Going(); i++)
    {
        MqlRates part[];
        if (ReadRates(r, run, los[i], his[i], part))
        {
            KeepRatesWithin(part, los[i], his[i]);
            counts[i] = ArraySize(part);
            const int at = ArraySize(rows);
            ArrayResize(rows, at + counts[i]);
            ArrayCopy(rows, part, at, 0, counts[i]);
        }
    }
    int offGrid = -1;
    if (run.Going())
        offGrid = FirstOffGrid(rows, r.period);
    if (offGrid >= 0)
        run.Stop(RUN_FAILED, StringFormat("%s %s: row broker %I64d is off the grid", r.symbol,
                                          SeriesName(r), (long)rows[offGrid].time),
                 0);
    // The chunks before the first that answered rows and those after the last, or the whole window
    // when none did; a chunk between two that answered rows is bracketed by them.
    int first = -1;
    int last = -1;
    for (int i = 0; i < chunks; i++)
    {
        if (counts[i] > 0 && first < 0)
            first = i;
        if (counts[i] > 0)
            last = i;
    }
    Gap gap;
    if (run.Going() && first < 0)
    {
        gap.lo = los[0];
        gap.hi = his[chunks - 1];
        gap.rowsBefore = false;
        gap.rowsAfter = false;
        ProveNoBars(r, run, gap, span, floor, kept);
    }
    if (run.Going() && first > 0)
    {
        gap.lo = los[0];
        gap.hi = his[first - 1];
        gap.rowsBefore = false;
        gap.rowsAfter = true;
        ProveNoBars(r, run, gap, span, floor, kept);
    }
    if (run.Going() && last >= 0 && last < chunks - 1)
    {
        gap.lo = los[last + 1];
        gap.hi = his[chunks - 1];
        gap.rowsBefore = true;
        gap.rowsAfter = false;
        ProveNoBars(r, run, gap, span, floor, kept);
    }
    // A coarse prefix can only lead a series, so it is sought only in an answer beginning at the
    // floor the gap proofs leave in force.
    if (run.Going() && floor.held && ArraySize(rows) > 0 && (long)rows[0].time == floor.value)
    {
        const int prefix = CoarsePrefix(rows, r.period);
        if (prefix > 0)
        {
            WriteFloor(run, run.series, (long)rows[prefix].time, FLOOR_ORIGIN_COARSE, floor);
            PrintFormat("ticks: '%s' %s: %d daily-spaced rows dropped; the floor is broker %I64d",
                        r.symbol, SeriesName(r), prefix, floor.value);
        }
    }
    if (run.Going() && floor.held)
        KeepRatesWithin(rows, floor.value, r.to);
    if (run.Going())
        ArraySwap(r.rates, rows);
}

void RunBars(HistoryRequest *r, HistoryRun &run)
{
    long span = 0;
    if (SelectSymbol(r, run) && SpanSeconds(r, run, span))
    {
        Floor floor;
        floor = run.floor;
        const bool kept = IsFresh(floor);
        if (!kept)
            MeasureBarFloor(r, run, floor);
        if (run.Going() && floor.held && r.to < floor.value)
            ArrayFree(r.rates);
        else if (run.Going() && floor.held)
            ReadBars(r, run, Latest(r.from, floor.value), span, floor, kept);
        else if (run.Going())
            ReadBars(r, run, r.from, span, floor, kept);
    }
}

// ── Ticks ────────────────────────────────────────────────────────────────────────────────────────

// Makes the tick read until the terminal settles its answer, into `rows`, its error the run's
// lastError: a call answering a loading error is made again, and between calls the hub's frames are
// read and every busy that falls due renewed; while a call waits, nothing else of this
// single-threaded EA runs. False, the run stopped unproven, for an answer with an error, a request
// past its busy's bound, or a connection or chart going meanwhile.
bool SettleTicks(const HistoryRequest *r, HistoryRun &run, const ENUM_TICK_READ read,
                 const long fromMsc, const long toOrCount, const uint flags, MqlTick &rows[])
{
    const ulong bound = (ulong)HUB_TICK_BUSY_S * 1000;
    const string unsettled = StringFormat("%s ticks: not settled within %d s", r.symbol,
                                          HUB_TICK_BUSY_S);
    bool settled = false;
    int copied = -1;
    int attempt = 0;
    while (run.Going() && !settled)
    {
        const ulong now = GetTickCount64();
        if (now - r.arrivedAt >= bound)
        {
            run.Stop(RUN_UNPROVEN, unsettled, ERR_HISTORY_TIMEOUT);
        }
        else if (g_closing)
        {
            run.Stop(RUN_UNPROVEN, "the chart is closing", 0);
        }
        else if (!wss.isConnected())
        {
            run.Stop(RUN_UNPROVEN, "the hub connection is lost", 0);
        }
        else
        {
            for (int i = 0; i < ArraySize(g_history); i++)
                RenewBusy(g_history[i], now);
            attempt++;
            const ulong started = GetMicrosecondCount();
            ResetLastError();
            if (read == TICK_READ_RANGE)
                copied = CopyTicksRange(r.symbol, rows, flags, (ulong)fromMsc, (ulong)toOrCount);
            else
                copied = CopyTicks(r.symbol, rows, flags, (ulong)fromMsc, (uint)toOrCount);
            run.lastError = GetLastError();
            long first = 0;
            if (copied > 0)
                first = rows[0].time_msc;
            PrintFormat("ticks: request %I64u reads %s %I64d,%I64d, call %d: %d rows, error %d, "
                        "first %I64d, in %.3f s", r.id, EnumToString(read), fromMsc, toOrCount,
                        attempt, copied, run.lastError, first,
                        (double)(GetMicrosecondCount() - started) / 1000000.0);
            const bool loading = run.lastError == ERR_HISTORY_NOT_FOUND
                                 || run.lastError == ERR_HISTORY_TIMEOUT
                                 || (copied < 0 && run.lastError == 0);
            // A call that returns past the bound answers nothing, whatever it read.
            if (GetTickCount64() - r.arrivedAt >= bound)
            {
                run.Stop(RUN_UNPROVEN, unsettled, ERR_HISTORY_TIMEOUT);
            }
            else if (loading)
            {
                const long left = TICK_CALL_SPACING_MS
                                  - (long)((GetMicrosecondCount() - started) / 1000);
                if (left > 0)
                    Sleep((int)left);
                ReadHub();
            }
            else
            {
                settled = true;
            }
        }
    }
    const bool succeeded = settled && copied >= 0 && run.lastError == 0;
    if (settled && !succeeded)
        run.Stop(RUN_UNPROVEN, StringFormat("%s ticks %s %I64d,%I64d: %d rows, last_error %d",
                                            r.symbol, EnumToString(read), fromMsc, toOrCount,
                                            copied, run.lastError),
                 run.lastError);
    return succeeded;
}

// The ticks' floor, recorded, and the floor then in force in `floor`: the first tick of every kind
// the terminal answers from 1970-01-02; none when it answers none.
void MeasureTickFloor(const HistoryRequest *r, HistoryRun &run, Floor &floor)
{
    MqlTick rows[];
    if (SettleTicks(r, run, TICK_READ_FROM, (long)BEFORE_HISTORY_SECONDS * 1000, 1, COPY_TICKS_ALL,
                    rows)
        && ArraySize(rows) > 0)
        WriteFloor(run, run.series, rows[0].time_msc, FLOOR_ORIGIN_STUB, floor);
}

// Whether the first tick from the gap's last second lies after it.
bool TickAfter(const HistoryRequest *r, HistoryRun &run, const Gap &gap)
{
    MqlTick rows[];
    return SettleTicks(r, run, TICK_READ_FROM, gap.hi * 1000, 1, r.flags, rows)
           && ArraySize(rows) > 0 && rows[0].time_msc > gap.hi * 1000 + 999;
}

// Whether the day, or failing that the week, of the window the gap is answers a tick before the gap
// and none in it; each only where it is wider than the read before it.
bool TicksBefore(const HistoryRequest *r, HistoryRun &run, const Gap &gap)
{
    long dayLo = 0;
    long dayHi = 0;
    long weekLo = 0;
    long weekHi = 0;
    Containing(gap.lo, gap.hi, DAY_SECONDS, 0, dayLo, dayHi);
    Containing(gap.lo, gap.hi, WEEK_SECONDS, FIRST_MONDAY_SECONDS, weekLo, weekHi);
    long los[];
    long his[];
    if (dayLo != gap.lo || dayHi != gap.hi)
    {
        ArrayResize(los, 1);
        ArrayResize(his, 1);
        los[0] = dayLo;
        his[0] = dayHi;
    }
    if (weekLo != dayLo || weekHi != dayHi)
    {
        const int n = ArraySize(los);
        ArrayResize(los, n + 1);
        ArrayResize(his, n + 1);
        los[n] = weekLo;
        his[n] = weekHi;
    }
    bool before = false;
    bool decided = false;
    for (int i = 0; i < ArraySize(los) && !decided; i++)
    {
        MqlTick rows[];
        if (!SettleTicks(r, run, TICK_READ_RANGE, los[i] * 1000, his[i] * 1000 + 999, r.flags,
                         rows))
        {
            decided = true;
        }
        else
        {
            bool inGap = false;
            bool earlier = false;
            for (int k = 0; k < ArraySize(rows); k++)
            {
                if (rows[k].time_msc >= gap.lo * 1000 && rows[k].time_msc <= gap.hi * 1000 + 999)
                    inGap = true;
                else if (rows[k].time_msc < gap.lo * 1000)
                    earlier = true;
            }
            if (inGap)
            {
                decided = true;
            }
            else if (earlier)
            {
                before = true;
                decided = true;
            }
        }
    }
    return before;
}

// Stops the run unproven unless no tick lies in the gap. The ticks' floor is measured only for a
// gap no tick precedes, the one gap it can prove, when none is held.
void ProveNoTicks(const HistoryRequest *r, HistoryRun &run, const Gap &gap, Floor &floor)
{
    if (!(floor.held && gap.hi * 1000 + 999 < floor.value))
    {
        ENUM_GAP_PROOF proof = ProveGap(r, run, gap, 0);
        if (run.Going() && proof == GAP_NO_ROW_BEFORE && !floor.held)
        {
            MeasureTickFloor(r, run, floor);
            if (run.Going() && floor.held && gap.hi * 1000 + 999 < floor.value)
                proof = GAP_PROVEN;
        }
        if (run.Going() && proof != GAP_PROVEN)
            run.Stop(RUN_UNPROVEN, StringFormat("no tick for broker %I64d..%I64d: %s", gap.lo,
                                                gap.hi, Unproven(r, proof)),
                     run.lastError);
    }
}

// The ticks from the start of second `from` through the end of the request's last second, in time
// order, into the request's ticks; stops the run unproven when there are none and nothing proves
// the window empty.
void ReadTickWindow(HistoryRequest *r, HistoryRun &run, const long from, Floor &floor)
{
    MqlTick rows[];
    if (SettleTicks(r, run, TICK_READ_RANGE, from * 1000, r.to * 1000 + 999, r.flags, rows))
    {
        KeepTicksWithin(rows, from * 1000, r.to * 1000 + 999);
        if (ArraySize(rows) == 0)
        {
            Gap gap;
            gap.lo = from;
            gap.hi = r.to;
            gap.rowsBefore = false;
            gap.rowsAfter = false;
            ProveNoTicks(r, run, gap, floor);
        }
        const int decrease = FirstDecrease(rows);
        if (run.Going() && decrease >= 0)
            run.Stop(RUN_FAILED, StringFormat("%s ticks: row broker msc %I64d is out of order",
                                              r.symbol, rows[decrease].time_msc),
                     0);
        if (run.Going())
            ArraySwap(r.ticks, rows);
    }
}

// The window's ticks, read first: the ticks' floor, once held, stands, and a window wholly before
// it is empty without a read.
void RunTicks(HistoryRequest *r, HistoryRun &run)
{
    if (SelectSymbol(r, run))
    {
        Floor floor;
        floor = run.floor;
        if (floor.held && r.to * 1000 + 999 < floor.value)
            ArrayFree(r.ticks);
        else if (floor.held)
            ReadTickWindow(r, run, Latest(r.from, floor.value / 1000), floor);
        else
            ReadTickWindow(r, run, r.from, floor);
    }
}

// ── A request's stages and answer ────────────────────────────────────────────────────────────────

int HistoryIndex(const HistoryRequest *r)
{
    for (int i = 0; i < ArraySize(g_history); i++)
        if (g_history[i] == r)
            return i;
    return -1;
}

// Takes the request off the ones in flight; the indicator forgets what it still syncs for it.
void DropHistory(HistoryRequest *r)
{
    if (ArraySize(r.stage) > r.readies
        && !EventChartCustom(0, (ushort)HUB_EVENT_FORGET, (long)r.id, 0.0, ""))
        PrintFormat("ticks: the indicator was not told to forget request %I64u: %d", r.id,
                    GetLastError());
    const int index = HistoryIndex(r);
    if (index >= 0)
        ArrayRemove(g_history, index, 1);
    delete r;
}

void DropAllHistory()
{
    for (int i = ArraySize(g_history) - 1; i >= 0; i--)
        DropHistory(g_history[i]);
}

void AnswerSyncing(HistoryRequest *r, const string reason, const int error)
{
    PrintFormat("ticks: request %I64u, %s: syncing: %s", r.id, Described(r), reason);
    SendError(r.id, HUB_ERROR_SYNCING, Described(r) + ": syncing", error);
    DropHistory(r);
}

// Asks the indicator to sync the windows as the request's next stage; the hub hears the request is
// busy for at most the history deadline.
void AskIndicator(HistoryRequest *r, const HistoryWindow &windows[])
{
    ArrayFree(r.stage);
    r.readies = 0;
    r.stageError = 0;
    r.askedAt = GetTickCount64();
    int failed = 0;
    for (int i = 0; i < ArraySize(windows) && failed == 0; i++)
    {
        const string text = StringFormat("%d,%I64d,%I64d", windows[i].timeframe, windows[i].from,
                                         windows[i].to);
        ResetLastError();
        if (EventChartCustom(0, (ushort)windows[i].ask, (long)r.id, 0.0, text))
        {
            const int n = ArraySize(r.stage);
            ArrayResize(r.stage, n + 1);
            r.stage[n] = windows[i];
            PrintFormat("ticks: request %I64u asks the indicator: %s %s", r.id,
                        EnumToString(windows[i].ask), text);
        }
        else
        {
            failed = GetLastError();
        }
    }
    if (failed != 0)
    {
        SendError(r.id, HUB_ERROR_EA_ERROR, "EventChartCustom failed", failed);
        DropHistory(r);
    }
    else
    {
        SendBusy(r.id, HUB_HISTORY_DEADLINE_S);
    }
}

// The floor fact a read's last part carries: the series' floor as the store holds it when the hub
// has not heard it, whichever request measured it.
int FloorFact(const HistoryRequest *r, uchar &fact[])
{
    const int index = FloorIndex(SeriesOf(r));
    BinaryWriter writer;
    int carried = -1;
    if (index >= 0 && g_floors[index].held && !g_floors[index].published)
    {
        writer.U8(1);
        writer.I64(g_floors[index].value);
        writer.String(FloorOriginName(g_floors[index].origin));
        carried = index;
    }
    else
    {
        writer.U8(0);
    }
    writer.Take(fact);
    return carried;
}

// Sends the request's next part; true once the last is sent. The last carries the floor fact as the
// store stands when it goes, and the floor it carries is the hub's from then on.
bool SendRowsPart(HistoryRequest *r)
{
    int size = HUB_TICK_SIZE;
    if (IsBars(r))
        size = HUB_RATES_SIZE;
    uchar fact[];
    const int carried = FloorFact(r, fact);
    const int rows = RowCount(r);
    const int count = (int)Earliest((HUB_PART_BYTES - ArraySize(fact)) / size, rows - r.sent);
    const bool last = r.sent + count == rows;
    ushort flags = HUB_FLAG_OK;
    if (last)
        flags = HUB_FLAG_OK | HUB_FLAG_LAST;
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_ANSWER, flags, r.id);
    frame.U32(r.part);
    frame.I32(r.lastError);
    if (IsBars(r))
        frame.Rows(r.rates, r.sent, count);
    else
        frame.Rows(r.ticks, r.sent, count);
    if (last)
        frame.Bytes(fact, 0, ArraySize(fact));
    frame.Seal((uint)count);
    Send(frame);
    if (last && carried >= 0)
        g_floors[carried].published = true;
    r.sent += count;
    r.part++;
    return last;
}

// Starts the request's answer. Rows past one part go out a part per timer pass, so no pass waits on
// them, under a busy: a bar request's sent now, a tick request's the one it holds while in flight.
void StartAnswer(HistoryRequest *r, const HistoryRun &run)
{
    r.answering = true;
    r.lastError = run.lastError;
    PrintFormat("ticks: request %I64u, %s: %d rows", r.id, Described(r), RowCount(r));
    if (SendRowsPart(r))
    {
        DropHistory(r);
    }
    else if (IsBars(r))
    {
        SendBusy(r.id, HUB_HISTORY_DEADLINE_S);
        r.busyAt = GetTickCount64();
    }
}

// Runs the request's rules — a bar request's over the windows synced for it, a tick request's over
// its reads — and acts on where they end: asks the indicator for the windows they need, answers
// syncing or the failure, or starts the answer.
void Evaluate(HistoryRequest *r)
{
    HistoryRun run;
    run.series = SeriesOf(r);
    StoredFloor(run.series, run.floor);
    switch (r.call)
    {
    case HUB_CALL_COPY_RATES_RANGE:
        RunBars(r, run);
        break;
    case HUB_CALL_COPY_TICKS_RANGE:
        RunTicks(r, run);
        break;
    }
    if (run.state != RUN_SYNCING)
        CommitFloors(run);
    switch (run.state)
    {
    case RUN_SYNCING:
        AskIndicator(r, run.needed);
        break;
    case RUN_UNPROVEN:
        AnswerSyncing(r, run.why, run.error);
        break;
    case RUN_FAILED:
        PrintFormat("ticks: request %I64u, %s: %s", r.id, Described(r), run.why);
        SendError(r.id, HUB_ERROR_EA_ERROR, run.why, run.error);
        DropHistory(r);
        break;
    case RUN_GOING:
        StartAnswer(r, run);
        break;
    }
}

// Takes a history request: refused at once when out of what this EA serves or a window that starts
// after it ends or after the terminal's time; otherwise a bar request is run at once, and a tick
// request, whose reads wait, from the timer — never inside another's — busy from its arrival.
void StartHistory(const ulong requestId, const ENUM_HUB_CALL call, RequestArguments &arguments)
{
    HistoryRequest *r = new HistoryRequest(requestId, call);
    switch (call)
    {
    case HUB_CALL_COPY_RATES_RANGE:
        r.symbol = arguments.String(HUB_ARG_COPY_RATES_RANGE_SYMBOL);
        r.timeframe = (int)arguments.Integer(HUB_ARG_COPY_RATES_RANGE_TIMEFRAME);
        r.from = arguments.Integer(HUB_ARG_COPY_RATES_RANGE_DATE_FROM);
        r.to = arguments.Integer(HUB_ARG_COPY_RATES_RANGE_DATE_TO);
        break;
    case HUB_CALL_COPY_TICKS_RANGE:
        r.symbol = arguments.String(HUB_ARG_COPY_TICKS_RANGE_SYMBOL);
        r.from = arguments.Integer(HUB_ARG_COPY_TICKS_RANGE_DATE_FROM);
        r.to = arguments.Integer(HUB_ARG_COPY_TICKS_RANGE_DATE_TO);
        r.flags = (uint)arguments.Integer(HUB_ARG_COPY_TICKS_RANGE_FLAGS);
        break;
    }
    const long now = (long)TimeTradeServer();
    string series = "";
    if (r.symbol != _Symbol)
    {
        SendError(requestId, HUB_ERROR_EA_ERROR,
                  StringFormat("the history of '%s' is not this chart's", r.symbol),
                  ERR_INVALID_PARAMETER);
        delete r;
    }
    else if (IsBars(r) && !HubSeries(r.timeframe, series, r.period))
    {
        SendError(requestId, HUB_ERROR_EA_ERROR,
                  StringFormat("timeframe %d has no history series", r.timeframe),
                  ERR_INVALID_PARAMETER);
        delete r;
    }
    else if (r.from > r.to)
    {
        SendError(requestId, HUB_ERROR_INVALID_WINDOW, "the window starts after it ends", 0);
        delete r;
    }
    else if (r.from > now)
    {
        SendError(requestId, HUB_ERROR_INVALID_WINDOW,
                  StringFormat("the window starts at broker %I64d, after the terminal's time %I64d",
                               r.from, now),
                  0);
        delete r;
    }
    else
    {
        MqlTick tick;
        ZeroMemory(tick);
        ResetLastError();
        r.quoteRead = SymbolInfoTick(r.symbol, tick);
        r.quoteError = GetLastError();
        r.quoteTime = (long)tick.time;
        r.present = now;
        const int n = ArraySize(g_history);
        ArrayResize(g_history, n + 1);
        g_history[n] = r;
        if (IsBars(r))
        {
            Evaluate(r);
        }
        else
        {
            r.arrivedAt = GetTickCount64();
            r.busyAt = r.arrivedAt;
            SendBusy(r.id, HUB_TICK_BUSY_S);
            PrintFormat("ticks: request %I64u, %s: busy for %d s", r.id, Described(r),
                        HUB_TICK_BUSY_S);
        }
    }
}

// A ready the indicator signalled for a request: once every window of its stage is ready, the rules
// run again; a window that carried an error leaves the request unproven.
void OnHistoryReady(const ENUM_HUB_EVENT kind, const long requestId, const double rows,
                    const string answer)
{
    HistoryRequest *r = NULL;
    for (int i = 0; i < ArraySize(g_history) && r == NULL; i++)
        if (g_history[i].id == (ulong)requestId && !g_history[i].answering)
            r = g_history[i];
    if (r == NULL || r.readies >= ArraySize(r.stage))
    {
        PrintFormat("ticks: %s for request %I64d, which waits for none", EnumToString(kind),
                    requestId);
    }
    else
    {
        // "<error>,<first row's time>"
        string fields[];
        int carried = ERR_INVALID_PARAMETER;
        long first = 0;
        if (StringSplit(answer, ',', fields) == 2)
        {
            carried = (int)StringToInteger(fields[0]);
            first = StringToInteger(fields[1]);
        }
        r.readies++;
        if (r.stageError == 0)
            r.stageError = carried;
        // Readies name no window, so only a stage of one window keeps what its read answered.
        if (ArraySize(r.stage) == 1)
        {
            r.stage[0].answered = true;
            r.stage[0].rows = (int)rows;
            r.stage[0].first = first;
        }
        PrintFormat("ticks: request %I64u: %s, %d rows, error %d, first %I64d (%d of %d)", r.id,
                    EnumToString(kind), (int)rows, carried, first, r.readies, ArraySize(r.stage));
        if (r.readies == ArraySize(r.stage) && r.stageError != 0)
        {
            AnswerSyncing(r, StringFormat("the indicator's read left error %d", r.stageError),
                          r.stageError);
        }
        else if (r.readies == ArraySize(r.stage))
        {
            for (int k = 0; k < ArraySize(r.stage); k++)
            {
                const int n = ArraySize(r.synced);
                ArrayResize(r.synced, n + 1);
                r.synced[n] = r.stage[k];
            }
            ArrayFree(r.stage);
            r.readies = 0;
            Evaluate(r);
        }
    }
}

// Tells the hub again that the request is busy once its last busy falls due: a tick request's at a
// third of its bound while in flight, since another's reads may hold the EA meanwhile; a bar
// request's every half deadline while its answer streams.
void RenewBusy(HistoryRequest *r, const ulong now)
{
    if (!IsBars(r) && now - r.busyAt >= (ulong)TICK_BUSY_RENEWAL_S * 1000)
    {
        SendBusy(r.id, HUB_TICK_BUSY_S);
        r.busyAt = now;
        PrintFormat("ticks: request %I64u: busy again for %d s", r.id, HUB_TICK_BUSY_S);
    }
    else if (IsBars(r) && r.answering && now - r.busyAt >= (ulong)HUB_HISTORY_DEADLINE_S * 500)
    {
        SendBusy(r.id, HUB_HISTORY_DEADLINE_S);
        r.busyAt = now;
    }
}

// Runs the oldest tick request not yet run, whose reads may hold this pass for minutes; then each
// request's next answer part, every busy that falls due, and syncing for a stage not ready within
// the history deadline.
void ServeHistory()
{
    HistoryRequest *waiting = NULL;
    for (int i = 0; i < ArraySize(g_history) && waiting == NULL; i++)
        if (!IsBars(g_history[i]) && !g_history[i].answering)
            waiting = g_history[i];
    if (waiting != NULL)
        Evaluate(waiting);
    const ulong now = GetTickCount64();
    for (int i = ArraySize(g_history) - 1; i >= 0; i--)
    {
        HistoryRequest *r = g_history[i];
        if (r.answering && SendRowsPart(r))
        {
            DropHistory(r);
        }
        else if (!r.answering && ArraySize(r.stage) > 0
                 && now - r.askedAt >= (ulong)HUB_HISTORY_DEADLINE_S * 1000)
        {
            AnswerSyncing(r, StringFormat("not synced within %d s", HUB_HISTORY_DEADLINE_S),
                          ERR_HISTORY_TIMEOUT);
        }
        else
        {
            RenewBusy(r, now);
        }
    }
}

// The history indicator on the chart, which the publisher's history reads sync through.
int g_historyIndicator = INVALID_HANDLE;

bool AttachHistoryIndicator()
{
    ResetLastError();
    g_historyIndicator = iCustom(_Symbol, PERIOD_M1, HUB_HISTORY_INDICATOR);
    bool attached = false;
    if (g_historyIndicator == INVALID_HANDLE)
        PrintFormat("ticks: iCustom %s failed: %d", HUB_HISTORY_INDICATOR, GetLastError());
    else if (!ChartIndicatorAdd(0, 0, g_historyIndicator))
        PrintFormat("ticks: ChartIndicatorAdd %s failed: %d", HUB_HISTORY_INDICATOR,
                    GetLastError());
    else
        attached = true;
    return attached;
}

// Takes the indicator off the chart as well as releasing it, so a re-initialised EA attaches the
// chart's one indicator again rather than a second.
void DetachHistoryIndicator()
{
    if (g_historyIndicator != INVALID_HANDLE)
    {
        if (!ChartIndicatorDelete(0, 0, HUB_HISTORY_INDICATOR))
            PrintFormat("ticks: ChartIndicatorDelete %s failed: %d", HUB_HISTORY_INDICATOR,
                        GetLastError());
        IndicatorRelease(g_historyIndicator);
        g_historyIndicator = INVALID_HANDLE;
    }
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

// The template that attaches the EA in the role.
string TemplateOf(const ENUM_CHART_ROLE role)
{
    if (role == ROLE_HISTORY)
        return HistoryTemplate;
    else
        return ChartTemplate;
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
        if (ChartApplyTemplate(marked, TemplateOf(role)))
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
        bool custom = false;
        ResetLastError();
        if (!SymbolExist(symbol, custom))
        {
            failing = "SymbolExist";
            error = ERR_MARKET_UNKNOWN_SYMBOL;
        }
        else if (!SymbolSelect(symbol, true))
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
            else if (!ChartApplyTemplate(chart, TemplateOf(role)))
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
    // The hub answered every request of the connection that left.
    DropAllHistory();
    UnpublishFloors();
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
    if (Role == ROLE_PUBLISHER && !AttachHistoryIndicator())
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
    DropAllHistory();
    DetachHistoryIndicator();
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
        if (Role != ROLE_SPAWNER)
            ServeHistory();
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

// The history indicator's readies; the chart's own events and this EA's asks, which come back to
// it, are not the EA's
void OnChartEvent(const int id, const long &lparam, const double &dparam, const string &sparam)
{
    if (!g_closing && id == CHARTEVENT_CUSTOM + HUB_EVENT_RATES_READY)
        OnHistoryReady(HUB_EVENT_RATES_READY, lparam, dparam, sparam);
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
