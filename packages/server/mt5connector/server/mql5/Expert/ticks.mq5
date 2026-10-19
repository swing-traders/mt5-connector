// The EA inside the terminal, in the role its chart's template sets: the spawner serves the
// account, relays the trade server's time and opens and closes the charts the hub asks for; a
// publisher serves its symbol's ticks, closed bars, transactions, reads and trade commands; a
// history EA serves its symbol's history, read in its own thread once it has measured the symbol.
// Every frame it sends and reads is the hub protocol's binary one.
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
// How long a chart the spawner closed may stay listed before the frames of its symbol are served
// again.
#define CHART_CLOSE_GONE_SECONDS 10
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

// The open_chart and close_chart frames the spawner has not served, in arrival order. It serves one
// per timer pass: the terminal tears a chart down, and loads a template's EA, after the call
// returns.
ENUM_HUB_FRAME g_chartFrameKinds[];
ENUM_CHART_ROLE g_chartFrameRoles[];
string g_chartFrameSymbols[];

// The charts the spawner closed that the terminal may still list, each with its symbol and when it
// was closed, by GetTickCount64. A synchronous chart call on a chart being torn down blocks the
// EA's thread, so no chart of the list is searched for its comment, and no frame of its symbol is
// served while it is listed.
long g_closingCharts[];
string g_closingSymbols[];
ulong g_closingSince[];

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

// Serve one request and answer it once: a history request once the symbol is measured and its reads
// settle, any other call at once
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
    else if (call == HUB_CALL_COPY_RATES_RANGE || call == HUB_CALL_COPY_TICKS_RANGE
             || call == HUB_CALL_RANGES)
        StartHistory(requestId, call, arguments);
    else
        Serve(requestId, call, arguments);
}

// ── History: the symbol measured, then each read settled, answers in parts ───────────────────────

// 1970-01-02 on the broker's clock, before any plausible history: where the deep read and a frame's
// build read look.
#define BEFORE_HISTORY_SECONDS 86400
#define MINUTE_SECONDS 60
#define MINUTE_MS (MINUTE_SECONDS * 1000)
// The rows one read of a walk covers: a day of M1 bars.
#define WALK_ROWS 1440
// How often a history request's busy is renewed, well within its bound.
#define HISTORY_BUSY_RENEWAL_S (HUB_HISTORY_BUSY_S / 3)
// The least time between two calls of one history read, so a call the terminal answers at once with
// a loading error is not made again in a spin.
#define HISTORY_CALL_SPACING_MS 1000
// The turns the store's first date may stand still before it is read deep again, the deep reads
// without a movement after which the store is taken as stalled, and the polls a frame a stalled
// store leaves unbuilt gets before it is empty history.
#define STORE_STILL_TURNS 30
#define STORE_DEEP_READS 3
#define STALLED_FRAME_POLLS 90
// The reads a walk makes again for an anchor a new bar moved.
#define WALK_RESHIFTS 3

// Where a series' rows begin, in broker seconds. An empty series has no rows to serve; a capped
// frame's oldest bars are trimmed as new ones form, so its floor is measured at every request.
struct Floor
{
    bool held;
    long value;
    bool empty;
    bool capped;
};

// What a history EA's call reads: the ticks of a range the request selects, the ticks of every kind
// in a range, or the first tick of the symbol's history; the bars of a range, or a count of them
// from a position toward the past.
enum ENUM_HISTORY_READ
{
    READ_TICKS_RANGE,
    READ_EVERY_TICK_RANGE,
    READ_FIRST_TICK,
    READ_BARS_RANGE,
    READ_BARS_POS
};

// What a walk forward seeks: the first of two rows one period apart, or a row recording a spread.
enum ENUM_WALK_TEST
{
    WALK_PERIOD_PAIR,
    WALK_SPREAD
};

// How a walk forward ended: at a row that passes its test, at the current bar with none, at an
// anchor the terminal finds no bar at, or with the run stopped.
enum ENUM_WALK_END
{
    WALK_PASSED,
    WALK_EXHAUSTED,
    WALK_UNANCHORED,
    WALK_STOPPED
};

// How a run of a request ended, or that it goes on: not settled within its bound, which answers
// syncing, or failed.
enum ENUM_RUN_STATE
{
    RUN_GOING,
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
    // A read's window in broker seconds, and a tick read's selection.
    long from;
    long to;
    uint flags;
    // The symbol's last quote and the terminal's time as a tick read's reads begin: whether
    // SymbolInfoTick answered, its time, zero for the zero tick.
    bool quoteRead;
    long quoteTime;
    long present;
    bool answering;
    // When the hub last heard the request is busy, and when its run began, which bounds the run.
    ulong busyAt;
    ulong startedAt;
    MqlRates rates[];
    MqlTick ticks[];
    // A ranges answer: the terminal's MaxBars, and each floor's timeframe and value.
    long maxbars;
    int rangeTimeframes[];
    long rangeFloors[];
    int sent;
    uint part;
    int lastError;

    HistoryRequest(const ulong requestId, const ENUM_HUB_CALL requested)
        : id(requestId), call(requested), symbol(""), timeframe(0), period(0), from(0), to(0),
          flags(0), quoteRead(false), quoteTime(0), present(0), answering(false), busyAt(0),
          startedAt(0), maxbars(0), sent(0), part(0), lastError(0)
    {
    }
};

HistoryRequest *g_history[];
// The floors of this EA's series by timeframe, the ticks' under HUB_TICK_TIMEFRAME.
int g_floorSeries[];
Floor g_floors[];
// Whether the symbol is measured, whether its store is settled for this EA's life and whether by a
// stall, and its spread floor: the first M1 bar from the M1 floor on recording a spread.
bool g_measured = false;
bool g_storeSettled = false;
bool g_storeStalled = false;
bool g_spreadHeld = false;
long g_spreadFloor = 0;

// One run of a request: the symbol's measurement where it is not done, then the request's reads.
class HistoryRun
{
public:
    ENUM_RUN_STATE state;
    string why;
    int error;
    // The error the latest read left.
    int lastError;

    HistoryRun() : state(RUN_GOING), why(""), error(0), lastError(0) {}

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

// A walk forward through a frame: the open of the bar it reads from, its latest read and the row of
// it that passed, how many reads it made, and the error a failed shift left.
class Walk
{
public:
    long anchor;
    MqlRates rows[];
    int k;
    int reads;
    int error;

    Walk() : anchor(0), k(-1), reads(0), error(0) {}
};

bool IsBars(const HistoryRequest *r)
{
    return r.call == HUB_CALL_COPY_RATES_RANGE;
}

int RowCount(const HistoryRequest *r)
{
    int rows = 0;
    switch (r.call)
    {
    case HUB_CALL_COPY_RATES_RANGE:
        rows = ArraySize(r.rates);
        break;
    case HUB_CALL_COPY_TICKS_RANGE:
        rows = ArraySize(r.ticks);
        break;
    case HUB_CALL_RANGES:
        rows = ArraySize(r.rangeFloors);
        break;
    }
    return rows;
}

string SeriesName(const HistoryRequest *r)
{
    string name = "";
    int period = 0;
    switch (r.call)
    {
    case HUB_CALL_COPY_RATES_RANGE:
        HubSeries(r.timeframe, name, period);
        break;
    case HUB_CALL_COPY_TICKS_RANGE:
        name = HUB_TICK_SERIES;
        break;
    case HUB_CALL_RANGES:
        name = "ranges";
        break;
    }
    return name;
}

string Described(const HistoryRequest *r)
{
    if (r.call == HUB_CALL_RANGES)
        return StringFormat("%s ranges", r.symbol);
    else
        return StringFormat("%s %s broker %I64d..%I64d", r.symbol, SeriesName(r), r.from, r.to);
}

// ── Floors ───────────────────────────────────────────────────────────────────────────────────────

int FloorIndex(const int timeframe)
{
    for (int i = 0; i < ArraySize(g_floorSeries); i++)
        if (g_floorSeries[i] == timeframe)
            return i;
    return -1;
}

bool FloorHeld(const int timeframe)
{
    const int index = FloorIndex(timeframe);
    return index >= 0 && g_floors[index].held;
}

bool FloorCapped(const int timeframe)
{
    const int index = FloorIndex(timeframe);
    return index >= 0 && g_floors[index].capped;
}

// Keeps a series' floor as measured; true when it is new or moved.
bool KeepFloor(const int timeframe, const long value, const bool empty, const bool capped)
{
    int index = FloorIndex(timeframe);
    if (index < 0)
    {
        index = ArraySize(g_floorSeries);
        ArrayResize(g_floorSeries, index + 1);
        ArrayResize(g_floors, index + 1);
        g_floorSeries[index] = timeframe;
        ZeroMemory(g_floors[index]);
    }
    const bool moved = !g_floors[index].held || g_floors[index].value != value
                       || g_floors[index].empty != empty;
    g_floors[index].held = true;
    g_floors[index].value = value;
    g_floors[index].empty = empty;
    g_floors[index].capped = capped;
    return moved;
}

// ── The reads' bounds ────────────────────────────────────────────────────────────────────────────

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

bool SelectSymbol(const HistoryRequest *r, HistoryRun &run)
{
    if (!SymbolSelect(r.symbol, true))
        run.Stop(RUN_FAILED, StringFormat("SymbolSelect '%s' failed", r.symbol), GetLastError());
    return run.Going();
}

bool ReadMaxbars(const HistoryRequest *r, HistoryRun &run, long &maxbars)
{
    ResetLastError();
    maxbars = TerminalInfoInteger(TERMINAL_MAXBARS);
    const int error = GetLastError();
    if (error != 0)
        run.Stop(RUN_FAILED, StringFormat("%s: TERMINAL_MAXBARS could not be read", r.symbol),
                 error);
    return run.Going();
}

// The widest window one read of a series of the period spans: the terminal answers a wider one with
// nothing.
bool SpanSeconds(const HistoryRequest *r, HistoryRun &run, const int period, long &span)
{
    long maxbars = 0;
    if (ReadMaxbars(r, run, maxbars))
    {
        if (maxbars <= HUB_SPAN_MARGIN)
            run.Stop(RUN_FAILED,
                     StringFormat("the terminal's maxbars %I64d spans nothing", maxbars), 0);
        else
            span = (maxbars - HUB_SPAN_MARGIN) * period;
    }
    return run.Going();
}

// The symbol's last quote and the terminal's time, which bound the minutes a tick read synthesises.
void Snapshot(HistoryRequest *r)
{
    MqlTick tick;
    ZeroMemory(tick);
    r.quoteRead = SymbolInfoTick(r.symbol, tick);
    r.quoteTime = (long)tick.time;
    r.present = (long)TimeTradeServer();
}

// The time no bar opens after: the symbol's last quote, or the terminal's time when it has none.
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

// ── The reads ────────────────────────────────────────────────────────────────────────────────────

bool IsBarRead(const ENUM_HISTORY_READ read)
{
    return read == READ_BARS_RANGE || read == READ_BARS_POS;
}

// Makes one call of the read, into `ticks` or `rates` by its kind, answering what it copied: a tick
// read's bounds in broker milliseconds, a bar read's of the timeframe in broker seconds or as a
// start position and a count.
int CallRead(const HistoryRequest *r, const ENUM_HISTORY_READ read, const int timeframe,
             const long from, const long toOrCount, MqlTick &ticks[], MqlRates &rates[])
{
    int copied = -1;
    switch (read)
    {
    case READ_TICKS_RANGE:
        copied = CopyTicksRange(r.symbol, ticks, r.flags, (ulong)from, (ulong)toOrCount);
        break;
    case READ_EVERY_TICK_RANGE:
        copied = CopyTicksRange(r.symbol, ticks, (uint)COPY_TICKS_ALL, (ulong)from,
                                (ulong)toOrCount);
        break;
    case READ_FIRST_TICK:
        // From the first millisecond on, which syncs the whole tick history before it answers.
        copied = CopyTicks(r.symbol, ticks, (uint)COPY_TICKS_ALL, 1, 1);
        break;
    case READ_BARS_RANGE:
        copied = CopyRates(r.symbol, (ENUM_TIMEFRAMES)timeframe, (datetime)from,
                           (datetime)toOrCount, rates);
        break;
    case READ_BARS_POS:
        copied = CopyRates(r.symbol, (ENUM_TIMEFRAMES)timeframe, (int)from, (int)toOrCount,
                           rates);
        break;
    }
    return copied;
}

// Tells the hub again that the request is busy once its last busy falls due.
void RenewBusy(HistoryRequest *r, const ulong now)
{
    if (now - r.busyAt >= (ulong)HISTORY_BUSY_RENEWAL_S * 1000)
    {
        SendBusy(r.id, HUB_HISTORY_BUSY_S);
        r.busyAt = now;
        PrintFormat("ticks: request %I64u: busy again for %d s", r.id, HUB_HISTORY_BUSY_S);
    }
}

void RenewEveryBusy(const ulong now)
{
    for (int i = 0; i < ArraySize(g_history); i++)
        RenewBusy(g_history[i], now);
}

// Makes the read until the terminal settles its answer, into `ticks` or `rates` by its kind; a bar
// read's answer stands once an identical call answers it again. False, the run stopped, past its
// bound, on the chart closing, the program stopping, the connection lost or an answer's error.
bool Settle(const HistoryRequest *r, HistoryRun &run, const ENUM_HISTORY_READ read,
            const int timeframe, const long from, const long toOrCount, MqlTick &ticks[],
            MqlRates &rates[])
{
    const ulong bound = (ulong)HUB_HISTORY_BUSY_S * 1000;
    string unsettled = StringFormat("%s %s: not settled within %d s", r.symbol, EnumToString(read),
                                    HUB_HISTORY_BUSY_S);
    if (read == READ_FIRST_TICK)
        unsettled = StringFormat("%s: the tick history is still syncing after %d s", r.symbol,
                                 HUB_HISTORY_BUSY_S);
    const bool bars = IsBarRead(read);
    bool settled = false;
    // A bar read's answer awaiting the identical call that confirms it: its count and its first and
    // last opens.
    bool held = false;
    int heldCopied = 0;
    long heldFirst = 0;
    long heldLast = 0;
    int copied = -1;
    int attempt = 0;
    while (run.Going() && !settled)
    {
        const ulong now = GetTickCount64();
        if (now - r.startedAt >= bound)
        {
            run.Stop(RUN_UNPROVEN, unsettled, ERR_HISTORY_TIMEOUT);
        }
        else if (g_closing)
        {
            run.Stop(RUN_UNPROVEN, "the chart is closing", 0);
        }
        else if (IsStopped())
        {
            run.Stop(RUN_UNPROVEN, "the program is stopping", 0);
        }
        else if (!wss.isConnected())
        {
            run.Stop(RUN_UNPROVEN, "the hub connection is lost", 0);
        }
        else
        {
            RenewEveryBusy(now);
            attempt++;
            const ulong started = GetMicrosecondCount();
            ResetLastError();
            copied = CallRead(r, read, timeframe, from, toOrCount, ticks, rates);
            run.lastError = GetLastError();
            long first = 0;
            long last = 0;
            if (copied > 0 && bars)
            {
                first = (long)rates[0].time;
                last = (long)rates[copied - 1].time;
            }
            else if (copied > 0)
            {
                first = ticks[0].time_msc;
                last = ticks[copied - 1].time_msc;
            }
            // A position read is one of a walk's thousands, never logged on its own.
            if (!held && read != READ_BARS_POS)
                PrintFormat("ticks: request %I64u reads %s %I64d,%I64d, call %d: %d rows, error %d,"
                            " first %I64d, in %.3f s", r.id, EnumToString(read), from, toOrCount,
                            attempt, copied, run.lastError, first,
                            (double)(GetMicrosecondCount() - started) / 1000000.0);
            const bool loading = run.lastError == ERR_HISTORY_NOT_FOUND
                                 || run.lastError == ERR_HISTORY_TIMEOUT
                                 || (copied < 0 && run.lastError == 0);
            const bool agreed = held && copied == heldCopied && first == heldFirst
                                && last == heldLast;
            // A call that returns past the bound answers nothing, whatever it read.
            if (GetTickCount64() - r.startedAt >= bound)
            {
                run.Stop(RUN_UNPROVEN, unsettled, ERR_HISTORY_TIMEOUT);
            }
            else if (loading)
            {
                held = false;
                const long left = HISTORY_CALL_SPACING_MS
                                  - (long)((GetMicrosecondCount() - started) / 1000);
                if (left > 0)
                    Sleep((int)left);
            }
            else if (bars && run.lastError == 0 && copied >= 0 && !agreed)
            {
                held = true;
                heldCopied = copied;
                heldFirst = first;
                heldLast = last;
            }
            else
            {
                settled = true;
            }
            ReadHub();
        }
    }
    const bool succeeded = settled && copied >= 0 && run.lastError == 0;
    if (settled && !succeeded)
    {
        const string failed = StringFormat("%s %s %I64d,%I64d: %d rows, last_error %d", r.symbol,
                                           EnumToString(read), from, toOrCount, copied,
                                           run.lastError);
        if (bars)
            run.Stop(RUN_FAILED, failed, run.lastError);
        else
            run.Stop(RUN_UNPROVEN, failed, run.lastError);
    }
    return succeeded;
}

bool SettleTicks(const HistoryRequest *r, HistoryRun &run, const ENUM_HISTORY_READ read,
                 const long fromMsc, const long toOrCount, MqlTick &rows[])
{
    MqlRates none[];
    return Settle(r, run, read, (int)PERIOD_M1, fromMsc, toOrCount, rows, none);
}

bool SettleBars(const HistoryRequest *r, HistoryRun &run, const ENUM_HISTORY_READ read,
                const int timeframe, const long from, const long toOrCount, MqlRates &rows[])
{
    MqlTick none[];
    return Settle(r, run, read, timeframe, from, toOrCount, none, rows);
}

// Renews every busy that falls due and answers whether the wait goes on: false, the run stopped, on
// the chart closing, the program stopping, the connection lost or the run past its bound, `late`
// then the reason.
bool KeepWaiting(const HistoryRequest *r, HistoryRun &run, const string late)
{
    const ulong now = GetTickCount64();
    if (g_closing)
        run.Stop(RUN_UNPROVEN, "the chart is closing", 0);
    else if (IsStopped())
        run.Stop(RUN_UNPROVEN, "the program is stopping", 0);
    else if (!wss.isConnected())
        run.Stop(RUN_UNPROVEN, "the hub connection is lost", 0);
    else if (now - r.startedAt >= (ulong)HUB_HISTORY_BUSY_S * 1000)
        run.Stop(RUN_UNPROVEN, late, ERR_HISTORY_TIMEOUT);
    else
        RenewEveryBusy(now);
    return run.Going();
}

// A second's sleep, then the hub's frames read.
void Pause()
{
    Sleep(1000);
    ReadHub();
}

// ── The measurement ──────────────────────────────────────────────────────────────────────────────

// The two facts of the symbol's history store, its server's first date and how far the store
// reaches back; false for a not-yet answer, a false or a zero.
bool StoreFacts(const string symbol, long &serverFirst, long &terminalFirst)
{
    ResetLastError();
    const bool server = SeriesInfoInteger(symbol, PERIOD_M1, SERIES_SERVER_FIRSTDATE, serverFirst);
    ResetLastError();
    const bool terminal = SeriesInfoInteger(symbol, PERIOD_M1, SERIES_TERMINAL_FIRSTDATE,
                                            terminalFirst);
    return server && terminal && serverFirst > 0 && terminalFirst > 0;
}

// The blocking read of a window before any history, which starts the store's backfill; what it
// answers proves nothing, so it is logged and never judged.
void DeepRead(const HistoryRequest *r)
{
    MqlRates rows[];
    const ulong started = GetMicrosecondCount();
    ResetLastError();
    const int copied = CopyRates(r.symbol, PERIOD_M1, (datetime)BEFORE_HISTORY_SECONDS,
                                 (datetime)(BEFORE_HISTORY_SECONDS + MINUTE_SECONDS), rows);
    const int error = GetLastError();
    PrintFormat("ticks: %s: the deep read answered %d rows, error %d, in %.3f s", r.symbol, copied,
                error, (double)(GetMicrosecondCount() - started) / 1000000.0);
    ReadHub();
}

// Brings the symbol's bar store to its server's first date, or measures it from where it stopped
// once three deep reads leave it standing; either way it stays settled for this EA's life.
void AwaitStore(const HistoryRequest *r, HistoryRun &run)
{
    long serverFirst = 0;
    long terminalFirst = 0;
    bool known = StoreFacts(r.symbol, serverFirst, terminalFirst);
    if (known && terminalFirst <= serverFirst)
    {
        g_storeSettled = true;
    }
    else
    {
        DeepRead(r);
        // The store's first date as last read, consecutive turns it stood still since the last deep
        // read or movement, and the deep reads since the last movement.
        bool watching = known;
        long watched = terminalFirst;
        int still = 0;
        int idleReads = 1;
        while (!g_storeSettled
               && KeepWaiting(r, run,
                              StringFormat("%s: the history store is still loading after %d s: "
                                           "terminal first broker %I64d, "
                                           "server first broker %I64d",
                                           r.symbol, HUB_HISTORY_BUSY_S, terminalFirst,
                                           serverFirst)))
        {
            known = StoreFacts(r.symbol, serverFirst, terminalFirst);
            if (known && terminalFirst <= serverFirst)
            {
                g_storeSettled = true;
            }
            else
            {
                // A turn whose facts are not yet known counts nothing.
                if (known && watching && terminalFirst != watched)
                {
                    still = 0;
                    idleReads = 0;
                }
                else if (known && watching)
                {
                    still++;
                }
                if (known)
                {
                    watching = true;
                    watched = terminalFirst;
                }
                WatchStill(r, still, idleReads, terminalFirst, serverFirst);
                if (!g_storeSettled)
                    Pause();
            }
        }
    }
}

// The progress watch's verdict once the store's first date has stood still STORE_STILL_TURNS turns:
// settled by a stall after STORE_DEEP_READS deep reads without a movement, else read deep again.
void WatchStill(const HistoryRequest *r, int &still, int &idleReads, const long terminalFirst,
                const long serverFirst)
{
    if (still == STORE_STILL_TURNS && idleReads == STORE_DEEP_READS)
    {
        PrintFormat("ticks: warning: %s: the history store stopped at terminal first broker %I64d,"
                    " server first broker %I64d after three deep reads; measuring from there",
                    r.symbol, terminalFirst, serverFirst);
        g_storeStalled = true;
        g_storeSettled = true;
    }
    else if (still == STORE_STILL_TURNS)
    {
        DeepRead(r);
        idleReads++;
        still = 0;
    }
}

// The frame's bar count once it is built, asked to build by one read before any history; 0, the run
// going, for a frame a stalled store leaves unbuilt after its polls.
int BuiltBars(const HistoryRequest *r, HistoryRun &run, const int timeframe, const int period,
              const string name)
{
    int bars = Bars(r.symbol, (ENUM_TIMEFRAMES)timeframe);
    if (bars == 0)
    {
        MqlRates rows[];
        ResetLastError();
        const int copied = CopyRates(r.symbol, (ENUM_TIMEFRAMES)timeframe,
                                     (datetime)BEFORE_HISTORY_SECONDS,
                                     (datetime)(BEFORE_HISTORY_SECONDS + period), rows);
        const int error = GetLastError();
        PrintFormat("ticks: %s %s: the build read answered %d rows, error %d", r.symbol, name,
                    copied, error);
        ReadHub();
        const string late = StringFormat("%s %s: the frame is not built within %d s", r.symbol,
                                         name, HUB_HISTORY_BUSY_S);
        int polls = 0;
        while (bars == 0 && !(g_storeStalled && polls >= STALLED_FRAME_POLLS)
               && KeepWaiting(r, run, late))
        {
            Pause();
            polls++;
            bars = Bars(r.symbol, (ENUM_TIMEFRAMES)timeframe);
        }
    }
    return bars;
}

bool FirstDate(const string symbol, const int timeframe, long &first)
{
    ResetLastError();
    return SeriesInfoInteger(symbol, (ENUM_TIMEFRAMES)timeframe, SERIES_FIRSTDATE, first)
           && first > 0;
}

// The open of the frame's first available row: once the frame holds more bars than the terminal's
// MaxBars, the bar at the last position a read may start from, else the frame's first date.
bool FirstRow(const HistoryRequest *r, HistoryRun &run, const int timeframe, const string name,
              const int bars, const long maxbars, long &first)
{
    if (bars > maxbars)
    {
        MqlRates rows[];
        ResetLastError();
        const int copied = CopyRates(r.symbol, (ENUM_TIMEFRAMES)timeframe, (int)(maxbars - 1), 1,
                                     rows);
        const int error = GetLastError();
        ReadHub();
        if (copied == 1)
            first = (long)rows[0].time;
        else
            run.Stop(RUN_FAILED, StringFormat("%s %s: the bar at position %I64d: %d rows, "
                                              "last_error %d", r.symbol, name, maxbars - 1,
                                              copied, error),
                     error);
    }
    else
    {
        const string late = StringFormat("%s %s: the first row is not known within %d s", r.symbol,
                                         name, HUB_HISTORY_BUSY_S);
        while (!FirstDate(r.symbol, timeframe, first) && KeepWaiting(r, run, late))
            Pause();
    }
    return run.Going();
}

// The index in a read of a walk of its first row that passes the test, or -1: the first of two rows
// one period apart, or a row recording a spread, the current bar never tested for one, since a
// forming bar's spread is not final.
int Passing(const MqlRates &rows[], const ENUM_WALK_TEST test, const int period,
            const bool current)
{
    int passing = -1;
    int tested = ArraySize(rows);
    if (current)
        tested = ArraySize(rows) - 1;
    if (test == WALK_PERIOD_PAIR)
    {
        for (int k = 0; k + 1 < ArraySize(rows) && passing < 0; k++)
            if ((long)rows[k + 1].time - (long)rows[k].time == period)
                passing = k;
    }
    else
    {
        for (int k = 0; k < tested && passing < 0; k++)
            if (rows[k].spread > 0)
                passing = k;
    }
    return passing;
}

// Walks the frame forward from the bar opened at walk.anchor, WALK_ROWS by position per read, each
// read anchored by time, since a bar's position grows as bars form, to the first row that passes
// the test, walk.rows[walk.k]; a read reaching the current bar with none ends it exhausted.
ENUM_WALK_END WalkForward(const HistoryRequest *r, HistoryRun &run, const int timeframe,
                          const int period, const ENUM_WALK_TEST test, Walk &walk)
{
    string name = "";
    int unused = 0;
    HubSeries(timeframe, name, unused);
    ENUM_WALK_END end = WALK_STOPPED;
    bool walking = true;
    int reshifts = 0;
    while (walking && run.Going())
    {
        ResetLastError();
        const int i = iBarShift(r.symbol, (ENUM_TIMEFRAMES)timeframe, (datetime)walk.anchor, true);
        walk.error = GetLastError();
        const int start = (int)Latest(i - (WALK_ROWS - 1), 0);
        const int count = i - start + 1;
        if (i < 0)
        {
            end = WALK_UNANCHORED;
            walking = false;
        }
        else if (SettleBars(r, run, READ_BARS_POS, timeframe, start, count, walk.rows))
        {
            walk.reads++;
            const int n = ArraySize(walk.rows);
            if (n != count)
            {
                run.Stop(RUN_FAILED, StringFormat("%s %s: the walk asked %d rows from position %d "
                                                  "and got %d", r.symbol, name, count, start, n),
                         0);
            }
            else if ((long)walk.rows[0].time != walk.anchor)
            {
                reshifts++;
                if (reshifts > WALK_RESHIFTS)
                    run.Stop(RUN_FAILED,
                             StringFormat("%s %s: the walk's anchor moved", r.symbol, name), 0);
            }
            else
            {
                reshifts = 0;
                walk.k = Passing(walk.rows, test, period, start == 0);
                if (walk.k >= 0)
                {
                    end = WALK_PASSED;
                    walking = false;
                }
                else if (start > 0)
                {
                    walk.anchor = (long)walk.rows[n - 1].time;
                }
                else
                {
                    end = WALK_EXHAUSTED;
                    walking = false;
                }
            }
        }
    }
    return end;
}

// Measures a built frame's floor from its first available row: that row for a daily or coarser
// frame, else the walk's first pair one period apart, or empty history when the walk finds none.
ENUM_WALK_END MeasureFrom(const HistoryRequest *r, HistoryRun &run, const int timeframe,
                          const int period, const string name, const int bars, const long maxbars,
                          Walk &walk)
{
    const bool capped = bars >= maxbars;
    ENUM_WALK_END end = WALK_STOPPED;
    long first = 0;
    if (FirstRow(r, run, timeframe, name, bars, maxbars, first))
    {
        walk.anchor = first;
        if (period >= DAY_SECONDS)
        {
            end = WALK_PASSED;
            if (KeepFloor(timeframe, first, false, capped))
                PrintFormat("ticks: %s %s: floor broker %I64d after %d reads", r.symbol, name,
                            first, walk.reads);
        }
        else
        {
            end = WalkForward(r, run, timeframe, period, WALK_PERIOD_PAIR, walk);
            KeepWalkedFloor(r, timeframe, period, name, capped, end, walk);
        }
    }
    return end;
}

// Keeps the floor a walk for a pair ended on: the pair's first row, or, past the current bar with
// none, empty history from the period after it.
void KeepWalkedFloor(const HistoryRequest *r, const int timeframe, const int period,
                     const string name, const bool capped, const ENUM_WALK_END end,
                     const Walk &walk)
{
    if (end == WALK_PASSED)
    {
        const long floor = (long)walk.rows[walk.k].time;
        if (KeepFloor(timeframe, floor, false, capped))
            PrintFormat("ticks: %s %s: floor broker %I64d after %d reads", r.symbol, name, floor,
                        walk.reads);
    }
    else if (end == WALK_EXHAUSTED)
    {
        const long after = (long)walk.rows[ArraySize(walk.rows) - 1].time + period;
        if (KeepFloor(timeframe, after, true, capped))
            PrintFormat("ticks: %s %s: empty history after %d reads", r.symbol, name, walk.reads);
    }
}

// Measures the frame's floor: the first of its first two rows one period apart, or a daily or
// coarser frame's first row; a frame holding no such pair, or one a stalled store leaves unbuilt,
// is empty history.
void MeasureFrame(const HistoryRequest *r, HistoryRun &run, const int timeframe, const int period)
{
    string name = "";
    int unused = 0;
    HubSeries(timeframe, name, unused);
    long maxbars = 0;
    int bars = 0;
    if (ReadMaxbars(r, run, maxbars))
        bars = BuiltBars(r, run, timeframe, period, name);
    if (run.Going() && bars == 0)
    {
        if (KeepFloor(timeframe, (long)TimeTradeServer(), true, false))
            PrintFormat("ticks: %s %s: empty history: the stalled store built no frame", r.symbol,
                        name);
    }
    else if (run.Going())
    {
        Walk walk;
        ENUM_WALK_END end = MeasureFrom(r, run, timeframe, period, name, bars, maxbars, walk);
        // A first row the frame no longer holds is taken afresh, once.
        if (end == WALK_UNANCHORED)
            end = MeasureFrom(r, run, timeframe, period, name,
                              Bars(r.symbol, (ENUM_TIMEFRAMES)timeframe), maxbars, walk);
        if (end == WALK_UNANCHORED)
            run.Stop(RUN_FAILED, StringFormat("%s %s: the first row broker %I64d is not loaded: "
                                              "error %d", r.symbol, name, walk.anchor, walk.error),
                     walk.error);
    }
}

// The first M1 bar from the M1 floor on that records a spread; false, the run going, when none
// does.
bool SpreadFloor(const HistoryRequest *r, HistoryRun &run, long &spreadFloor)
{
    const Floor m1 = g_floors[FloorIndex((int)PERIOD_M1)];
    bool found = false;
    if (!m1.empty)
    {
        Walk walk;
        walk.anchor = m1.value;
        ENUM_WALK_END end = WalkForward(r, run, (int)PERIOD_M1, MINUTE_SECONDS, WALK_SPREAD, walk);
        // A row the frame no longer holds restarts the walk at the M1 floor measured afresh, once.
        if (end == WALK_UNANCHORED)
            end = WalkSpreadAfresh(r, run, walk);
        if (end == WALK_UNANCHORED)
            run.Stop(RUN_FAILED, StringFormat("%s M1: the spread walk's row broker %I64d is not "
                                              "loaded: error %d", r.symbol, walk.anchor,
                                              walk.error),
                     walk.error);
        found = end == WALK_PASSED;
        if (found)
            spreadFloor = (long)walk.rows[walk.k].time;
        PrintFormat("ticks: %s M1: spread floor broker %I64d after %d reads", r.symbol,
                    spreadFloor, walk.reads);
    }
    return found;
}

// Measures the M1 frame afresh and walks it for a spread again from its floor; a frame then of
// empty history holds none.
ENUM_WALK_END WalkSpreadAfresh(const HistoryRequest *r, HistoryRun &run, Walk &walk)
{
    ENUM_WALK_END end = WALK_STOPPED;
    MeasureFrame(r, run, (int)PERIOD_M1, MINUTE_SECONDS);
    const Floor m1 = g_floors[FloorIndex((int)PERIOD_M1)];
    if (run.Going() && m1.empty)
    {
        end = WALK_EXHAUSTED;
    }
    else if (run.Going())
    {
        walk.anchor = m1.value;
        end = WalkForward(r, run, (int)PERIOD_M1, MINUTE_SECONDS, WALK_SPREAD, walk);
    }
    return end;
}

// Measures the quote floor: the earlier of the first real tick, read once the whole tick history is
// synced, and the spread floor; both absent, the ticks are empty history.
void MeasureTicks(const HistoryRequest *r, HistoryRun &run)
{
    MqlTick first[];
    long firstTick = 0;
    long spreadFloor = 0;
    bool ticked = false;
    bool spread = false;
    if (SettleTicks(r, run, READ_FIRST_TICK, 1, 1, first))
    {
        ticked = ArraySize(first) > 0;
        if (ticked)
            firstTick = first[0].time_msc / 1000;
        spread = SpreadFloor(r, run, spreadFloor);
    }
    if (run.Going())
    {
        long quote = 0;
        if (ticked && spread)
            quote = Earliest(firstTick, spreadFloor);
        else if (ticked)
            quote = firstTick;
        else if (spread)
            quote = spreadFloor;
        KeepFloor(HUB_TICK_TIMEFRAME, quote, !ticked && !spread, false);
        g_spreadHeld = spread;
        g_spreadFloor = spreadFloor;
        g_measured = true;
        PrintFormat("ticks: %s ticks: first tick broker %I64d, spread floor broker %I64d, quote "
                    "floor broker %I64d", r.symbol, firstTick, spreadFloor, quote);
    }
}

// Brings the terminal to the symbol's full picture before its first answer, each part once: the
// store settled, every frame's floor, the whole tick history and the quote floor; measured, it
// measures again only the capped frames, whose oldest bars are trimmed as new ones form.
void MeasureSymbol(const HistoryRequest *r, HistoryRun &run)
{
    if (!g_storeSettled)
        AwaitStore(r, run);
    for (int s = 0; s < ArraySize(HUB_SERIES) && run.Going(); s++)
    {
        string name = "";
        int period = 0;
        HubSeries(HUB_SERIES[s], name, period);
        if (!FloorHeld(HUB_SERIES[s]) || FloorCapped(HUB_SERIES[s]))
            MeasureFrame(r, run, HUB_SERIES[s], period);
    }
    if (run.Going() && !g_measured)
        MeasureTicks(r, run);
}

// ── Bars ─────────────────────────────────────────────────────────────────────────────────────────

// The bars opened in [lo, the request's end], read chunk by chunk, on the series' grid and from its
// floor on, into the request's rates; a chunk answering no rows is a hole.
void ReadBars(HistoryRequest *r, HistoryRun &run, const long lo, const long span, const long floor)
{
    long los[];
    long his[];
    Chunks(lo, r.to, span, los, his);
    MqlRates rows[];
    for (int i = 0; i < ArraySize(los) && run.Going(); i++)
    {
        MqlRates part[];
        if (SettleBars(r, run, READ_BARS_RANGE, r.timeframe, los[i], his[i], part))
        {
            KeepRatesWithin(part, los[i], his[i]);
            const int at = ArraySize(rows);
            ArrayResize(rows, at + ArraySize(part));
            ArrayCopy(rows, part, at, 0, ArraySize(part));
        }
    }
    int offGrid = -1;
    if (run.Going())
        offGrid = FirstOffGrid(rows, r.period);
    if (offGrid >= 0)
    {
        run.Stop(RUN_FAILED, StringFormat("%s %s: row broker %I64d is off the grid", r.symbol,
                                          SeriesName(r), (long)rows[offGrid].time),
                 0);
    }
    else if (run.Going())
    {
        KeepRatesWithin(rows, floor, r.to);
        ArraySwap(r.rates, rows);
    }
}

void RunBars(HistoryRequest *r, HistoryRun &run)
{
    if (SelectSymbol(r, run))
    {
        const Floor floor = g_floors[FloorIndex(r.timeframe)];
        long span = 0;
        if (floor.empty || r.to < floor.value)
            ArrayFree(r.rates);
        else if (SpanSeconds(r, run, r.period, span))
            ReadBars(r, run, Latest(r.from, floor.value), span, floor.value);
    }
}

// ── Ticks ────────────────────────────────────────────────────────────────────────────────────────

// The M1 bars opened in [lo, hi], read in spans within the cap; false, the run stopped, for a read
// that fails.
bool ReadM1(const HistoryRequest *r, HistoryRun &run, const long lo, const long hi,
            MqlRates &bars[])
{
    long span = 0;
    bool read = SpanSeconds(r, run, MINUTE_SECONDS, span);
    if (read)
    {
        long los[];
        long his[];
        Chunks(lo, hi, span, los, his);
        for (int i = 0; i < ArraySize(los) && read; i++)
        {
            MqlRates part[];
            read = SettleBars(r, run, READ_BARS_RANGE, (int)PERIOD_M1, los[i], his[i], part);
            if (read)
            {
                KeepRatesWithin(part, los[i], his[i]);
                const int at = ArraySize(bars);
                ArrayResize(bars, at + ArraySize(part));
                ArrayCopy(bars, part, at, 0, ArraySize(part));
            }
        }
    }
    return read;
}

// Appends the bar's four ticks, each ask `spread` above its bid at the symbol's digits.
void AppendBarTicks(const MqlRates &bar, const double spread, const int digits, MqlTick &rows[])
{
    double bids[4];
    bids[0] = bar.open;
    if (bar.close < bar.open)
    {
        bids[1] = bar.high;
        bids[2] = bar.low;
    }
    else
    {
        bids[1] = bar.low;
        bids[2] = bar.high;
    }
    bids[3] = bar.close;
    const long open = (long)bar.time * 1000;
    long stamps[4];
    stamps[0] = open;
    stamps[1] = open + MINUTE_MS / 3;
    stamps[2] = open + 2 * MINUTE_MS / 3;
    stamps[3] = open + MINUTE_MS - 1;
    const int n = ArraySize(rows);
    ArrayResize(rows, n + 4, 4096);
    for (int k = 0; k < 4; k++)
    {
        ZeroMemory(rows[n + k]);
        rows[n + k].time = (datetime)(stamps[k] / 1000);
        rows[n + k].time_msc = stamps[k];
        rows[n + k].bid = bids[k];
        rows[n + k].ask = NormalizeDouble(bids[k] + spread, digits);
        rows[n + k].flags = (uint)(TICK_FLAG_BID | TICK_FLAG_ASK);
    }
}

// Clears the mark of each bar whose minute holds one of the ticks, which lie in time order.
void UnmarkTicked(const MqlRates &bars[], const MqlTick &ticks[], bool &tickless[])
{
    int next = 0;
    for (int i = 0; i < ArraySize(bars); i++)
    {
        const long open = (long)bars[i].time * 1000;
        while (next < ArraySize(ticks) && ticks[next].time_msc < open)
            next++;
        if (next < ArraySize(ticks) && ticks[next].time_msc < open + MINUTE_MS)
            tickless[i] = false;
    }
}

// Marks each bar whose minute holds no real tick of any kind: the request's selection of ticks
// first, then, where it selects fewer than every kind and a bar is left marked, every tick of the
// marked bars' minutes, read once.
void MarkTickless(const HistoryRequest *r, HistoryRun &run, const MqlRates &bars[],
                  const MqlTick &real[], bool &tickless[])
{
    ArrayResize(tickless, ArraySize(bars));
    for (int i = 0; i < ArraySize(bars); i++)
        tickless[i] = true;
    UnmarkTicked(bars, real, tickless);
    int first = -1;
    int last = -1;
    for (int i = 0; i < ArraySize(bars); i++)
    {
        if (tickless[i] && first < 0)
            first = i;
        if (tickless[i])
            last = i;
    }
    MqlTick every[];
    if (first >= 0 && r.flags != (uint)COPY_TICKS_ALL
        && SettleTicks(r, run, READ_EVERY_TICK_RANGE, (long)bars[first].time * 1000,
                       (long)bars[last].time * 1000 + MINUTE_MS - 1, every))
    {
        const int decrease = FirstDecrease(every);
        if (decrease >= 0)
            run.Stop(RUN_FAILED, StringFormat("%s ticks: row broker msc %I64d is out of order",
                                              r.symbol, every[decrease].time_msc),
                     0);
        else
            UnmarkTicked(bars, every, tickless);
    }
}

// Four ticks synthesised by the kline-quote rule from each M1 bar opened in [from, hi] whose minute
// holds no real tick, in time order, each with its bar's own spread. A bar opened after the time no
// bar opens after may be forming, its ticks unread, and yields none.
void SynthesiseBars(const HistoryRequest *r, HistoryRun &run, const long from, const long hi,
                    const MqlTick &real[], MqlTick &rows[])
{
    MqlRates bars[];
    if (ReadM1(r, run, from, hi, bars))
    {
        KeepRatesWithin(bars, from, Earliest(hi, Present(r)));
        const int offGrid = FirstOffGrid(bars, MINUTE_SECONDS);
        if (offGrid >= 0)
            run.Stop(RUN_FAILED, StringFormat("%s %s: row broker %I64d is off the grid", r.symbol,
                                              EnumToString(PERIOD_M1), (long)bars[offGrid].time),
                     0);
    }
    bool tickless[];
    if (run.Going())
        MarkTickless(r, run, bars, real, tickless);
    long digits = 0;
    if (run.Going() && ArraySize(bars) > 0)
    {
        ResetLastError();
        if (!SymbolInfoInteger(r.symbol, SYMBOL_DIGITS, digits))
            run.Stop(RUN_FAILED, StringFormat("SYMBOL_DIGITS of '%s' could not be read", r.symbol),
                     GetLastError());
    }
    // One point is 10^-digits, the unit MqlRates.spread counts in.
    const double point = MathPow(10.0, -(double)digits);
    for (int i = 0; i < ArraySize(bars) && run.Going(); i++)
        if (tickless[i])
            AppendBarTicks(bars[i], bars[i].spread * point, (int)digits, rows);
}

// The ticks synthesised for the window's tickless minutes from the spread floor on: none before it,
// where no spread was recorded, and none for a minute whose bar the terminal no longer holds.
void Synthesise(const HistoryRequest *r, HistoryRun &run, const long lo, const long hi,
                const MqlTick &real[], MqlTick &rows[])
{
    if (g_spreadHeld && hi >= g_spreadFloor)
    {
        const long m1lo = Latest(lo, Latest(g_floors[FloorIndex((int)PERIOD_M1)].value,
                                            g_spreadFloor));
        if (m1lo <= hi)
            SynthesiseBars(r, run, m1lo, hi, real, rows);
    }
}

// The two runs of ticks, each in time order, merged in time order.
void Merge(const MqlTick &a[], const MqlTick &b[], MqlTick &rows[])
{
    const int na = ArraySize(a);
    const int nb = ArraySize(b);
    ArrayResize(rows, na + nb);
    int i = 0;
    int j = 0;
    for (int k = 0; k < na + nb; k++)
    {
        if (j >= nb || (i < na && a[i].time_msc <= b[j].time_msc))
        {
            rows[k] = a[i];
            i++;
        }
        else
        {
            rows[k] = b[j];
            j++;
        }
    }
}

// The request's ticks of [lo, hi] in time order, cut to its window: the real ticks of the minutes,
// read first, merged with those synthesised from the M1 bar of each minute holding none; a minute
// is real ticks or synthesised ones whole, so a minute the window cuts is read whole.
void ReadTickWindow(HistoryRequest *r, HistoryRun &run, const long lo, const long hi)
{
    MqlTick real[];
    if (SettleTicks(r, run, READ_TICKS_RANGE, lo * 1000, hi * 1000 + 999, real))
    {
        KeepTicksWithin(real, lo * 1000, hi * 1000 + 999);
        const int decrease = FirstDecrease(real);
        if (decrease >= 0)
            run.Stop(RUN_FAILED, StringFormat("%s ticks: row broker msc %I64d is out of order",
                                              r.symbol, real[decrease].time_msc),
                     0);
        MqlTick synthesised[];
        // Synthesised ticks are quote ticks, which a selection of trade ticks holds none of.
        if (run.Going() && (r.flags & (uint)COPY_TICKS_INFO) == (uint)COPY_TICKS_INFO)
            Synthesise(r, run, lo, hi, real, synthesised);
        if (run.Going())
        {
            MqlTick rows[];
            Merge(real, synthesised, rows);
            KeepTicksWithin(rows, r.from * 1000, r.to * 1000 + 999);
            PrintFormat("ticks: request %I64u: %d real ticks in its minutes, %d synthesised", r.id,
                        ArraySize(real), ArraySize(synthesised));
            ArraySwap(r.ticks, rows);
        }
    }
}

void RunTicks(HistoryRequest *r, HistoryRun &run)
{
    if (SelectSymbol(r, run))
    {
        long lo = 0;
        long hi = 0;
        Containing(r.from, r.to, MINUTE_SECONDS, 0, lo, hi);
        const Floor floor = g_floors[FloorIndex(HUB_TICK_TIMEFRAME)];
        if (floor.empty || hi < floor.value)
        {
            ArrayFree(r.ticks);
        }
        else
        {
            Snapshot(r);
            ReadTickWindow(r, run, Latest(lo, floor.value), hi);
        }
    }
}

// ── Ranges ───────────────────────────────────────────────────────────────────────────────────────

// The terminal's MaxBars and every held floor of a series with rows, in broker seconds.
void RunRanges(HistoryRequest *r, HistoryRun &run)
{
    long maxbars = 0;
    if (ReadMaxbars(r, run, maxbars))
    {
        r.maxbars = maxbars;
        ArrayFree(r.rangeTimeframes);
        ArrayFree(r.rangeFloors);
        for (int i = 0; i < ArraySize(g_floors); i++)
        {
            if (g_floors[i].held && !g_floors[i].empty)
            {
                const int n = ArraySize(r.rangeFloors);
                ArrayResize(r.rangeTimeframes, n + 1);
                ArrayResize(r.rangeFloors, n + 1);
                r.rangeTimeframes[n] = g_floorSeries[i];
                r.rangeFloors[n] = g_floors[i].value;
            }
        }
    }
}

// ── A request's run and answer ───────────────────────────────────────────────────────────────────

int HistoryIndex(const HistoryRequest *r)
{
    for (int i = 0; i < ArraySize(g_history); i++)
        if (g_history[i] == r)
            return i;
    return -1;
}

// Takes the request off the ones in flight.
void DropHistory(HistoryRequest *r)
{
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

// A ranges answer's records: the terminal's MaxBars, then each floor's timeframe and value.
void WriteRanges(BinaryWriter &body, const HistoryRequest *r)
{
    body.I64(r.maxbars);
    for (int i = 0; i < ArraySize(r.rangeFloors); i++)
    {
        body.I32(r.rangeTimeframes[i]);
        body.I64(r.rangeFloors[i]);
    }
}

// Sends the request's next part; true once the last is sent. A ranges answer is one part.
bool SendRowsPart(HistoryRequest *r)
{
    const int rows = RowCount(r);
    int count = 0;
    BinaryWriter body;
    switch (r.call)
    {
    case HUB_CALL_COPY_RATES_RANGE:
        count = (int)Earliest(HUB_PART_BYTES / HUB_RATES_SIZE, rows - r.sent);
        body.Rows(r.rates, r.sent, count);
        break;
    case HUB_CALL_COPY_TICKS_RANGE:
        count = (int)Earliest(HUB_PART_BYTES / HUB_TICK_SIZE, rows - r.sent);
        body.Rows(r.ticks, r.sent, count);
        break;
    case HUB_CALL_RANGES:
        count = rows;
        WriteRanges(body, r);
        break;
    }
    const bool last = r.sent + count == rows;
    ushort flags = HUB_FLAG_OK;
    if (last)
        flags = HUB_FLAG_OK | HUB_FLAG_LAST;
    uchar bytes[];
    body.Take(bytes);
    BinaryWriter frame;
    frame.Begin(HUB_FRAME_ANSWER, flags, r.id);
    frame.U32(r.part);
    frame.I32(r.lastError);
    frame.Bytes(bytes, 0, ArraySize(bytes));
    frame.Seal((uint)count);
    Send(frame);
    r.sent += count;
    r.part++;
    return last;
}

// Starts the request's answer: its first part now, and any after it one per timer pass, so no pass
// waits on them.
void StartAnswer(HistoryRequest *r, const HistoryRun &run)
{
    r.answering = true;
    r.lastError = run.lastError;
    PrintFormat("ticks: request %I64u, %s: %d rows", r.id, Described(r), RowCount(r));
    if (SendRowsPart(r))
        DropHistory(r);
}

// Runs the request — the symbol's measurement first, then its reads — and acts on where the run
// ends: answers syncing or the failure, or starts the answer.
void Evaluate(HistoryRequest *r)
{
    HistoryRun run;
    MeasureSymbol(r, run);
    if (run.Going())
    {
        switch (r.call)
        {
        case HUB_CALL_RANGES:
            RunRanges(r, run);
            break;
        case HUB_CALL_COPY_RATES_RANGE:
            RunBars(r, run);
            break;
        case HUB_CALL_COPY_TICKS_RANGE:
            RunTicks(r, run);
            break;
        }
    }
    switch (run.state)
    {
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
// after it ends or after the terminal's time; otherwise queued for the timer, busy from arrival.
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
    case HUB_CALL_RANGES:
        r.symbol = arguments.String(HUB_ARG_RANGES_SYMBOL);
        break;
    }
    const long now = (long)TimeTradeServer();
    string series = "";
    // A ranges request has no window: its zero bounds pass the window's checks.
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
        const int n = ArraySize(g_history);
        ArrayResize(g_history, n + 1);
        g_history[n] = r;
        r.busyAt = GetTickCount64();
        SendBusy(r.id, HUB_HISTORY_BUSY_S);
        PrintFormat("ticks: request %I64u, %s: busy for %d s", r.id, Described(r),
                    HUB_HISTORY_BUSY_S);
    }
}

// Sends each answering request's next part; once none is answering, runs the oldest request not yet
// run, which may hold this pass for minutes; then renews every busy that falls due.
void ServeHistory()
{
    for (int i = ArraySize(g_history) - 1; i >= 0; i--)
    {
        HistoryRequest *r = g_history[i];
        if (r.answering && SendRowsPart(r))
            DropHistory(r);
    }
    bool answering = false;
    HistoryRequest *next = NULL;
    for (int i = 0; i < ArraySize(g_history); i++)
    {
        if (g_history[i].answering)
            answering = true;
        else if (next == NULL)
            next = g_history[i];
    }
    if (!answering && next != NULL)
    {
        next.startedAt = GetTickCount64();
        Evaluate(next);
    }
    RenewEveryBusy(GetTickCount64());
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

// The chart of the symbol whose comment marks the role, its EA running or gone, other than one the
// spawner closed; -1 when none.
long MarkedChart(const ENUM_CHART_ROLE role, const string symbol)
{
    long chart = ChartFirst();
    while (chart != -1)
    {
        if (!IsClosing(chart) && ChartSymbol(chart) == symbol
            && ChartGetString(chart, CHART_COMMENT) == Marker(role))
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

void RememberClosing(const long chart, const string symbol)
{
    const int n = ArraySize(g_closingCharts);
    ArrayResize(g_closingCharts, n + 1);
    ArrayResize(g_closingSymbols, n + 1);
    ArrayResize(g_closingSince, n + 1);
    g_closingCharts[n] = chart;
    g_closingSymbols[n] = symbol;
    g_closingSince[n] = GetTickCount64();
}

bool IsClosing(const long chart)
{
    for (int i = 0; i < ArraySize(g_closingCharts); i++)
        if (g_closingCharts[i] == chart)
            return true;
    return false;
}

// Whether a chart of the symbol the spawner closed may still be listed.
bool HasClosingChart(const string symbol)
{
    for (int i = 0; i < ArraySize(g_closingSymbols); i++)
        if (g_closingSymbols[i] == symbol)
            return true;
    return false;
}

bool Listed(const long chart)
{
    long listed = ChartFirst();
    while (listed != -1)
    {
        if (listed == chart)
            return true;
        listed = ChartNext(listed);
    }
    return false;
}

void ForgetClosing(const int index)
{
    ArrayRemove(g_closingCharts, index, 1);
    ArrayRemove(g_closingSymbols, index, 1);
    ArrayRemove(g_closingSince, index, 1);
}

// Forget each chart the spawner closed once the terminal no longer lists it, or, still listed,
// CHART_CLOSE_GONE_SECONDS after its close
void ForgetGoneClosing()
{
    const ulong now = GetTickCount64();
    for (int i = ArraySize(g_closingCharts) - 1; i >= 0; i--)
    {
        if (!Listed(g_closingCharts[i]))
        {
            ForgetClosing(i);
        }
        else if (now - g_closingSince[i] >= (ulong)CHART_CLOSE_GONE_SECONDS * 1000)
        {
            PrintFormat("ticks: chart %I64d of '%s' is still listed %d s after its close",
                        g_closingCharts[i], g_closingSymbols[i], CHART_CLOSE_GONE_SECONDS);
            ForgetClosing(i);
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
                if (ChartClose(chart))
                    RememberClosing(chart, symbol);
                else
                    PrintFormat("ticks: ChartClose of the %s chart of '%s' failed: %d",
                                ChartRoleName(role), symbol, GetLastError());
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
// it is open; while the account holds positions or pending orders on the symbol its publisher's
// chart stays, and the hub is told
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
    if (role == ROLE_PUBLISHER && (positions > 0 || orders > 0))
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
        RememberClosing(chart, symbol);
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

// An open_chart or close_chart frame, which the hub sends the spawner, queued to be served.
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
    else
        QueueChartFrame(kind, role, symbol);
}

void QueueChartFrame(const ENUM_HUB_FRAME kind, const ENUM_CHART_ROLE role, const string symbol)
{
    const int n = ArraySize(g_chartFrameKinds);
    ArrayResize(g_chartFrameKinds, n + 1);
    ArrayResize(g_chartFrameRoles, n + 1);
    ArrayResize(g_chartFrameSymbols, n + 1);
    g_chartFrameKinds[n] = kind;
    g_chartFrameRoles[n] = role;
    g_chartFrameSymbols[n] = symbol;
}

// Serve the oldest chart frame while the hub takes this EA's frames, so its answer reaches the
// hub, unless a chart of its symbol the spawner closed is still listed; then it waits, and every
// frame behind it
void ServeChartFrame()
{
    ForgetGoneClosing();
    if (wss.isConnected() && !g_refused && ArraySize(g_chartFrameKinds) > 0
        && !HasClosingChart(g_chartFrameSymbols[0]))
    {
        const ENUM_HUB_FRAME kind = g_chartFrameKinds[0];
        const ENUM_CHART_ROLE role = g_chartFrameRoles[0];
        const string symbol = g_chartFrameSymbols[0];
        ArrayRemove(g_chartFrameKinds, 0, 1);
        ArrayRemove(g_chartFrameRoles, 0, 1);
        ArrayRemove(g_chartFrameSymbols, 0, 1);
        if (kind == HUB_FRAME_OPEN_CHART)
            OpenChart(role, symbol);
        else
            CloseChart(role, symbol);
    }
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
    DropAllHistory();
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
    if (!g_closing)
        ServeChartFrame();
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
        if (Role == ROLE_HISTORY)
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
