//+------------------------------------------------------------------+
//|                                                       ticks.mq5  |
//|  Streams ticks and the trade server's time to the WS hub         |
//+------------------------------------------------------------------+
#property script_show_inputs

//+------------------------------------------------------------------+
//| I N P U T S                                                      |
//+------------------------------------------------------------------+
input string Server = "ws://127.0.0.1:9000/";
input int ReconnectIntervalSec = 0;
input int RelaySeconds = 5;

#include <MQL5Book/AutoPtr.mqh>
#include <MQL5Book/ws/wsclient.mqh>
#include <JAson.mqh>

WebSocketClient<Hybi> *wss = NULL;
string symbol;
bool g_helloSent = false;
datetime g_lastReconnect = 0;

//+------------------------------------------------------------------+
//| Announce this EA connection to the hub (once per connect)        |
//+------------------------------------------------------------------+
void SendHello()
{
    if (g_helloSent || wss == NULL || !wss.isConnected())
        return;
    CJAVal hello(jtOBJ, "");
    hello["type"] = "hello";
    hello["role"] = "ea";
    wss.send(hello.Serialize());
    g_helloSent = true;
    Print("Hello Send");
}

//+------------------------------------------------------------------+
//| Relay the trade server's time to the server through the hub      |
//+------------------------------------------------------------------+
void SendServerTime()
{
    CJAVal frame(jtOBJ, "");
    frame["v"] = 1;
    frame["type"] = "server_time";
    frame["symbol"] = symbol;
    frame["trade_server"] = (long)TimeTradeServer();
    frame["current"] = (long)TimeCurrent();
    frame["gmt"] = (long)TimeGMT();
    frame["connected"] = (int)TerminalInfoInteger(TERMINAL_CONNECTED);
    wss.send(frame.Serialize());
}

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
    Print("ticks EA initializing");
    symbol = Symbol();
    if (RelaySeconds < 1)
    {
        Print("RelaySeconds ", RelaySeconds, " is not positive");
        return INIT_PARAMETERS_INCORRECT;
    }
    // The timer fires whether or not the market ticks, so it reconnects and relays the server time
    // while the market is closed too.
    if (!EventSetTimer(RelaySeconds))
    {
        Print("EventSetTimer failed: ", GetLastError());
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
    // TimeLocal, not TimeCurrent: the time of the last quote stands still while the market is
    // closed.
    if (!wss.isConnected() && TimeLocal() - g_lastReconnect >= ReconnectIntervalSec)
    {
        g_lastReconnect = TimeLocal();
        Print("Reconnecting");
        g_helloSent = false;
        if (!wss.open())
        {
            Print("Failed to reconnect to server");
        }
        else
        {
            SendHello();
        }
    }

    if (wss.isConnected())
    {
        SendServerTime();
    }
}

//+------------------------------------------------------------------+
//| onTick method is called every tick                               |
//+------------------------------------------------------------------+
void OnTick()
{
    MqlTick tick;
    if (wss.isConnected() && SymbolInfoTick(symbol, tick))
    {
        CJAVal tickObj(jtOBJ, "");
        tickObj["symbol"] = symbol;
        tickObj["time"] = TimeToString(tick.time, TIME_DATE|TIME_MINUTES|TIME_SECONDS);
        tickObj["ask"] = DoubleToString(tick.ask);
        tickObj["bid"] = DoubleToString(tick.bid);
        tickObj["volume"] = DoubleToString(tick.volume);
        tickObj["last"] = DoubleToString(tick.last);
        tickObj["time_msec"] = IntegerToString(tick.time_msc);
        tickObj["flags"] = IntegerToString(tick.flags);

        string msg = tickObj.Serialize();
        wss.send(msg);
    }
}
