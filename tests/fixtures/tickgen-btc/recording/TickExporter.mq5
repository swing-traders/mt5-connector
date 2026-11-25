#property strict
#property version "1.00"

MqlTick ticks[];
int count=0;

int OnInit()
{
   return INIT_SUCCEEDED;
}

void OnTick()
{
   MqlTick tick;
   if(!SymbolInfoTick(_Symbol,tick))
   {
      Print("EXPORT_ERROR SymbolInfoTick ",GetLastError());
      ExpertRemove();
      return;
   }
   if(ArrayResize(ticks,count+1,100000)<0)
   {
      Print("EXPORT_ERROR ArrayResize ",GetLastError());
      ExpertRemove();
      return;
   }
   ticks[count++]=tick;
}

void OnDeinit(const int reason)
{
   int f=FileOpen("ticks.csv",FILE_WRITE|FILE_CSV|FILE_ANSI,',');
   if(f==INVALID_HANDLE) { Print("EXPORT_ERROR ticks file ",GetLastError()); return; }
   FileWrite(f,"seq","time","time_msc","bid","ask","last","flags");
   for(int i=0;i<count;i++)
      FileWrite(f,i,(long)ticks[i].time,ticks[i].time_msc,
                DoubleToString(ticks[i].bid,_Digits),DoubleToString(ticks[i].ask,_Digits),
                DoubleToString(ticks[i].last,_Digits),ticks[i].flags);
   FileClose(f);
   if(count==0) { Print("EXPORT_ERROR no ticks"); return; }
   MqlRates bars[];
   int n=CopyRates(_Symbol,PERIOD_M1,ticks[0].time-ticks[0].time%60,ticks[count-1].time,bars);
   if(n<=0) { Print("EXPORT_ERROR CopyRates ",GetLastError()); return; }
   f=FileOpen("bars.csv",FILE_WRITE|FILE_CSV|FILE_ANSI,',');
   if(f==INVALID_HANDLE) { Print("EXPORT_ERROR bars file ",GetLastError()); return; }
   FileWrite(f,"time","open","high","low","close","tick_volume","spread");
   for(int i=0;i<n;i++)
      FileWrite(f,(long)bars[i].time,DoubleToString(bars[i].open,_Digits),
                DoubleToString(bars[i].high,_Digits),DoubleToString(bars[i].low,_Digits),
                DoubleToString(bars[i].close,_Digits),bars[i].tick_volume,bars[i].spread);
   FileClose(f);
   f=FileOpen("metadata.csv",FILE_WRITE|FILE_CSV|FILE_ANSI,',');
   if(f==INVALID_HANDLE) { Print("EXPORT_ERROR metadata file ",GetLastError()); return; }
   FileWrite(f,"key","value");
   FileWrite(f,"terminal_build",TerminalInfoInteger(TERMINAL_BUILD));
   FileWrite(f,"symbol",_Symbol);
   FileWrite(f,"digits",_Digits);
   FileWrite(f,"point",DoubleToString(_Point,10));
   FileWrite(f,"tick_size",DoubleToString(SymbolInfoDouble(_Symbol,SYMBOL_TRADE_TICK_SIZE),10));
   FileWrite(f,"tester",MQLInfoInteger(MQL_TESTER));
   FileWrite(f,"deinit_reason",reason);
   FileWrite(f,"tick_count",count);
   FileWrite(f,"bar_count",n);
   FileWrite(f,"first_time_msc",ticks[0].time_msc);
   FileWrite(f,"last_time_msc",ticks[count-1].time_msc);
   FileWrite(f,"series_server_first_date_m1",SeriesInfoInteger(_Symbol,PERIOD_M1,SERIES_SERVER_FIRSTDATE));
   FileWrite(f,"series_terminal_first_date_m1",SeriesInfoInteger(_Symbol,PERIOD_M1,SERIES_TERMINAL_FIRSTDATE));
   FileWrite(f,"series_first_date_m1",SeriesInfoInteger(_Symbol,PERIOD_M1,SERIES_FIRSTDATE));
   FileClose(f);
   Print("EXPORT_OK ticks=",count," bars=",n);
}
