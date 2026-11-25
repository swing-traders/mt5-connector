#property strict
#property version "1.00"

const datetime THROUGH=D'2023.01.15 00:00';
const datetime JAN_FROM=D'2023.01.02 02:00';
const datetime JAN_TO=D'2023.01.14 02:00';
const int BOUND_S=14400;

void Failure(string message)
{
   int f=FileOpen("reference-floor/outcome.txt",FILE_WRITE|FILE_TXT|FILE_ANSI);
   if(f!=INVALID_HANDLE) { FileWriteString(f,message); FileClose(f); }
}

void WriteBar(int f,MqlRates &bar,int digits)
{
   FileWrite(f,(long)bar.time,DoubleToString(bar.open,digits),
             DoubleToString(bar.high,digits),DoubleToString(bar.low,digits),
             DoubleToString(bar.close,digits),bar.tick_volume,bar.spread);
}

int OpenBars(string name)
{
   int f=FileOpen("reference-floor/"+name,FILE_WRITE|FILE_CSV|FILE_ANSI,',');
   if(f!=INVALID_HANDLE)
      FileWrite(f,"time","open","high","low","close","tick_volume","spread");
   return f;
}

void OnStart()
{
   string symbol="BTCUSD.a";
   ulong deadline=GetTickCount64()+(ulong)BOUND_S*1000;
   while(!TerminalInfoInteger(TERMINAL_CONNECTED) && !IsStopped())
   {
      if(GetTickCount64()>deadline) { Failure("not connected"); return; }
      Sleep(1000);
   }
   if(!SymbolSelect(symbol,true)) { Failure("symbol not selected"); return; }
   MqlRates bars[];
   int n=-1,previous=-1;
   while(!IsStopped())
   {
      ResetLastError();
      n=CopyRates(symbol,PERIOD_M1,(datetime)1,THROUGH,bars);
      int error=GetLastError();
      long server_first=SeriesInfoInteger(symbol,PERIOD_M1,SERIES_SERVER_FIRSTDATE);
      long terminal_first=SeriesInfoInteger(symbol,PERIOD_M1,SERIES_TERMINAL_FIRSTDATE);
      if(n>1 && n==previous && error==0 && server_first>0 &&
         terminal_first>0 && terminal_first<=server_first)
         break;
      previous=n;
      if(GetTickCount64()>deadline) { Failure("history not settled"); return; }
      Sleep(1000);
   }
   if(IsStopped()) return;
   int pair=-1,spread=-1;
   for(int i=0;i<n-1;i++)
      if(bars[i+1].time-bars[i].time==60) { pair=i; break; }
   if(pair<0) { Failure("no adjacent minutes"); return; }
   for(int i=pair;i<n;i++)
      if(bars[i].spread>0) { spread=i; break; }
   if(spread<0) { Failure("no recorded spread"); return; }

   MqlTick first[];
   int ticks=-1;
   long previous_msc=-1;
   while(!IsStopped())
   {
      ResetLastError();
      ticks=CopyTicks(symbol,first,COPY_TICKS_ALL,1,1);
      int error=GetLastError();
      if(ticks==1 && error==0 && first[0].time_msc==previous_msc) break;
      if(ticks==1) previous_msc=first[0].time_msc;
      if(GetTickCount64()>deadline) { Failure("tick history not settled"); return; }
      Sleep(1000);
   }
   if(IsStopped()) return;
   int digits=(int)SymbolInfoInteger(symbol,SYMBOL_DIGITS);
   int f=OpenBars("history-first.csv");
   if(f==INVALID_HANDLE) { Failure("history-first file"); return; }
   for(int i=0;i<=pair+1;i++) WriteBar(f,bars[i],digits);
   FileClose(f);
   f=OpenBars("spread-first.csv");
   if(f==INVALID_HANDLE) { Failure("spread-first file"); return; }
   WriteBar(f,bars[spread],digits);
   FileClose(f);
   f=OpenBars("january.csv");
   if(f==INVALID_HANDLE) { Failure("january file"); return; }
   int january_rows=0;
   for(int i=0;i<n;i++)
      if(bars[i].time>=JAN_FROM && bars[i].time<=JAN_TO)
      {
         WriteBar(f,bars[i],digits);
         january_rows++;
      }
   FileClose(f);
   f=FileOpen("reference-floor/first-real-tick.csv",FILE_WRITE|FILE_CSV|FILE_ANSI,',');
   if(f==INVALID_HANDLE) { Failure("first-tick file"); return; }
   FileWrite(f,"time","time_msc","bid","ask","last","volume","flags","volume_real");
   FileWrite(f,(long)first[0].time,first[0].time_msc,
             DoubleToString(first[0].bid,digits),DoubleToString(first[0].ask,digits),
             DoubleToString(first[0].last,digits),first[0].volume,
             first[0].flags,first[0].volume_real);
   FileClose(f);
   f=FileOpen("reference-floor/metadata.csv",FILE_WRITE|FILE_CSV|FILE_ANSI,',');
   if(f==INVALID_HANDLE) { Failure("metadata file"); return; }
   FileWrite(f,"key","value");
   FileWrite(f,"terminal_build",TerminalInfoInteger(TERMINAL_BUILD));
   FileWrite(f,"symbol",symbol);
   FileWrite(f,"digits",digits);
   FileWrite(f,"tester",MQLInfoInteger(MQL_TESTER));
   FileWrite(f,"copied_history_rows",n);
   FileWrite(f,"m1_floor_broker",(long)bars[pair].time);
   FileWrite(f,"spread_floor_broker",(long)bars[spread].time);
   FileWrite(f,"first_real_tick_broker",(long)first[0].time);
   FileWrite(f,"quote_floor_broker",(long)MathMin(bars[spread].time,first[0].time));
   FileWrite(f,"january_from_broker",(long)JAN_FROM);
   FileWrite(f,"january_to_broker",(long)JAN_TO);
   FileWrite(f,"january_rows",january_rows);
   FileWrite(f,"through_broker",(long)THROUGH);
   FileClose(f);
   Failure("done");
}
