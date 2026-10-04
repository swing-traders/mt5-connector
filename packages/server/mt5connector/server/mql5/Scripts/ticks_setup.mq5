//+------------------------------------------------------------------+
//|                                        ticks_setup.mq5           |
//| Startup script: close every chart the terminal restored but the  |
//| spawner's - the chart of the symbol the script runs on that runs |
//| the ticks EA - and open that chart with the spawner template     |
//| when none is kept. Every other chart opens on demand. No         |
//| includes - compiles with a bare MetaEditor.                      |
//+------------------------------------------------------------------+
#property script_show_inputs

input string TemplateFile = "ticks_spawner.tpl";
input string ExpertName   = "ticks";

//+------------------------------------------------------------------+
//| Script program start function                                    |
//+------------------------------------------------------------------+
void OnStart()
{
    const long spawner = CloseAllButSpawner();
    if (spawner != 0)
    {
        PrintFormat("ticks_setup: '%s' already runs on a '%s' chart - keeping it", ExpertName,
                    Symbol());
    }
    else
    {
        long chart = ChartOpen(Symbol(), PERIOD_M1);
        if (chart == 0)
        {
            PrintFormat("ticks_setup: ChartOpen failed for '%s': %d", Symbol(), GetLastError());
        }
        else if (!ChartApplyTemplate(chart, TemplateFile))
        {
            PrintFormat("ticks_setup: ChartApplyTemplate failed for '%s': %d", Symbol(),
                        GetLastError());
        }
        else
        {
            PrintFormat("ticks_setup: '%s' attached to a '%s' chart as the spawner", ExpertName,
                        Symbol());
        }
    }

    // close the setup chart
    ChartClose(ChartID());
}

//+------------------------------------------------------------------+
//| Close every chart but this script's and the first chart of its   |
//| symbol that runs the EA 'ExpertName', which it returns; 0 when   |
//| there is none                                                    |
//+------------------------------------------------------------------+
long CloseAllButSpawner()
{
    // Collected before any closes, so the walk never steps from a closed chart.
    long charts[];
    long chart = ChartFirst();
    while (chart != -1)
    {
        if (chart != ChartID())
        {
            const int n = ArraySize(charts);
            ArrayResize(charts, n + 1);
            charts[n] = chart;
        }
        chart = ChartNext(chart);
    }

    long spawner = 0;
    for (int i = 0; i < ArraySize(charts); i++)
    {
        string expert;
        if (spawner == 0
            && ChartSymbol(charts[i]) == Symbol()
            && ChartGetString(charts[i], CHART_EXPERT_NAME, expert)
            && expert == ExpertName)
        {
            spawner = charts[i];
        }
        else if (!ChartClose(charts[i]))
        {
            PrintFormat("ticks_setup: ChartClose of a '%s' chart failed: %d",
                        ChartSymbol(charts[i]), GetLastError());
        }
    }
    return spawner;
}
