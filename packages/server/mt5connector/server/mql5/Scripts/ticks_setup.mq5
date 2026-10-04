//+------------------------------------------------------------------+
//|                                        ticks_setup.mq5           |
//| Startup script: open the spawner's chart - an M1 chart of the    |
//| symbol the script runs on - with the spawner template, then      |
//| close the chart the script runs on, which would hold a           |
//| CHARTS_MAX slot for nothing. The terminal starts from an empty   |
//| profile, so a chart already running the ticks EA is a boot       |
//| defect: the script reports it and opens and closes nothing.      |
//| Every other chart opens on demand. No includes - compiles with a |
//| bare MetaEditor.                                                 |
//+------------------------------------------------------------------+
#property script_show_inputs

input string TemplateFile = "ticks_spawner.tpl";
input string ExpertName   = "ticks";

//+------------------------------------------------------------------+
//| Script program start function                                    |
//+------------------------------------------------------------------+
void OnStart()
{
    const long running = ExpertChart();
    if (running != -1)
    {
        PrintFormat("ticks_setup: a '%s' chart runs '%s' at start - not opening the spawner",
                    ChartSymbol(running), ExpertName);
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
            ChartClose(ChartID());
        }
    }
}

//+------------------------------------------------------------------+
//| The first chart that runs the EA 'ExpertName', or -1 when none   |
//| does                                                             |
//+------------------------------------------------------------------+
long ExpertChart()
{
    long chart = ChartFirst();
    while (chart != -1)
    {
        string expert;
        if (ChartGetString(chart, CHART_EXPERT_NAME, expert) && expert == ExpertName)
            return chart;
        chart = ChartNext(chart);
    }
    return -1;
}
