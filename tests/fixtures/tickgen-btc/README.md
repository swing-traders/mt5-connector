# Native BTC reference exports

Each directory contains compressed, unmodified CSV exports and their recording provenance. `manifest.json` registers the uncompressed CSV hashes, the native worked-doji path, and the canonical history floors and January row count. The per-capture `provenance.json` gives the terminal build, symbol, range, tester settings, recording time in UTC, reference-program hash, and CSV hashes.

`recording/TickExporter.mq5` runs as an expert in MT5's own Strategy Tester with the supplied `tester.ini`: M1, Every Tick (`Model=0`), local agents, no optimization or visual mode. It buffers native `SymbolInfoTick` results from `OnTick` and exports the corresponding native `CopyRates` bars. No connector generator is involved.

`recording/NativeHistoryExport.mq5` runs as a native script outside the tester. It exports the M1 prefix through the first adjacent minute pair, the first positive-spread bar, the first real tick obtained by native `CopyTicks` from the first millisecond, and the January native bars. These are the witnesses for the floors and row count. Broker wall epochs map to UTC using New York wall time plus seven hours; the registered UTC values were calculated independently of the connector's clock implementation.

The fitted-rule calibration uses the independent test-side oracle, whose source hash is recorded in the manifest. Both weeks require every full-volume minute to meet the native path within one point and every eligible scaffold to embed exactly. The worked path is taken from the native CSV.

To reproduce a recording, compile the supplied programs with the recorded terminal build, run the expert with the supplied tester settings, and run the history script after its native history reads settle. Keep connection settings outside the exports. Registration uses the resulting native CSV bytes; a functional run only verifies and decompresses the frozen references.
