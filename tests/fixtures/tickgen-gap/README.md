# Native custom-symbol gap input

The broker inputs are independent native `BTCUSD.a` captures made with `CopyTicksRange(COPY_TICKS_ALL)` and `CopyRates(PERIOD_M1)`. They cover 2026-11-19 22:00 UTC through 2026-11-20 00:05 UTC, with the end excluded. The 125 M1 bars start at broker midnight, matching the native store's first date. Importing them into `BTCUSD.a.GAPTEST`, removing the ticks in the minute at 2026-11-20 00:02 UTC, and retaining every M1 bar produces the gap input.

The tick reference is MT5's own post-import native read of the custom symbol, also checked after a clean terminal restart. Custom-symbol import clears bit 128 in the recorded tick flags. The hub's answer must match this native reference field for field, without masking flags in the test assertion.

Input verification additionally compares the native custom-symbol ticks to the original broker capture. It requires 52,242 retained ticks and 51,273 preceding ticks, in capture order, with every field equal except that flags may have bit 128 cleared. No other alteration or count difference is accepted. The retained active M1 bar must match the original bar and its minute must contain no tick of any kind.

The compressed files preserve the captured CSV bytes. `provenance.json` records their uncompressed SHA-256 hashes, the source capture hashes, terminal build, native reader hash, UTC recording date, range and removed minute.

Acceptance setup installs the fixture and awaits the hub's normal history and re-sync completion. It then reloads the original tick input with the same native installer, removes the named minute again, restores the original M1 bars, and verifies the installed data against these references before the gap request. The completed re-sync record is retained across the native reload. A subsequent re-sync before the gap answer fails acceptance.
