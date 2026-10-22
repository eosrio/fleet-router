`wire_ship_abi.json` is the unmodified JSON raw string from
[Wire Network's SHiP ABI](https://github.com/Wire-Network/wire-sysio/blob/a13257a41c6375a750325279cc4e5b9848bb7a00/libraries/state_history/abi.cpp),
at commit `a13257a41c6375a750325279cc4e5b9848bb7a00` (retrieved 2026-10-22).
Its SHA-256 is `94c5661306467f8c49f22180213e390db10d290be9ebf512fb63314f440ef6f9`.

The fixture contains `sysio::abi/1.1`, 78 structs and 30 variants. Tests use it
to validate the production ABI acceptance path and the SHiP v0/v1 envelope
through a running router. Block, trace, delta and finality payloads remain
opaque test bytes; these tests do not qualify Wire payload deserialization
or a live node deployment.
