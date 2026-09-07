"""Keep the reviewed Cup queue alive across lease and retry windows."""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict

from scripts.run_cup_import import run


RETRY_POLL_SECONDS = 10


async def main() -> None:
    while True:
        report = await run()
        print(json.dumps(asdict(report), default=str, sort_keys=True), flush=True)
        if report.status == "succeeded":
            return
        await asyncio.sleep(RETRY_POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
