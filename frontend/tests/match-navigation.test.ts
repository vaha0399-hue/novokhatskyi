import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { getBrowserTimeZone } from "../lib/browser-timezone.ts";
import { matchNavigationQuery } from "../lib/match-navigation.ts";

test("match navigation sends selected date and browser IANA timezone", () => {
  assert.equal(
    matchNavigationQuery({ date: "2026-08-31", timezone: "Asia/Tokyo" }),
    "date=2026-08-31&timezone=Asia%2FTokyo",
  );
  assert.equal(
    matchNavigationQuery({
      date: "2026-08-31",
      timezone: "America/Los_Angeles",
      leagueId: 3,
    }),
    "date=2026-08-31&timezone=America%2FLos_Angeles&league_id=3",
  );
});

test("browser timezone helper returns the browser-resolved IANA zone", () => {
  const windowDescriptor = Object.getOwnPropertyDescriptor(globalThis, "window");
  const formatterDescriptor = Object.getOwnPropertyDescriptor(
    Intl,
    "DateTimeFormat",
  );

  try {
    Object.defineProperty(globalThis, "window", {
      configurable: true,
      value: {},
    });
    Object.defineProperty(Intl, "DateTimeFormat", {
      configurable: true,
      value: () => ({ resolvedOptions: () => ({ timeZone: "Asia/Tokyo" }) }),
    });

    assert.equal(getBrowserTimeZone(), "Asia/Tokyo");
  } finally {
    if (windowDescriptor) {
      Object.defineProperty(globalThis, "window", windowDescriptor);
    } else {
      Reflect.deleteProperty(globalThis, "window");
    }
    if (formatterDescriptor) {
      Object.defineProperty(Intl, "DateTimeFormat", formatterDescriptor);
    }
  }
});

test("browser timezone helper rejects server use and missing IANA zones", () => {
  const windowDescriptor = Object.getOwnPropertyDescriptor(globalThis, "window");
  const formatterDescriptor = Object.getOwnPropertyDescriptor(
    Intl,
    "DateTimeFormat",
  );

  try {
    Reflect.deleteProperty(globalThis, "window");
    assert.throws(() => getBrowserTimeZone(), /only available in the browser/);

    Object.defineProperty(globalThis, "window", {
      configurable: true,
      value: {},
    });
    Object.defineProperty(Intl, "DateTimeFormat", {
      configurable: true,
      value: () => ({ resolvedOptions: () => ({ timeZone: "" }) }),
    });
    assert.throws(() => getBrowserTimeZone(), /did not provide an IANA timezone/);
  } finally {
    if (windowDescriptor) {
      Object.defineProperty(globalThis, "window", windowDescriptor);
    } else {
      Reflect.deleteProperty(globalThis, "window");
    }
    if (formatterDescriptor) {
      Object.defineProperty(Intl, "DateTimeFormat", formatterDescriptor);
    }
  }
});

test("browser timezone helper has no product timezone fallback", async () => {
  const source = await readFile(
    new URL("../lib/browser-timezone.ts", import.meta.url),
    "utf8",
  );

  assert.doesNotMatch(source, /Europe\/London/);
});

test("server-only API functions use the shared navigation query", async () => {
  const source = await readFile(new URL("../lib/api.ts", import.meta.url), "utf8");

  assert.match(source, /import "server-only"/);
  assert.match(source, /getMatchDateLeagues/);
  assert.match(source, /getLeagueMatches/);
  assert.match(source, /matchNavigationQuery\(\{ date, timezone \}\)/);
  assert.match(source, /matchNavigationQuery\(\{ date, timezone, leagueId \}\)/);
});
