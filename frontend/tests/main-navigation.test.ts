import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function source(relativePath: string): Promise<string> {
  return readFile(new URL(relativePath, import.meta.url), "utf8");
}

test("global navigation exposes the approved product sections", async () => {
  const header = await source("../components/app-header.tsx");

  for (const [href, label] of [
    ["/matches", "Матчи"],
    ["/leagues", "Лиги"],
    ["/analytics", "Аналитика"],
    ["/predictions", "Прогнозы"],
    ["/favorites", "Избранное"],
  ]) {
    assert.match(header, new RegExp(`href: "${href}".*label: "${label}"`));
  }
  assert.match(header, /aria-label="Основная навигация"/);
  assert.match(header, /lang="ru"/);
});

test("navigation reports the current product section accessibly", async () => {
  const header = await source("../components/app-header.tsx");

  assert.match(header, /usePathname\(\)/);
  assert.match(header, /aria-current=\{active \? "page" : undefined\}/);
  assert.match(header, /pathname\.startsWith\(`\$\{prefix\}\/`\)/);
});

test("mobile navigation remains visible and horizontally scrollable", async () => {
  const styles = await source("../app/globals.css");

  assert.match(styles, /@media \(max-width: 760px\)/);
  assert.match(styles, /\.main-nav \{[^}]*overflow-x: auto;/s);
  assert.match(styles, /scroll-snap-type: x proximity/);
  assert.match(styles, /\.main-nav-link \{[^}]*min-height: 45px;/s);
  assert.doesNotMatch(styles, /\.main-nav \{ display: none; \}/);
});

test("candidate palette values are not promoted during menu preview", async () => {
  const files = await Promise.all([
    source("../components/app-header.tsx"),
    source("../app/globals.css"),
    source("../../DESIGN.md"),
  ]);
  const previewSources = files.join("\n").toLowerCase();

  for (const candidate of [
    "#050914",
    "#0b1220",
    "#0d1626",
    "#0a1322",
    "#202d42",
    "#2f6bff",
    "#ef3340",
    "#20c967",
  ]) {
    assert.doesNotMatch(previewSources, new RegExp(candidate));
  }
});
