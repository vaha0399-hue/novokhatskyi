import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function source(relativePath: string): Promise<string> {
  return readFile(new URL(relativePath, import.meta.url), "utf8");
}

test("global navigation exposes the approved product sections", async () => {
  const header = await source("../components/app-header.tsx");

  for (const [href, label] of [
    ["/matches", "Matches"],
    ["/leagues", "Leagues"],
    ["/analytics", "Signals"],
    ["/predictions", "Predictions"],
    ["/account", "Profile"],
  ]) {
    assert.match(header, new RegExp(`href: "${href}".*label: "${label}"`));
  }
  assert.match(header, /aria-label="Primary navigation"/);
  assert.match(header, /aria-label="Mobile navigation"/);
  assert.doesNotMatch(header, /badge:\s*12/);
});

test("navigation reports the current product section accessibly", async () => {
  const header = await source("../components/app-header.tsx");

  assert.match(header, /usePathname\(\)/);
  assert.match(header, /aria-current=\{active \? "page" : undefined\}/);
  assert.match(header, /pathname\.startsWith\(`\$\{prefix\}\/`\)/);
});

test("desktop and fixed mobile navigation follow the approved breakpoint", async () => {
  const styles = await source("../app/globals.css");

  assert.match(styles, /--fa-header-height: 76px/);
  assert.match(styles, /--fa-desktop-nav-height: 59px/);
  assert.match(styles, /--fa-content-max: 1320px/);
  assert.match(styles, /@media \(max-width: 650px\)/);
  assert.match(styles, /\.fa-desktop-nav \{ display: none; \}/);
  assert.match(styles, /\.fa-mobile-nav \{[^}]*position: fixed;/s);
  assert.match(styles, /env\(safe-area-inset-bottom\)/);
  assert.match(styles, /grid-template-columns: repeat\(5, 1fr\)/);
  assert.match(styles, /\.fa-mobile-nav-link \{[^}]*min-height: 62px;/s);
  assert.match(styles, /@media \(max-width: 365px\)/);
});

test("header includes accessible search, notifications and routed profile controls", async () => {
  const header = await source("../components/app-header.tsx");

  assert.match(header, /placeholder="Find a match, league or team"/);
  assert.match(header, /label="Open search"/);
  assert.match(header, /event\.key === "Escape"/);
  assert.match(header, /label="Notifications"/);
  assert.match(header, /const profileHref = identity \? "\/account" : "\/login\?next=\/account"/);
  assert.match(header, /aria-current=\{active \? "page" : undefined\}/);
});

test("auth pages hide the permanent application navigation shell", async () => {
  const authStyles = await source("../app/auth.css");

  assert.match(authStyles, /> \.fa-desktop-nav/);
  assert.match(authStyles, /> \.fa-mobile-nav/);
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
