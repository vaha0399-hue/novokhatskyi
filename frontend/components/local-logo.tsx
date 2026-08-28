"use client";

import { useState } from "react";

type LogoKind = "teams" | "leagues";
const LOGO_CACHE_VERSION = "1";

function monogram(name: string): string {
  return name
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0])
    .join("")
    .toUpperCase() || "FA";
}

/**
 * Uses only the same-origin Next.js media route. If a local cache file is
 * missing, a deterministic monogram preserves the layout without any
 * browser-side provider request.
 */
export function LocalLogo({ kind, id, name, className }: { kind: LogoKind; id: number; name: string; className?: string }) {
  const [loaded, setLoaded] = useState(false);
  const [failed, setFailed] = useState(false);
  const classes = `local-logo ${className ?? ""}`.trim();

  return (
    <span className={`${classes} ${loaded ? "local-logo-loaded" : ""}`} aria-hidden="true">
      <span className="local-logo-fallback">{monogram(name)}</span>
      {!failed && (
        <img
          className="local-logo-image"
          src={`/media/${kind}/${id}?v=${LOGO_CACHE_VERSION}`}
          alt=""
          onLoad={() => setLoaded(true)}
          onError={() => setFailed(true)}
        />
      )}
    </span>
  );
}
