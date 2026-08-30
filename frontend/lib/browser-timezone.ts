"use client";

export function getBrowserTimeZone(): string {
  if (typeof window === "undefined") {
    throw new Error("Browser timezone is only available in the browser");
  }

  const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  if (!timezone) {
    throw new Error("Browser did not provide an IANA timezone");
  }
  return timezone;
}
