export function positiveId(value: string): number | null {
  if (!/^\d+$/.test(value)) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

export function safeNextPath(value: string | null | undefined, fallback = "/account"): string {
  return value && value.startsWith("/") && !value.startsWith("//") && !value.includes("\\") && !/[\r\n]/.test(value)
    ? value
    : fallback;
}
