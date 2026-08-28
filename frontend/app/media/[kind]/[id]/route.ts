import { NextResponse } from "next/server";

import { backendBaseUrl } from "@/lib/api";

const assetKinds = new Set(["teams", "leagues"]);

export async function GET(_: Request, { params }: { params: Promise<{ kind: string; id: string }> }) {
  const { kind, id } = await params;
  if (!assetKinds.has(kind) || !/^\d+$/.test(id) || Number(id) <= 0) {
    return new NextResponse(null, { status: 404 });
  }

  let response: Response;
  try {
    response = await fetch(`${backendBaseUrl()}/web/v1/assets/${kind}/${id}/logo`, {
      headers: { Accept: "image/png" },
      next: { revalidate: 604800 },
    });
  } catch {
    return new NextResponse(null, { status: 503 });
  }

  if (!response.ok || !response.body) {
    return new NextResponse(null, { status: response.status === 404 ? 404 : 502 });
  }

  return new NextResponse(response.body, {
    headers: {
      "Content-Type": "image/png",
      "Cache-Control": "public, max-age=604800, immutable",
    },
  });
}
