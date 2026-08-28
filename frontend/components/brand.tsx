import Link from "next/link";

export function Brand() {
  return (
    <Link className="brand" href="/" aria-label="Football Analytics home">
      <span className="brand-mark" aria-hidden="true">FA</span>
      <span>
        <strong>Football Analytics</strong>
        <small>Historical intelligence</small>
      </span>
    </Link>
  );
}
