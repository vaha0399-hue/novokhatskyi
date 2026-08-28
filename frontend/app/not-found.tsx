import Link from "next/link";

export default function NotFound() {
  return (
    <section className="shell error-page">
      <p className="eyebrow"><span /> 404</p>
      <h1>This data route does not exist.</h1>
      <p>The league, season, fixture or team may not be available in the current historical dataset.</p>
      <Link className="button button-primary" href="/">Back to competitions</Link>
    </section>
  );
}
