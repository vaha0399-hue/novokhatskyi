"use client";

export default function ErrorPage({ reset }: { reset: () => void }) {
  return (
    <section className="shell error-page">
      <p className="eyebrow"><span /> Data unavailable</p>
      <h1>We could not load this analytical view.</h1>
      <p>The source data has not changed. You can safely try the read request again.</p>
      <button className="button button-primary" onClick={reset}>Try again</button>
    </section>
  );
}
