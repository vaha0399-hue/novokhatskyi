import Link from "next/link";

export default function MatchesPage() {
  return (
    <section className="shell section page-intro">
      <p className="eyebrow"><span /> Matches</p>
      <h1>Matches</h1>
      <p>Select a date and league to open the match schedule.</p>
      <Link className="button button-primary" href="/">Open league list</Link>
    </section>
  );
}
