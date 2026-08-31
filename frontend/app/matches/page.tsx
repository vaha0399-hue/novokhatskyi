import Link from "next/link";

export default function MatchesPage() {
  return (
    <section className="shell section page-intro">
      <p className="eyebrow"><span /> Matches</p>
      <h1>Матчи</h1>
      <p>Выберите дату и лигу, чтобы открыть календарь матчей.</p>
      <Link className="button button-primary" href="/">Открыть список лиг</Link>
    </section>
  );
}
