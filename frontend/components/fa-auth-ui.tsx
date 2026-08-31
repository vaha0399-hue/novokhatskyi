import type { ChangeEvent, InputHTMLAttributes, ReactNode } from "react";

export type FAAuthView = "signin" | "signup" | "forgot" | "success" | "update";
export type FASuccessKind = "signin" | "signup" | "reset";

export function FAAuthFrame({ view, children }: { view: FAAuthView; children: ReactNode }) {
  return (
    <main className="fa-auth-experience">
      <div className="fa-ambient-grid" aria-hidden="true" />
      <FAAnalyticsBackdrop />

      <header className="fa-site-header">
        <FABrand compact />
        <div className="fa-model-status"><i /> MODEL ONLINE <span>v4.8</span></div>
      </header>

      <section className="fa-auth-stage" aria-live="polite">
        <div className="fa-auth-glow" aria-hidden="true" />
        <div className={`fa-auth-card fa-auth-card-${view}`}>
          <div className="fa-auth-card-inner">{children}</div>
        </div>
      </section>

      <footer className="fa-site-footer">
        <span>© 2026 FA SPORTS INTELLIGENCE</span>
        <span>DATA-DRIVEN · RESPONSIBLE · INDEPENDENT</span>
      </footer>
    </main>
  );
}

export function FABrand({ compact = false }: { compact?: boolean }) {
  return (
    <div className={compact ? "fa-brand fa-brand-compact" : "fa-brand"} aria-label="FA Sports Intelligence">
      <span className="fa-brand-orbit"><b>FA</b></span>
      <span className="fa-brand-name"><strong>FA</strong><small>SPORTS INTELLIGENCE</small></span>
    </div>
  );
}

function FAAnalyticsBackdrop() {
  return (
    <section className="fa-analytics-backdrop" aria-hidden="true">
      <article className="fa-data-card fa-match-overview">
        <FACardLabel icon="◉">PREMIER LEAGUE · ROUND 5</FACardLabel>
        <div className="fa-fixture">
          <div><span className="fa-team-badge fa-badge-blue">MC</span><strong>Man City</strong><small>W W D W L</small></div>
          <div className="fa-versus"><small>TODAY · 20:00</small><b>VS</b><span>ETIHAD STADIUM</span></div>
          <div><span className="fa-team-badge fa-badge-red">A</span><strong>Arsenal</strong><small>D L W W W</small></div>
        </div>
      </article>

      <article className="fa-data-card fa-win-probability">
        <FACardLabel>WIN PROBABILITY</FACardLabel>
        <div className="fa-probability-bar"><i /><i /><i /></div>
        <div className="fa-probability-values"><b>58%<small>HOME</small></b><b>20%<small>DRAW</small></b><b>22%<small>AWAY</small></b></div>
      </article>

      <article className="fa-data-card fa-xg-card">
        <FACardLabel>EXPECTED GOALS (xG)</FACardLabel>
        <div className="fa-xg-values"><b>1.84</b><span>VS</span><b>1.13</b></div>
        <FAMiniChart />
      </article>

      <article className="fa-data-card fa-model-edge">
        <FACardLabel>MODEL CONFIDENCE</FACardLabel>
        <div className="fa-confidence"><b>74%</b><span>HIGH SIGNAL</span></div>
        <div className="fa-confidence-bar"><i /></div>
        <p>Home performance trend exceeds market baseline</p>
      </article>

      <article className="fa-data-card fa-h2h-card">
        <FACardLabel>HEAD TO HEAD · LAST 5</FACardLabel>
        <div className="fa-h2h"><b>3<small>WINS</small></b><b>1<small>DRAW</small></b><b>1<small>WIN</small></b></div>
        <FAFormDots sequence={["w", "w", "d", "w", "l"]} />
      </article>

      <article className="fa-data-card fa-form-card">
        <FACardLabel>RECENT FORM</FACardLabel>
        <FAFormDots sequence={["w", "d", "w", "w", "l"]} />
        <div className="fa-form-lines"><i /><i /><i /></div>
      </article>

      <div className="fa-data-scanline" />
    </section>
  );
}

function FACardLabel({ children, icon }: { children: ReactNode; icon?: string }) {
  return <div className="fa-card-label">{icon && <span>{icon}</span>}{children}<i>•••</i></div>;
}

function FAFormDots({ sequence }: { sequence: string[] }) {
  return <div className="fa-form-dots">{sequence.map((item, index) => <span className={item} key={`${item}-${index}`}>{item.toUpperCase()}</span>)}</div>;
}

function FAMiniChart() {
  return (
    <div className="fa-mini-chart">
      <span className="fa-chart-grid" />
      <span className="fa-chart-line fa-line-blue" />
      <span className="fa-chart-line fa-line-red" />
    </div>
  );
}

export function FAAuthHeading({ eyebrow, title, text }: { eyebrow: string; title: string; text: string }) {
  return (
    <div className="fa-auth-heading">
      <span>{eyebrow}</span>
      <h1>{title}</h1>
      <p>{text}</p>
    </div>
  );
}

export function FAField({ label, icon, action, onValueChange, ...input }: {
  label: string;
  icon: string;
  action?: ReactNode;
  onValueChange?: (value: string) => void;
} & Omit<InputHTMLAttributes<HTMLInputElement>, "onChange">) {
  return (
    <label className="fa-field">
      <span className="fa-field-label">{label}</span>
      <span className="fa-field-control">
        <i aria-hidden="true">{icon}</i>
        <input {...input} onChange={onValueChange ? (event: ChangeEvent<HTMLInputElement>) => onValueChange(event.target.value) : undefined} />
        {action}
      </span>
    </label>
  );
}

export function FASubmitButton({ children, disabled = false }: { children: ReactNode; disabled?: boolean }) {
  return <button className="fa-submit-button" type="submit" disabled={disabled}>{children}<b aria-hidden="true">→</b></button>;
}

export function FAFormMessage({ value, kind = "error" }: { value: string | null; kind?: "error" | "success" }) {
  return value ? <p className={`fa-auth-message fa-auth-message-${kind}`} role={kind === "error" ? "alert" : "status"}>{value}</p> : null;
}

export function FASuccess({ kind, onAction }: { kind: FASuccessKind; onAction: () => void }) {
  const content = {
    signin: { eyebrow: "ACCESS GRANTED", title: "Welcome back", text: "Your personalised analytics workspace is ready." },
    signup: { eyebrow: "ACCOUNT CREATED", title: "You’re all set", text: "Your FA workspace has been created successfully." },
    reset: { eyebrow: "CHECK YOUR INBOX", title: "Link sent", text: "We sent password reset instructions to your email address." },
  }[kind];

  return (
    <div className="fa-success-view">
      <FABrand />
      <div className="fa-success-orbit"><span>✓</span></div>
      <FAAuthHeading {...content} />
      <button className="fa-submit-button" type="button" onClick={onAction}>{kind === "signin" ? "Open analytics" : "Return to sign in"}<b aria-hidden="true">→</b></button>
      <small>Protected with encrypted session security</small>
    </div>
  );
}
