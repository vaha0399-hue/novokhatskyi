import { Brand } from "@/components/brand";

export function AuthShell({ eyebrow, title, description, children }: {
  eyebrow: string;
  title: string;
  description: string;
  children: React.ReactNode;
}) {
  return <section className="auth-page shell"><div className="auth-side"><Brand /><p className="eyebrow"><span /> {eyebrow}</p><h1>{title}</h1><p>{description}</p><div className="auth-side-note"><span>✓</span> Cookie-based session · Secure Supabase Auth</div></div><div className="auth-card">{children}</div></section>;
}
