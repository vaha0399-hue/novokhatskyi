import type { Metadata } from "next";

import { AppHeader } from "@/components/app-header";

import "./globals.css";
import "./auth.css";

export const metadata: Metadata = {
  title: "FA — Sports Intelligence",
  description: "Model-backed sports analytics, match insights and predictions.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <div className="site-frame fa-shell">
          <div className="fa-shell-grid" aria-hidden="true" />
          <AppHeader />
          <main className="fa-content-slot">{children}</main>
          <footer className="site-footer">
            <div className="shell footer-inner">
              <span>Football Analytics</span>
              <span>Historical data. Factual comparisons. No predictions.</span>
            </div>
          </footer>
        </div>
      </body>
    </html>
  );
}
