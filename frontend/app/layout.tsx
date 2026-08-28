import type { Metadata } from "next";

import { AppHeader } from "@/components/app-header";

import "./globals.css";

export const metadata: Metadata = {
  title: "Football Analytics — Historical intelligence",
  description: "Factual football analytics, match history and team comparisons.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <div className="site-frame">
          <AppHeader />
          <main>{children}</main>
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
