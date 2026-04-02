import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'MX8 | Media Transforms at Scale',
  description: 'Point MX8 at your data, define the transform, and get transformed outputs where you need them.',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body
        className="bg-[#09090b] font-sans text-white selection:bg-zinc-800 selection:text-white"
        style={
          {
            "--font-heading":
              '"IBM Plex Sans Condensed", "IBM Plex Sans", "Arial Narrow", sans-serif',
          } as React.CSSProperties
        }
      >
        {children}
      </body>
    </html>
  );
}
