import type { Metadata } from "next";

import { SiteHeader } from "../../components/site-header";

export const metadata: Metadata = {
  title: "Book Demo | MX8",
  description:
    "Talk to the MX8 team about search and transforms for massive media datasets.",
};

export default function BookDemoPage() {
  return (
    <div className="relative min-h-screen overflow-hidden bg-[#09090b] text-zinc-300 selection:bg-zinc-200 selection:text-zinc-950">
      <div className="absolute inset-0 bg-[linear-gradient(to_right,#27272a1a_1px,transparent_1px),linear-gradient(to_bottom,#27272a1a_1px,transparent_1px)] bg-[size:24px_24px] [mask-image:radial-gradient(ellipse_60%_50%_at_50%_0%,#000_70%,transparent_100%)]" />

      <main className="relative z-10 mx-auto flex min-h-screen max-w-5xl flex-col px-6 pt-32 pb-24">
        <SiteHeader current="book-demo" />

        <div className="mt-12 max-w-3xl">
          <div className="mb-6 inline-flex items-center gap-2 border border-zinc-800 bg-zinc-900/80 px-3 py-1 text-xs text-zinc-400">
            Book Demo
          </div>
          <h1 className="font-[family:var(--font-heading)] text-5xl font-bold leading-tight tracking-tight text-white md:text-7xl">
            Bring a real media workload.
          </h1>
          <p className="mt-6 max-w-2xl text-lg leading-relaxed text-zinc-400 md:text-xl">
            If your team needs to search, resize, convert, clip, extract, or
            transcode large media datasets, we want to see the real job.
          </p>
        </div>

        <div className="mt-16 grid gap-10 border-t border-zinc-800 pt-10 md:grid-cols-[220px_1fr]">
          <div className="text-[12px] font-semibold uppercase tracking-[0.18em] text-zinc-200">
            What To Bring
          </div>
          <div className="space-y-4 text-base leading-relaxed text-zinc-400">
            <p>The dataset shape: images, video, or both.</p>
            <p>The job you run today: resize, convert, clip, extract, transcode, or search.</p>
            <p>The output you need and where it should land.</p>
            <p>The size of the workload and how often it happens.</p>
          </div>
        </div>

        <div className="mt-16 border-t border-zinc-800 pt-10">
          <p className="max-w-2xl text-base leading-relaxed text-zinc-400">
            The fastest way to start is with one real job. Bring the workload,
            and we’ll tell you whether MX8 is the right fit.
          </p>
        </div>
      </main>
    </div>
  );
}
