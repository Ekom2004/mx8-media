"use client";

import { motion } from "framer-motion";
import { ArrowRight, Check, Copy } from "lucide-react";
import React from "react";
import { SiteHeader } from "../components/site-header";

const INSTALL_COMMAND = "pip install mx8";

const fadeUp = {
  initial: { opacity: 0, y: 18 },
  animate: { opacity: 1, y: 0 },
};

export default function Home() {
  const [copied, setCopied] = React.useState(false);

  const handleCopyInstall = async () => {
    try {
      await navigator.clipboard.writeText(INSTALL_COMMAND);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  };

  return (
    <div className="relative min-h-screen overflow-hidden bg-[#09090b] font-sans text-zinc-300 selection:bg-zinc-200 selection:text-zinc-950">
      {/* Subtle Grid Background */}
      <div className="absolute inset-0 bg-[linear-gradient(to_right,#27272a1a_1px,transparent_1px),linear-gradient(to_bottom,#27272a1a_1px,transparent_1px)] bg-[size:24px_24px] [mask-image:radial-gradient(ellipse_60%_50%_at_50%_0%,#000_70%,transparent_100%)]"></div>
      <div className="pointer-events-none absolute inset-x-0 top-0 h-[420px] bg-[radial-gradient(circle_at_top_left,rgba(255,255,255,0.07),transparent_34%),radial-gradient(circle_at_top_right,rgba(255,255,255,0.04),transparent_28%)]" />

      <main className="relative z-10 mx-auto flex w-full max-w-[1360px] flex-col px-6 pt-28 pb-24 lg:px-8">
        
        <SiteHeader current="home" />
        
        {/* Hero Section */}
        <motion.div
          variants={fadeUp}
          initial="initial"
          animate="animate"
          transition={{ duration: 0.45, ease: "easeOut" }}
          className="mt-4 flex w-full justify-center"
        >
          <div className="inline-flex w-fit items-center gap-2 rounded-none border border-zinc-800 bg-zinc-900/80 px-3 py-1 text-xs text-zinc-400">
            <span className="flex h-2 w-2 rounded-full bg-emerald-400 animate-pulse"></span>
            v0.2.0 API is live
          </div>
        </motion.div>

        <div className="mt-10 grid w-full grid-cols-1 gap-12 lg:grid-cols-[minmax(0,1fr)_320px] lg:items-end">
          <motion.div
            initial="initial"
            animate="animate"
            transition={{ staggerChildren: 0.08 }}
            className="flex max-w-5xl flex-col gap-5"
          >
            <motion.h1
              variants={fadeUp}
              transition={{ duration: 0.5, ease: "easeOut" }}
              className="max-w-5xl font-[family:var(--font-heading)] text-5xl font-bold leading-[0.96] tracking-tight text-white md:text-7xl xl:text-[5.75rem]"
            >
              The refinery
              <br />
              <span className="text-zinc-500">for unstructured media.</span>
            </motion.h1>
            
            <motion.p
              variants={fadeUp}
              transition={{ duration: 0.48, ease: "easeOut" }}
              className="mt-3 max-w-2xl text-lg leading-relaxed text-zinc-400 md:text-xl"
            >
              MX8 is the industrial pipeline that turns raw Petabyte-scale video, audio, and image logs into clean, standardized datasets for AI training and media workflows.
            </motion.p>

            <motion.div
              variants={fadeUp}
              transition={{ duration: 0.48, ease: "easeOut" }}
              className="mt-6 flex flex-wrap items-center gap-4"
            >
              <button className="inline-flex items-center gap-2 rounded-none bg-zinc-200 px-6 py-3 font-semibold text-black transition-colors hover:bg-zinc-300">
                Book Demo
              </button>
              <button className="inline-flex items-center gap-2 rounded-none border border-zinc-800 bg-zinc-900/90 px-6 py-3 font-semibold text-zinc-100 transition-colors hover:border-zinc-700 hover:bg-zinc-800">
                Read Documentation
                <ArrowRight size={16} />
              </button>
            </motion.div>
          </motion.div>

          <motion.div
            {...fadeUp}
            transition={{ duration: 0.55, ease: "easeOut", delay: 0.16 }}
            className="border-l border-zinc-800/80 pl-6 lg:pb-3"
          >
            <span className="block text-[11px] font-medium uppercase tracking-[0.22em] text-zinc-500">
              Built For Media Workloads
            </span>
            <div className="mt-4 space-y-5">
              <div>
                <span className="block text-2xl font-semibold text-white">Batch</span>
                <span className="mt-1 block text-sm leading-relaxed text-zinc-400">
                  Submit large media jobs and let them run in the background.
                </span>
              </div>
              <div>
                <span className="block text-2xl font-semibold text-white">Transform</span>
                <span className="mt-1 block text-sm leading-relaxed text-zinc-400">
                  Extract, resize, clip, transcode, and export media in the same workflow.
                </span>
              </div>
            </div>
          </motion.div>
        </div>

        <motion.div
          {...fadeUp}
          transition={{ duration: 0.55, ease: "easeOut", delay: 0.22 }}
          className="mt-14 flex w-full flex-col items-start"
        >
          <div className="mb-3 inline-flex w-full max-w-[720px] items-center justify-between gap-3 rounded-none border border-zinc-800 bg-zinc-950/90 px-4 py-3 shadow-[0_0_0_1px_rgba(39,39,42,0.45)]">
            <div className="flex min-w-0 items-center gap-3">
              <span className="font-mono text-sm text-zinc-500">$</span>
              <code className="truncate font-mono text-sm tracking-wide text-zinc-100 md:text-base">
                {INSTALL_COMMAND}
              </code>
            </div>
            <button
              type="button"
              onClick={handleCopyInstall}
              className="inline-flex shrink-0 appearance-none items-center gap-2 rounded-none border border-zinc-800 bg-zinc-950 px-3 py-1.5 text-xs font-medium uppercase tracking-[0.18em] text-zinc-400 transition-colors hover:border-zinc-700 hover:text-zinc-100"
              title="Copy install command"
            >
              {copied ? <Check size={14} /> : <Copy size={14} />}
              {copied ? "Copied" : "Copy"}
            </button>
          </div>

          {/* The Code Editor Window */}
          <div className="relative w-full max-w-[720px] overflow-hidden rounded-none border border-zinc-800 bg-[linear-gradient(180deg,rgba(6,6,8,0.98),rgba(14,14,18,0.96))] shadow-2xl backdrop-blur-sm">
            <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-[linear-gradient(90deg,transparent,rgba(255,255,255,0.35),transparent)]" />
            <div className="flex items-center justify-between border-b border-zinc-800 bg-zinc-950/70 px-4 py-2">
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-1.5">
                  <span className="h-2 w-2 rounded-full bg-[#ff5f57]" />
                  <span className="h-2 w-2 rounded-full bg-[#febc2e]" />
                  <span className="h-2 w-2 rounded-full bg-[#28c840]" />
                </div>
                <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-zinc-500">
                  job.py
                </span>
              </div>
              <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-zinc-600">
                python
              </span>
            </div>
            <div className="relative overflow-x-auto p-6 font-mono text-sm leading-relaxed md:text-base">
              <div className="pointer-events-none absolute right-10 top-8 h-24 w-24 rounded-full bg-white/[0.03] blur-2xl" />
              <pre className="text-[#d4d4d4]">
                <code>
<span className="text-[#c586c0]">import</span> <span className="text-[#d4d4d4]">mx8</span><br/>
<br/>
<span className="text-[#6a9955]"># Resize and convert a large image dataset</span><br/>
<span className="inline-block bg-zinc-900/80 px-3 py-0.5"><span className="text-[#d4d4d4]">mx8</span>.<span className="text-[#dcdcaa]">run</span>(</span><br/>
<span className="inline-block px-3">{"    "}<span className="text-[#9cdcfe]">input</span>=<span className="text-[#ce9178]">"s3://catalog-images/"</span>,</span><br/>
<span className="inline-block bg-zinc-900/50 px-3">{"    "}<span className="text-[#9cdcfe]">work</span>=[</span><br/>
<span className="inline-block bg-zinc-900/50 px-3">{"        "}<span className="text-[#d4d4d4]">mx8</span>.<span className="text-[#dcdcaa]">resize</span>(<span className="text-[#9cdcfe]">width</span>=<span className="text-[#b5cea8]">1024</span>, <span className="text-[#9cdcfe]">height</span>=<span className="text-[#b5cea8]">1024</span>, <span className="text-[#9cdcfe]">media</span>=<span className="text-[#ce9178]">"image"</span>),</span><br/>
<span className="inline-block bg-zinc-900/50 px-3">{"        "}<span className="text-[#d4d4d4]">mx8</span>.<span className="text-[#dcdcaa]">convert</span>(<span className="text-[#9cdcfe]">format</span>=<span className="text-[#ce9178]">"webp"</span>, <span className="text-[#9cdcfe]">quality</span>=<span className="text-[#b5cea8]">85</span>),</span><br/>
<span className="inline-block bg-zinc-900/50 px-3">{"    "}],</span><br/>
<span className="inline-block px-3">{"    "}<span className="text-[#9cdcfe]">output</span>=<span className="text-[#ce9178]">"s3://catalog-images-web/"</span>,</span><br/>
)
                </code>
              </pre>
            </div>
          </div>
        </motion.div>

        <motion.div
          {...fadeUp}
          transition={{ duration: 0.55, ease: "easeOut", delay: 0.32 }}
          className="mt-16 w-full"
        >
          <div className="mb-6">
            <span className="block text-[11px] font-medium uppercase tracking-[0.24em] text-zinc-500">
              Why MX8
            </span>
          </div>
          <div className="border-t border-zinc-800">
            <div className="border-b border-zinc-800 px-2 py-7 md:px-6">
              <span className="block text-[11px] font-medium uppercase tracking-[0.22em] text-zinc-500">
                One Job Shape
              </span>
              <span className="mt-3 block max-w-4xl text-2xl font-medium tracking-tight text-zinc-100 md:text-[2rem]">
                Input, work, and output stay consistent across the job.
              </span>
            </div>
            <div className="border-b border-zinc-800 px-2 py-7 md:px-6">
              <span className="block text-[11px] font-medium uppercase tracking-[0.22em] text-zinc-500">
                Throughput
              </span>
              <span className="mt-3 block max-w-4xl text-2xl font-medium tracking-tight text-zinc-100 md:text-[2rem]">
                Large media transforms stay practical at dataset scale.
              </span>
            </div>
            <div className="px-2 py-7 md:px-6">
              <span className="block text-[11px] font-medium uppercase tracking-[0.22em] text-zinc-500">
                Placement
              </span>
              <span className="mt-3 block max-w-4xl text-2xl font-medium tracking-tight text-zinc-100 md:text-[2rem]">
                Run against media where it already lives.
              </span>
            </div>
          </div>
        </motion.div>

      </main>
    </div>
  );
}
