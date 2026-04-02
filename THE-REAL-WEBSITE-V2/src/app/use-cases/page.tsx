import type { Metadata } from "next";
import type { ReactNode } from "react";

import { SiteHeader } from "../../components/site-header";

export const metadata: Metadata = {
  title: "Use Cases | MX8",
  description:
    "How teams use MX8 for e-commerce media processing, dataset preparation, photography, audio workflows, and large-scale media workflows.",
};

type UseCase = {
  label: string;
  title: string;
  body: string;
  code: ReactNode;
};

const useCases: UseCase[] = [
  {
    label: "AI Dataset Preparation",
    title: "Extract, resize, and reshape 50TB of raw sensor data.",
    body:
      "Autonomous driving and robotics teams use MX8 to turn continuous 4K driving logs into perfectly synchronized, identically sized frames for downstream labeling and training.",
    code: (
      <>
        <span className="text-[#c586c0]">import</span>{" "}
        <span className="text-[#d4d4d4]">mx8</span>
        <br />
        <br />
        <span className="text-[#6a9955]"># Process 50TB of raw fleet video into frames</span>
        <br />
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">run</span>(
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">input</span>=
        <span className="text-[#ce9178]">"s3://fleet-video/2026_03_cam_array/"</span>,
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">work</span>=[
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">extract_frames</span>(
        <span className="text-[#9cdcfe]">fps</span>=
        <span className="text-[#b5cea8]">2</span>,{" "}
        <span className="text-[#9cdcfe]">format</span>=
        <span className="text-[#ce9178]">"jpg"</span>),
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">resize</span>(
        <span className="text-[#9cdcfe]">width</span>=
        <span className="text-[#b5cea8]">1024</span>,{" "}
        <span className="text-[#9cdcfe]">height</span>=
        <span className="text-[#b5cea8]">1024</span>,{" "}
        <span className="text-[#9cdcfe]">media</span>=
        <span className="text-[#ce9178]">"image"</span>),
        <br />
        {"    "}],
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">output</span>=
        <span className="text-[#ce9178]">"s3://training-dataset/vision_v4/"</span>,
        <br />
        )
      </>
    ),
  },
  {
    label: "Film & VFX Pipelines",
    title: "Generate proxies and EXR pulls instantly from RAW shoots.",
    body:
      "Post-production and VFX teams replace 40-hour local render queues by generating 1080p edit proxies directly from 8K ARRI/RED AWS archives using 1,000 concurrent GPUs.",
    code: (
      <>
        <span className="text-[#c586c0]">import</span>{" "}
        <span className="text-[#d4d4d4]">mx8</span>
        <br />
        <br />
        <span className="text-[#6a9955]"># Generate edit proxies without downloading RAWs</span>
        <br />
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">run</span>(
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">input</span>=
        <span className="text-[#ce9178]">"s3://a24-archives/shoot-day-04/"</span>,
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">work</span>=[
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">proxy</span>(),
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">transcode</span>(
        <span className="text-[#9cdcfe]">codec</span>=
        <span className="text-[#ce9178]">"h264"</span>,{" "}
        <span className="text-[#9cdcfe]">crf</span>=
        <span className="text-[#b5cea8]">28</span>,{" "}
        <span className="text-[#9cdcfe]">resolution</span>=
        <span className="text-[#ce9178]">"1080p"</span>),
        <br />
        {"    "}],
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">output</span>=
        <span className="text-[#ce9178]">"s3://edit-bay/proxies-day-04/"</span>,
        <br />
        )
      </>
    ),
  },
  {
    label: "Studio Photography",
    title: "Develop 50,000 RAW files in 60 seconds.",
    body:
      "Photography agencies batch-develop, resize, and deliver massive archives of proprietary formats (.CR3, .ARW) directly in the cloud, unblocking local hardware.",
    code: (
      <>
        <span className="text-[#c586c0]">import</span>{" "}
        <span className="text-[#d4d4d4]">mx8</span>
        <br />
        <br />
        <span className="text-[#6a9955]"># Post-shoot batch RAW development</span>
        <br />
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">run</span>(
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">input</span>=
        <span className="text-[#ce9178]">"s3://raw-camera-cards/wedding-0329/"</span>,
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">work</span>=[
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">develop_raw</span>(),
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">resize</span>(
        <span className="text-[#9cdcfe]">width</span>=
        <span className="text-[#b5cea8]">2048</span>,{" "}
        <span className="text-[#9cdcfe]">height</span>=
        <span className="text-[#b5cea8]">2048</span>,{" "}
        <span className="text-[#9cdcfe]">media</span>=
        <span className="text-[#ce9178]">"image"</span>),
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">convert</span>(
        <span className="text-[#9cdcfe]">format</span>=
        <span className="text-[#ce9178]">"jpg"</span>,{" "}
        <span className="text-[#9cdcfe]">quality</span>=
        <span className="text-[#b5cea8]">90</span>),
        <br />
        {"    "}],
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">output</span>=
        <span className="text-[#ce9178]">"s3://delivered-jpegs/"</span>,
        <br />
        )
      </>
    ),
  },
  {
    label: "E-Commerce & Digital Asset Management",
    title: "Remove backgrounds at catalog scale.",
    body:
      "Marketplaces use MX8 to automatically standardize millions of unstructured merchant uploads, removing messy backgrounds and explicitly rewriting formats to modern WebP.",
    code: (
      <>
        <span className="text-[#c586c0]">import</span>{" "}
        <span className="text-[#d4d4d4]">mx8</span>
        <br />
        <br />
        <span className="text-[#6a9955]"># Seller inventory normalization</span>
        <br />
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">run</span>(
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">input</span>=
        <span className="text-[#ce9178]">"s3://merchant-uploads/piles/"</span>,
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">work</span>=[
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">remove_background</span>(),
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">crop</span>(),
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">convert</span>(
        <span className="text-[#9cdcfe]">format</span>=
        <span className="text-[#ce9178]">"webp"</span>,{" "}
        <span className="text-[#9cdcfe]">quality</span>=
        <span className="text-[#b5cea8]">85</span>),
        <br />
        {"    "}],
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">output</span>=
        <span className="text-[#ce9178]">"s3://clean-catalog/storefront/"</span>,
        <br />
        )
      </>
    ),
  },
  {
    label: "Audio & Transcription",
    title: "Resample and normalize 10,000 hours of AI audio feeds.",
    body:
      "Teams preparing Whisper datasets use MX8 to extract raw audio from massive video archives, resampling and standardizing loudness for the model ingest layer.",
    code: (
      <>
        <span className="text-[#c586c0]">import</span>{" "}
        <span className="text-[#d4d4d4]">mx8</span>
        <br />
        <br />
        <span className="text-[#6a9955]"># Whisper model audio prep</span>
        <br />
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">run</span>(
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">input</span>=
        <span className="text-[#ce9178]">"s3://Zoom_Archive/"</span>,
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">work</span>=[
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">extract_audio</span>(
        <span className="text-[#9cdcfe]">format</span>=
        <span className="text-[#ce9178]">"wav"</span>,{" "}
        <span className="text-[#9cdcfe]">bitrate</span>=
        <span className="text-[#ce9178]">"192k"</span>),
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">resample</span>(
        <span className="text-[#9cdcfe]">rate</span>=
        <span className="text-[#b5cea8]">16000</span>,{" "}
        <span className="text-[#9cdcfe]">channels</span>=
        <span className="text-[#b5cea8]">1</span>),
        <br />
        {"        "}
        <span className="text-[#d4d4d4]">mx8</span>.
        <span className="text-[#dcdcaa]">normalize</span>(
        <span className="text-[#9cdcfe]">loudness</span>=
        <span className="text-[#b5cea8]">-14.0</span>),
        <br />
        {"    "}],
        <br />
        {"    "}
        <span className="text-[#9cdcfe]">output</span>=
        <span className="text-[#ce9178]">"s3://whisper-ready-eval/"</span>,
        <br />
        )
      </>
    ),
  },
];

export default function UseCasesPage() {
  return (
    <div className="relative min-h-screen overflow-hidden bg-[#09090b] text-zinc-300 selection:bg-zinc-200 selection:text-zinc-950">
      <div className="absolute inset-0 bg-[linear-gradient(to_right,#27272a1a_1px,transparent_1px),linear-gradient(to_bottom,#27272a1a_1px,transparent_1px)] bg-[size:24px_24px] [mask-image:radial-gradient(ellipse_60%_50%_at_50%_0%,#000_70%,transparent_100%)]" />

      <main className="relative z-10 mx-auto flex min-h-screen max-w-6xl flex-col px-6 pt-32 pb-24">
        <SiteHeader current="use-cases" />

        <div className="mt-12 max-w-4xl">
          <div className="mb-6 inline-flex items-center gap-2 border border-zinc-800 bg-zinc-900/80 px-3 py-1 text-xs text-zinc-400">
            Use Cases
          </div>
          <h1 className="font-[family:var(--font-heading)] text-5xl font-bold leading-tight tracking-tight text-white md:text-7xl">
            From RAW to training datasets.
          </h1>
          <p className="mt-6 max-w-3xl text-lg leading-relaxed text-zinc-400 md:text-xl">
            MX8 is the industrial pipeline for ML engineers, VFX teams, and e-commerce platforms who process Petabytes of media, but refuse to build the infrastructure to do it.
          </p>
        </div>

        <div className="mt-16 space-y-12">
          {useCases.map((item) => (
            <section
              key={item.label}
              className="grid gap-8 border-t border-zinc-800 pt-8 lg:grid-cols-[minmax(0,1fr)_max-content]"
            >
              <div>
                <span className="block text-[11px] font-medium uppercase tracking-[0.22em] text-zinc-500">
                  {item.label}
                </span>
                <h2 className="mt-4 max-w-3xl text-3xl font-semibold tracking-tight text-zinc-100 md:text-4xl">
                  {item.title}
                </h2>
                <p className="mt-4 max-w-2xl text-base leading-relaxed text-zinc-400 md:text-lg">
                  {item.body}
                </p>
              </div>

              <div className="w-fit max-w-full overflow-hidden rounded-none border border-zinc-800 bg-black/90 shadow-2xl backdrop-blur-sm lg:justify-self-start">
                <div className="overflow-x-auto p-6 font-mono text-sm leading-relaxed md:text-base">
                  <pre className="text-[#d4d4d4]">
                    <code>{item.code}</code>
                  </pre>
                </div>
              </div>
            </section>
          ))}
        </div>
      </main>
    </div>
  );
}
