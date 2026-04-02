import Link from "next/link";

type SiteHeaderProps = {
  current?: "home" | "use-cases" | "about";
};

function MX8Mark() {
  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 24 24"
      className="h-5 w-5 text-white"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.9"
      strokeLinecap="square"
      strokeLinejoin="miter"
    >
      <path d="M3.5 5.5V18.5H20.5" />
      <path d="M6.75 8.5L11 12L6.75 15.5" />
      <path d="M13.5 15.5H18" />
      <path d="M3.5 5.5H15.5" />
    </svg>
  );
}

export function SiteHeader({ current = "home" }: SiteHeaderProps) {
  return (
    <div className="absolute top-8 left-8 right-8 flex items-center justify-between">
      <Link
        href="/"
        className="flex items-center gap-2.5 text-[17px] font-semibold tracking-widest text-white"
      >
        <MX8Mark /> MX8
      </Link>
      <nav className="flex items-center gap-6 text-sm text-zinc-400">
        <Link
          href="/use-cases"
          className={`transition-colors hover:text-white ${
            current === "use-cases" ? "text-white" : ""
          }`}
        >
          Use Cases
        </Link>
        <Link
          href="/about"
          className={`transition-colors hover:text-white ${
            current === "about" ? "text-white" : ""
          }`}
        >
          About
        </Link>
      </nav>
    </div>
  );
}
