export function Logo({ size = 22 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 460 460" aria-hidden="true">
      <rect width="460" height="460" rx="102" fill="var(--brand)" />
      <path
        d="M 283.33 301.78 A 106.25 106.25 0 1 1 283.33 158.22"
        fill="none"
        stroke="var(--brand-ink)"
        strokeWidth="67.5"
        strokeLinecap="butt"
      />
      <polygon points="340.5,179 391.5,230 340.5,281 289.5,230" fill="#e0484e" />
    </svg>
  );
}
