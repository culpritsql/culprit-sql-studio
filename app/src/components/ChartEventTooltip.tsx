import { useLayoutEffect, useRef } from "react";
import { createPortal } from "react-dom";
import { t } from "../lib/i18n";
import { EventIcon, eventTone } from "./EventIcon";
import type { ChartEvent } from "./TimeChart";

const VIEWPORT_MARGIN = 8;

function placeTooltip(panel: HTMLElement, marker: HTMLButtonElement) {
  const anchor = marker.getBoundingClientRect();
  const above = anchor.top - VIEWPORT_MARGIN;
  const below = window.innerHeight - anchor.bottom - VIEWPORT_MARGIN;
  const left = Math.min(anchor.left, window.innerWidth - VIEWPORT_MARGIN - panel.offsetWidth);
  panel.style.left = `${Math.max(VIEWPORT_MARGIN, left)}px`;
  panel.style.top = below > above ? `${anchor.bottom}px` : "auto";
  panel.style.bottom = below > above ? "auto" : `${window.innerHeight - anchor.top}px`;
  panel.style.maxHeight = `${Math.max(0, above, below)}px`;
}

/** The marker keeps keyboard focus and its comparison action while the reader scrolls through its changes. */
function scrollChanges(panel: HTMLElement, key: string): boolean {
  if (panel.scrollHeight <= panel.clientHeight) return false;
  const rowHeight = panel.firstElementChild?.clientHeight ?? panel.clientHeight;
  switch (key) {
    case "ArrowDown": panel.scrollTop += rowHeight; break;
    case "ArrowUp": panel.scrollTop -= rowHeight; break;
    case "PageDown": panel.scrollTop += panel.clientHeight; break;
    case "PageUp": panel.scrollTop -= panel.clientHeight; break;
    case "Home": panel.scrollTop = 0; break;
    case "End": panel.scrollTop = panel.scrollHeight; break;
    default: return false;
  }
  return true;
}

export function ChartEventTooltip({ id, marker, events, onDismiss }: {
  id: string; marker: HTMLButtonElement; events: ChartEvent[]; onDismiss: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => {
    const panel = ref.current!;
    const place = () => placeTooltip(panel, marker);
    const onScroll = (event: Event) => {
      if (!panel.contains(event.target as Node)) place();
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") onDismiss();
      else if (event.target !== marker || !scrollChanges(panel, event.key)) return;
      event.preventDefault();
      event.stopPropagation();
    };
    place();
    const resized = new ResizeObserver(place);
    resized.observe(panel);
    resized.observe(marker);
    const visibility = new IntersectionObserver(([entry]) => {
      if (entry && !entry.isIntersecting) onDismiss();
    });
    visibility.observe(marker);
    window.addEventListener("resize", place);
    document.addEventListener("scroll", onScroll, true);
    document.addEventListener("keydown", onKey, true);
    return () => {
      resized.disconnect();
      visibility.disconnect();
      window.removeEventListener("resize", place);
      document.removeEventListener("scroll", onScroll, true);
      document.removeEventListener("keydown", onKey, true);
    };
  }, [marker, onDismiss]);

  return createPortal(
    <div ref={ref} id={id} className="tc-ev-tip" role="tooltip" data-own-click tabIndex={-1}>
      {events.map((event) => (
        <div key={event.id} className="tc-ev-item">
          <span className={`tc-ev-dot ${eventTone(event.kind)}`}><EventIcon kind={event.kind} size={11} /></span>
          <span><b>{event.title}</b><span className="muted">{event.when}</span></span>
        </div>
      ))}
      {events[0]?.onClick && <div className="tc-ev-hint">{t("ui.time_chart.click_to_compare")}</div>}
    </div>,
    document.body,
  );
}
