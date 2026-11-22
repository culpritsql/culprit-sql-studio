import type { ReactNode } from "react";
import { PlanDiagram, type DiagramLens, type Mark } from "./PlanDiagram.js";
import type { PlanViewerTheme } from "./PlanViewer.js";
import type { Plan, PlanNode, QueryStat } from "./types.js";

export type PlanCanvasProps = {
  plan: Plan;
  query?: QueryStat | null;
  /** Controlled active metric lens ("time" | "cost" | "rows" | "reads"). */
  lens?: DiagramLens;
  /** Initial metric lens when uncontrolled. */
  defaultLens?: DiagramLens;
  /** Selected operator node id. */
  selectedId?: number | null;
  /** Callback fired when an operator card is clicked. */
  onSelect?: (id: number) => void;
  /** Initial zoom factor (0.25 to 1.8). Auto-fits when omitted. */
  initialZoom?: number;
  /** Controlled zoom factor. */
  zoom?: number;
  onZoomChange?: (zoom: number) => void;
  /** Show insight count badges and severity borders on operator cards. Defaults to false for pure flow rendering. */
  showInsights?: boolean;
  /** Show floating zoom/fit/minimap controls in the top-right corner. Defaults to true. */
  showControls?: boolean;
  /** Show the corner minimap. Defaults to automatic when >= 6 operators are visible. */
  showMinimap?: boolean;
  /** Explicit canvas height (e.g. 480 or "100%"). */
  height?: number | string;
  /** Diff change markers when rendering custom comparison views. */
  marks?: Map<number, Mark>;
  /** Shared region color tints (0..5). */
  tints?: Map<number, number>;
  /** Custom node overlay renderer. */
  overlay?: (node: PlanNode, box: { x: number; y: number; w: number; h: number }) => ReactNode;
  theme?: PlanViewerTheme;
  className?: string;
};

/**
 * Pure interactive execution-plan flow canvas: drag-to-pan, Ctrl/Cmd + wheel zoom, and optional minimap,
 * with zero insight banners, prose summaries, headers, or built-in property drawers.
 */
export function PlanCanvas({
  plan,
  query,
  lens,
  defaultLens,
  selectedId,
  onSelect,
  initialZoom,
  zoom,
  onZoomChange,
  showInsights = false,
  showControls = true,
  showMinimap,
  height,
  marks,
  tints,
  overlay,
  theme,
  className,
}: PlanCanvasProps) {
  return (
    <PlanDiagram
      plan={plan}
      query={query}
      bare
      showProps={false}
      showInsights={showInsights}
      showControls={showControls}
      showMinimap={showMinimap ?? undefined}
      lens={lens}
      defaultLens={defaultLens}
      height={height}
      selectedId={selectedId}
      onSelect={onSelect}
      initialZoom={initialZoom}
      zoom={zoom}
      onZoomChange={onZoomChange}
      marks={marks}
      tints={tints}
      overlay={overlay}
      theme={theme}
      className={className}
    />
  );
}
