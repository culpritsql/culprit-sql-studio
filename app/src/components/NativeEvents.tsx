import { useEffect, useId, useRef, useState } from "react";
import type { NativeLogAnalysis } from "../gen/NativeLogAnalysis";
import { api, errorMessage } from "../lib/api";
import { contextPreview } from "../lib/context-preview";
import { useFocusTrap } from "../lib/focus";
import { t, useLanguage } from "../lib/i18n";
import { RetainedNativeFailures } from "./RetainedNativeFailures";

type OpenArtifact = (signal: AbortSignal) => Promise<NativeLogAnalysis | null>;
const openArtifact: OpenArtifact = async (signal) => (await api()).openNativeEvents(signal);

/** Offline source-free entry: a selected file is never attached to the current database or its health. */
export function NativeEventsImport({ open = openArtifact }: { open?: OpenArtifact }) {
  const language = useLanguage();
  return <EventSelection key={language} open={open} />;
}

function EventSelection({ open }: { open: OpenArtifact }) {
  const titleId = useId();
  const pending = useRef<AbortController | null>(null);
  const dialog = useRef<HTMLElement>(null);
  const choose = useRef<HTMLButtonElement>(null);
  const [visible, setVisible] = useState(false);
  const [busy, setBusy] = useState(false);
  const [analysis, setAnalysis] = useState<NativeLogAnalysis | null>(null);
  const [error, setError] = useState<string | null>(null);
  useFocusTrap(dialog, visible, close);
  useEffect(() => { if (visible) choose.current?.focus(); }, [visible]);
  useEffect(() => () => pending.current?.abort(), []);
  function close() {
    pending.current?.abort();
    pending.current = null;
    setBusy(false);
    setVisible(false);
  }
  async function select() {
    pending.current?.abort();
    const owner = new AbortController();
    pending.current = owner;
    setBusy(true);
    setAnalysis(null);
    setError(null);
    try {
      const result = await open(owner.signal);
      if (!owner.signal.aborted && pending.current === owner) setAnalysis(result);
    } catch (failure) {
      if (!owner.signal.aborted && pending.current === owner) setError(errorMessage(failure).message);
    } finally {
      if (pending.current === owner) {
        pending.current = null;
        setBusy(false);
      }
    }
  }
  return <>
    <button type="button" className="btn ghost" onClick={() => setVisible(true)}>{t("native_events.import.open")}</button>
    {visible && <div className="overlay">
      <section ref={dialog} className="dialog" role="dialog" aria-modal="true" aria-labelledby={titleId} style={{ width: "min(72rem, 94vw)", maxHeight: "90vh", overflow: "auto" }} onKeyDown={(event) => {
        event.stopPropagation();
        if (event.key === "Escape") { event.preventDefault(); close(); }
      }}>
        <h2 id={titleId}>{t("native_events.import.title")}</h2>
        <p className="muted">{t("native_events.import.intro")}</p>
        <div className="actions">
          <button ref={choose} type="button" className="btn" onClick={() => void select()} aria-busy={busy}>{busy ? t("view.incident_history.import.replace") : t("native_events.import.choose")}</button>
          <button type="button" className="btn ghost" onClick={close}>{t("ui.common.close")}</button>
        </div>
        {error && <p role="alert">{contextPreview(error).text}</p>}
        {analysis && <NativeEventsRead analysis={analysis} />}
      </section>
    </div>}
  </>;
}

export function NativeEventsRead({ analysis }: { analysis: NativeLogAnalysis }) {
  return <>
    {(!analysis.input.complete || analysis.stopped !== null || analysis.issues.length > 0) && <p role="status">{t("native_logs.cli.incomplete")}</p>}
    <RetainedNativeFailures evidence={analysis.evidence} report={analysis.report} importedDeadlocks={analysis.native_events?.deadlocks} />
  </>;
}
