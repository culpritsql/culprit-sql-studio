import { useMemo } from "react";
import { prettySql, tokenizeSql } from "./host.js";

export function Sql({ text, pretty = true, className }: { text: string; pretty?: boolean; className?: string }) {
  const tokens = useMemo(() => tokenizeSql(pretty ? prettySql(text) : text), [text, pretty]);
  return (
    <pre dir="ltr" className={`sql ${className ?? ""}`}>
      {tokens.map((tok, i) => (tok.kind === "t" ? tok.text : <span key={i} className={tok.kind}>{tok.text}</span>))}
    </pre>
  );
}
