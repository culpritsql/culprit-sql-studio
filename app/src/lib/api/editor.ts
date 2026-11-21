import type { Catalog } from "../../gen/Catalog";
import type { CheckInfo } from "../../gen/CheckInfo";
import type { Finding } from "../../gen/Finding";
import type { ResultSet } from "../../gen/ResultSet";
import type { TablePreview } from "../../gen/TablePreview";
import type { TablePreviewOptions } from "../../gen/TablePreviewOptions";
import type { SavedQuery } from "../../gen/SavedQuery";
import type { SaveQuery } from "../../gen/SaveQuery";
import type { Engine } from "../../gen/Engine";
import type { StatementCheck } from "../../gen/StatementCheck";
import { ownedRequests, type Invoke } from "../owned-requests";

export interface EditorApi {
  previewTable(name: string, options: TablePreviewOptions, signal?: AbortSignal): Promise<TablePreview>;
  run(name: string, sql: string, limit: number, confirmed: boolean): Promise<ResultSet>;
  checkStatement(engine: Engine, sql: string): Promise<StatementCheck>;
  lintSql(name: string, sql: string): Promise<Finding[]>;
  checks(): Promise<CheckInfo[]>;
  catalog(name: string): Promise<Catalog>;
  savedQueries(): Promise<SavedQuery[]>;
  saveQuery(query: SaveQuery): Promise<SavedQuery>;
  deleteSavedQuery(name: string): Promise<void>;
  savedQueryRan(name: string): Promise<void>;
}

export function desktopEditor(invoke: Invoke): EditorApi {
  const preview = ownedRequests(invoke, "begin_preview", "cancel_preview");
  return {
    previewTable: (name, options, signal) => preview("preview_table", { name, options }, signal),
    run: (name, sql, limit, confirmed) => invoke("run_sql", { name, sql, limit, confirmed }),
    checkStatement: (engine, sql) => invoke("check_statement", { engine, sql }),
    lintSql: (name, sql) => invoke("lint_sql", { name, sql }),
    checks: () => invoke("checks"),
    catalog: (name) => invoke("catalog", { name }),
    savedQueries: () => invoke("saved_queries"),
    saveQuery: (query) => invoke("save_query", { query }),
    deleteSavedQuery: (name) => invoke("delete_saved_query", { name }),
    savedQueryRan: (name) => invoke("saved_query_ran", { name }),
  };
}
