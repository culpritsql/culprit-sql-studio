# @culpritsql/plan-viewer

Interactive React execution plan diagrams, structural plan diffs, deadlock cycle graphs, and SQL Server Extended Events (`.xel` / XML) viewers extracted from [Culprit SQL Studio](https://github.com/culpritsql/culprit-sql-studio).

- **Standalone repository & docs**: [github.com/culpritsql/culprit-plan-viewer](https://github.com/culpritsql/culprit-plan-viewer)
- **WebAssembly parser companion**: [`@culpritsql/plans`](https://github.com/culpritsql/culprit-plans)

## Monorepo Commands

From the repository root:

```sh
python3 dev/build-plan-viewer.py
python3 dev/check-boundaries.py --plan-viewer-artifacts
node dev/smoke-plan-viewer.mjs
```

Culprit's desktop UI (`app/`) imports directly from `packages/culprit-plan-viewer/src/*` so desktop and npm stay a single source of truth.
