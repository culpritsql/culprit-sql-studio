# Culprit archive client

Read fleet status, database resources, incident revisions, historical windows and original evidence with the types generated from Culprit's Rust model.

```ts
import { createArchiveClient } from "@culpritsql/archive-client";

const client = createArchiveClient();
const [archive] = await client.warehouseConnections();
```

The default transport uses the browser's current origin and session. Pass a transport to handle a host's session expiry, localization or error model. Every read accepts an optional abort signal. A server must authorize each route for its authenticated user.

To build the package from a checkout, install the locked dependencies under `app/` and run `python3 dev/build-archive-client.py` from the repository root. The npm package contains the six route adapters and their TypeScript declarations.
