import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { uiCatalogs } from "./build/catalogs.ts";

const host = process.env.TAURI_DEV_HOST;

const appRoot = fileURLToPath(new URL(".", import.meta.url)).replace(/\/$/, "");

export default defineConfig({
  plugins: [uiCatalogs(appRoot), react()],
  clearScreen: false,
  // CodeMirror breaks when two copies of its core load (instanceof checks); keep one of each.
  // Alias shared UI runtime packages so workspace sources under ../packages/ resolve app/node_modules cleanly.
  resolve: {
    dedupe: ["react", "react-dom", "@codemirror/state", "@codemirror/view", "@codemirror/language", "@codemirror/autocomplete"],
    alias: {
      "lucide-react": `${appRoot}/node_modules/lucide-react`,
      "react/jsx-runtime": `${appRoot}/node_modules/react/jsx-runtime.js`,
      "react/jsx-dev-runtime": `${appRoot}/node_modules/react/jsx-dev-runtime.js`,
      react: `${appRoot}/node_modules/react`,
    },
  },
  optimizeDeps: { include: ["codemirror", "@codemirror/state", "@codemirror/view", "@codemirror/lang-sql", "@codemirror/autocomplete"] },
  server: {
    port: 1420,
    strictPort: true,
    host: host || false,
    hmr: host ? { protocol: "ws", host, port: 1421 } : undefined,
    watch: { ignored: ["**/src-tauri/**"] },
    fs: { allow: [".."] },
  },
  envPrefix: ["VITE_", "TAURI_ENV_"],
  build: {
    target: "safari16",
    minify: !process.env.TAURI_ENV_DEBUG,
    sourcemap: !!process.env.TAURI_ENV_DEBUG,
    cssMinify: true,
  },
});
