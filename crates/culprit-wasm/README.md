# culprit-wasm

A small WebAssembly bridge to Culprit’s portable plan operations.

## Purpose

Lets browser projects parse plans and deadlocks, compare plans and export a plan without database drivers or the desktop application.

## Ownership

`culprit-plans` owns the behavior. `culprit-core` owns the models. This crate converts the existing JSON representation at the WebAssembly boundary; it contains no duplicate parsers or models.

## Browser package

The typed JavaScript API and build instructions are in [`packages/culprit-wasm`](../../packages/culprit-wasm/README.md). The build measures the actual binary and compressed sizes.

Run `python3 dev/licenses.py` from the repository root before the package build. The existing generator supplies the third-party notices bundled with the module.

English is embedded by default. Enable `locales` to include the other catalogs.
