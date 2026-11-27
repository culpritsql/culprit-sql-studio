#!/usr/bin/env python3
"""Build an installable ESM client against the checked-in Rust-generated archive model."""
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from ts_model_declarations import declaration_closure

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "packages/culprit-archive-client"
EXPORT = re.compile(r'export type \{ (\w+) \} from "../../../app/src/gen/(\w+)";')


def build(scratch: Path):
    published = scratch / "pkg"
    published.mkdir()
    exports = []
    for line in (PACKAGE / "src/types.ts").read_text().splitlines():
        match = EXPORT.fullmatch(line)
        if not match or match[1] != match[2]:
            raise ValueError(f"Unexpected public model export {line}")
        exports.append(match[1])
    declaration_closure(exports, published / "models", ROOT / "app/src/gen")
    (published / "types.d.ts").write_text("".join(
        f'export type {{ {name} }} from "./models/{name}.js";\n' for name in exports
    ))
    source = published / "index.ts"
    shutil.copyfile(PACKAGE / "src/index.ts", source)
    (published / "package.json").write_text('{"type":"module"}\n')
    subprocess.run(["node", ROOT / "app/node_modules/typescript/bin/tsc", "--ignoreConfig", "--strict",
                    "--target", "ES2022", "--module", "NodeNext", "--declaration", source],
                   cwd=scratch, check=True)
    source.unlink()
    (published / "package.json").unlink()
    output = PACKAGE / "pkg"
    if output.exists():
        shutil.rmtree(output)
    shutil.copytree(published, output)
    print(f"Built {len(exports)} exported models and {len(list((output / 'models').glob('*.d.ts')))} declarations")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="culprit-archive-client-") as temporary:
        build(Path(temporary))
