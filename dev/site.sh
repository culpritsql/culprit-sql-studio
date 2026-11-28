#!/usr/bin/env bash
# Assemble culpritsql.com into _site/: 9-locale platform pages, database provider guides, GitHub release notes,
# Markdown docs for llms.txt / llms-full.txt, versioned assets, enforced CSP, and SEO verification.
#
#   dev/site.sh          # build and verify _site/
#   dev/site.sh serve    # build, verify, and serve on http://localhost:4321
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p _site
find _site -mindepth 1 -maxdepth 1 -exec rm -rf {} +
mkdir -p _site/docs
cp -R site/. _site/

lastmod=$(git log -1 --format=%cs -- site docs README.md CHANGELOG.md 2>/dev/null || date -u +%F)

python3 dev/site-build.py "--lastmod=${lastmod:-$(date -u +%F)}"

BLOG_DIST="${BLOG_DIST:-}"
if [[ -z "$BLOG_DIST" && -d "../culprit-blog/dist" ]]; then
  BLOG_DIST="../culprit-blog/dist"
fi
if [[ -n "$BLOG_DIST" && -d "$BLOG_DIST" ]]; then
  mkdir -p _site/blog
  cp -R "$BLOG_DIST"/. _site/blog/
  python3 dev/site-build.py --merge-blog
fi

python3 dev/site-verify.py

echo "built _site/ ($(du -sh _site | cut -f1))"
if [[ "${1:-}" == "serve" ]]; then
  python3 -m http.server 4321 --directory _site
fi
