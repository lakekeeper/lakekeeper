#!/usr/bin/env bash
#
# Regenerate the Open Graph / Twitter card image referenced by
# `overrides/main.html` (docs/assets/img/og-banner.png, 1200x630).
#
# The card is a committed build artifact rather than a generated one: it changes
# roughly never, and generating it at build time would put librsvg in the docs
# toolchain for a single static file.
#
# Requires librsvg: `brew install librsvg`.

set -euo pipefail

cd "$(dirname "$0")/.."

command -v rsvg-convert >/dev/null || {
  echo "rsvg-convert not found — run: brew install librsvg" >&2
  exit 127
}

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

# The wordmark is vector paths, so it renders without needing the brand fonts
# installed. The taglines below fall back to a system sans.
rsvg-convert -w 760 docs/assets/logos/LAKEKEEPER_IMAGE_TEXT_WHITE_SIDE.svg \
  -o "${WORK}/logo.png"

cat > "${WORK}/og.svg" <<'EOF'
<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="1200" height="630" viewBox="0 0 1200 630">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#0B2440"/>
      <stop offset="55%" stop-color="#091F34"/>
      <stop offset="100%" stop-color="#13314F"/>
    </linearGradient>
  </defs>
  <rect width="1200" height="630" fill="url(#bg)"/>
  <rect x="0" y="0" width="1200" height="6" fill="#BCD4EB"/>
  <image xlink:href="logo.png" x="220" y="168" width="760" height="130"/>
  <text x="600" y="404" text-anchor="middle" font-family="Helvetica Neue, Helvetica, Arial, sans-serif" font-size="54" font-weight="600" fill="#FFFFFF" letter-spacing="0.5">Apache Iceberg REST Catalog</text>
  <text x="600" y="470" text-anchor="middle" font-family="Helvetica Neue, Helvetica, Arial, sans-serif" font-size="30" font-weight="400" fill="#8DA5BA" letter-spacing="1.6">OPEN SOURCE  ·  WRITTEN IN RUST</text>
  <text x="600" y="556" text-anchor="middle" font-family="Helvetica Neue, Helvetica, Arial, sans-serif" font-size="25" font-weight="500" fill="#BCD4EB" letter-spacing="0.8">docs.lakekeeper.io</text>
</svg>
EOF

rsvg-convert -w 1200 -h 630 "${WORK}/og.svg" -o docs/assets/img/og-banner.png

echo "wrote docs/assets/img/og-banner.png"
