#!/usr/bin/env bash
# Package addin/ as a double-clickable JMP add-in.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p dist
rm -f dist/DatabricksConnector.jmpaddin
(cd addin && zip -q -r ../dist/DatabricksConnector.jmpaddin . -x '__pycache__/*' '*.pyc')
echo "Built dist/DatabricksConnector.jmpaddin"
