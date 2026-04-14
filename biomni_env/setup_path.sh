#!/bin/bash
# Added by biomni setup
# Remove any old paths first to avoid duplicates
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
if [ -n "${BIOMNI_TOOLS_DIR:-}" ] && [ -d "$BIOMNI_TOOLS_DIR/bin" ]; then
	TOOLS_DIR="$BIOMNI_TOOLS_DIR"
elif [ -d "$SCRIPT_DIR/biomni_tools/bin" ]; then
	TOOLS_DIR="$SCRIPT_DIR/biomni_tools"
elif [ -d "$SCRIPT_DIR/\`/bin" ]; then
	TOOLS_DIR="$SCRIPT_DIR/\`"
else
	TOOLS_DIR="$SCRIPT_DIR"
fi

PATH=$(echo "$PATH" | tr ':' '\n' | grep -vF "$TOOLS_DIR/bin" | tr '\n' ':' | sed 's/:$//')
export PATH="$TOOLS_DIR/bin:$PATH"
