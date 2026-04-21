#!/bin/bash
echo "Testing if tools are in the PATH..."
echo "Current PATH: $PATH"
echo ""
echo "Looking for tools in: `/bin"
ls -la "`/bin"
echo ""
echo "Checking for path caching issues..."
for tool in $(ls "`/bin"); do
  which $tool 2>/dev/null | grep -q "/afs/cs.stanford.edu" && {
    echo "WARNING: $tool is still pointing to the old AFS location!"
    echo "Run 'hash -r' (bash) or 'rehash' (zsh) to clear the command cache."
    break
  }
done
echo ""
for tool in $(ls "`/bin"); do
  if command -v $tool &> /dev/null; then
    echo "$tool: $(which $tool)"
  else
    echo "$tool: NOT FOUND IN PATH"
  fi
done
