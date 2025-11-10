#!/bin/bash
for file in *.ipynb; do
  echo "Cleaning $file"
  sed -i 's/sk-ant-api03-[A-Za-z0-9_-]\+/"YOUR_ANTHROPIC_API_KEY_HERE"/g' "$file"
done
