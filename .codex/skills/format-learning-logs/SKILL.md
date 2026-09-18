---
name: format-learning-logs
description: Convert this project's plain-text learning logs to readable GitHub Markdown while preserving every word exactly. Use when formatting files in the learning-log directory, not when changing their content.
---

# Format Learning Logs

Format the project's learning logs without changing their authored content.

## Scope

This project currently stores its logs in `learning-logs/`. Treat a request that says `learning_logs/` as referring to that directory unless the requester identifies a different directory. Inspect the existing log files before making changes. Do not modify files outside that directory.

## Content preservation

- Do not rewrite, rephrase, correct, summarize, improve, add, or remove content.
- Preserve wording, spelling, grammar, sentence structure, explanations, terminology, and original order exactly as written.
- Do not generate new learning content.

## Formatting and file changes

- Change only formatting needed to make each log easier to read as Markdown on GitHub.
- Convert appropriate existing lines into `#`, `##`, or `###` headings.
- Place terminal commands in fenced code blocks, Python code in fenced `python` code blocks, and diagrams or plain-text architecture drawings in fenced `text` code blocks.
- Use bullet or numbered lists only when the original text is already clearly intended as a list.
- Use inline code for commands, filenames, ports, topic names, variables, and similar technical terms, without changing their words.
- Rename each converted `.txt` file to `.md` after formatting it.
