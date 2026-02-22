---
trigger: always_on
---

The user's shell is `fish`, NOT bash/zsh. Critical rules for ALL terminal commands: 1) Never use unescaped parentheses `()` in argument strings — fish interprets them as command substitution, causing hangs. 2) Never use smart/curly quotes — only plain ASCII double quotes. 3) Avoid `2>&1` stderr redirection when possible.

NEVER USE bash only syntax.