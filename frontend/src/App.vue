<template>
  <router-view />
</template>

<script setup>
// Using Vue Router for page management
</script>

<style>
/* CSS Variable Palette */
:root {
  --c-bg: #ffffff;
  --c-surface: #ffffff;
  --c-surface-2: #f5f5f5;
  --c-surface-3: #f0f0f0;
  --c-border: #000000;
  --c-border-light: #e0e0e0;
  --c-text: #000000;
  --c-text-2: #333333;
  --c-text-muted: #666666;
  --c-accent: #ff6b00;
  --c-accent-hover: #e65c00;
  --c-inverse-bg: #000000;
  --c-inverse-text: #ffffff;
  --c-shadow: rgba(0,0,0,0.08);
}

html.dark {
  --c-bg: #0a0a0a;
  --c-surface: #111111;
  --c-surface-2: #1a1a1a;
  --c-surface-3: #222222;
  --c-border: #333333;
  --c-border-light: #2a2a2a;
  --c-text: #e8e8e8;
  --c-text-2: #b0b0b0;
  --c-text-muted: #666666;
  --c-accent: #ff6b00;
  --c-accent-hover: #ff8c00;
  --c-inverse-bg: #e8e8e8;
  --c-inverse-text: #0a0a0a;
  --c-shadow: rgba(0,0,0,0.4);
}

html, body {
  background-color: var(--c-bg);
  color: var(--c-text);
}

/* Global style reset */
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

#app {
  font-family: 'JetBrains Mono', 'Space Grotesk', 'Noto Sans SC', monospace;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  color: var(--c-text);
  background-color: var(--c-bg);
}

/* Scrollbar styles */
::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

::-webkit-scrollbar-track {
  background: var(--c-surface-2);
}

::-webkit-scrollbar-thumb {
  background: var(--c-border);
}

::-webkit-scrollbar-thumb:hover {
  background: var(--c-text-2);
}

html.dark ::-webkit-scrollbar-track {
  background: #111111;
}

html.dark ::-webkit-scrollbar-thumb {
  background: #333333;
}

html.dark ::-webkit-scrollbar-thumb:hover {
  background: #555555;
}

/* Global button styles */
button {
  font-family: inherit;
}
</style>
