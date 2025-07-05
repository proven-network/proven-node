import { defineConfig } from "vite";

// https://vitejs.dev/config/
export default defineConfig({
  build: {
    lib: {
      entry: "src/index.ts",
      name: "crypto",
    },
  },
  define: { "process.env.NODE_ENV": '"production"' },
});
