import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  base: "/static/dashboard/",
  build: {
    outDir: "../coordinator/static/dashboard",
    emptyOutDir: true,
    sourcemap: true,
  },
  server: {
    port: 3000,
    proxy: {
      "/api": "http://127.0.0.1:8000",
      "/tasks": "http://127.0.0.1:8000",
      "/deploy": "http://127.0.0.1:8000",
    },
  },
});
