import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"

// Tauri 期望固定端口；移动端 dev 需监听 0.0.0.0 让设备/模拟器可访问。
const host = process.env.TAURI_DEV_HOST

export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  server: {
    port: 1420,
    strictPort: true,
    host: host || "0.0.0.0",
    hmr: host ? { protocol: "ws", host, port: 1421 } : undefined,
    watch: { ignored: ["**/src-tauri/**"] },
  },
  envPrefix: ["VITE_", "TAURI_ENV_"],
  build: {
    target: "es2021",
    minify: false,
    sourcemap: true,
  },
})
