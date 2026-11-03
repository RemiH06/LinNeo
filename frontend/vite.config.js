import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    // 5135 (35 = carpeta) para no chocar con otros Vite en 5173; ver 48.SkillShot/PORTS.md
    port: 5135,
    strictPort: true,
    // Proxy a la API para evitar problemas de CORS en desarrollo
    proxy: {
      '/api': {
        target: 'http://localhost:8035',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ''),
      },
    },
  },
})