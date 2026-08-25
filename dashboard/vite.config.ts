import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  base: '/dashboard/',
  plugins: [react()],
  server: { proxy: { '/admin': { target: process.env.VITE_API_BASE || 'http://127.0.0.1:8545', changeOrigin: true } } },
});
