import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  base: '/',
  plugins: [react()],
  define: {
    __APP_COMMIT_SHA__: JSON.stringify(process.env.APP_COMMIT_SHA || ''),
    __APP_BUILD_TIME__: JSON.stringify(process.env.APP_BUILD_TIME || ''),
    __APP_RELEASE__: JSON.stringify(process.env.APP_RELEASE || process.env.APP_RELEASE_ID || '')
  },
  build: {
    outDir: 'dist',
    sourcemap: false
  },
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: {
      '/api': 'http://127.0.0.1:3001'
    }
  }
});
