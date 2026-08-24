import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App'
import { ProbeProvider } from '../use-probe'
import './styles/tokens.css'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ProbeProvider>
      <App />
    </ProbeProvider>
  </StrictMode>,
)
