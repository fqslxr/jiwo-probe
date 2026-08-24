import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import MapApp from './MapApp'
import { ProbeProvider } from '../use-probe'
import './styles/tokens.css'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ProbeProvider>
      <MapApp />
    </ProbeProvider>
  </StrictMode>,
)
