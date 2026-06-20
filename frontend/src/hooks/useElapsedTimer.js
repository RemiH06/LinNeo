import { useState, useRef, useCallback, useEffect } from 'react'

/*
  useElapsedTimer -- cronometro de carga reusable (ms en vivo mientras corre,
  se congela al llamar stop()). Mismo patron que ya se uso a mano en
  SearchResults.jsx, extraido aqui para reusar en todas las vistas con carga
  (TaxonNode, SpeciesDetail, MapExplorer, MapCountry, carga inicial de Shui).

  Uso:
    const { elapsedMs, start, stop } = useElapsedTimer()
    useEffect(() => {
      start()
      api.algo().then(() => stop()).catch(() => stop())
    }, [dep])
    ...
    {elapsedMs != null && <span>{formatElapsed(elapsedMs)}</span>}
*/
export function useElapsedTimer() {
  const [elapsedMs, setElapsedMs] = useState(null)
  const startRef = useRef(null)
  const tickRef = useRef(null)
  const aliveRef = useRef(true)

  const start = useCallback(() => {
    aliveRef.current = true
    setElapsedMs(0)
    startRef.current = performance.now()
    clearInterval(tickRef.current)
    tickRef.current = setInterval(() => {
      if (aliveRef.current) setElapsedMs(performance.now() - startRef.current)
    }, 47) // refresco visual, no afecta el tiempo real medido
  }, [])

  const stop = useCallback(() => {
    clearInterval(tickRef.current)
    if (startRef.current != null) setElapsedMs(performance.now() - startRef.current)
  }, [])

  const reset = useCallback(() => {
    clearInterval(tickRef.current)
    startRef.current = null
    setElapsedMs(null)
  }, [])

  // cleanup al desmontar el componente que usa el hook
  useEffect(() => {
    return () => { aliveRef.current = false; clearInterval(tickRef.current) }
  }, [])

  return { elapsedMs, start, stop, reset }
}

// Formato compartido "S.mmm s", igual al que ya se ve en SearchResults.
export function formatElapsed(ms) {
  if (ms == null) return null
  const s = Math.floor(ms / 1000)
  const rest = Math.round(ms % 1000)
  return `${s}.${String(rest).padStart(3, '0')}s`
}