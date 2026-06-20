import { useEffect, useState, useRef } from 'react'

/*
  LoadingSpinner -- animacion de carga compartida, un solo diseno en toda la
  app. Usa var(--accent), que cada tema (shui/bookworm, y kingdomStyleVars
  cuando hay un reino activo) ya sobreescribe, asi se tine solo sin logica
  condicional por tema.

  Ademas del spinner: una frase aleatoria ("Cargando nodos...", etc.) que
  rota cada ~1.2s mientras dura la carga (es decoracion, no refleja progreso
  real), y una barra de progreso SIMULADA que avanza rapido al inicio y se
  desacelera sin llegar nunca al 100% (no hay progreso real medible en
  llamadas de red simples; al desmontarse el componente, el ciclo se detiene
  solo, sin necesidad de que el padre indique "termine").

  Props:
    label    - prefijo fijo opcional antes de la frase rotativa (ej. "Cargando")
    timeText - texto del cronometro ya formateado (ej. "1.234s"), se muestra
               al final de la linea si se pasa
    size     - diametro del spinner en px (default 18)
    inline   - true: en fila junto al texto (default). false: bloque
               centrado, con barra de progreso debajo (para pantallas
               completas / overlays grandes)
    showBar  - si true (default cuando inline=false) muestra la barra de
               progreso simulada debajo del texto
*/
const LOADING_PHRASES = [
  'Cargando nodos...',
  'Consultando especies...',
  'Recorriendo taxones...',
  'Sincronizando el grafo...',
  'Buscando relaciones...',
  'Cargando reinos...',
  'Trazando ramas...',
  'Consultando el backbone...',
  'Cargando fronteras...',
  'Reuniendo datos...',
]

function useRotatingPhrase(intervalMs = 1200) {
  const [phrase, setPhrase] = useState(() => LOADING_PHRASES[Math.floor(Math.random() * LOADING_PHRASES.length)])
  useEffect(() => {
    const id = setInterval(() => {
      setPhrase((prev) => {
        let next = prev
        // evita repetir la misma frase dos veces seguidas
        while (next === prev) next = LOADING_PHRASES[Math.floor(Math.random() * LOADING_PHRASES.length)]
        return next
      })
    }, intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])
  return phrase
}

// Progreso simulado: avanza rapido al inicio, se desacelera asintoticamente
// hacia un techo (92%) sin llegar nunca al 100% mientras siga montado.
function useSimulatedProgress(ceiling = 92) {
  const [pct, setPct] = useState(0)
  const rafRef = useRef(null)
  const startRef = useRef(null)
  useEffect(() => {
    startRef.current = performance.now()
    function tick(now) {
      const elapsed = now - startRef.current
      // curva de desaceleracion: se acerca al techo sin tocarlo (asintota)
      const next = ceiling * (1 - Math.exp(-elapsed / 1400))
      setPct(next)
      rafRef.current = requestAnimationFrame(tick)
    }
    rafRef.current = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(rafRef.current)
  }, [ceiling])
  return pct
}

export default function LoadingSpinner({ label = '', timeText = null, size = 18, inline = true, showBar = null }) {
  const phrase = useRotatingPhrase(1200)
  const pct = useSimulatedProgress()
  const text = [label || null, phrase, timeText].filter(Boolean).join(' ')
  const displayBar = showBar != null ? showBar : !inline

  const spinner = (
    <svg
      className="ln-spinner"
      width={size} height={size} viewBox="0 0 50 50"
      style={{ flexShrink: 0 }}
    >
      <circle
        className="ln-spinner-track"
        cx="25" cy="25" r="20"
        fill="none" strokeWidth="5"
      />
      <circle
        className="ln-spinner-arc"
        cx="25" cy="25" r="20"
        fill="none" strokeWidth="5"
        strokeDasharray="80 200"
        strokeLinecap="round"
      />
    </svg>
  )

  const bar = displayBar && (
    <div className="ln-progress-wrap">
      <div className="ln-progress-bar">
        <div className="ln-progress-fill" style={{ width: `${pct}%` }} />
      </div>
      <span className="ln-progress-pct">{Math.round(pct)}%</span>
    </div>
  )

  if (!inline) {
    return (
      <div className="ln-loading-block">
        {spinner}
        {text && <span className="ln-loading-label">{text}</span>}
        {bar}
      </div>
    )
  }

  return (
    <span className="ln-loading-inline">
      {spinner}
      {text && <span className="ln-loading-label">{text}</span>}
      {bar}
    </span>
  )
}