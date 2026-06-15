import { useEffect, useRef } from 'react'
import { useTheme } from '../theme/ThemeContext'

// Grafo neural animado de fondo, portado del demo elixir.
const PALETTES = {
  light: { nodes: ['#0A5C58','#2A3A7A','#1A5C30','#3A2A6A','#1A4A6A','#3A4A6A'], edge: '#B8BDD0', pulse: ['#0A5C58','#2A3A7A','#1A5C30','#3A2A6A','#1A4A6A'] },
  dark:  { nodes: ['#00E8D8','#4D8FFF','#00F090','#8060FF','#00C8FF','#7090C0'], edge: '#2A3A5A', pulse: ['#00E8D8','#4D8FFF','#00F090','#8060FF','#00C8FF'] },
}
const N = 40, DIST = 150, SPEED = 0.25

export default function ElixirGraph() {
  const canvasRef = useRef(null)
  const { dark } = useTheme()
  const darkRef = useRef(dark)
  darkRef.current = dark

  useEffect(() => {
    const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    if (reduce) return

    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    let W, H, nodes = [], raf, last = 0, pulseTimer

    const pal = () => (darkRef.current ? PALETTES.dark : PALETTES.light)
    const rnd = (arr) => arr[Math.floor(Math.random() * arr.length)]
    const resize = () => { W = canvas.width = window.innerWidth; H = canvas.height = window.innerHeight }
    const makeNode = () => ({
      x: Math.random() * W, y: Math.random() * H,
      vx: (Math.random() - .5) * SPEED, vy: (Math.random() - .5) * SPEED,
      r: 3.5 + Math.random() * 3, color: rnd(pal().nodes),
      pColor: null, pT: 0, pDur: 0,
    })
    const spawnPulse = () => {
      const n = nodes[Math.floor(Math.random() * nodes.length)]
      if (n) { n.pColor = rnd(pal().pulse); n.pT = 0; n.pDur = 600 + Math.random() * 900 }
      pulseTimer = setTimeout(spawnPulse, 250 + Math.random() * 900)
    }
    const draw = (ts) => {
      const dt = Math.min(ts - last, 50); last = ts
      ctx.clearRect(0, 0, W, H)
      const p = pal()
      for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const a = nodes[i], b = nodes[j]
          const dx = a.x - b.x, dy = a.y - b.y
          const d = Math.sqrt(dx*dx + dy*dy)
          if (d < DIST) {
            ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y)
            ctx.strokeStyle = p.edge; ctx.globalAlpha = (1 - d/DIST); ctx.lineWidth = 1.5; ctx.stroke()
          }
        }
      }
      for (const n of nodes) {
        n.x += n.vx; n.y += n.vy
        if (n.x < 0 || n.x > W) { n.vx *= -1; n.x = Math.max(0, Math.min(W, n.x)) }
        if (n.y < 0 || n.y > H) { n.vy *= -1; n.y = Math.max(0, Math.min(H, n.y)) }
        let color = n.color, r = n.r
        if (n.pColor && n.pDur > 0) {
          n.pT += dt
          const t = Math.min(n.pT / n.pDur, 1)
          const ease = t < .5 ? 2*t*t : -1 + (4 - 2*t)*t
          r = n.r + ease * 6; color = n.pColor
          ctx.globalAlpha = .5 + ease * .5
          if (n.pT >= n.pDur) { n.color = n.pColor; n.pColor = null; n.pDur = 0 }
        } else { ctx.globalAlpha = .92 }
        ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI*2); ctx.fillStyle = color; ctx.fill()
      }
      ctx.globalAlpha = 1
      raf = requestAnimationFrame(draw)
    }

    resize()
    nodes = Array.from({ length: N }, makeNode)
    pulseTimer = setTimeout(spawnPulse, 400)
    raf = requestAnimationFrame(draw)
    window.addEventListener('resize', resize)
    return () => { cancelAnimationFrame(raf); clearTimeout(pulseTimer); window.removeEventListener('resize', resize) }
  }, [])

  return <canvas id="elixir-graph" ref={canvasRef} />
}