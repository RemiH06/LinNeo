import { useEffect, useRef } from 'react'
import { useTheme } from '../theme/ThemeContext'

// Fondo oceanico de shui: gradiente + haz de luz + burbujas (claro) / bioluminiscencia (oscuro).
// Portado del tema shui. Va dentro del .shui-scope, detras del contenido.
const BIOLUM = ['#C040FF', '#FF40A0', '#00E8D8', '#40FF80', '#8040FF', '#FF6090']

export default function ShuiBackground() {
  const bgRef = useRef(null)
  const fxRef = useRef(null)
  const { dark } = useTheme()
  const darkRef = useRef(dark); darkRef.current = dark

  useEffect(() => {
    const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches
    const bg = bgRef.current, fx = fxRef.current
    const bgc = bg.getContext('2d'), fxc = fx.getContext('2d')
    let raf, bubbles = [], bio = [], W, H

    const resize = () => {
      W = bg.width = fx.width = window.innerWidth
      H = bg.height = fx.height = window.innerHeight
      drawBg()
    }
    function drawBg() {
      const g = bgc.createLinearGradient(0, 0, 0, H)
      if (darkRef.current) {
        g.addColorStop(0, '#010306'); g.addColorStop(0.4, '#020408'); g.addColorStop(1, '#010204')
      } else {
        g.addColorStop(0, '#40D8E8'); g.addColorStop(0.25, '#20A8C8'); g.addColorStop(0.6, '#0E6898'); g.addColorStop(1, '#083058')
      }
      bgc.fillStyle = g; bgc.fillRect(0, 0, W, H)
    }
    const makeBubble = () => ({ x: Math.random()*W, y: H+10, vy: -(0.4+Math.random()*0.8), vx: (Math.random()-0.5)*0.3, r: 2+Math.random()*6, wobble: Math.random()*Math.PI*2, wobbleSpeed: 0.02+Math.random()*0.03, alpha: 0.25+Math.random()*0.35 })
    const makeBio = () => ({ x: Math.random()*W, y: H+10+Math.random()*60, vy: -(0.15+Math.random()*0.35), vx: (Math.random()-0.5)*0.2, r: 1+Math.random()*2.5, col: BIOLUM[Math.floor(Math.random()*BIOLUM.length)], alpha: 0.4+Math.random()*0.5, flicker: Math.random()*Math.PI*2, flickerSpeed: 0.02+Math.random()*0.04 })

    function frame() {
      fxc.clearRect(0, 0, W, H)
      if (!darkRef.current) {
        // haz de luz desde esquina superior izquierda
        const grad = fxc.createLinearGradient(0, 0, W*0.75, H)
        grad.addColorStop(0, 'rgba(220,248,255,0.30)'); grad.addColorStop(0.25, 'rgba(180,238,252,0.18)')
        grad.addColorStop(0.6, 'rgba(140,220,248,0.08)'); grad.addColorStop(1, 'rgba(80,180,230,0)')
        fxc.save(); fxc.filter = 'blur(32px)'
        fxc.beginPath(); fxc.moveTo(-W*0.05, -H*0.05); fxc.lineTo(W*0.18, -H*0.05); fxc.lineTo(W*0.85, H*1.05); fxc.lineTo(W*0.05, H*1.05); fxc.closePath()
        fxc.fillStyle = grad; fxc.fill(); fxc.filter = 'none'; fxc.restore()
        // burbujas
        if (Math.random() < 0.06) bubbles.push(makeBubble())
        bubbles = bubbles.filter((b) => {
          b.wobble += b.wobbleSpeed; b.x += b.vx + Math.sin(b.wobble)*0.4; b.y += b.vy
          fxc.save()
          fxc.globalAlpha = b.alpha*0.3; fxc.beginPath(); fxc.arc(b.x, b.y, b.r*1.6, 0, Math.PI*2); fxc.strokeStyle = 'rgba(200,248,255,0.5)'; fxc.lineWidth = 0.8; fxc.stroke()
          fxc.globalAlpha = b.alpha; fxc.beginPath(); fxc.arc(b.x, b.y, b.r, 0, Math.PI*2); fxc.strokeStyle = 'rgba(220,252,255,0.8)'; fxc.lineWidth = 1; fxc.stroke()
          fxc.globalAlpha = b.alpha*0.6; fxc.beginPath(); fxc.arc(b.x-b.r*0.3, b.y-b.r*0.3, b.r*0.25, 0, Math.PI*2); fxc.fillStyle = 'rgba(255,255,255,0.7)'; fxc.fill()
          fxc.restore()
          return b.y > -20
        })
      } else {
        // particulas bioluminiscentes
        if (Math.random() < 0.08) bio.push(makeBio())
        bio = bio.filter((p) => {
          p.x += p.vx; p.y += p.vy; p.flicker += p.flickerSpeed
          const fa = p.alpha * (0.7 + Math.sin(p.flicker)*0.3)
          const halo = fxc.createRadialGradient(p.x, p.y, 0, p.x, p.y, p.r*5)
          halo.addColorStop(0, p.col+'CC'); halo.addColorStop(0.4, p.col+'44'); halo.addColorStop(1, p.col+'00')
          fxc.save()
          fxc.globalAlpha = fa*0.6; fxc.beginPath(); fxc.arc(p.x, p.y, p.r*5, 0, Math.PI*2); fxc.fillStyle = halo; fxc.fill()
          fxc.globalAlpha = fa; fxc.beginPath(); fxc.arc(p.x, p.y, p.r, 0, Math.PI*2); fxc.fillStyle = p.col; fxc.fill()
          fxc.restore()
          return p.y > -20
        })
      }
      raf = requestAnimationFrame(frame)
    }

    resize()
    window.addEventListener('resize', resize)
    if (!reduce) raf = requestAnimationFrame(frame)
    return () => { cancelAnimationFrame(raf); window.removeEventListener('resize', resize) }
  }, [dark])

  return (
    <>
      <canvas ref={bgRef} style={{ position: 'absolute', inset: 0, zIndex: 0, pointerEvents: 'none' }} />
      <canvas ref={fxRef} style={{ position: 'absolute', inset: 0, zIndex: 1, pointerEvents: 'none' }} />
    </>
  )
}