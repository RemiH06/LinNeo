import { useTheme } from '../theme/ThemeContext'
import { useKingdom } from '../theme/KingdomContext'
import { KINGDOM_HUE } from '../theme/kingdomColor'

// Velo de fondo MUY tenue tenido del color del reino activo.
// Va detras del ElixirGraph; transicion suave al cambiar de reino.
export default function KingdomBackdrop() {
  const { dark } = useTheme()
  const { kingdom } = useKingdom()

  let bg = 'transparent'
  if (kingdom) {
    const h = KINGDOM_HUE[kingdom]
    const gray = (kingdom === 'incertae sedis' || h == null)
    const hue = gray ? 210 : h
    const sat = gray ? 8 : (dark ? 60 : 45)
    const light = dark ? 8 : 94
    const alpha = dark ? 0.5 : 0.45
    // gradiente radial suave desde el centro-superior
    bg = `radial-gradient(circle at 50% 0%, hsla(${hue}, ${sat}%, ${light}%, ${alpha}) 0%, transparent 70%)`
  }

  return (
    <div
      aria-hidden
      style={{
        position: 'fixed', inset: 0, zIndex: 0, pointerEvents: 'none',
        background: bg, transition: 'background 0.6s ease',
      }}
    />
  )
}