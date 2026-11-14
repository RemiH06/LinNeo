import { useTheme } from '../theme/ThemeContext'

// Dice a qué modo cambia, con texto: folio (día) o cianotipo (noche).
export default function ThemeToggle() {
  const { dark, toggle } = useTheme()
  return (
    <button type="button" className="ctl theme-toggle" onClick={toggle}>
      {dark ? 'Modo día' : 'Modo noche'}
    </button>
  )
}
