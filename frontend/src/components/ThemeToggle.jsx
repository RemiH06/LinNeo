import { useTheme } from '../theme/ThemeContext'

export default function ThemeToggle() {
  const { dark, toggle } = useTheme()
  return (
    <button className="theme-btn" onClick={toggle}>
      {dark ? '\u25D1 light' : '\u25D0 dark'}
    </button>
  )
}
