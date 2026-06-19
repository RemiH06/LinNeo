import { useEffect } from 'react'

/**
 * useKeyboardShortcuts — atajos globales de teclado para LinNeo.
 *
 * Todos los atajos usan Ctrl (o Cmd en Mac). Se ignoran cuando el foco
 * esta en un input/textarea/select para no interferir con la escritura.
 *
 * Atajos globales (todas las vistas):
 *   Ctrl+←        navegar atras en el historial
 *   Ctrl+→        navegar adelante en el historial
 *   Ctrl+H        ir al home (Shui)
 *   Ctrl+U        alternar tema claro/oscuro
 *
 * Atajos de Shui (pasados via `shui` object):
 *   Ctrl+F        enfocar el input de busqueda
 *   Ctrl+R        reiniciar el grafo
 *   Ctrl+D        re-tirar ejemplos (barra inferior)
 *   Ctrl+M        ir directamente al mapa (barra derecha)
 *   Ctrl+L        explorar como lista (si hay foco en un nodo)
 *   Ctrl+G        seleccionar todos los reinos
 *   Ctrl+J        deseleccionar todos los reinos
 *
 * Atajos de TaxonNode (pasados via `taxon` object):
 *   Ctrl+1..9     navegar al hijo N de la lista filtrada
 *
 * @param {object} handlers
 *   navigate     - funcion navigate de react-router
 *   toggleTheme  - funcion toggle del ThemeContext
 *   shui?        - { searchRef, resetAll, reloadExamples, openDrawer,
 *                    navigateToList, selectAllKingdoms, deselectAllKingdoms }
 *   taxon?       - { filteredChildren, openChild }
 */
export function useKeyboardShortcuts({ navigate, toggleTheme, shui, taxon }) {
  useEffect(() => {
    function handler(e) {
      // ignorar si el foco esta en un campo de texto
      const tag = document.activeElement?.tagName
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return

      const ctrl = e.ctrlKey || e.metaKey
      if (!ctrl) return

      const key = e.key

      // ── Globales ────────────────────────────────────────────────
      if (key === 'ArrowLeft') {
        e.preventDefault(); window.history.back(); return
      }
      if (key === 'ArrowRight') {
        e.preventDefault(); window.history.forward(); return
      }
      if (key === 'h' || key === 'H') {
        e.preventDefault(); navigate('/'); return
      }
      if (key === 'u' || key === 'U') {
        e.preventDefault(); toggleTheme?.(); return
      }

      // ── Shui ────────────────────────────────────────────────────
      if (shui) {
        if (key === 'f' || key === 'F') {
          e.preventDefault(); shui.searchRef?.current?.focus(); return
        }
        if (key === 'r' || key === 'R') {
          e.preventDefault(); shui.resetAll?.(); return
        }
        if (key === 'd' || key === 'D') {
          e.preventDefault(); shui.reloadExamples?.(); return
        }
        if (key === 'm' || key === 'M') {
          e.preventDefault(); shui.openDrawer?.(); return
        }
        if (key === 'l' || key === 'L') {
          e.preventDefault(); shui.navigateToList?.(); return
        }
        if (key === 'g' || key === 'G') {
          e.preventDefault(); shui.selectAllKingdoms?.(); return
        }
        if (key === 'j' || key === 'J') {
          e.preventDefault(); shui.deselectAllKingdoms?.(); return
        }
      }

      // ── TaxonNode ───────────────────────────────────────────────
      if (taxon) {
        const num = parseInt(key, 10)
        if (!isNaN(num) && num >= 1 && num <= 9) {
          e.preventDefault()
          const child = taxon.filteredChildren?.[num - 1]
          if (child) taxon.openChild(child)
        }
      }
    }

    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [navigate, toggleTheme, shui, taxon])
}