import { createContext, useContext, useState, useEffect } from 'react'

// Guarda el reino "activo" (el de la vista actual). Se refleja en body[data-kingdom]:
// de día pone el lavado de su pigmento, de noche vira el papel (ver systema.css).
const KingdomContext = createContext({ kingdom: null, setKingdom: () => {} })

export function KingdomProvider({ children }) {
  const [kingdom, setKingdom] = useState(null)
  useEffect(() => {
    if (kingdom) document.body.dataset.kingdom = kingdom
    else delete document.body.dataset.kingdom
  }, [kingdom])
  return (
    <KingdomContext.Provider value={{ kingdom, setKingdom }}>
      {children}
    </KingdomContext.Provider>
  )
}

export const useKingdom = () => useContext(KingdomContext)

// Hook para que una pagina declare su reino activo mientras esta montada.
export function useSetKingdom(kingdom) {
  const { setKingdom } = useKingdom()
  useEffect(() => {
    setKingdom(kingdom || null)
    return () => setKingdom(null)
  }, [kingdom, setKingdom])
}