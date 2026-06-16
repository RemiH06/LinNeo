import { createContext, useContext, useState, useEffect } from 'react'

// Guarda el reino "activo" (el de la vista actual) para que el fondo se tinte.
const KingdomContext = createContext({ kingdom: null, setKingdom: () => {} })

export function KingdomProvider({ children }) {
  const [kingdom, setKingdom] = useState(null)
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