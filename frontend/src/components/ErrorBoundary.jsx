import { Component } from 'react'

// Evita que un fallo de render deje la pantalla en blanco; muestra el error.
export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }
  static getDerivedStateFromError(error) {
    return { error }
  }
  componentDidCatch(error, info) {
    console.error('Error de render:', error, info)
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 20, border: '1px solid #F04050', borderRadius: 8, background: 'var(--bg2)', color: 'var(--text)', margin: 20 }}>
          <h3 style={{ color: '#F04050', marginTop: 0 }}>Algo fallo al renderizar</h3>
          <pre style={{ whiteSpace: 'pre-wrap', fontSize: 12, color: 'var(--text2)' }}>{String(this.state.error?.message || this.state.error)}</pre>
          <button className="btn" onClick={() => this.setState({ error: null })}>Reintentar</button>
        </div>
      )
    }
    return this.props.children
  }
}