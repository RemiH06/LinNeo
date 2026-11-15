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
        <div className="folio" role="alert">
          <h1 className="folio-h2 taxon-error">Algo falló al mostrar esta página</h1>
          <pre className="error-detail">{String(this.state.error?.message || this.state.error)}</pre>
          <button type="button" className="ctl" onClick={() => this.setState({ error: null })}>Reintentar</button>
          {' · '}<a href="/">Volver al inicio</a>
        </div>
      )
    }
    return this.props.children
  }
}