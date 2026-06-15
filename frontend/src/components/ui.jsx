// Componentes UI del tema elixir, como envoltorios ligeros de las clases CSS.

export function Card({ children, ...props }) {
  return <div className="card" {...props}>{children}</div>
}

export function Badge({ children, variant = '' }) {
  return <span className={`badge ${variant}`}>{children}</span>
}

export function Metric({ value, label, variant = '' }) {
  return (
    <div className="metric">
      <div className={`metric-val ${variant}`}>{value}</div>
      <div className="metric-lbl">{label}</div>
    </div>
  )
}

export function Callout({ title, variant = '', children }) {
  return (
    <div className={`callout ${variant}`}>
      {title && <div className="callout-title">{title}</div>}
      <p>{children}</p>
    </div>
  )
}