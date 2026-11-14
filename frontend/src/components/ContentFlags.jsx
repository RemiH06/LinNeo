// Sellos de contenido disponible para un hijo (especie o taxon superior).
// Para especie: cuenta su propio contenido. Para taxon: cuantas especies descendientes lo tienen.
export default function ContentFlags({ flags, isSpecies, conservation, commonNames }) {
  if (!flags) return null
  const items = []
  const Seal = ({ title, icon, n, color }) => (
    <span title={title} style={{
      display: 'inline-flex', alignItems: 'center', gap: 3,
      fontFamily: 'var(--bw-mono)', fontSize: 10, padding: '1px 6px',
      borderRadius: 10, border: `1px solid ${color}`, color,
      background: 'var(--bw-bg3)', lineHeight: 1.6,
    }}>
      <span>{icon}</span>{n != null && n > 0 ? <span>{n}</span> : null}
    </span>
  )

  if (isSpecies) {
    if (flags.images > 0) items.push(<Seal key="i" title="Imagenes" icon={'\u25A3'} n={flags.images} color="var(--bw-moss)" />)
    if (flags.sounds > 0) items.push(<Seal key="s" title="Sonidos" icon={'\u266A'} n={flags.sounds} color="var(--bw-amber)" />)
    if (flags.descriptions > 0) items.push(<Seal key="d" title="Descripciones" icon={'\u2261'} n={flags.descriptions} color="var(--bw-bark)" />)
    if (flags.etymology > 0) items.push(<Seal key="e" title="Etimologia" icon={'\u00A7'} color="var(--bw-ochre)" />)
    if (commonNames?.length > 0) items.push(<Seal key="cn" title={commonNames.join(', ')} icon={'\u2605'} color="var(--bw-ochre)" />)
    if (conservation) items.push(<Seal key="c" title={`Conservacion ${conservation}`} icon={conservation} color="var(--bw-danger)" />)
  } else {
    // taxon superior: cuantas especies descendientes con cada contenido
    if (flags.species > 0) items.push(<Seal key="sp" title="Especies descendientes" icon={'\u273F'} n={flags.species} color="var(--bw-accent)" />)
    if (flags.images > 0) items.push(<Seal key="i" title="Especies con imagenes" icon={'\u25A3'} n={flags.images} color="var(--bw-moss)" />)
    if (flags.sounds > 0) items.push(<Seal key="s" title="Especies con sonidos" icon={'\u266A'} n={flags.sounds} color="var(--bw-amber)" />)
    if (flags.descriptions > 0) items.push(<Seal key="d" title="Especies con descripcion" icon={'\u2261'} n={flags.descriptions} color="var(--bw-bark)" />)
    if (flags.common_names > 0) items.push(<Seal key="cn" title="Especies con nombre comun" icon={'\u2605'} n={flags.common_names} color="var(--bw-ochre)" />)
  }
  if (!items.length) return null
  return <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginTop: 6 }}>{items}</div>
}
