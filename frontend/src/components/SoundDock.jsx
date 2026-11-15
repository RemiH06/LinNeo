import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { thumb, webUrl } from '../api/media'

/*
  Atril de grabaciones: un reproductor independiente por especie (pueden sonar
  varias a la vez), fijo abajo. Controles de texto y barras nativas de posición
  y volumen, así funcionan con teclado, lector de pantalla y dedo.
  Props: tabs [{ id, name, commonName, image, soundUrl, speciesKey }], onClose(id).
*/
const clock = (s) => (Number.isFinite(s) ? `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, '0')}` : '–:––')

export default function SoundDock({ tabs, onClose }) {
  if (!tabs.length) return null
  return (
    <section className="dock" aria-label="Atril de grabaciones">
      <ul>{tabs.map((t) => <Recording key={t.id} tab={t} onClose={() => onClose(t.id)} />)}</ul>
    </section>
  )
}

function Recording({ tab, onClose }) {
  const audio = useRef(null)
  const [playing, setPlaying] = useState(false)
  const [time, setTime] = useState(0)
  const [duration, setDuration] = useState(NaN)
  const [volume, setVolume] = useState(1)
  const [failed, setFailed] = useState(false)

  useEffect(() => { audio.current?.play().catch(() => setPlaying(false)) }, [])   // se abre sonando
  const toggle = () => (playing ? audio.current.pause() : audio.current.play().catch(() => {}))

  return (
    <li className="rec">
      <audio ref={audio} src={tab.soundUrl} preload="metadata"
        onPlay={() => setPlaying(true)} onPause={() => setPlaying(false)} onEnded={() => setPlaying(false)}
        onTimeUpdate={(e) => setTime(e.currentTarget.currentTime)}
        onLoadedMetadata={(e) => setDuration(e.currentTarget.duration)}
        onDurationChange={(e) => setDuration(e.currentTarget.duration)}   // los WAV sin longitud la dan después
        onError={() => setFailed(true)} />
      <span className="rec-plate">{tab.image && <img src={thumb(tab.image, 120)} alt="" />}</span>
      <div className="rec-body">
        <p className="rec-title">
          <Link className="binomial" to={`/species/${tab.speciesKey}`}>{tab.name}</Link>
          {tab.commonName && <span className="entry-note">{tab.commonName}</span>}
        </p>
        {failed ? <p className="margin-note">No se pudo cargar la grabación.</p> : (
          <div className="rec-controls">
            <button type="button" className="ctl rec-toggle" onClick={toggle}>{playing ? 'Pausar' : 'Reproducir'}</button>
            <input type="range" min="0" max={Number.isFinite(duration) ? duration : 0} step="0.1" value={time}
              aria-label={`Posición en la grabación de ${tab.name}`}
              onChange={(e) => { audio.current.currentTime = Number(e.target.value) }} />
            <span className="rec-time">{clock(time)} / {clock(duration)}</span>
            <label className="rec-vol">
              <span className="sr-only">Volumen de {tab.name}</span>
              <input type="range" min="0" max="1" step="0.05" value={volume}
                onChange={(e) => { const v = Number(e.target.value); setVolume(v); audio.current.volume = v }} />
            </label>
          </div>
        )}
      </div>
      <div className="rec-actions">
        <a className="ctl" href={webUrl(tab.soundUrl) ?? undefined} target="_blank" rel="noreferrer" download>Descargar</a>
        <button type="button" className="ctl" onClick={onClose}>Cerrar</button>
      </div>
    </li>
  )
}
