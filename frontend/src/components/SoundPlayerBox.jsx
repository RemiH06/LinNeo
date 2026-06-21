import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'

/*
  SoundPlayerBox -- caja flotante en la esquina inferior derecha. Cada tab
  abierta es su PROPIO reproductor independiente (audio, play/pause,
  volumen, progreso, descarga) -- todas pueden sonar a la vez, no hay una
  sola tab "activa" que silencie a las demas. La caja crece hacia arriba
  con cada tab nueva, hasta un alto maximo con scroll interno.

  Props:
    tabs       - [{ id, name, image, kingdom, soundUrl, speciesKey }]
    onCloseTab(id)
*/
export default function SoundPlayerBox({ tabs, onCloseTab }) {
  if (tabs.length === 0) return null
  return (
    <div className="spb-box">
      <div className="spb-list">
        {tabs.map((t) => (
          <SoundCard key={t.id} tab={t} onClose={() => onCloseTab(t.id)} />
        ))}
      </div>
    </div>
  )
}

function SoundCard({ tab, onClose }) {
  const navigate = useNavigate()
  const audioRef = useRef(null)
  const [playing, setPlaying] = useState(true) // autoplay al abrir
  const [progress, setProgress] = useState(0) // 0-1
  const [duration, setDuration] = useState(0)
  const [volume, setVolume] = useState(1)

  // autoplay al montar (se abre la tarjeta -> empieza a sonar)
  useEffect(() => {
    const audio = audioRef.current
    if (!audio) return
    audio.volume = volume
    audio.play().then(() => setPlaying(true)).catch(() => setPlaying(false))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  function togglePlay() {
    const audio = audioRef.current
    if (!audio) return
    if (playing) { audio.pause(); setPlaying(false) }
    else { audio.play().then(() => setPlaying(true)).catch(() => {}) }
  }
  function onTimeUpdate() {
    const audio = audioRef.current
    if (!audio || !audio.duration) return
    setProgress(audio.currentTime / audio.duration)
  }
  function onLoadedMetadata() {
    const audio = audioRef.current
    if (audio) setDuration(audio.duration || 0)
  }
  function seekTo(e) {
    const audio = audioRef.current
    if (!audio || !duration) return
    const rect = e.currentTarget.getBoundingClientRect()
    const ratio = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width))
    audio.currentTime = ratio * duration
    setProgress(ratio)
  }
  function onVolumeChange(e) {
    const v = parseFloat(e.target.value)
    setVolume(v)
    if (audioRef.current) audioRef.current.volume = v
  }
  function downloadFile() {
    // descarga el archivo tal cual viene del backend (sin conversion de
    // formato); el nombre sugerido usa la extension real de la URL si la
    // tiene, o .audio como generico si no se puede determinar.
    const a = document.createElement('a')
    a.href = tab.soundUrl
    const rawExt = tab.soundUrl.split('?')[0].split('.').pop() || ''
    const ext = /^[a-zA-Z0-9]{2,4}$/.test(rawExt) ? rawExt : 'audio'
    a.download = `${tab.name.replace(/\s+/g, '_')}.${ext}`
    a.target = '_blank'
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
  }

  return (
    <div className="spb-card">
      <audio
        ref={audioRef}
        src={tab.soundUrl}
        onTimeUpdate={onTimeUpdate}
        onLoadedMetadata={onLoadedMetadata}
        onEnded={() => setPlaying(false)}
      />
      <div className={`spb-disc ${playing ? 'spinning' : ''}`} onClick={togglePlay}>
        {tab.image
          ? <img src={tab.image} alt={tab.name} />
          : <span className="spb-disc-icon">{'\u266A'}</span>}
        <span className="spb-disc-playbtn">{playing ? '\u23F8' : '\u25B6'}</span>
      </div>

      <div className="spb-info">
        <div className="spb-info-top">
          <div className="spb-info-name" title={tab.name}>{tab.name}</div>
          <button className="spb-close" onClick={onClose} title="Cerrar">{'\u00D7'}</button>
        </div>
        {tab.kingdom && <div className="spb-info-kingdom">{tab.kingdom}</div>}

        <div className="spb-progress" onClick={seekTo}>
          <div className="spb-progress-fill" style={{ width: `${progress * 100}%` }} />
        </div>

        <div className="spb-controls-row">
          <input
            className="spb-volume"
            type="range" min="0" max="1" step="0.01" value={volume}
            onChange={onVolumeChange}
            title="Volumen"
          />
          <button className="spb-detail-btn" onClick={() => navigate(`/species/${tab.speciesKey}`)}>
            Ficha {'\u2192'}
          </button>
          <button className="spb-download-btn" onClick={downloadFile} title="Descargar audio">
            {'\u2913'}
          </button>
        </div>
      </div>
    </div>
  )
}