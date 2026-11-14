// OKLCH -> sRGB hex con recorte de croma hasta caber en gamut
const toLin=(L,a,b)=>{const l=(L+.3963377774*a+.2158037573*b)**3,m=(L-.1055613458*a-.0638541728*b)**3,s=(L-.0894841775*a-1.291485548*b)**3;
 return[4.0767416621*l-3.3077115913*m+.2309699292*s,-1.2684380046*l+2.6097574011*m-.3413193965*s,-.0041960863*l-.7034186147*m+1.707614701*s]};
const gam=v=>v<=.0031308?12.92*v:1.055*v**(1/2.4)-.055;
function oklch(L,C,h){for(let c=C;c>=0;c-=.002){const r=h*Math.PI/180,lin=toLin(L,c*Math.cos(r),c*Math.sin(r));if(lin.every(x=>x>=-1e-4&&x<=1+1e-4))return'#'+lin.map(x=>Math.round(Math.min(1,Math.max(0,gam(x)))*255).toString(16).padStart(2,'0')).join('').toUpperCase()}}
const lum=h=>{const c=[1,3,5].map(i=>parseInt(h.slice(i,i+2),16)/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4);return .2126*c[0]+.7152*c[1]+.0722*c[2]};
const cr=(a,b)=>{const[x,y]=[lum(a),lum(b)].sort((p,q)=>q-p);return(x+.05)/(y+.05)};
// sRGB hex -> OKLab para distancias
function lab(h){const c=[1,3,5].map(i=>parseInt(h.slice(i,i+2),16)/255).map(v=>v<=.04045?v/12.92:((v+.055)/1.055)**2.4);
 const l=Math.cbrt(.4122214708*c[0]+.5363325363*c[1]+.0514459929*c[2]),m=Math.cbrt(.2119034982*c[0]+.6806995451*c[1]+.1073969566*c[2]),s=Math.cbrt(.0883024619*c[0]+.2817188376*c[1]+.6299787005*c[2]);
 return[.2104542553*l+.793617785*m-.0040720468*s,1.9779984951*l-2.428592205*m+.4505937099*s,.0259040371*l+.7827717662*m-.808675766*s]}
const dE=(a,b)=>{const[x,y]=[lab(a),lab(b)];return Math.hypot(x[0]-y[0],x[1]-y[1],x[2]-y[2])};

const DAY={bg:'#EDEAE3',text:'#1E1B18',text2:'#4E4840',ink:'#1F3F6B'};
const NIGHT_BASE=oklch(.235,.058,258), NIGHT={text:'#E4EAF0',text2:'#A9BCD3',acc:'#EADCB4'};
const K=[['Animalia','bermellón',42],['Archaea','ocre',83],['Plantae','verde savia',133],['Chromista','verdigrís',175],
 ['Viruses','esmalte',235],['Bacteria','violeta de genciana',280],['Fungi','orchilla',321],['Protozoa','carmín',1],['incertae sedis','gris de Payne',255,true]];
const rows=K.map(([k,p,h,gray])=>{const g=gray?.25:1;return{k,p,h,
 wash:oklch(.905,.07*g,h), dink:oklch(.47,.14*g,h), nbg:oklch(.235,.05*g,h), nink:oklch(.76,.14*g,h)}});
console.log('noche base (cianotipo):',NIGHT_BASE,'text',cr(NIGHT.text,NIGHT_BASE).toFixed(1),'text2',cr(NIGHT.text2,NIGHT_BASE).toFixed(1),'acc',cr(NIGHT.acc,NIGHT_BASE).toFixed(1));
console.log('reino'.padEnd(15),'wash    texto/wash text2/wash | dink  dink/bg dink/wash | nbg    texto/nbg text2/nbg acc/nbg | nink  nink/nbg nink/base');
let min={};const upd=(n,v)=>min[n]=Math.min(min[n]??99,v);
for(const r of rows){const c=[cr(DAY.text,r.wash),cr(DAY.text2,r.wash),cr(r.dink,DAY.bg),cr(r.dink,r.wash),cr(NIGHT.text,r.nbg),cr(NIGHT.text2,r.nbg),cr(NIGHT.acc,r.nbg),cr(r.nink,r.nbg),cr(r.nink,NIGHT_BASE)];
 ['t/wash','t2/wash','dink/bg','dink/wash','t/nbg','t2/nbg','acc/nbg','nink/nbg','nink/base'].forEach((n,i)=>upd(n,c[i]));
 console.log(r.k.padEnd(15),r.wash,c[0].toFixed(1),c[1].toFixed(1),'|',r.dink,c[2].toFixed(1),c[3].toFixed(1),'|',r.nbg,c[4].toFixed(1),c[5].toFixed(1),c[6].toFixed(1),'|',r.nink,c[7].toFixed(1),c[8].toFixed(1))}
console.log('mínimos:',Object.entries(min).map(([k,v])=>k+' '+v.toFixed(2)).join('  '));
// distinguibilidad: distancia OKLab mínima entre pares de reinos (sin incertae)
for(const f of ['dink','nink','wash']){let m=[9,''];for(let i=0;i<8;i++)for(let j=i+1;j<8;j++){const d=dE(rows[i][f],rows[j][f]);if(d<m[0])m=[d,rows[i].k+'/'+rows[j].k]}console.log('ΔE mínimo',f,m[0].toFixed(3),m[1])}


// --write: genera los tokens que usa el front (fuente única de los colores por reino).
//   node scripts/kingdom-palette.cjs --write
if (process.argv.includes('--write')) {
  const path = require('path'), fs = require('fs'), dir = path.join(__dirname, '../src/theme');
  const head = '/* Generado por scripts/kingdom-palette.cjs --write. No editar a mano. */\n';
  const css = rows.map(r => `[data-kingdom="${r.k}"] { --kd-wash: ${r.wash}; --kd-ink: ${r.dink}; --kn-paper: ${r.nbg}; --kn-ink: ${r.nink}; }`).join('\n');
  fs.writeFileSync(path.join(dir, 'kingdoms.generated.css'), head + css + '\n');
  const js = Object.fromEntries(rows.map(r => [r.k, { pigment: r.p, day: { wash: r.wash, ink: r.dink }, night: { paper: r.nbg, ink: r.nink } }]));
  fs.writeFileSync(path.join(dir, 'kingdoms.generated.js'), head + `export const NIGHT_PAPER = '${NIGHT_BASE}'\nexport const KINGDOMS = ${JSON.stringify(js, null, 2)}\n`);
  console.log('escritos src/theme/kingdoms.generated.{css,js}');
}
