/**
 * ZapIt — Backend Server
 * Real device discovery via SSDP (UDP multicast) + network scan
 * Controls: Samsung TV (WS), LG WebOS (WS), Sony (REST), Wake-on-LAN
 */

'use strict';

const express = require('express');
const cors    = require('cors');
const dgram   = require('dgram');
const http    = require('http');
const { WebSocket } = require('ws');
const path    = require('path');
const net     = require('net');
const os      = require('os');

const app  = express();
const PORT = 3737;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── HELPERS ──────────────────────────────────────────────────

function getLocalSubnet() {
  const ifaces = os.networkInterfaces();
  for (const name of Object.keys(ifaces)) {
    for (const iface of ifaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        const parts = iface.address.split('.');
        return parts.slice(0, 3).join('.');  // e.g. "192.168.1"
      }
    }
  }
  return '192.168.1';
}

// Quick port probe (200ms timeout)
function probePort(ip, port) {
  return new Promise(resolve => {
    const sock = new net.Socket();
    const timer = setTimeout(() => { sock.destroy(); resolve(false); }, 200);
    sock.connect(port, ip, () => { clearTimeout(timer); sock.destroy(); resolve(true); });
    sock.on('error', () => { clearTimeout(timer); resolve(false); });
  });
}

// ── SSDP DISCOVERY ───────────────────────────────────────────
// Real UDP M-SEARCH multicast — discovers UPnP/DLNA devices

const SSDP_ADDR = '239.255.255.250';
const SSDP_PORT = 1900;
const SSDP_MSEARCH = [
  'M-SEARCH * HTTP/1.1',
  `HOST: ${SSDP_ADDR}:${SSDP_PORT}`,
  'MAN: "ssdp:discover"',
  'MX: 3',
  'ST: ssdp:all',
  '',
  ''
].join('\r\n');

let ssdpDevices = [];

function runSsdp() {
  return new Promise(resolve => {
    const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
    const found = new Map();

    sock.on('message', (msg, rinfo) => {
      const raw = msg.toString();
      const ip  = rinfo.address;

      // Parse LOCATION header
      const locMatch = raw.match(/LOCATION:\s*(http[^\r\n]+)/i);
      const stMatch  = raw.match(/ST:\s*([^\r\n]+)/i);
      const svMatch  = raw.match(/SERVER:\s*([^\r\n]+)/i);

      if (locMatch && !found.has(ip)) {
        found.set(ip, {
          ip,
          location: locMatch[1].trim(),
          st:       stMatch  ? stMatch[1].trim()  : 'unknown',
          server:   svMatch  ? svMatch[1].trim()  : 'unknown',
          type:     guessType(raw),
          name:     guessName(raw, ip),
        });
      }
    });

    sock.on('error', () => resolve([...found.values()]));

    sock.bind(() => {
      const buf = Buffer.from(SSDP_MSEARCH);
      sock.send(buf, 0, buf.length, SSDP_PORT, SSDP_ADDR);

      // Collect responses for 3s
      setTimeout(() => {
        sock.close();
        resolve([...found.values()]);
      }, 3000);
    });
  });
}

function guessType(raw) {
  const r = raw.toLowerCase();
  if (r.includes('samsung') || r.includes('tizen'))       return 'samsung_tv';
  if (r.includes('lg') || r.includes('webos'))            return 'lg_tv';
  if (r.includes('sony') || r.includes('bravia'))         return 'sony_tv';
  if (r.includes('philips'))                              return 'philips_tv';
  if (r.includes('chromecast') || r.includes('google'))   return 'chromecast';
  if (r.includes('sonos'))                                return 'sonos';
  if (r.includes('denon') || r.includes('marantz'))       return 'receiver';
  if (r.includes('roku'))                                 return 'roku';
  if (r.includes('printer') || r.includes('ipp'))         return 'printer';
  if (r.includes('router') || r.includes('gateway'))      return 'router';
  return 'unknown';
}

function guessName(raw, ip) {
  const fn = raw.match(/friendlyName[^>]*>([^<]+)</i);
  if (fn) return fn[1].trim();
  const type = guessType(raw);
  const names = {
    samsung_tv: 'Samsung Smart TV',
    lg_tv:      'LG Smart TV',
    sony_tv:    'Sony Bravia',
    chromecast: 'Chromecast',
    sonos:      'Sonos',
    receiver:   'AV Receiver',
    roku:       'Roku',
    printer:    'Printer',
    router:     'Router',
    philips_tv: 'Philips TV',
  };
  return names[type] || `Device (${ip})`;
}

// ── LAN PORT SCAN ────────────────────────────────────────────
// Scans local subnet for common smart TV / media device ports

const SMART_PORTS = [
  { port: 8001, type: 'samsung_tv', label: 'Samsung TV API' },
  { port: 8002, type: 'samsung_tv', label: 'Samsung TV API (TLS)' },
  { port: 3000, type: 'lg_tv',      label: 'LG WebOS' },
  { port: 3001, type: 'lg_tv',      label: 'LG WebOS (TLS)' },
  { port: 80,   type: 'sony_tv',    label: 'Sony IRCC' },
  { port: 9080, type: 'roku',       label: 'Roku ECP' },
  { port: 8008, type: 'chromecast', label: 'Chromecast' },
  { port: 8009, type: 'chromecast', label: 'Chromecast Control' },
  { port: 1925, type: 'philips_tv', label: 'Philips JointSpace' },
  { port: 11000,type: 'receiver',   label: 'Denon/Marantz Telnet' },
];

async function scanLAN() {
  const subnet = getLocalSubnet();
  const found  = [];
  const batch  = 32; // concurrent probes

  // Scan .1 through .254
  const ips = Array.from({ length: 254 }, (_, i) => `${subnet}.${i + 1}`);

  for (let i = 0; i < ips.length; i += batch) {
    const chunk = ips.slice(i, i + batch);
    const results = await Promise.all(
      chunk.flatMap(ip =>
        SMART_PORTS.map(async ({ port, type, label }) => {
          const open = await probePort(ip, port);
          return open ? { ip, port, type, label } : null;
        })
      )
    );
    found.push(...results.filter(Boolean));
  }

  // Deduplicate by IP (prefer more specific type)
  const byIp = new Map();
  for (const d of found) {
    if (!byIp.has(d.ip) || d.port < byIp.get(d.ip).port) {
      byIp.set(d.ip, d);
    }
  }
  return [...byIp.values()].map(d => ({
    ip:   d.ip,
    type: d.type,
    port: d.port,
    name: guessNameByType(d.type),
    label: d.label,
  }));
}

function guessNameByType(type) {
  return {
    samsung_tv: 'Samsung Smart TV',
    lg_tv:      'LG Smart TV',
    sony_tv:    'Sony Bravia',
    chromecast: 'Chromecast',
    roku:       'Roku',
    philips_tv: 'Philips TV',
    receiver:   'AV Receiver',
  }[type] || 'Smart Device';
}

// ── SAMSUNG TV CONTROL ────────────────────────────────────────
// WebSocket API — works on 2016-2023 Samsung TVs

function samsungSendKey(ip, key) {
  return new Promise((resolve, reject) => {
    const name = Buffer.from('ZapIt').toString('base64');
    const url  = `ws://${ip}:8001/api/v2/channels/samsung.remote.control?name=${name}`;
    const ws   = new WebSocket(url, { handshakeTimeout: 4000 });

    ws.on('open', () => {
      const msg = JSON.stringify({
        method: 'ms.remote.control',
        params: { Cmd: 'Click', DataOfCmd: key, Option: 'false', TypeOfRemote: 'SendRemoteKey' }
      });
      ws.send(msg);
      setTimeout(() => { ws.close(); resolve({ ok: true, key }); }, 600);
    });
    ws.on('error', err => reject(err));
    setTimeout(() => { ws.close(); reject(new Error('timeout')); }, 5000);
  });
}

// ── LG WEBOS CONTROL ─────────────────────────────────────────
// SSAP protocol over WebSocket

function lgSendCmd(ip, uri, payload = {}) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://${ip}:3000`, { handshakeTimeout: 4000 });
    let msgId = 1;
    let paired = false;

    const handshake = {
      type: 'register',
      id:   'reg0',
      payload: {
        forcePairing: false,
        pairingType:  'PROMPT',
        manifest: {
          manifestVersion: 1,
          appVersion:      '2.0',
          signed: { created: '20201020', appId: 'com.zapit.remote', vendorId: 'com.zapit' },
          permissions: ['LAUNCH','CONTROL_AUDIO','CONTROL_DISPLAY','CONTROL_POWER']
        }
      }
    };

    ws.on('open', () => ws.send(JSON.stringify(handshake)));

    ws.on('message', raw => {
      const msg = JSON.parse(raw);
      if (msg.type === 'registered' && !paired) {
        paired = true;
        const cmd = { type: 'request', id: `cmd${msgId++}`, uri, payload };
        ws.send(JSON.stringify(cmd));
        setTimeout(() => { ws.close(); resolve({ ok: true }); }, 800);
      }
    });

    ws.on('error', err => reject(err));
    setTimeout(() => { ws.close(); reject(new Error('timeout')); }, 6000);
  });
}

// ── SONY BRAVIA CONTROL ───────────────────────────────────────
// REST IRCC API

function sonyIrcc(ip, code) {
  return new Promise((resolve, reject) => {
    const body = `<?xml version="1.0"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><u:X_SendIRCC xmlns:u="urn:schemas-sony-com:service:IRCC:1"><IRCCCode>${code}</IRCCCode></u:X_SendIRCC></s:Body></s:Envelope>`;
    const opts = {
      hostname: ip, port: 80, path: '/sony/IRCC', method: 'POST',
      headers: { 'Content-Type': 'text/xml; charset=UTF-8', 'SOAPACTION': '"urn:schemas-sony-com:service:IRCC:1#X_SendIRCC"', 'Content-Length': Buffer.byteLength(body) }
    };
    const req = http.request(opts, res => resolve({ ok: true, status: res.statusCode }));
    req.on('error', reject);
    req.setTimeout(4000, () => { req.destroy(); reject(new Error('timeout')); });
    req.write(body);
    req.end();
  });
}

// ── WAKE-ON-LAN ───────────────────────────────────────────────

function sendWol(macAddress) {
  return new Promise((resolve, reject) => {
    const mac    = macAddress.replace(/[:\-]/g, '');
    const magic  = Buffer.concat([
      Buffer.alloc(6, 0xff),
      ...Array(16).fill(Buffer.from(mac, 'hex'))
    ]);
    const sock = dgram.createSocket('udp4');
    sock.once('error', reject);
    sock.send(magic, 0, magic.length, 9, '255.255.255.255', err => {
      sock.close();
      if (err) reject(err); else resolve({ ok: true });
    });
  });
}

// ── ROUTES ────────────────────────────────────────────────────

// Discover devices: SSDP + LAN scan in parallel
app.get('/api/discover', async (req, res) => {
  try {
    console.log('[ZapIt] Starting discovery...');
    const [ssdp, lan] = await Promise.all([
      runSsdp().catch(() => []),
      scanLAN().catch(() => []),
    ]);

    // Merge, dedupe by IP
    const all = new Map();
    for (const d of [...ssdp, ...lan]) {
      if (!all.has(d.ip)) all.set(d.ip, d);
    }

    const devices = [...all.values()];
    ssdpDevices = devices;
    console.log(`[ZapIt] Found ${devices.length} device(s)`);
    res.json({ devices });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Control a device
app.post('/api/control', async (req, res) => {
  const { ip, type, action, mac } = req.body;
  console.log(`[ZapIt] Control: ${action} → ${type} @ ${ip}`);

  try {
    let result;

    if (type === 'samsung_tv') {
      const keyMap = { off: 'KEY_POWER', on: 'KEY_POWER', mute: 'KEY_MUTE', vol_up: 'KEY_VOLUP', vol_down: 'KEY_VOLDOWN' };
      result = await samsungSendKey(ip, keyMap[action] || 'KEY_POWER');

    } else if (type === 'lg_tv') {
      const uriMap = {
        off:      'ssap://system/turnOff',
        on:       'ssap://system/turnOn',
        mute:     'ssap://audio/setMute',
        vol_up:   'ssap://audio/volumeUp',
        vol_down: 'ssap://audio/volumeDown',
      };
      result = await lgSendCmd(ip, uriMap[action] || 'ssap://system/turnOff');

    } else if (type === 'sony_tv') {
      const codeMap = {
        off:      'AAAAAQAAAAEAAAAvAw==',
        on:       'AAAAAQAAAAEAAAAuAw==',
        mute:     'AAAAAQAAAAEAAAAkAw==',
        vol_up:   'AAAAAQAAAAEAAAASAw==',
        vol_down: 'AAAAAQAAAAEAAAATAw==',
      };
      result = await sonyIrcc(ip, codeMap[action] || codeMap.off);

    } else if (type === 'wol' && mac) {
      result = await sendWol(mac);

    } else {
      result = { ok: false, reason: 'unsupported device type' };
    }

    res.json(result);
  } catch (e) {
    console.error('[ZapIt] Control error:', e.message);
    res.json({ ok: false, error: e.message });
  }
});

// Health
app.get('/api/status', (req, res) => res.json({ ok: true, version: '1.0.0', devices: ssdpDevices.length }));

// ── START ─────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`\n⚡ ZapIt backend running at http://localhost:${PORT}`);
  console.log(`   Subnet: ${getLocalSubnet()}.x`);
  console.log(`   Open public/index.html in browser\n`);
});
