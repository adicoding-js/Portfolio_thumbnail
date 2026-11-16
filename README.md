# PinkWire LiveClipper (Stage 1)

Ultra-lightweight 24/7 Live Stream Auto-Clipping & AI Triage Engine engineered for **Hack Club Nest (2GB RAM, 16GB disk quota, shared CPU)** paired with a local **Windows 11 Web Control Room**.

---

## 1. Hack Club Nest (Linux SSH VPS) Setup

### Prerequisites
SSH into your Hack Club Nest container and verify Python 3.11+ and `ffmpeg`:

```bash
ssh <your_nest_user>@hackclub.app
python3 --version
ffmpeg -version
```

### Clone & Install
```bash
git clone <your-repo-url> ~/pinkwire-liveclipper
cd ~/pinkwire-liveclipper

python3 -m venv .venv
source .venv/bin/activate
pip install --no-cache-dir -r requirements-vps.txt
```

Edit `.env` and replace `HACKCLUB_API_KEY=your_hackclub_api_key_here` with your real Hack Club AI Proxy key.

### Run Spike Simulator First
```bash
python test_spike_simulator.py
```

### Option A: Run 24/7 via `systemd --user` (Recommended on Nest)
```bash
mkdir -p ~/.config/systemd/user
cat << 'EOF' > ~/.config/systemd/user/pinkwire-clipper.service
[Unit]
Description=PinkWire LiveClipper 24/7 VPS Engine
After=network.target

[Service]
WorkingDirectory=%h/pinkwire-liveclipper
ExecStart=%h/pinkwire-liveclipper/.venv/bin/python -m vps_engine.main
Restart=always
RestartSec=5
MemoryMax=700M

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now pinkwire-clipper.service
systemctl --user status pinkwire-clipper.service
journalctl --user -u pinkwire-clipper.service -f
```

### Option B: Run in `tmux`
```bash
tmux new -s pinkwire
cd ~/pinkwire-liveclipper && source .venv/bin/activate
python -m vps_engine.main
# Detach with Ctrl+B then D
```

### Exposing the API (Choose One)
1. **Hack Club Nest Caddy Reverse Proxy (`*.hackclub.app`):**
   Add to your `~/Caddyfile` (or via Nest's `nest caddy` helper):
   ```caddy
   <your-subdomain>.hackclub.app {
       reverse_proxy 127.0.0.1:8765
   }
   ```
   Then reload Caddy: `systemctl --user reload caddy`.

2. **SSH Port-Forwarding Tunnel (Zero Public Exposure):**
   Run this from your Windows 11 PowerShell terminal:
   ```powershell
   ssh -N -L 8765:127.0.0.1:8765 <your_nest_user>@hackclub.app
   ```

---

## 2. Windows 11 Local Dashboard Setup

Open PowerShell on your Windows 11 laptop:

```powershell
git clone <your-repo-url> C:\pinkwire-liveclipper
cd C:\pinkwire-liveclipper

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-local.txt

python local_dashboard/app.py
```

Open **`http://127.0.0.1:8080`** in your browser:
- Set **VPS URL** to `https://<your-subdomain>.hackclub.app` (or `http://localhost:8765` if using the SSH tunnel).
- Set **API Secret** to `pinkwire_nest_secret_2026` (from `.env`).
- Click **Connect**.

---

## 3. Adding New Games or Streamers
Edit `config/profiles.json` on the VPS (or switch the active game live from the dashboard dropdown). All game categories, PinkWire channel rosters, and default tags are hot-loaded from `config/profiles.json`.
