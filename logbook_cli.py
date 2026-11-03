#!/usr/bin/env python3
import os
import sys
import json
import sqlite3
import argparse
import subprocess
import hashlib
import urllib.request
import urllib.error
import time
import ssl
import socket
import shutil
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime

__version__ = "1.0.0"

GITHUB_REPO = "Oktavian19/logbook-cli"
GITHUB_RELEASES_API = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
GITHUB_RAW_SCRIPT = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/logbook_cli.py"

# Prioritaskan IPv4 daripada IPv6 untuk menghindari kegagalan routing/NAT64 atau rekaman DNS AAAA yang bermasalah
_orig_getaddrinfo = socket.getaddrinfo
def _getaddrinfo_prefer_ipv4(*args, **kwargs):
    responses = _orig_getaddrinfo(*args, **kwargs)
    return sorted(responses, key=lambda x: 0 if x[0] == socket.AF_INET else 1)
socket.getaddrinfo = _getaddrinfo_prefer_ipv4


if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent

DEFAULT_CONFIG_PATH = BASE_DIR / "config.json"
QUEUE_DB_PATH = BASE_DIR / "pending_queue.db"

def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> dict:
    config = {
        "vps_endpoint": os.environ.get("LOGBOOK_VPS_URL", "http://127.0.0.1:8088/api/logbook"),
        "api_token": os.environ.get("LOGBOOK_API_TOKEN", ""),
        "openai_api_key": os.environ.get("OPENAI_API_KEY", ""),
        "openai_base_url": os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        "openai_model": os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
        "custom_prompt": os.environ.get("LOGBOOK_CUSTOM_PROMPT", ""),
        "jam_masuk": os.environ.get("LOGBOOK_JAM_MASUK", "07:10"),
        "jam_pulang": os.environ.get("LOGBOOK_JAM_PULANG", "16:25"),
        "verify_ssl": os.environ.get("LOGBOOK_VERIFY_SSL", "true").lower() in ("true", "1", "yes"),
        "repositories": {}
    }


    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                file_cfg = json.load(f)
                for k, v in file_cfg.items():
                    if v is not None:
                        config[k] = v
        except Exception as e:
            print(f"[WARN] Gagal membaca {config_path}: {e}", file=sys.stderr)

    return config

def init_queue_db():
    conn = sqlite3.connect(QUEUE_DB_PATH)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_payloads (
                idempotency_key TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                last_error TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()

def save_pending_payload(idempotency_key: str, payload: dict, error_msg: str):
    init_queue_db()
    conn = sqlite3.connect(QUEUE_DB_PATH)
    try:
        conn.execute("""
            INSERT OR REPLACE INTO pending_payloads 
            (idempotency_key, payload_json, created_at, retry_count, last_error)
            VALUES (?, ?, ?, COALESCE((SELECT retry_count + 1 FROM pending_payloads WHERE idempotency_key = ?), 0), ?)
        """, (idempotency_key, json.dumps(payload), datetime.now().isoformat(), idempotency_key, error_msg))
        conn.commit()
    finally:
        conn.close()

def remove_pending_payload(idempotency_key: str):
    init_queue_db()
    conn = sqlite3.connect(QUEUE_DB_PATH)
    try:
        conn.execute("DELETE FROM pending_payloads WHERE idempotency_key = ?", (idempotency_key,))
        conn.commit()
    finally:
        conn.close()

def get_pending_payloads() -> list:
    init_queue_db()
    conn = sqlite3.connect(QUEUE_DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT idempotency_key, payload_json, retry_count FROM pending_payloads ORDER BY created_at ASC")
        return cur.fetchall()
    finally:
        conn.close()


SKIP_DIRS = {
    'node_modules', 'vendor', '.venv', 'venv', 'build', 'dist', 
    '.cache', '.dart_tool', '.cargo', 'target', '.pub-cache', '__pycache__'
}

def discover_git_repos(root_path: Path, max_depth: int = 5) -> dict:
    """
    Memindai root_path secara rekursif untuk menemukan seluruh direktori git.
    Mengembalikan dict {project_name: str(repo_path)}.
    Jika repo bersarang di bawah root_path, nama proyek diambil dari folder tingkat pertama
    di bawah root_path (misal: sai/SHIN-SEMUT/shin_semut -> SHIN-SEMUT).
    """
    found_repos = {}
    root_resolved = root_path.expanduser().resolve()
    if not root_resolved.exists() or not root_resolved.is_dir():
        return found_repos

    # Jika root_path sendiri adalah git repo langsung
    if (root_resolved / ".git").exists():
        return {root_resolved.name: str(root_resolved)}

    root_parts_len = len(root_resolved.parts)

    for current_dir, dirs, files in os.walk(root_resolved, topdown=True):
        cur_path = Path(current_dir)
        rel_depth = len(cur_path.parts) - root_parts_len

        # Batasi kedalaman traverse
        if rel_depth >= max_depth:
            dirs[:] = []
            continue

        # Prune folder blacklist / hidden
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith('.')]

        # Cek apakah direktori saat ini memiliki .git
        if (cur_path / ".git").exists() and cur_path != root_resolved:
            rel_parts = cur_path.relative_to(root_resolved).parts
            proj_name = rel_parts[0] if rel_parts else cur_path.name
            found_repos[proj_name] = str(cur_path)
            # Jangan traverse ke dalam repo yang sudah ditemukan
            dirs[:] = []

    return found_repos

def resolve_all_repositories(repositories_config: dict) -> dict:
    """
    Menyelesaikan mapping konfigurasi repositori:
    - Jika entri berupa git repository langsung, gunakan langsung.
    - Jika entri berupa folder biasa (seperti folder induk 'sai'), jalankan auto-discovery.
    """
    resolved = {}
    for alias, path_str in repositories_config.items():
        path_obj = Path(path_str).expanduser().resolve()
        if not path_obj.exists():
            print(f"[WARN] Path {path_str} ({alias}) tidak ditemukan.", file=sys.stderr)
            continue

        if (path_obj / ".git").exists():
            resolved[alias] = str(path_obj)
        elif path_obj.is_dir():
            discovered = discover_git_repos(path_obj)
            if not discovered:
                print(f"[WARN] Tidak ditemukan repositori Git di dalam {path_str}.", file=sys.stderr)
            for proj_name, repo_dir in discovered.items():
                resolved[proj_name] = repo_dir
    return resolved

def get_commits_for_date(repo_name: str, repo_path: str, date_str: str) -> list:
    """
    Membaca commit tanggal target dari repositori lokal secara read-only.
    Hanya membaca metadata (hash, author, date, subject) dari branch aktif (HEAD).
    """
    path_obj = Path(repo_path).expanduser().resolve()
    if not path_obj.exists() or not (path_obj / ".git").exists():
        print(f"[WARN] Repository {repo_name} pada {repo_path} tidak valid atau bukan git repo.", file=sys.stderr)
        return []

    # Filter rentang waktu hari tersebut: 00:00:00 s.d. 23:59:59
    since_arg = f"--since={date_str} 00:00:00"
    until_arg = f"--until={date_str} 23:59:59"
    cmd = [
        "git", "log",
        since_arg, until_arg,
        "--format=%h|%an|%ad|%s",
        "--date=iso-strict"
    ]

    try:
        res = subprocess.run(cmd, cwd=str(path_obj), capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Gagal membaca git log di {repo_name}: {e}", file=sys.stderr)
        return []

    commits = []
    for line in res.stdout.strip().splitlines():
        if not line:
            continue
        parts = line.split("|", 3)
        if len(parts) == 4:
            commits.append({
                "project": repo_name,
                "hash": parts[0],
                "author": parts[1],
                "date": parts[2],
                "subject": parts[3]
            })
    return commits

def collect_all_commits(repositories: dict, target_date: str) -> list:
    resolved_repos = resolve_all_repositories(repositories)
    all_commits = []
    for repo_name, repo_path in resolved_repos.items():
        commits = get_commits_for_date(repo_name, repo_path, target_date)
        all_commits.extend(commits)
    return all_commits


def generate_summary(commits: list, config: dict, force_no_ai: bool = False) -> str:
    """
    Menghasilkan ringkasan naratif Bahasa Indonesia.
    Menggunakan OpenAI jika API key tersedia dan tidak --no-ai,
    jika tidak menggunakan fallback non-AI terstruktur.
    """
    if not commits:
        return ""

    api_key = config.get("openai_api_key", "").strip()
    if not force_no_ai and api_key:
        try:
            # Format metadata untuk AI prompt (project dan subject)
            items = [f"- [{c['project']}] {c['subject']}" for c in commits]
            commit_text = "\n".join(items)

            default_system_prompt = (
                "Anda adalah asisten penyusun logbook kerja magang software engineer profesional. "
                "Tugas Anda adalah merangkum commit harian menjadi deskripsi kegiatan logbook yang sangat ringkas, padat, dan elegan.\n\n"
                "Aturan Penulisan:\n"
                "1. Panjang tepat 1 kalimat ringkas (maksimal 2 kalimat jika cakupan pekerjaan sangat luas dan beragam).\n"
                "2. WAJIB diawali langsung dengan kata kerja aktif formal berimbuhan Me- (seperti: Mengembangkan, Menyempurnakan, Menambahkan, Memperbaiki, Melanjutkan pengembangan).\n"
                "3. Dilarang menggunakan kata pengantar, pembuka basa-basi (seperti 'Hari ini...', 'Melakukan...', 'Aktivitas yang dilakukan...'), maupun bullet point.\n"
                "4. Penempatan nama proyek:\n"
                "   - Untuk 1 proyek: letakkan di akhir klausa kegiatan dengan pola '... pada proyek [NAMA_PROYEK].'\n"
                "   - Untuk lebih dari 1 proyek: gabungkan secara alami dengan konjungsi, seperti '... pada proyek [PROYEK_A], serta ... pada [PROYEK_B].' (atau '... pada proyek A dan B' jika pekerjaannya serupa).\n"
                "5. Gunakan istilah teknis utama secara padat dan wajar tanpa mencantumkan hash commit atau detail mikro kode."
            )
            system_prompt = config.get("system_prompt", default_system_prompt).strip()
            custom_prompt = config.get("custom_prompt", "").strip()
            if custom_prompt:
                system_prompt = f"{system_prompt}\n{custom_prompt}"

            user_prompt = f"Berikut daftar commit hari ini:\n{commit_text}\n\nBuat deskripsi kegiatan logbook:"

            req_body = json.dumps({
                "model": config.get("openai_model", "gpt-4o-mini"),
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.2
            }).encode("utf-8")

            base_url = config.get("openai_base_url", "https://api.openai.com/v1").rstrip("/")
            endpoint_url = f"{base_url}/chat/completions"

            req = urllib.request.Request(
                endpoint_url,
                data=req_body,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
            )


            with urllib.request.urlopen(req, timeout=30) as resp:
                res_data = json.loads(resp.read().decode("utf-8"))
                return res_data["choices"][0]["message"]["content"].strip()

        except Exception as e:
            print(f"[WARN] OpenAI summarization gagal ({e}). Beralih ke fallback non-AI.", file=sys.stderr)

    # Fallback Non-AI: Gabungkan subject commit per proyek
    grouped = {}
    for c in commits:
        grouped.setdefault(c["project"], []).append(c["subject"])

    lines = []
    for proj, subjs in grouped.items():
        subjs_str = "; ".join(subjs)
        lines.append(f"Mengerjakan pengembangan pada proyek {proj}: {subjs_str}.")
    return " ".join(lines)

def compute_idempotency_key(date_str: str, commits: list) -> str:
    sorted_signatures = sorted(f"{c['project']}:{c['hash']}" for c in commits)
    raw = f"{date_str}|" + "|".join(sorted_signatures)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def get_ssl_context(verify: bool = True) -> ssl.SSLContext:
    """Membuat SSL context yang resilien dengan auto-discovery root CA atau opsi insecure."""
    if not verify:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    # 1. Coba certifi jika tersedia
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        pass

    # 2. Coba path CA bundle sistem Linux yang umum
    ca_paths = [
        "/etc/ssl/certs/ca-certificates.crt",                  # Debian/Ubuntu
        "/etc/pki/tls/certs/ca-bundle.crt",                    # CentOS/RHEL/Fedora
        "/etc/ssl/ca-bundle.pem",                              # OpenSUSE
        "/etc/ssl/cert.pem",                                  # Alpine/Arch/macOS
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",  # RHEL modern
    ]
    for ca in ca_paths:
        if os.path.isfile(ca):
            try:
                return ssl.create_default_context(cafile=ca)
            except Exception:
                pass

    return ssl.create_default_context()

def send_payload_to_vps(payload: dict, config: dict) -> tuple:
    """Mengirim payload JSON ke endpoint VPS. Mengembalikan (success: bool, msg: str)."""
    endpoint = config.get("vps_endpoint")
    token = config.get("api_token")
    verify_ssl = config.get("verify_ssl", True)

    if not endpoint:
        return False, "vps_endpoint belum dikonfigurasi"

    body_bytes = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=body_bytes,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "logbook-cli/1.0"
        }
    )

    ctx = get_ssl_context(verify=verify_ssl)

    try:
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            resp_body = json.loads(resp.read().decode("utf-8"))
            return True, resp_body.get("message", "OK")
    except urllib.error.HTTPError as he:
        err_content = he.read().decode("utf-8", errors="replace")
        return False, f"HTTP Error {he.code}: {err_content}"
    except Exception as e:
        return False, f"Network Error: {str(e)} (Pastikan koneksi internet aktif dan vps_endpoint dapat dijangkau)"

# --- CLI COMMANDS ---

def cmd_preview(args, config):
    target_date = args.date or datetime.now().strftime("%Y-%m-%d")
    repos = config.get("repositories", {})
    if not repos:
        print("Belum ada repositori yang dikonfigurasi di config.json.")
        return

    commits = collect_all_commits(repos, target_date)
    if not commits:
        print(f"Tidak ada commit pada tanggal {target_date}.")
        return

    summary = generate_summary(commits, config, force_no_ai=args.no_ai)
    idemp_key = compute_idempotency_key(target_date, commits)
    projects = sorted(list({c["project"] for c in commits}))

    print("=" * 60)
    print(f"PREVIEW LOGBOOK ({target_date})")
    print("=" * 60)
    print(f"Proyek      : {', '.join(projects)}")
    print(f"Jam Kerja   : {config.get('jam_masuk', '07:10')} - {config.get('jam_pulang', '16:25')}")
    print(f"Idempotency : {idemp_key[:16]}...")
    print(f"Total Commit: {len(commits)}")
    print("-" * 60)
    print("RINGKASAN NARATIF:")
    print(summary)
    print("-" * 60)
    print("DAFTAR COMMIT:")
    for c in commits:
        print(f"  • [{c['project']}] {c['hash']} - {c['subject']}")
    print("=" * 60)

def cmd_run(args, config):
    target_date = args.date or datetime.now().strftime("%Y-%m-%d")
    repos = config.get("repositories", {})
    if not repos:
        print("Belum ada repositori yang dikonfigurasi di config.json.")
        return

    commits = collect_all_commits(repos, target_date)
    if not commits:
        print(f"ℹ️ Tidak ada commit pada tanggal {target_date}. Tidak ada request yang dikirim.")
        return

    summary = generate_summary(commits, config, force_no_ai=args.no_ai)
    idemp_key = compute_idempotency_key(target_date, commits)
    projects = sorted(list({c["project"] for c in commits}))

    payload = {
        "date": target_date,
        "projects": projects,
        "summary": summary,
        "idempotency_key": idemp_key,
        "commits": commits,
        "jam_masuk": config.get("jam_masuk", "07:10"),
        "jam_pulang": config.get("jam_pulang", "16:25")
    }

    success, msg = send_payload_to_vps(payload, config)
    if success:
        print(f"✅ Draf logbook tanggal {target_date} berhasil dikirim ke Telegram! ({msg})")
    else:
        print(f"⚠️ Gagal mengirim payload ke Telegram: {msg}")
        save_pending_payload(idemp_key, payload, msg)
        print("💾 Payload telah disimpan ke antrean pending lokal untuk dicoba ulang berikutnya.")

def cmd_send_pending(args, config):
    pending = get_pending_payloads()
    if not pending:
        print("Tidak ada payload logbook yang tertunda.")
        return

    print(f"Ditemukan {len(pending)} payload tertunda. Mencoba mengirim ulang...")
    for idemp_key, payload_json, retries in pending:
        payload = json.loads(payload_json)
        date_str = payload.get("date", "-")
        success, msg = send_payload_to_vps(payload, config)
        if success:
            print(f"✅ Payload tanggal {date_str} ({idemp_key[:8]}...) berhasil terkirim!")
            remove_pending_payload(idemp_key)
        else:
            print(f"❌ Gagal mengirim ulang {date_str}: {msg}")
            save_pending_payload(idemp_key, payload, msg)

def cmd_scan(args, config):
    repos = config.get("repositories", {})
    if not repos:
        print("Belum ada repositori atau folder induk yang dikonfigurasi di config.json.")
        return

    resolved = resolve_all_repositories(repos)
    print("=" * 65)
    print(f"DAFTAR REPOSITORI GIT TERDETEKSI ({len(resolved)} Repositori)")
    print("=" * 65)
    if not resolved:
        print("Tidak ada repositori Git yang ditemukan.")
        return

    today_str = datetime.now().strftime("%Y-%m-%d")
    for name, path in sorted(resolved.items()):
        commits_today = get_commits_for_date(name, path, today_str)
        status_commit = f"({len(commits_today)} commit hari ini)" if commits_today else "(0 commit hari ini)"
        print(f"• {name:<18} : {path} {status_commit}")
    print("=" * 65)

def parse_version(ver_str: str) -> tuple:
    """Mengubah string versi seperti 'v1.0.1' atau '1.0.0' menjadi tuple angka (1, 0, 1)."""
    ver_clean = ver_str.lstrip("v").strip()
    parts = []
    for p in ver_clean.split("."):
        try:
            parts.append(int(p))
        except ValueError:
            break
    return tuple(parts)

def cleanup_old_executables():
    """Membersihkan file .old sisa pembaruan Windows pada proses sebelumnya."""
    if getattr(sys, "frozen", False):
        try:
            old_file = Path(sys.executable + ".old")
            if old_file.exists():
                old_file.unlink()
        except Exception:
            pass

def check_latest_version(timeout: int = 3) -> dict:
    """
    Memeriksa rilis terbaru dari GitHub.
    Mengembalikan dict dengan status pembaruan dan metadata rilis.
    """
    result = {
        "has_update": False,
        "latest_version": __version__,
        "current_version": __version__,
        "release_notes": "",
        "assets": [],
        "tag_name": f"v{__version__}",
        "error": None
    }
    try:
        req = urllib.request.Request(
            GITHUB_RELEASES_API,
            headers={
                "User-Agent": "Logbook-CLI-Updater",
                "Accept": "application/vnd.github.v3+json"
            }
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            tag_name = data.get("tag_name", "")
            latest_clean = tag_name.lstrip("v")
            result["latest_version"] = latest_clean
            result["tag_name"] = tag_name
            result["release_notes"] = data.get("body", "")
            result["assets"] = data.get("assets", [])

            if parse_version(latest_clean) > parse_version(__version__):
                result["has_update"] = True
    except Exception as e:
        result["error"] = str(e)
    return result

def restart_application():
    """Me-restart aplikasi CLI ke versi terbaru."""
    print("\n🔄 Memulai ulang Logbook CLI...")
    time.sleep(0.5)
    try:
        if sys.platform == "win32" and getattr(sys, "frozen", False):
            subprocess.Popen([sys.executable] + sys.argv[1:])
            sys.exit(0)
        else:
            os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        print(f"⚠️ Gagal me-restart otomatis: {e}")
        print("Silakan jalankan ulang Logbook CLI secara manual.")
        sys.exit(0)

def _download_and_replace_script(target_path: Path):
    req = urllib.request.Request(GITHUB_RAW_SCRIPT, headers={"User-Agent": "Logbook-CLI-Updater"})
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
        content = resp.read()
    if b"__version__" not in content:
        raise ValueError("File yang diunduh bukan script Logbook CLI yang valid!")
    target_path.write_bytes(content)

def perform_self_update(release_info: dict = None, force: bool = False) -> bool:
    """Melakukan proses unduh dan instalasi pembaruan Logbook CLI."""
    if not release_info:
        print("🔍 Memeriksa versi terbaru di GitHub...")
        release_info = check_latest_version(timeout=5)

    if release_info.get("error") and not force:
        print(f"❌ Gagal memeriksa pembaruan: {release_info['error']}")
        return False

    has_update = release_info.get("has_update", False)
    latest_ver = release_info.get("latest_version", __version__)
    tag_name = release_info.get("tag_name", f"v{latest_ver}")

    if not has_update and not force:
        print(f"✅ Anda sudah menggunakan versi terbaru: v{__version__}")
        return True

    print(f"\n🚀 Memulai pembaruan Logbook CLI (v{__version__} -> v{latest_ver})...")
    if release_info.get("release_notes"):
        print("\n📝 Catatan Rilis:")
        print("-" * 50)
        notes_lines = [line for line in release_info["release_notes"].strip().split("\n") if line.strip()][:8]
        print("\n".join(notes_lines))
        print("-" * 50)

    is_frozen = getattr(sys, "frozen", False)

    try:
        if is_frozen:
            # Mode Standalone Executable
            is_win = sys.platform == "win32"
            target_asset_name = "logbook-cli-windows-x64.zip" if is_win else "logbook-cli-linux-x64.zip"
            binary_name = "logbook_cli.exe" if is_win else "logbook_cli"

            download_url = None
            for asset in release_info.get("assets", []):
                if asset.get("name") == target_asset_name:
                    download_url = asset.get("browser_download_url")
                    break

            if not download_url:
                download_url = f"https://github.com/{GITHUB_REPO}/releases/download/{tag_name}/{target_asset_name}"

            print(f"📦 Mengunduh paket: {target_asset_name}")
            print(f"🌐 Dari: {download_url}")

            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_dir_path = Path(tmp_dir)
                zip_path = tmp_dir_path / target_asset_name

                req = urllib.request.Request(download_url, headers={"User-Agent": "Logbook-CLI-Updater"})
                ctx = ssl.create_default_context()
                with urllib.request.urlopen(req, timeout=30, context=ctx) as resp, open(zip_path, "wb") as f_out:
                    total_size = resp.getheader("Content-Length")
                    total_bytes = int(total_size) if total_size else 0
                    downloaded = 0
                    while True:
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        f_out.write(chunk)
                        downloaded += len(chunk)
                        if total_bytes > 0:
                            percent = int(downloaded / total_bytes * 100)
                            print(f"\r⏳ Mengunduh: {percent}% ({downloaded // 1024} KB / {total_bytes // 1024} KB)", end="", flush=True)
                print("\n✅ Unduhan selesai, mengekstrak binary...")

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmp_dir_path)

                new_binary = tmp_dir_path / binary_name
                if not new_binary.exists():
                    raise FileNotFoundError(f"File binary {binary_name} tidak ditemukan di dalam paket rilis!")

                current_exe = Path(sys.executable).resolve()
                print(f"🔄 Mengganti executable: {current_exe.name}")

                if is_win:
                    old_exe = Path(str(current_exe) + ".old")
                    if old_exe.exists():
                        try:
                            old_exe.unlink()
                        except Exception:
                            pass
                    current_exe.rename(old_exe)
                    shutil.copy2(new_binary, current_exe)
                else:
                    new_binary.chmod(0o755)
                    os.replace(new_binary, current_exe)

        else:
            # Mode Script Python
            script_path = Path(__file__).resolve()
            git_dir = BASE_DIR / ".git"

            if git_dir.is_dir():
                print("🌿 Repositori Git lokal terdeteksi, menjalankan 'git pull'...")
                res = subprocess.run(["git", "pull"], cwd=BASE_DIR, capture_output=True, text=True)
                print(res.stdout)
                if res.returncode != 0:
                    print(f"⚠️ Git pull gagal ({res.stderr.strip()}), mencoba mengunduh script langsung...")
                    _download_and_replace_script(script_path)
            else:
                print("📥 Mengunduh script terbaru dari GitHub...")
                _download_and_replace_script(script_path)

        print("\n🎉 Pembaruan berhasil dipasang!")

        # Minta konfirmasi restart
        try:
            prompt = input("\n👉 Apakah Anda ingin me-restart Logbook CLI ke versi terbaru sekarang? [Y/n]: ").strip().lower()
            if prompt in ('', 'y', 'yes'):
                restart_application()
        except (KeyboardInterrupt, EOFError):
            print("\n👋 Pembaruan selesai. Silakan jalankan ulang CLI secara manual.")
        return True

    except Exception as e:
        print(f"\n❌ Terjadi kesalahan saat melakukan pembaruan: {e}")
        return False

def cmd_update(args, config):
    """Perintah CLI untuk memeriksa atau melakukan pembaruan."""
    check_only = getattr(args, "check", False)
    force = getattr(args, "force", False)

    print("🔍 Memeriksa versi terbaru di GitHub...")
    rel = check_latest_version(timeout=5)
    if rel.get("error"):
        print(f"⚠️ Gagal menghubungi server rilis: {rel['error']}")
        return

    latest_ver = rel["latest_version"]
    has_update = rel["has_update"]

    print(f"📌 Versi saat ini : v{__version__}")
    print(f"🌐 Versi terbaru : v{latest_ver}")

    if check_only:
        if has_update:
            print(f"\n✨ Pembaruan tersedia! Jalankan 'logbook_cli update' atau pilih menu [6] untuk memperbarui.")
        else:
            print("\n✅ Logbook CLI sudah menggunakan versi terbaru.")
        return

    if not has_update and not force:
        print("\n✅ Logbook CLI Anda sudah menggunakan versi paling mutakhir.")
        return

    perform_self_update(release_info=rel, force=force)

# --- INTERACTIVE TERMINAL MENU ---

def run_interactive_menu(config: dict):
    """Menampilkan menu interaktif berbasis nomor di terminal dengan status ringkas dan panduan jelas."""
    cleanup_old_executables()
    if os.name == 'nt':
        os.system('')  # Mengaktifkan dukungan ANSI colors di Windows Console

    print("🔍 Memeriksa pembaruan Logbook CLI...", flush=True)
    update_info = check_latest_version(timeout=3)

    while True:
        # 1. Ambil status ringkas
        today_str = datetime.now().strftime("%Y-%m-%d")
        pending_list = get_pending_payloads()
        pending_count = len(pending_list)

        # 2. Gambar header status ringkas
        print("\033[2J\033[H", end="")
        print("┌────────────────────────────────────────────────────────┐")
        title = f"🚀 Git Logbook Automation CLI v{__version__}"
        print(f"│{title:^56}│")
        print(f"│  📅 Tanggal : {today_str:<39}  │")
        if pending_count > 0:
            print(f"│  ⚠️ Antrean : {pending_count} draf tertunda (perlu dikirim ulang){' '*4}│")
        else:
            print(f"│  🟢 Antrean : Bersih (tidak ada draf tertunda){' '*10}│")
        print("└────────────────────────────────────────────────────────┘")
        if update_info.get("has_update"):
            latest_v = update_info.get('latest_version', '')
            print(f"\n  \033[1;33m📢 Notifikasi: Versi baru v{latest_v} tersedia! Pilih menu [6] untuk update.\033[0m")
        print("\nPILIHAN MENU (Ketik angka menu yang diinginkan):\n")

        print("  \033[1;36m[1] 🚀 Kirim Logbook Hari Ini\033[0m")
        print("      Membaca commit hari ini, buat ringkasan AI, lalu kirim draf ke Telegram VPS.\n")

        print("  \033[1;36m[2] 👁️  Preview Draf Hari Ini\033[0m")
        print("      Melihat daftar commit & draf ringkasan di terminal tanpa mengirim ke server.\n")

        print("  \033[1;36m[3] 🔍 Scan Repositori Git\033[0m")
        print("      Melihat daftar folder proyek lokal yang terdeteksi beserta status commit hari ini.\n")

        if pending_count > 0:
            print(f"  \033[1;33m[4] 🔄 Kirim Ulang Draf Tertunda ({pending_count} Draf Menunggu!)\033[0m")
            print("      \033[33mMencoba mengirim ulang draf yang sebelumnya gagal karena offline/gangguan jaringan.\033[0m\n")
        else:
            print("  \033[1;36m[4] 🔄 Kirim Ulang Draf Tertunda\033[0m")
            print("      Mencoba mengirim ulang draf yang sebelumnya gagal karena offline/gangguan jaringan.\n")

        print("  \033[1;36m[5] 📅 Logbook Tanggal Tertentu\033[0m")
        print("      Membuat draf atau mengirim logbook untuk tanggal kustom lampau (YYYY-MM-DD).\n")

        if update_info.get("has_update"):
            print(f"  \033[1;32m[6] 🔄 Update Logbook CLI\033[0m \033[1;33m(🚀 Update Tersedia: v{update_info.get('latest_version')}!)\033[0m")
            print("      Mengunduh dan memasang versi terbaru dari GitHub secara otomatis.\n")
        else:
            print(f"  \033[1;36m[6] 🔄 Update Logbook CLI\033[0m (Versi saat ini v{__version__} sudah terbaru)")
            print("      Periksa atau instal ulang versi rilis terbaru dari repositori.\n")

        print("  \033[1;31m[0] ❌ Keluar\033[0m (atau ketik 'q')\n")

        # 3. Minta input nomor
        try:
            choice = input("👉 Masukkan nomor pilihan [1-6, atau 0 untuk keluar]: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\n👋 Keluar dari Logbook CLI.")
            return

        if not choice:
            print("\n⚠️ Pilihan tidak boleh kosong. Silakan masukkan angka 1 sampai 6 (atau 0 untuk keluar).")
            time.sleep(1.5)
            continue

        if choice in ('0', 'q', 'Q', 'exit'):
            print("\n👋 Keluar dari Logbook CLI. Sampai jumpa!")
            return

        if choice not in ('1', '2', '3', '4', '5', '6', 'u', 'update'):
            print(f"\n⚠️ Pilihan '{choice}' tidak valid. Silakan masukkan angka 1 sampai 6 (atau 0 untuk keluar).")
            time.sleep(1.8)
            continue

        # 4. Eksekusi aksi yang dipilih
        print("\n" + "=" * 65)
        if choice == "1":
            cmd_run(argparse.Namespace(date=None, no_ai=False), config)
        elif choice == "2":
            cmd_preview(argparse.Namespace(date=None, no_ai=False), config)
        elif choice == "3":
            cmd_scan(argparse.Namespace(), config)
        elif choice == "4":
            cmd_send_pending(argparse.Namespace(), config)
        elif choice == "5":
            print("📅 PILIH TANGGAL KUSTOM")
            print("-" * 65)
            try:
                target_date = input("Masukkan tanggal commit (YYYY-MM-DD): ").strip()
                datetime.strptime(target_date, "%Y-%m-%d")
                mode = input("Pilih tindakan: [1] Preview  [2] Kirim ke VPS (default 1): ").strip()
                print("-" * 65)
                if mode == "2":
                    cmd_run(argparse.Namespace(date=target_date, no_ai=False), config)
                else:
                    cmd_preview(argparse.Namespace(date=target_date, no_ai=False), config)
            except ValueError:
                print("❌ Format tanggal tidak valid. Harus YYYY-MM-DD (contoh: 2026-10-28).")
            except (KeyboardInterrupt, EOFError):
                print("\nOperasi dibatalkan.")
        elif choice in ("6", "u", "update"):
            print("🔄 PEMBARUAN LOGBOOK CLI")
            print("-" * 65)
            updated = perform_self_update(release_info=update_info)
            if updated:
                update_info = check_latest_version(timeout=3)

        print("\n" + "-" * 65)
        try:
            input("👉 Tekan [Enter] untuk kembali ke menu utama...")
        except (KeyboardInterrupt, EOFError):
            print("\n👋 Keluar dari Logbook CLI.")
            return

def main():
    cleanup_old_executables()
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {__version__}")
    base_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Path ke file konfigurasi json")
    base_parser.add_argument("--insecure", action="store_true", help="Abaikan verifikasi sertifikat SSL")

    parser = argparse.ArgumentParser(description="Git Logbook Automation CLI", parents=[base_parser])
    subparsers = parser.add_subparsers(dest="command", required=False)

    # scan
    subparsers.add_parser("scan", parents=[base_parser], help="Pindai dan tampilkan daftar repositori Git yang terdeteksi")

    # preview
    p_prev = subparsers.add_parser("preview", parents=[base_parser], help="Lihat draf logbook tanpa kirim ke VPS")
    p_prev.add_argument("--date", help="Tanggal commit (format YYYY-MM-DD, default hari ini)")
    p_prev.add_argument("--no-ai", action="store_true", help="Pakai ringkasan manual non-AI")

    # run
    p_run = subparsers.add_parser("run", parents=[base_parser], help="Jalankan pembuatan dan pengiriman logbook")
    p_run.add_argument("--date", help="Tanggal commit (format YYYY-MM-DD, default hari ini)")
    p_run.add_argument("--no-ai", action="store_true", help="Pakai ringkasan manual non-AI")

    # send-pending
    subparsers.add_parser("send-pending", parents=[base_parser], help="Kirim ulang draf yang tertunda")

    # update
    p_upd = subparsers.add_parser("update", parents=[base_parser], help="Periksa dan perbarui Logbook CLI ke versi terbaru")
    p_upd.add_argument("--check", action="store_true", help="Hanya cek versi terbaru tanpa melakukan update")
    p_upd.add_argument("--force", action="store_true", help="Paksa download dan pasang ulang versi terbaru")

    args = parser.parse_args()
    config = load_config(args.config)
    if getattr(args, "insecure", False):
        config["verify_ssl"] = False

    if not args.command:
        if sys.stdin.isatty():
            run_interactive_menu(config)
        else:
            parser.print_help()
            sys.exit(1)
        return

    if args.command == "scan":
        cmd_scan(args, config)
    elif args.command == "preview":
        cmd_preview(args, config)
    elif args.command == "run":
        cmd_run(args, config)
    elif args.command == "send-pending":
        cmd_send_pending(args, config)
    elif args.command == "update":
        cmd_update(args, config)

if __name__ == "__main__":
    main()
