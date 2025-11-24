import time, datetime, random, re, math
from typing import List
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import google_auth_httplib2, httplib2
from googleapiclient.errors import HttpError

# === CONFIG ===
ORIGEM_ID   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ORIGEM  = "BD_Carteira"
DESTINO_ID  = "1T6HVLBQi21CIeS64tAjI314TYi2795COOCAakzLV-q0"
ABA_DESTINO = "BD_Esteira"
CRED_FILE   = r"C:\Users\Sirtec\Desktop\Esteira\credenciais.json"

WRITE_CHUNK   = 1200           # linhas por escrita no destino
INIT_READ_ALL = True           # tenta 1 leitura total A:AC primeiro
MAX_RETRIES   = 8
BACKOFF_BASE  = 3.0
HTTP_TIMEOUT  = 600
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

# Leitura segmentada (quando all-in-one falha)
SEG_INIT = 2000                # tamanho inicial do bloco de leitura
SEG_MIN  = 200                 # não baixar abaixo disso

def log(msg: str) -> None:
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def retry(fn, desc):
    for att in range(1, MAX_RETRIES+1):
        try:
            return fn()
        except Exception as e:
            wait = min(90, BACKOFF_BASE*(2**(att-1)) + random.uniform(0,1.5))
            log(f"⚠️ {desc} — tentativa {att}/{MAX_RETRIES} falhou: {e} | aguardando {round(wait,1)}s")
            time.sleep(wait)
    raise RuntimeError(f"❌ {desc} — falhou após {MAX_RETRIES} tentativas.")

_num_re = re.compile(r"[^\d,.\-]")
def clean_number_br(v):
    if v is None or v == "": return ""
    v = _num_re.sub("", str(v))
    if "," in v and "." in v:
        v = v.replace(".", "").replace(",", ".")
    else:
        v = v.replace(",", ".")
    try:
        return float(v)
    except:
        return ""

def get_services():
    creds = Credentials.from_service_account_file(CRED_FILE, scopes=SCOPES)
    http  = google_auth_httplib2.AuthorizedHttp(creds, http=httplib2.Http(timeout=HTTP_TIMEOUT))
    api   = build("sheets","v4", http=http).spreadsheets()
    return api

def read_all_once(api):
    """Tenta ler A:AC de uma vez só (rápido)."""
    return api.values().get(
        spreadsheetId=ORIGEM_ID,
        range=f"{ABA_ORIGEM}!A:AC"   # <<< agora inclui AC
    ).execute()

def count_rows_adaptive(api):
    """Conta linhas pela coluna A com retry; robusto a quedas intermitentes."""
    res = retry(lambda: api.values().get(
        spreadsheetId=ORIGEM_ID,
        range=f"{ABA_ORIGEM}!A:A"
    ).execute(), "Ler total de linhas (A:A)")
    return len(res.get("values", []))

def read_segmented(api, total: int):
    """
    Lê A:AC em segmentos adaptativos.
    Se der 503 no segmento, reduz o tamanho pela metade e tenta de novo.
    """
    seg_size = SEG_INIT
    rows = []
    pos = 0
    while pos < total:
        r1 = min(pos + seg_size, total)
        rng = f"{ABA_ORIGEM}!A{pos+1}:AC{r1}"  # <<< inclui AC
        try:
            res = retry(lambda: api.values().get(
                spreadsheetId=ORIGEM_ID, range=rng
            ).execute(), f"Ler bloco A:AC {pos+1}-{r1}")
            bloco = res.get("values", [])
            # normaliza o bloco para caber no intervalo
            if len(bloco) < (r1 - pos):
                bloco = bloco + [[] for _ in range((r1 - pos) - len(bloco))]
            rows.extend(bloco)
            log(f"📥 Lido {len(rows)}/{total}")
            pos = r1
            time.sleep(0.2)
            if seg_size < 4000:
                seg_size = min(seg_size + 200, 4000)
        except Exception as e:
            new_seg = max(seg_size // 2, SEG_MIN)
            if new_seg == seg_size:
                raise
            log(f"🔻 Reduzindo segmento: {seg_size} → {new_seg}")
            seg_size = new_seg
    return rows

def main():
    log("🚀 BD_Carteira → BD_Esteira (A→A, AB→B, Z→C, X→D, AC→E | leitura adaptativa)")

    api = get_services()

    # 1) TENTA LEITURA ÚNICA (A:AC)
    rows = None
    if INIT_READ_ALL:
        log("📥 Tentando leitura única A:AC…")
        try:
            leitura = None
            for i in range(2):  # duas tentativas rápidas
                try:
                    leitura = read_all_once(api)
                    break
                except Exception as e:
                    wait = 2 + i * 3
                    log(f"⚠️ Leitura única falhou (tentativa {i+1}/2): {e} | aguardando {wait}s")
                    time.sleep(wait)
            if leitura:
                rows = leitura.get("values", [])
        except Exception:
            rows = None

    # 2) Fallback: leitura segmentada
    if rows is None:
        log("🔁 Fallback: leitura segmentada adaptativa.")
        total = count_rows_adaptive(api)
        if total == 0:
            log("⚠️ Nenhuma linha encontrada na origem."); return
        log(f"🔢 Total detectado: {total}")
        rows = read_segmented(api, total)
    else:
        total = len(rows)
        log(f"🔢 Linhas carregadas: {total}")

    if total == 0:
        log("⚠️ Nada para escrever."); return

    # 3) Monta saída com cabeçalho preservado
    #    Colunas: A(0) → A; AB(27) → B; Z(25) → C; X(23) → D; AC(28) → E
    log("🧪 Preparando dados (A, AB, Z, X, AC)…")
    out = []
    for idx, r in enumerate(rows):
        r = (r + [""]*29)[:29]  # garante 29 colunas até AC
        a  = r[0]
        ab = r[27] if idx == 0 else clean_number_br(r[27])  # preserva cabeçalho na 1ª linha
        z  = r[25]
        x  = r[23] if idx == 0 else clean_number_br(r[23])  # preserva cabeçalho na 1ª linha
        ac = r[28]  # AC vai como está (sem limpeza)
        out.append([a, ab, z, x, ac])

    # 4) Limpa destino e escreve A:E
    retry(lambda: api.values().clear(
        spreadsheetId=DESTINO_ID, range=f"{ABA_DESTINO}!A:E"
    ).execute(), "Limpar destino")
    log("🧹 Destino limpo.")

    chunks = math.ceil(total / WRITE_CHUNK)
    enviados = 0
    for i in range(chunks):
        r0 = i * WRITE_CHUNK
        r1 = min((i+1) * WRITE_CHUNK, total)
        bloco = out[r0:r1]
        log(f"📦 Gravando {i+1}/{chunks}: linhas {r0+1}-{r1}…")
        retry(lambda: api.values().update(
            spreadsheetId=DESTINO_ID,
            range=f"{ABA_DESTINO}!A{r0+1}",
            valueInputOption="USER_ENTERED",
            body={"values": bloco}
        ).execute(), f"Gravar destino {r0+1}-{r1}")
        enviados = r1
        log(f"✅ Gravado {enviados}/{total}")
        time.sleep(0.2)

    log(f"🏁 Concluído: {enviados} linhas.")

if __name__ == "__main__":
    main()
