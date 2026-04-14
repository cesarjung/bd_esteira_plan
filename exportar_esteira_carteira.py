import time, datetime, random, re, math, os, json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import google_auth_httplib2, httplib2

# === CONFIG ===
ORIGEM_ID   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ORIGEM  = "BD_Carteira"
DESTINO_ID  = "1T6HVLBQi21CIeS64tAjI314TYi2795COOCAakzLV-q0"
ABA_DESTINO = "BD_Esteira"

CRED_FILE = "credenciais.json"

WRITE_CHUNK  = 1200
MAX_RETRIES  = 8
BACKOFF_BASE = 3.0
HTTP_TIMEOUT = 180

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Leitura segmentada mais leve
SEG_INIT = 800
SEG_MIN  = 200
SEG_MAX  = 2000

def log(msg: str) -> None:
    br_now = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
    print(f"[{br_now.strftime('%H:%M:%S')}] {msg}", flush=True)

def retry(fn, desc):
    for att in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            wait = min(90, BACKOFF_BASE * (2 ** (att - 1)) + random.uniform(0, 1.5))
            log(f"⚠️ {desc} — tentativa {att}/{MAX_RETRIES} falhou: {e} | aguardando {round(wait, 1)}s")
            time.sleep(wait)
    raise RuntimeError(f"❌ {desc} — falhou após {MAX_RETRIES} tentativas.")

_num_re = re.compile(r"[^\d,.\-]")

def clean_number_br(v):
    if v is None or v == "":
        return ""
    v = _num_re.sub("", str(v))
    if "," in v and "." in v:
        v = v.replace(".", "").replace(",", ".")
    else:
        v = v.replace(",", ".")
    try:
        return float(v)
    except Exception:
        return ""

def get_credentials():
    env_json = os.getenv("GOOGLE_CREDENTIALS")
    if env_json:
        log("🔑 Usando credenciais do ambiente (GOOGLE_CREDENTIALS).")
        info = json.loads(env_json)
        return Credentials.from_service_account_info(info, scopes=SCOPES)

    log("🔑 Usando credenciais do arquivo local (credenciais.json).")
    return Credentials.from_service_account_file(CRED_FILE, scopes=SCOPES)

def get_services():
    creds = get_credentials()
    http = google_auth_httplib2.AuthorizedHttp(
        creds,
        http=httplib2.Http(timeout=HTTP_TIMEOUT)
    )
    api = build("sheets", "v4", http=http).spreadsheets()
    return api

def count_rows_adaptive(api):
    """
    Conta linhas pela coluna A.
    """
    res = retry(
        lambda: api.values().get(
            spreadsheetId=ORIGEM_ID,
            range=f"{ABA_ORIGEM}!A:A"
        ).execute(),
        "Ler total de linhas (A:A)"
    )
    return len(res.get("values", []))

def read_col_segmented(api, col_letter: str, total: int):
    """
    Lê uma única coluna em blocos adaptativos.
    Retorna lista com tamanho = total, preservando posições.
    """
    seg_size = SEG_INIT
    values = []
    pos = 0

    while pos < total:
        r1 = min(pos + seg_size, total)
        rng = f"{ABA_ORIGEM}!{col_letter}{pos+1}:{col_letter}{r1}"

        try:
            res = retry(
                lambda: api.values().get(
                    spreadsheetId=ORIGEM_ID,
                    range=rng
                ).execute(),
                f"Ler bloco {col_letter} {pos+1}-{r1}"
            )

            bloco = res.get("values", [])
            flat = [row[0] if row else "" for row in bloco]

            # completa linhas ausentes
            expected = r1 - pos
            if len(flat) < expected:
                flat.extend([""] * (expected - len(flat)))

            values.extend(flat)
            pos = r1

            log(f"📥 Coluna {col_letter}: lido {len(values)}/{total}")
            time.sleep(0.15)

            if seg_size < SEG_MAX:
                seg_size = min(seg_size + 100, SEG_MAX)

        except Exception:
            new_seg = max(seg_size // 2, SEG_MIN)
            if new_seg == seg_size:
                raise
            log(f"🔻 Reduzindo segmento da coluna {col_letter}: {seg_size} → {new_seg}")
            seg_size = new_seg

    return values

def ensure_dest_rows(api, required_rows: int):
    ss = retry(
        lambda: api.get(spreadsheetId=DESTINO_ID).execute(),
        "Ler propriedades do destino"
    )

    sheet_props = None
    for sh in ss.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == ABA_DESTINO:
            sheet_props = props
            break

    if not sheet_props:
        raise RuntimeError(f"❌ Aba destino '{ABA_DESTINO}' não encontrada no arquivo destino.")

    sheet_id = sheet_props.get("sheetId")
    current_rows = sheet_props.get("gridProperties", {}).get("rowCount", 0)

    if current_rows >= required_rows:
        log(f"📏 Linhas atuais em {ABA_DESTINO}: {current_rows} (>= {required_rows}) — nenhum ajuste necessário.")
        return

    log(f"📏 Aumentando linhas de {ABA_DESTINO}: {current_rows} → {required_rows}…")

    body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "rowCount": required_rows
                        }
                    },
                    "fields": "gridProperties.rowCount"
                }
            }
        ]
    }

    retry(
        lambda: api.batchUpdate(
            spreadsheetId=DESTINO_ID,
            body=body
        ).execute(),
        f"Aumentar linhas do destino para {required_rows}"
    )

    log(f"✅ Linhas de {ABA_DESTINO} ajustadas para {required_rows}.")

def main():
    log("🚀 BD_Carteira → BD_Esteira (A→A, AB→B, Z→C, X→D, AC→E | leitura otimizada por colunas)")

    api = get_services()

    # 1) Conta total de linhas
    total = count_rows_adaptive(api)
    if total == 0:
        log("⚠️ Nenhuma linha encontrada na origem.")
        return

    log(f"🔢 Total detectado: {total}")

    # 2) Lê apenas as colunas necessárias
    log("📥 Iniciando leitura segmentada somente das colunas necessárias...")
    col_a  = read_col_segmented(api, "A",  total)
    col_x  = read_col_segmented(api, "X",  total)
    col_z  = read_col_segmented(api, "Z",  total)
    col_ab = read_col_segmented(api, "AB", total)
    col_ac = read_col_segmented(api, "AC", total)

    # 3) Monta saída
    log("🧪 Preparando dados finais...")
    out = []
    for idx in range(total):
        a  = col_a[idx] if idx < len(col_a) else ""
        x  = col_x[idx] if idx < len(col_x) else ""
        z  = col_z[idx] if idx < len(col_z) else ""
        ab = col_ab[idx] if idx < len(col_ab) else ""
        ac = col_ac[idx] if idx < len(col_ac) else ""

        # preserva cabeçalho na linha 1
        if idx == 0:
            out.append([a, ab, z, x, ac])
        else:
            out.append([
                a,
                clean_number_br(ab),
                z,
                clean_number_br(x),
                ac
            ])

    # 4) Garante linhas suficientes no destino
    ensure_dest_rows(api, total)

    # 5) Limpa destino
    retry(
        lambda: api.values().clear(
            spreadsheetId=DESTINO_ID,
            range=f"{ABA_DESTINO}!A:E"
        ).execute(),
        "Limpar destino"
    )
    log("🧹 Destino limpo.")

    # 6) Escreve em blocos
    chunks = math.ceil(total / WRITE_CHUNK)
    enviados = 0

    for i in range(chunks):
        r0 = i * WRITE_CHUNK
        r1 = min((i + 1) * WRITE_CHUNK, total)
        bloco = out[r0:r1]

        log(f"📦 Gravando {i+1}/{chunks}: linhas {r0+1}-{r1}…")

        retry(
            lambda: api.values().update(
                spreadsheetId=DESTINO_ID,
                range=f"{ABA_DESTINO}!A{r0+1}",
                valueInputOption="USER_ENTERED",
                body={"values": bloco}
            ).execute(),
            f"Gravar destino {r0+1}-{r1}"
        )

        enviados = r1
        log(f"✅ Gravado {enviados}/{total}")
        time.sleep(0.15)

    # 7) Timestamp em G2
    br_now = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
    timestamp = br_now.strftime("%d/%m/%Y %H:%M:%S")

    retry(
        lambda: api.values().update(
            spreadsheetId=DESTINO_ID,
            range=f"{ABA_DESTINO}!G2",
            valueInputOption="USER_ENTERED",
            body={"values": [[timestamp]]}
        ).execute(),
        "Gravar timestamp em G2"
    )

    log(f"🕒 Timestamp gravado em {ABA_DESTINO}!G2: {timestamp}")
    log(f"🏁 Concluído: {enviados} linhas.")

if __name__ == "__main__":
    main()
