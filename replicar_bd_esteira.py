import datetime, time, random, math, re, os, json
from typing import List, Tuple, Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google_auth_httplib2, httplib2

# ============== CONFIG ==============
ORIGEM_ID   = "1T6HVLBQi21CIeS64tAjI314TYi2795COOCAakzLV-q0"  # planilha origem
CONFIG_CANDIDATAS = ["Config", "BD_Config", "config", "CONFIG"]

COL_FILTRO  = "BH"   # valor comparado à COLUNA E da BD_Esteira
COL_DESTID  = "BI"   # ID da planilha destino
START_ROW   = 3

ABA_FONTE   = "BD_Esteira"
ABA_DESTINO = "BD_Esteira"

# usado só como fallback local
CRED_FILE   = "credenciais.json"

SCOPES       = ["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
HTTP_TIMEOUT = 600
MAX_RETRIES  = 8
BACKOFF_BASE = 3.0
WRITE_CHUNK  = 1500

DEFAULT_HEADER = [
    "Projeto",
    "Valor Considerado",
    "Status Esteira",
    "Valor Recebido",
    "Unidade"
]

# ============== LOG / RETRY ==============
def log(msg: str):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def retry(fn, desc: str):
    for att in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            wait = min(90, BACKOFF_BASE * (2 ** (att - 1)) + random.uniform(0, 1.5))
            log(f"Aviso: {desc} — tentativa {att}/{MAX_RETRIES} falhou: {e} | aguardando {round(wait,1)}s")
            time.sleep(wait)
    raise RuntimeError(f"{desc} — falhou após {MAX_RETRIES} tentativas.")

# ============== AUTENTICAÇÃO ==============
def get_api():
    env_json = os.getenv("GOOGLE_CREDENTIALS")
    if env_json:
        try:
            info = json.loads(env_json)
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
            log("🔑 Credenciais carregadas de GOOGLE_CREDENTIALS (env).")
        except Exception as e:
            log(f"❌ Erro ao ler GOOGLE_CREDENTIALS, tentando credenciais.json: {e}")
            creds = Credentials.from_service_account_file(CRED_FILE, scopes=SCOPES)
    else:
        log("ℹ️ GOOGLE_CREDENTIALS não definido, usando credenciais.json (local).")
        creds = Credentials.from_service_account_file(CRED_FILE, scopes=SCOPES)

    http  = google_auth_httplib2.AuthorizedHttp(creds, http=httplib2.Http(timeout=HTTP_TIMEOUT))
    return build("sheets", "v4", http=http)

# ============== AUXILIAR ==============
def listar_abas(service, spreadsheet_id: str) -> List[str]:
    meta = retry(lambda: service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute(),
                 f"Ler metadados da planilha {spreadsheet_id}")
    return [s["properties"]["title"] for s in meta.get("sheets", [])]

def get_sheet_properties(service, spreadsheet_id: str, sheet_title: str) -> Optional[dict]:
    meta = retry(lambda: service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute(),
                 f"Ler metadados da planilha {spreadsheet_id}")
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_title:
            gp = props.get("gridProperties", {})
            return {
                "sheetId": props.get("sheetId"),
                "rows": gp.get("rowCount", 1000),
                "cols": gp.get("columnCount", 26)
            }
    return None

def ensure_sheet_size(service, spreadsheet_id, sheet_title, min_rows, min_cols=5):
    props = get_sheet_properties(service, spreadsheet_id, sheet_title)
    if not props:
        raise RuntimeError(f"Aba '{sheet_title}' não encontrada em {spreadsheet_id}")

    rows = props["rows"]
    cols = props["cols"]

    if rows >= min_rows and cols >= min_cols:
        return

    body = {
        "requests": [{
            "updateSheetProperties": {
                "properties": {
                    "sheetId": props["sheetId"],
                    "gridProperties": {"rowCount": max(rows, min_rows),
                                       "columnCount": max(cols, min_cols)}
                },
                "fields": "gridProperties"
            }
        }]
    }

    retry(lambda: service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body).execute(),
        f"Aumentar grade da aba {sheet_title}")

# ============== CONFIG ==============
def col_letter_to_index(letter: str) -> int:
    n = 0
    for ch in letter.upper():
        n = n * 26 + (ord(ch) - 64)
    return n

def achar_aba_config(service):
    for nome in CONFIG_CANDIDATAS:
        if get_sheet_properties(service, ORIGEM_ID, nome):
            log(f"Aba de Config encontrada: {nome}")
            return nome
    raise RuntimeError("Nenhuma aba de config encontrada")

def ler_pares_config(service, aba_config):
    props = get_sheet_properties(service, ORIGEM_ID, aba_config)
    max_col = max(col_letter_to_index(COL_FILTRO), col_letter_to_index(COL_DESTID))

    if props["cols"] < max_col:
        raise RuntimeError(f"Aba '{aba_config}' não tem colunas até {COL_DESTID}")

    rows = props["rows"]
    bh_rng = f"{aba_config}!{COL_FILTRO}{START_ROW}:{COL_FILTRO}{rows}"
    bi_rng = f"{aba_config}!{COL_DESTID}{START_ROW}:{COL_DESTID}{rows}"

    res = retry(lambda: service.spreadsheets().values().batchGet(
        spreadsheetId=ORIGEM_ID, ranges=[bh_rng, bi_rng]).execute(),
        "Ler BH/BI")

    bh_vals = res["valueRanges"][0].get("values", [])
    bi_vals = res["valueRanges"][1].get("values", [])

    pares = []
    for i in range(max(len(bh_vals), len(bi_vals))):
        bh = bh_vals[i][0].strip() if i < len(bh_vals) and bh_vals[i] else ""
        bi = bi_vals[i][0].strip() if i < len(bi_vals) and bi_vals[i] else ""
        if bi:
            pares.append((bh, bi))

    return pares

# ============== BD_ESTEIRA ORIGEM ==============
def ler_esteira_origem(service):
    res = retry(lambda: service.spreadsheets().values().get(
        spreadsheetId=ORIGEM_ID,
        range=f"{ABA_FONTE}!A:E").execute(),
        "Ler origem A:E")
    return res.get("values", [])

def _tem_cabecalho_aparente(linha):
    for c in linha:
        if c and not re.match(r"^-?\d+[.,]?\d*$", str(c)):
            return True
    return False

def obter_header(vals):
    if vals and _tem_cabecalho_aparente(vals[0]):
        return (vals[0] + [""]*5)[:5]
    return DEFAULT_HEADER

def filtrar_por_col_E(vals, filtro):
    header = obter_header(vals)
    data = vals[1:] if _tem_cabecalho_aparente(vals[0]) else vals
    out = [header]
    for r in data:
        r = (r + [""]*5)[:5]
        if r[4].strip() == filtro.strip():
            out.append(r)
    return out

# ============== DESTINO ==============
def limpar_destino(service, dest_id, sheet_title):
    retry(lambda: service.spreadsheets().values().clear(
        spreadsheetId=dest_id, range=f"{sheet_title}!A:E").execute(),
        f"Limpar {dest_id}:{sheet_title}")

def escrever_destino(service, dest_id, sheet_title, dados):
    total = len(dados)
    ensure_sheet_size(service, dest_id, sheet_title, total, 5)

    chunks = math.ceil(total / WRITE_CHUNK)
    for i in range(chunks):
        r0 = i * WRITE_CHUNK
        r1 = min((i+1)*WRITE_CHUNK, total)
        bloco = dados[r0:r1]

        retry(lambda: service.spreadsheets().values().update(
            spreadsheetId=dest_id,
            range=f"{sheet_title}!A{r0+1}",
            valueInputOption="USER_ENTERED",
            body={"values": bloco}
        ).execute(), f"Escrever linhas {r0+1}-{r1} no destino")

# ============== MAIN ==============
def main():
    log("Iniciando replicação BD_Esteira → destinos")
    service = get_api()

    aba_config = achar_aba_config(service)
    pares = ler_pares_config(service, aba_config)

    if not pares:
        log("Nenhum destino encontrado.")
        return

    fonte = ler_esteira_origem(service)
    header_present = _tem_cabecalho_aparente(fonte[0])

    for idx, (filtro, dest_id) in enumerate(pares, start=START_ROW):

        try:
            log(f"➡️ {idx}: Filtro '{filtro}' → destino {dest_id}")

            if not planilha_tem_aba(service, dest_id, ABA_DESTINO):
                log(f"Aba '{ABA_DESTINO}' não existe em {dest_id}. Pulando.")
                continue

            dados = filtrar_por_col_E(fonte, filtro)
            limpar_destino(service, dest_id, ABA_DESTINO)
            escrever_destino(service, dest_id, ABA_DESTINO, dados)

            # ========== TIMESTAMP G2 ==========
            timestamp = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            retry(lambda: service.spreadsheets().values().update(
                spreadsheetId=dest_id,
                range=f"{ABA_DESTINO}!G2",
                valueInputOption="USER_ENTERED",
                body={"values": [[timestamp]]}
            ).execute(), f"Escrever timestamp em {dest_id}:{ABA_DESTINO}!G2")
            log(f"🕒 Timestamp gravado em {dest_id} → G2")

        except Exception as e:
            log(f"❌ Erro no destino {dest_id}: {e}")

    log("🎉 Concluído com sucesso.")


if __name__ == "__main__":
    main()
