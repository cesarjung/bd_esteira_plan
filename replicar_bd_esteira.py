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

# Cabeçalho padrão solicitado:
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


def get_api():
    """Retorna o serviço completo do Sheets, lendo credenciais do secret GOOGLE_CREDENTIALS ou do arquivo local."""
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

# ============== AUXILIARES ==============
def listar_abas(service, spreadsheet_id: str) -> List[str]:
    meta = retry(
        lambda: service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute(),
        f"Ler metadados da planilha {spreadsheet_id}"
    )
    return [s.get("properties", {}).get("title", "") for s in meta.get("sheets", [])]


def get_sheet_properties(service, spreadsheet_id: str, sheet_title: str) -> Optional[dict]:
    meta = retry(
        lambda: service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute(),
        f"Ler metadados da planilha {spreadsheet_id}"
    )
    for s in meta.get("sheets", []):
        props = s.get("properties", {}) or {}
        if props.get("title") == sheet_title:
            gp = props.get("gridProperties", {}) or {}
            return {
                "sheetId": props.get("sheetId"),
                "rows": gp.get("rowCount", 1000),
                "cols": gp.get("columnCount", 26)
            }
    return None


def ensure_sheet_size(service, spreadsheet_id: str, sheet_title: str,
                      min_rows: int, min_cols: int = 5):
    """
    Garante que a aba tenha pelo menos min_rows linhas e min_cols colunas.
    Se precisar, aumenta o grid via batchUpdate.
    """
    props = get_sheet_properties(service, spreadsheet_id, sheet_title)
    if not props:
        raise RuntimeError(f"Aba '{sheet_title}' não encontrada em {spreadsheet_id}.")

    current_rows = props["rows"]
    current_cols = props["cols"]
    sheet_id     = props["sheetId"]

    new_rows = max(current_rows, min_rows)
    new_cols = max(current_cols, min_cols)

    if new_rows == current_rows and new_cols == current_cols:
        return  # já está grande o suficiente

    body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "rowCount": new_rows,
                            "columnCount": new_cols
                        }
                    },
                    "fields": "gridProperties.rowCount,gridProperties.columnCount"
                }
            }
        ]
    }

    log(f"Ajustando grade de {spreadsheet_id}:{sheet_title} de {current_rows}x{current_cols} para {new_rows}x{new_cols}")
    retry(
        lambda: service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute(),
        f"Ajustar linhas/colunas de {spreadsheet_id}:{sheet_title}"
    )


def col_letter_to_index(letter: str) -> int:
    n = 0
    for ch in letter.strip().upper():
        n = n * 26 + (ord(ch) - ord('A') + 1)
    return n


def achar_aba_config(service) -> str:
    for nome in CONFIG_CANDIDATAS:
        props = get_sheet_properties(service, ORIGEM_ID, nome)
        if props:
            log(f"Aba de Config encontrada: {nome} ({props['rows']} linhas, {props['cols']} colunas)")
            return nome
    disponíveis = listar_abas(service, ORIGEM_ID)
    raise RuntimeError(f"Nenhuma aba de configuração encontrada. Abas disponíveis: {', '.join(disponíveis)}")

# ============== LEITURA DE CONFIG ==============
def ler_pares_config(service, aba_config: str) -> List[Tuple[str, str]]:
    props = get_sheet_properties(service, ORIGEM_ID, aba_config)
    need_col = max(col_letter_to_index(COL_FILTRO), col_letter_to_index(COL_DESTID))
    if props["cols"] < need_col:
        raise RuntimeError(f"Aba '{aba_config}' não tem colunas suficientes (precisa até {COL_DESTID}).")

    rows = max(props["rows"], START_ROW + 1)
    bh_rng = f"{aba_config}!{COL_FILTRO}{START_ROW}:{COL_FILTRO}{rows}"
    bi_rng = f"{aba_config}!{COL_DESTID}{START_ROW}:{COL_DESTID}{rows}"

    res = retry(
        lambda: service.spreadsheets().values().batchGet(
            spreadsheetId=ORIGEM_ID, ranges=[bh_rng, bi_rng]
        ).execute(),
        "Ler Config (BH/BI)"
    )
    vrs = res.get("valueRanges", [])
    bh_vals = vrs[0].get("values", []) if len(vrs) > 0 else []
    bi_vals = vrs[1].get("values", []) if len(vrs) > 1 else []

    pares = []
    for i in range(max(len(bh_vals), len(bi_vals))):
        bh = (bh_vals[i][0].strip() if i < len(bh_vals) and bh_vals[i] else "")
        bi = (bi_vals[i][0].strip() if i < len(bi_vals) and bi_vals[i] else "")
        if bi:
            pares.append((bh, bi))
    return pares

# ============== FONTE (BD_ESTEIRA) ==============
def ler_esteira_origem(service) -> List[List[str]]:
    res = retry(
        lambda: service.spreadsheets().values().get(
            spreadsheetId=ORIGEM_ID, range=f"{ABA_FONTE}!A:E"
        ).execute(),
        f"Ler origem {ABA_FONTE}!A:E"
    )
    return res.get("values", [])


_num_like = re.compile(r"^\s*[-+]?\d+([.,]\d+)?\s*$")


def _tem_cabecalho_aparente(primeira_linha: List[str]) -> bool:
    for cel in (primeira_linha or []):
        s = str(cel or "").strip()
        if not s:
            continue
        if not _num_like.match(s):
            return True
    return False


def obter_header(orig_values: List[List[str]]) -> List[str]:
    if orig_values and _tem_cabecalho_aparente(orig_values[0]):
        return (orig_values[0] + [""]*5)[:5]
    return DEFAULT_HEADER[:]


def filtrar_por_col_E(values: List[List[str]], filtro: str) -> List[List[str]]:
    """Filtra linhas onde a coluna E == filtro; sempre inclui cabeçalho definido."""
    header = obter_header(values)
    data   = values[1:] if values and _tem_cabecalho_aparente(values[0]) else values
    out = [header]
    alvo = (filtro or "").strip()
    for r in data:
        r = (r + [""]*5)[:5]
        if r[4].strip() == alvo:
            out.append(r)
    return out

# ============== DESTINO ==============
def planilha_tem_aba(service, spreadsheet_id: str, sheet_title: str) -> bool:
    return get_sheet_properties(service, spreadsheet_id, sheet_title) is not None


def limpar_destino(service, dest_id: str, sheet_title: str):
    retry(
        lambda: service.spreadsheets().values().clear(
            spreadsheetId=dest_id, range=f"{sheet_title}!A:E"
        ).execute(),
        f"Limpar {dest_id}:{sheet_title}"
    )


def escrever_destino(service, dest_id: str, sheet_title: str, dados: List[List[str]]):
    if not dados:
        return

    total = len(dados)

    # garante que a aba tenha linhas suficientes
    ensure_sheet_size(service, dest_id, sheet_title, min_rows=total, min_cols=5)

    chunks = math.ceil(total / WRITE_CHUNK)
    enviados = 0
    for i in range(chunks):
        r0, r1 = i * WRITE_CHUNK, min((i + 1) * WRITE_CHUNK, total)
        bloco = dados[r0:r1]

        retry(
            lambda: service.spreadsheets().values().update(
                spreadsheetId=dest_id,
                range=f"{sheet_title}!A{r0+1}",
                valueInputOption="USER_ENTERED",
                body={"values": bloco}
            ).execute(),
            f"Escrever {dest_id}:{sheet_title} {r0+1}-{r1}"
        )
        enviados = r1
        log(f"Gravado {enviados}/{total} no destino")

# ============== MAIN ==============
def main():
    log("Iniciando replicação BD_Esteira → destinos (via Config!BH/BI)")
    service = get_api()

    aba_config = achar_aba_config(service)
    pares = ler_pares_config(service, aba_config)
    if not pares:
        log("Nenhum destino encontrado em Config. Encerrando.")
        return
    log(f"Destinos detectados: {len(pares)}")

    fonte = ler_esteira_origem(service)
    if not fonte:
        log("Origem BD_Esteira vazia. Encerrando.")
        return
    tem_header = _tem_cabecalho_aparente(fonte[0]) if fonte else False
    log(f"Fonte carregada: {max(0, len(fonte)-(1 if tem_header else 0))} linhas + {'c/ cabeçalho' if tem_header else 's/ cabeçalho'}")

    for idx, (filtro, dest_id) in enumerate(pares, start=START_ROW):
        try:
            filtro_show = filtro or "(vazio)"
            log(f"Linha {idx} ({aba_config}): '{filtro_show}' → {dest_id}")

            if not planilha_tem_aba(service, dest_id, ABA_DESTINO):
                log(f"Aviso: {dest_id} sem aba '{ABA_DESTINO}'. Pulando.")
                continue

            dados = filtrar_por_col_E(fonte, filtro)
            if not dados:
                dados = [DEFAULT_HEADER[:]]

            log(f"Filtrado: {max(0, len(dados)-1)} linhas (E == '{filtro_show}') – cabeçalho garantido")
            limpar_destino(service, dest_id, ABA_DESTINO)
            escrever_destino(service, dest_id, ABA_DESTINO, dados)
        except HttpError as he:
            log(f"Erro API em {dest_id}: {he}")
        except Exception as e:
            log(f"Erro inesperado em {dest_id}: {e}")

    log("Concluído com sucesso.")


if __name__ == "__main__":
    main()
