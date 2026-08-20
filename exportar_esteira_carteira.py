import time, datetime, random, re, math, os, json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google_auth_httplib2, httplib2

# === CONFIG ===
ORIGEM_ID   = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ABA_ORIGEM  = "BD_Carteira"
DESTINO_ID  = "1T6HVLBQi21CIeS64tAjI314TYi2795COOCAakzLV-q0"
ABA_DESTINO = "BD_Esteira"

CRED_FILE = "credenciais.json"

WRITE_CHUNK  = 1200
MAX_RETRIES  = 4
BACKOFF_BASE = 3.0
# A origem (planilha de ~5,5 MB, editada durante o expediente) chega a levar
# mais de um minuto por resposta. Com 60s nenhuma tentativa completava.
HTTP_TIMEOUT = 180

# Status HTTP em que insistir nao adianta (permissao, range invalido, aba
# inexistente). Levanta na hora em vez de gastar MAX_RETRIES tentativas.
STATUS_PERMANENTES = {400, 401, 403, 404}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Colunas lidas da origem, na ordem em que sao usadas na saida.
COLS_ORIGEM = ("A", "X", "Z", "AB", "AC")

# Leitura da origem: caminho rapido (colunas inteiras de uma vez) e, se ele
# falhar, caminho degradado em faixas de linhas - requisicoes menores, que a
# origem consegue servir mesmo quando esta lenta.
RETRIES_RAPIDO = 2      # ~6 min no pior caso
RETRIES_BLOCO  = 3
READ_BLOCO     = 2000   # linhas por faixa no caminho degradado
READ_BUDGET_S  = 900    # teto de 15 min para a leitura inteira da origem
MAX_LINHAS     = 200000 # trava contra laco infinito no caminho degradado

def log(msg: str) -> None:
    br_now = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
    print(f"[{br_now.strftime('%H:%M:%S')}] {msg}", flush=True)

def retry(fn, desc, tentativas=None, deadline=None):
    """
    `deadline` e um instante de time.monotonic(). Serve para limitar a leitura
    da origem como um todo, e nao so cada chamada isolada.
    """
    tentativas = tentativas or MAX_RETRIES

    for att in range(1, tentativas + 1):
        if deadline is not None and time.monotonic() >= deadline:
            raise RuntimeError(f"❌ {desc} — orçamento de tempo esgotado.")

        try:
            return fn()
        except HttpError as e:
            if getattr(e, "resp", None) is not None and e.resp.status in STATUS_PERMANENTES:
                log(f"❌ {desc} — erro permanente HTTP {e.resp.status}, sem retry: {e}")
                raise
            err = e
        except Exception as e:
            err = e

        if att == tentativas:
            log(f"⚠️ {desc} — tentativa {att}/{tentativas} falhou: {err}")
            break

        wait = min(30, BACKOFF_BASE * (2 ** (att - 1)) + random.uniform(0, 1.5))
        log(f"⚠️ {desc} — tentativa {att}/{tentativas} falhou: {err} | aguardando {round(wait, 1)}s")
        time.sleep(wait)

    raise RuntimeError(f"❌ {desc} — falhou após {tentativas} tentativas.")

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

def _extrair_colunas(res):
    """Achata a resposta de um batchGet com majorDimension=COLUMNS em 5 listas."""
    brutas = []
    for vr in res.get("valueRanges", []):
        vals = vr.get("values") or []
        brutas.append(list(vals[0]) if vals else [])

    if len(brutas) != len(COLS_ORIGEM):
        raise RuntimeError(
            f"batchGet retornou {len(brutas)} faixas, esperado {len(COLS_ORIGEM)}."
        )
    return brutas

def _ler_colunas_inteiras(api, deadline):
    """Caminho rápido: as 5 colunas inteiras em uma única requisição."""
    ranges = [f"{ABA_ORIGEM}!{c}:{c}" for c in COLS_ORIGEM]
    res = retry(
        lambda: api.values().batchGet(
            spreadsheetId=ORIGEM_ID,
            ranges=ranges,
            majorDimension="COLUMNS"
        ).execute(),
        f"Ler colunas {'/'.join(COLS_ORIGEM)} da origem (batchGet único)",
        tentativas=RETRIES_RAPIDO,
        deadline=deadline
    )
    return _extrair_colunas(res)

def _linhas_da_aba_origem(api, deadline):
    """
    Altura da grade da aba de origem, via metadados (não lê célula nenhuma).
    Serve de limite superior para o caminho degradado.
    """
    res = retry(
        lambda: api.get(
            spreadsheetId=ORIGEM_ID,
            fields="sheets(properties(title,gridProperties(rowCount)))"
        ).execute(),
        "Ler grade da aba de origem",
        tentativas=RETRIES_BLOCO,
        deadline=deadline
    )

    for sh in res.get("sheets", []):
        props = sh.get("properties", {}) or {}
        if props.get("title") == ABA_ORIGEM:
            return props.get("gridProperties", {}).get("rowCount", 0)

    raise RuntimeError(f"Aba de origem '{ABA_ORIGEM}' não encontrada em {ORIGEM_ID}.")

def _ler_colunas_em_blocos(api, deadline):
    """
    Caminho degradado: as 5 colunas em faixas de READ_BLOCO linhas.

    Requisições menores, que a origem consegue servir mesmo lenta. Continua
    sendo 1 request por faixa (~7 para 12k linhas), não os ~80 da versão que
    lia coluna por coluna.
    """
    limite = min(_linhas_da_aba_origem(api, deadline), MAX_LINHAS)
    log(f"📐 Grade da origem: {limite} linhas. Lendo em faixas de {READ_BLOCO}.")

    acc = {c: [] for c in COLS_ORIGEM}
    pos = 0
    vazias_seguidas = 0

    while pos < limite:
        r0, r1 = pos + 1, min(pos + READ_BLOCO, limite)
        ranges = [f"{ABA_ORIGEM}!{c}{r0}:{c}{r1}" for c in COLS_ORIGEM]

        res = retry(
            lambda: api.values().batchGet(
                spreadsheetId=ORIGEM_ID,
                ranges=ranges,
                majorDimension="COLUMNS"
            ).execute(),
            f"Ler faixa {r0}-{r1} das 5 colunas",
            tentativas=RETRIES_BLOCO,
            deadline=deadline
        )

        bloco = _extrair_colunas(res)
        largura = r1 - r0 + 1

        for letra, vals in zip(COLS_ORIGEM, bloco):
            vals = vals[:largura]
            vals.extend([""] * (largura - len(vals)))
            acc[letra].extend(vals)

        pos = r1

        # Uma faixa vazia nas 5 colunas pode ser só um vão no meio dos dados,
        # então não encerra na primeira. Duas seguidas (READ_BLOCO*2 linhas em
        # branco) significam que o fim já passou — evita varrer a grade toda.
        if all(len(v) == 0 for v in bloco):
            vazias_seguidas += 1
            if vazias_seguidas >= 2:
                log(f"⏹️ Duas faixas vazias seguidas até {r1}: fim dos dados.")
                break
        else:
            vazias_seguidas = 0
            log(f"📥 Faixa {r0}-{r1} lida.")

        time.sleep(0.15)

    return [acc[c] for c in COLS_ORIGEM]

def ler_colunas(api):
    """
    Lê as colunas necessárias da origem.

    Tenta primeiro em uma requisição só. Se a origem não der conta (é uma
    planilha de ~5,5 MB editada durante o expediente), cai para faixas de
    linhas. Os dois caminhos compartilham um orçamento de READ_BUDGET_S, e o
    `timeout-minutes` do workflow é a trava final.

    A versão original lia coluna por coluna em blocos adaptativos: ~80 requests,
    cada bloco lento multiplicado por MAX_RETRIES e depois repetido com o
    segmento pela metade — daí os runs de 3 a 6 horas.

    Retorna (total, {letra: lista de tamanho `total`}).
    """
    deadline = time.monotonic() + READ_BUDGET_S

    try:
        brutas = _ler_colunas_inteiras(api, deadline)
    except Exception as e:
        log(f"⚠️ Leitura em uma tomada falhou: {e}")
        log(f"↩️ Caindo para leitura em faixas de {READ_BLOCO} linhas.")
        brutas = _ler_colunas_em_blocos(api, deadline)

    # A coluna A define o total de linhas (mesmo critério da versão anterior:
    # comprimento de A:A, que já vem sem as linhas vazias do fim).
    col_a = brutas[0]
    total = len(col_a)
    while total > 0 and str(col_a[total - 1]).strip() == "":
        total -= 1

    cols = {}
    for letra, vals in zip(COLS_ORIGEM, brutas):
        vals = vals[:total]
        vals.extend([""] * (total - len(vals)))   # ranges abertos vêm truncados
        cols[letra] = vals
        log(f"📥 Coluna {letra}: {len(vals)} linhas")

    return total, cols

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

    # 1) Lê apenas as colunas necessárias, em uma única chamada
    log("📥 Lendo colunas necessárias da origem (batchGet único)...")
    total, cols = ler_colunas(api)

    if total == 0:
        log("⚠️ Nenhuma linha encontrada na origem.")
        return

    log(f"🔢 Total detectado: {total}")

    col_a  = cols["A"]
    col_x  = cols["X"]
    col_z  = cols["Z"]
    col_ab = cols["AB"]
    col_ac = cols["AC"]

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
