import os
import sys
import logging
import math
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import date, datetime as dt
from typing import Any, Dict, List, Tuple
import pymysql
import argparse
from dotenv import load_dotenv 
import time
from typing import Any, Dict, List
import logging
from collections import defaultdict
import traceback



# ----------- CARREGA .env / CONFIGURAÇÃO EXTERNA ----------------
load_dotenv()  # carrega variáveis de ambiente de um arquivo .env opcional

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "bancotr")
DB_CHARSET = os.getenv("DB_CHARSET", "utf8mb4")

CodNoh = os.getenv("COD_NOH", "1")
VersaoBase = os.getenv("VERSAO_BASE", "v1.0")

BASE_ROOT = Path(os.getenv("BASE_ROOT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "bancotr")))


# flags e constantes iniciais (equivalente ao PHP)
NO_COS = False
NO_COR = False
NO_CPS = False
NO_ONS = 20
CONEX_COS_ONS = 21
CONEX_ONS_COS = 22
CONEX_COR_ONS = 54
CONEX_ONS_COR = 55
CONEX_COR_COS = 125
SES_GRPS_440_525 = ["YTA", "MAT", "PRT"]  # equivalente a "'YTA','MAT','PRT'" (lista para facilitar uso)
CodNoh = "1"  # será sobrescrito abaixo se passado por argumento
VersaoBase = "."
VersaoNumBase = 0
Regerar = ""
MaxIdSize = 63
GestaoDaComunicacao = 0
VarrBaseHist = 15  # período base para varredura do histórico
TimeIni = int(time.time())

# sobrescreve com argumentos posicionais como no PHP: primeiro cod_noh, segundo versão
# os argumentos já estão sendo parseados por argparse; aqui assumimos que você tem `args`
# caso esteja antes de parse_args, use sys.argv diretamente:

if len(sys.argv) > 1:
    CodNoh = sys.argv[1]
if len(sys.argv) > 2:
    try:
        VersaoNumBase = int(sys.argv[2])
        VersaoBase = f"v{VersaoNumBase}"
    except ValueError:
        VersaoBase = str(sys.argv[2])
else:
    VersaoBase = "."

if len(sys.argv) > 3:
    Regerar = sys.argv[3]

# comportamento do switch PHP
if CodNoh == "1" or CodNoh == 1:
    NO_COS = True
elif CodNoh == "181" or CodNoh == 181:
    NO_COR = True
elif str(CodNoh).lower() == "cps":
    NO_CPS = True
    CodNoh = "181"  # trata 'cps' como alias de 181

# EMS
EMS = 1 if (NO_COS or NO_COR or NO_CPS) else 0

DescrNoh = ""
NumReg: dict = {}

# valores adicionais
COMENT = 1
MaxPontosPorTAC = 2550
MaxPontosDigPorTAC = 2550
MaxPontosPorTAC_Calc = 1020
MaxPontosPorTDD = 1024
MaxPontosDigPorTDD = 2560
MaxPontosAnaPorTDD = 1024
# ------------------------------------------------------


def setup_logging(log_path: Path, level: int = logging.INFO) -> None:
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(level)
    if not any(isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "") == str(log_path) for h in root.handlers):
        root.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)


def build_paths() -> Dict[str, Path]:
    base = BASE_ROOT / f"no_{CodNoh}"
    paths = {
        "manuais": base / "manuais",
        "automaticos": base / "automaticos",
        "base_gerada": base / "base-gerada" / VersaoBase,
        "dats_unir": base / "dats_unir"
    }
    for name, p in paths.items():
        p.mkdir(parents=True, exist_ok=True)
        logging.info(f"[setup] Diretório '{name}' garantido em: {p}")
    return paths


def connect_db():
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            charset=DB_CHARSET,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        logging.info("Conectado ao MySQL.")
        return conn
    except Exception as e:
        logging.error(f"Falha ao conectar no banco: {e}")
        raise

def load_conexoes(conn, cod_noh: str):
    """
    Retorna um dict com:
      - conexoes_dst: lista de cod_conexao (ou [-1] se vazio)
      - conexoes_org: lista de cod_conexao (ou [-1] se vazio)
      - descr_noh: última descricao lida
      - lia_bidirecional: lista de id_sage_aq onde cod_protocolo == 10 (destino)
    """
    descr_noh = ""
    lia_bidirecional = []
    conexoes_dst = []
    conexoes_org = []

    # destino (onde este nó é destino), desconsidera end_dst == 0
    dst_sql = """
SELECT 
    c.cod_conexao AS cod_conexao,
    n.nome AS nome_noh,
    n.descricao AS descr_noh,
    c.cod_protocolo AS cod_protocolo,
    c.id_sage_aq AS id_sage_aq
FROM
    id_conexoes c,
    id_nohsup n
WHERE
    n.cod_nohsup = %s
        AND n.cod_nohsup = c.cod_noh_dst
        AND c.end_dst != 0
    """
    try:
        with conn.cursor() as cur:
            cur.execute(dst_sql, (cod_noh,))
            rows = cur.fetchall()
            if not rows:
                logging.info(f"[load_conexoes] Nenhuma conexão de destino para cod_noh={cod_noh}")
            for linha in rows:
                conexoes_dst.append(linha.get("cod_conexao"))
                descr_noh = linha.get("descr_noh", "") or descr_noh
                if linha.get("cod_protocolo") == 10:
                    id_sage_aq = linha.get("id_sage_aq")
                    if id_sage_aq:
                        lia_bidirecional.append(id_sage_aq)
    except Exception as e:
        logging.error(f"[load_conexoes] Erro carregando conexoes destino: {e}", exc_info=True)

    # origem (onde este nó é origem), desconsidera end_org == 0
    org_sql = """
SELECT 
    c.cod_conexao AS cod_conexao,
    n.nome AS nome_noh,
    n.descricao AS descr_noh
FROM
    id_conexoes c,
    id_nohsup n
WHERE
    n.cod_nohsup = %s
        AND n.cod_nohsup = c.cod_noh_org
        AND c.end_org != 0
    """
    try:
        with conn.cursor() as cur:
            cur.execute(org_sql, (cod_noh,))
            rows = cur.fetchall()
            if not rows:
                logging.info(f"[load_conexoes] Nenhuma conexão de origem para cod_noh={cod_noh}")
            for linha in rows:
                conexoes_org.append(linha.get("cod_conexao"))
                descr_noh = linha.get("descr_noh", "") or descr_noh
    except Exception as e:
        logging.error(f"[load_conexoes] Erro carregando conexoes origem: {e}", exc_info=True)

    # fallback igual ao PHP que adiciona "-1" para evitar IN vazio
    if not conexoes_dst:
        conexoes_dst = [-1]
    if not conexoes_org:
        conexoes_org = [-1]

    return {
        "conexoes_dst": conexoes_dst,
        "conexoes_org": conexoes_org,
        "descr_noh": descr_noh,
        "lia_bidirecional": lia_bidirecional,
    }

#---------------------------------------------------------------------------------------------------------
# ARQUIVO GRUPO.DAT
# Grupos de Transformadores
def generate_grupo_transformadores_dat(
    paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "grupo"
    destino = Path(paths["dats_unir"]) / f"{ent}-tr.dat"
    first_write = not destino.exists() or force

    sql = """
SELECT DISTINCT
    (e.estacao) AS estacao,
    m.id AS modulo,
    e.descricao AS descr_est
FROM
    id_ponto i
        JOIN
    id_nops n ON i.cod_nops = n.cod_nops
        JOIN
    id_modulos m ON m.cod_modulo = n.cod_modulo
        JOIN
    id_ptlog_noh l ON i.nponto = l.nponto
        JOIN
    id_tpeq t ON t.cod_tpeq = i.cod_tpeq
        JOIN
    id_estacao e ON e.cod_estacao = m.cod_estacao
WHERE
    l.cod_nohsup = %s
        AND i.evento != 'S'
        AND (t.tipo_eq NOT LIKE 'Z%%')
        AND (t.tipo_eq NOT LIKE 'C%%')
        AND ((m.cod_tpmodulo = 3)
        OR (m.cod_tpmodulo = 2
        AND NOT (i.cod_tpeq = 27 AND i.cod_info = 0
        AND i.cod_prot = 0)
        AND NOT (i.cod_tpeq = 28 AND i.cod_info = 0
        AND i.cod_prot = 0)
        AND (t.tipo_eq NOT LIKE 'XC%%')
        AND (t.tipo_eq NOT LIKE 'XS%%')
        AND (n.tipo_nops = 'O')
        AND (t.tipo_eq NOT LIKE 'R%%')))
ORDER BY e.estacao , m.id
    """

    logging.info(f"[{ent}] Executando SQL de grupo.")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (cod_noh,))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    estacoes_count = 0
    grupos_count = 0

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            # separador se for append
            if not first_write:
                fp.write("\n")

            # cabeçalho decorado comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            se_ant = None
            grupo_ant = None

            for pt in rows:
                estacao = str(pt.get("estacao", "")).strip()
                modulo_raw = str(pt.get("modulo", "")).strip()
                descr_est = str(pt.get("descr_est", "")).strip()

                mod = modulo_raw[:4]
                mod = mod.strip(" -")
                grupo = f"{estacao}-{mod}"

                if se_ant != estacao:
                    se_ant = estacao
                    estacoes_count += 1
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"ID=\tTRAFOS-{estacao}\n")
                    fp.write(f"NOME=\tTRAFOS da {descr_est}\n")
                    fp.write("APLIC=\tVTelas\n")
                    fp.write("TIPO=\tOUTROS\n")
                    fp.write("COR_BG=\tOCEANO\n")
                    fp.write("COR_FG=\tAZUL_MEDIO\n")

                if grupo_ant != grupo:
                    grupo_ant = grupo
                    grupos_count += 1
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"ID=\t{grupo}\n")
                    fp.write(f"NOME=\t{mod} da {descr_est}\n")
                    fp.write("APLIC=\tVTelas\n")
                    fp.write("TIPO=\tOUTROS\n")

            # rodapé decorado comentado
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA COM {estacoes_count} ESTAÇÕES E {grupos_count} GRUPOS\n")
            fp.write(f"// Total de registros processados: {len(rows)}\n")
            fp.write(f"{linha_top}\n")
        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {len(rows)} registros processados. Estaçoes={estacoes_count}, Grupos={grupos_count}")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO GRUPO.DAT
# Grupos de Barras
def generate_grupo_barras_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "grupo"
    destino = Path(paths["dats_unir"]) / f"{ent}-barras.dat"
    first_write = not destino.exists() or force

    sql = """
    select distinct(e.estacao) as estacao, m.id as modulo, e.descricao as descr_est, m.descricao as descr_mod
    from id_ponto i
    join id_nops n on i.cod_nops=n.cod_nops
    join id_modulos m on m.cod_modulo=n.cod_modulo
    join id_ptlog_noh l on i.nponto=l.nponto
    join id_tpeq t on t.cod_tpeq=i.cod_tpeq
    join id_estacao e on e.cod_estacao=m.cod_estacao
    where
      i.cod_origem=7
      and l.cod_nohsup=%s
    order by
      e.estacao, m.id
    """

    logging.info(f"[{ent}-barras] Executando SQL de GRUPO BARRAS.")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (cod_noh,))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}-barras] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}-barras] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    estacoes_count = 0
    grupos_count = 0

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # Cabeçalho bonito comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE GRUPO BARRAS  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            se_ant = None
            grupo_ant = None

            for pt in rows:
                estacao = str(pt.get("estacao", "")).strip()
                modulo_raw = str(pt.get("modulo", "")).strip()
                descr_est = str(pt.get("descr_est", "")).strip()
                descr_mod = str(pt.get("descr_mod", "")).strip()

                mod = modulo_raw.strip(" -")  # usa o módulo completo
                grupo = f"CMD-{estacao}-{mod}"

                if se_ant != estacao:
                    se_ant = estacao
                    estacoes_count += 1
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"ID=\tCMD-{estacao}\n")
                    fp.write(f"NOME=\tComandos da {descr_est}\n")
                    fp.write("APLIC=\tVTelas\n")
                    fp.write("TIPO=\tOUTROS\n")
                    fp.write("COR_BG=\tOCEANO\n")
                    fp.write("COR_FG=\tAZUL_MEDIO\n")

                if grupo_ant != grupo:
                    grupo_ant = grupo
                    grupos_count += 1
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"ID=\t{grupo}\n")
                    fp.write(f"NOME=\t{descr_est}: {descr_mod}\n")
                    fp.write("APLIC=\tVTelas\n")
                    fp.write("TIPO=\tOUTROS\n")
                    # cores comentadas conforme original
                    # fp.write("COR_BG=\tOCEANO\n")
                    # fp.write("COR_FG=\tAZUL_MEDIO\n")

            # rodapé decorado comentado
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE GRUPO BARRAS COM {estacoes_count} ESTAÇÕES E {grupos_count} GRUPOS\n")
            fp.write(f"// Total de registros processados: {len(rows)}\n")
            fp.write(f"{linha_top}\n")
        logging.info(f"[{ent}-cmd] gerado em '{destino}' (modo={mode}), {len(rows)} registros. Estaçoes={estacoes_count}, Grupos={grupos_count}")
    except Exception as e:
        logging.error(f"[{ent}-cmd] Erro escrevendo '{destino}': {e}")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO GRUPO.DAT
# Grupos de Disjuntores
def generate_grupo_disjuntor_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "grupo"
    destino = Path(paths["dats_unir"]) / f"{ent}-dj.dat"
    first_write = not destino.exists() or force

    ses_grps_440_525 = tuple(globals().get("SES_GRPS_440_525", ["YTA", "MAT", "PRT"]))

    sql = f"""
    select 
       distinct(e.estacao) as estacao, 
       m.id as modulo, 
       m.descricao as descr_mod, 
       e.descricao as descr_est, 
       i.id, 
       n.nops,
       n.tipo_nops
    from id_ponto i
    join id_nops n on i.cod_nops=n.cod_nops
    join id_modulos m on m.cod_modulo=n.cod_modulo
    join id_ptlog_noh l on i.nponto=l.nponto
    join id_tpeq t on t.cod_tpeq=i.cod_tpeq
    join id_estacao e on e.cod_estacao=m.cod_estacao
    where 
      l.cod_nohsup=%s
      and i.evento!='S' 
      and i.cod_origem not in (5, 6, 11, 24, 16, 17)
      and m.cod_tpmodulo in (1,2,4,5,6,7,9,12,18,19)
      and (
        m.cod_nivtensao not in (0,3,4,5,7,8,9)
        or (e.estacao in {ses_grps_440_525} and m.cod_nivtensao in (4,5))
      )
    order by
      e.estacao, m.id
    """

    logging.info(f"[{ent}-dj] Executando SQL de grupo disjuntores.")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (cod_noh,))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}-dj] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}-dj] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    estacoes_count = 0
    grupos_count = 0

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho decorado comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()} (DISJUNTORES)  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            se_ant = None
            grupo_ant = None

            for pt in rows:
                estacao = str(pt.get("estacao", "")).strip()
                modulo_raw = str(pt.get("modulo", "")).strip()
                descr_mod = str(pt.get("descr_mod", "")).strip()
                descr_est = str(pt.get("descr_est", "")).strip()
                id_pt = str(pt.get("id", "")).strip()

                mod = modulo_raw.strip(" -")
                grupo = f"DJ-{id_pt[:9]}"

                if se_ant != estacao:
                    se_ant = estacao
                    estacoes_count += 1
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"ID=\tDJS-{estacao}\n")
                    fp.write(f"NOME=\tDISJUNTORES da {descr_est}\n")
                    fp.write("APLIC=\tVTelas\n")
                    fp.write("TIPO=\tOUTROS\n")
                    fp.write("COR_BG=\tOCEANO\n")
                    fp.write("COR_FG=\tAZUL_MEDIO\n")

                if grupo_ant != grupo:
                    grupo_ant = grupo
                    grupos_count += 1
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"ID=\t{grupo}\n")
                    fp.write(f"NOME=\t{estacao}:{descr_mod}\n")
                    fp.write("APLIC=\tVTelas\n")
                    fp.write("TIPO=\tOUTROS\n")
                    # fp.write("COR_BG=\tCINZA_PROFUNDO\n")
                    # fp.write("COR_FG=\tCOR_IDENTIFICADOR\n")

            # rodapé decorado comentado
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE DISJUNTORES COM {estacoes_count} ESTAÇÕES E {grupos_count} GRUPOS\n")
            fp.write(f"// Total de registros processados: {len(rows)}\n")
            fp.write(f"{linha_top}\n")
        logging.info(f"[{ent}-dj] gerado em '{destino}' (modo={mode}), {len(rows)} registros processados. Estaçoes={estacoes_count}, Grupos={grupos_count}")
    except Exception as e:
        logging.error(f"[{ent}-dj] Erro escrevendo '{destino}': {e}")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO GRCMP.DAT
# GRCMP de Disjuntores
def generate_grcmp_dj_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ses_grps_440_525: List[str],
    dry_run: bool = False,
    force: bool = False,
):
    ent = "grcmp"
    destino = Path(paths["dats_unir"]) / f"{ent}-dj.dat"
    first_write = not destino.exists() or force

    # Monta cláusula para a lista de SEs (equivalente ao $SES_GRPS_440_525 do PHP)
    if ses_grps_440_525:
        ph_ses = ",".join(["%s"] * len(ses_grps_440_525))
        ses_clause = f"(e.estacao in ({ph_ses}) and m.cod_nivtensao in (4,5))"
        ses_params: Tuple[Any, ...] = tuple(ses_grps_440_525)
    else:
        # Se a lista vier vazia, força a parte do OR a ser falsa,
        # mantendo o mesmo efeito lógico do PHP quando a lista não entra.
        ses_clause = "(0)"
        ses_params = tuple()

    sql = f"""
select 
  e.estacao as estacao, 
  m.id as modulo, 
  m.descricao as descr_mod, 
  e.descricao as descr_est, 
  i.id as id, 
  tpnt.tipo as tipo, 
  i.cod_origem as cod_origem,
  tpnt.unidade as unidade,
  i.traducao_id as traducao_id,
  m.cod_tpmodulo as cod_tpmodulo,
  n.cod_modulo as cod_modulo,
  tpnt.abrev_0 as pres_0,
  tpnt.abrev_1 as pres_1,
  tpnt.cmd_0 as cmd_0,
  tpnt.cmd_1 as cmd_1,
  tp.descricao as tpdescr,
  pr.prot as prot,
  fs.fases as fases,
  i.nponto as nponto,
  coalesce(ik.nponto, 0) as nponto_cmd,
  coalesce(ik.id, '') as id_cmd,
  tpnt.cod_tipopnt as cod_tipopnt,
  case 
    when t.tipo_eq = 'ZTCO' then 0    
    when t.tipo_eq = 'XCBR' then 1   
    when t.tipo_eq = 'RREC' then 2
    when t.tipo_eq = 'RSYN' and tpnt.tipo = 'D' then 3
    when t.tipo_eq like 'RBL%%' then 4
    when t.tipo_eq like 'ATC%%' then 5
    when t.tipo_eq like 'YPA%%' then 6
    when t.tipo_eq like 'RT%%' then 7
    when t.tipo_eq = 'RCLR' then 8
    when t.tipo_eq = 'RTDD' then 9
    when f.info = 'TDDi' then 10
    when f.info = 'POTi' then 11
    when t.tipo_eq = 'RCLV' then 12
    when ik.id is not null then 15
    when t.tipo_eq like 'XCP%%' then 19
    when t.tipo_eq like 'RB%%' then 20
    when t.tipo_eq like 'RC%%' then 22
    when t.tipo_eq like 'RPC%%' then 23
    when f.info = 'AngI' then 900 
    when f.info = 'HzIn' then 901
    when f.info = 'Vind' then 902
    when t.tipo_eq = 'MTVA' then 903
    when t.tipo_eq = 'MTWT' then 904 
    when t.tipo_eq = 'MTVR' then 905 
    when t.tipo_eq = 'MAPH' then 906 
    when t.tipo_eq = 'MFHZ' then 907 
    when t.tipo_eq = 'MVPP' then 908
    when f.info = 'FDkm' then 909
    when tpnt.tipo = 'A' then 910
    else 800 end as sord
from id_ponto i
join id_nops n on i.cod_nops=n.cod_nops
join id_modulos m on m.cod_modulo=n.cod_modulo
join id_ptlog_noh l on i.nponto=l.nponto
join id_tpeq t on t.cod_tpeq=i.cod_tpeq
join id_info f on f.cod_info=i.cod_info
join id_estacao e on e.cod_estacao=m.cod_estacao
join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
join id_prot pr on pr.cod_prot=i.cod_prot
join id_fases fs on fs.cod_fases=i.cod_fases
left outer join id_ponto ik on i.nponto = ik.nponto_sup and ik.nponto in (select nponto from id_ptlog_noh where COD_NOHSUP=%s)
where 
l.cod_nohsup=%s
and i.evento!='S' and i.cod_origem not in (5, 6, 7, 11, 24, 16, 17)
and m.cod_tpmodulo in (1,2,4,5,6,7,9,12,18,19)
and ( m.cod_nivtensao not in (0,3,4,5,7,8,9) or (e.estacao in {ses_clause} and m.cod_nivtensao in (4,5)) )
and f.info not in ('LoDC', 'LoAC', 'PwFl')
and n.tipo_nops!='S'
and (
    (ik.nponto is not null and i.cod_tpeq not in (28,16)) OR
    (i.cod_tpeq = 27 and i.cod_info = 0) OR
    ( t.tipo_eq in ('ZTCO') ) OR
    ( t.tipo_eq in ('RTPR') AND f.info not in ('RcBk','SdPm','RcPm','Recv','Send','Snd1','Alrm','InFl','POTr','POTe','TDDe') ) OR
    ( t.tipo_eq in ('RTFP','RCTF','RCLR','RCLV') ) OR
    ( t.tipo_eq in ('RTPS') ) OR
    ( t.tipo_eq in ('RTPT') ) OR
    ( t.tipo_eq in ('RSYN') ) OR
    ( t.tipo_eq in ('RPC1', 'RPC2') and f.info in ('Locl','Remt')) OR
    ( t.tipo_eq in ('RREC') ) OR
    ( t.tipo_eq in ('ATMT','RFLO') ) OR
    -- ( t.tipo_eq in ('ATCC', 'RBLT', 'YCDC') ) OR
    ( t.tipo_eq like ('RBL%%') ) OR
  	-- ( t.tipo_eq in ('MTWT','MTVR','MTVA','MAPH','MFHZ','MVPP') ) OR
    ( f.info in ('AngI','HzIn','Vind', 'PrmC') ) OR
    ( t.tipo_eq in ('XCPD','XCDS','XCCM','XCDP','XCDS','XCCP') or t.tipo_eq like 'XCP%%' ) 
    -- ( t.tipo_eq in ('XCHD','XCMD','XCBO','XCB1','XCB2','XCBC','XCBI','XCBX','XCC1','XCC2','XCCB','XCCC','XCMJ') or t.tipo_eq like 'XCP%%' )  OR
    -- ( t.tipo_eq like 'XC%%' and f.info in ('LoDC','LoAC') )           
	)
order by
e.estacao, m.id, sord, i.id desc
    """

    params: Tuple[Any, ...] = (cod_noh, cod_noh) + ses_params

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # contadores análogos ao PHP
    cntgrptr = 0
    cnttrest = 0
    cntpntgrp = 0
    cntmodgrp = 0
    cntpntmod = 0
    cod_modant = None

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh}\n")
            fp.write(f"{linha_top}\n\n")

            se_ant = None
            grupo_ant = None

            for pt in rows:
                try:
                    estacao     = (pt.get("estacao") or "").strip()
                    modulo_raw  = (pt.get("modulo") or "").strip()
                    descr_mod   = (pt.get("descr_mod") or "").strip()
                    descr_est   = (pt.get("descr_est") or "").strip()
                    ponto_id    = (pt.get("id") or "").strip()
                    tipo        = (pt.get("tipo") or "").strip()
                    cod_origem  = pt.get("cod_origem")
                    unidade     = (pt.get("unidade") or "").strip()
                    traducao_id = (pt.get("traducao_id") or "").strip()
                    cmd_0       = (pt.get("cmd_0") or "").strip()
                    cmd_1       = (pt.get("cmd_1") or "").strip()
                    cod_modulo  = str(pt.get("cod_modulo") or "").strip()

                    # derivações
                    mod = modulo_raw[:4].strip(" -")
                    grupo = f"{estacao}-{mod}"

                    # mudança de estação
                    if se_ant != estacao:
                        se_ant = estacao
                        ordem1 = 1 + (cntgrptr // 6)
                        ordem2 = 1 + (cntgrptr % 6)
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"GRUPO=\tTRAFOS\n")
                        fp.write(f"PNT=\tTRAFOS-{estacao}\n")
                        fp.write(f"TPPNT=\tGRUPO\n")
                        fp.write(f"ORDEM1=\t{ordem1}\n")
                        fp.write(f"ORDEM2=\t{ordem2}\n")
                        fp.write(f"TPTXT=\tID\n")
                        cntgrptr += 1
                        cnttrest = 0
                        cntpntgrp = 0
                        cntmodgrp = 0
                        cntpntmod = 0
                        cod_modant = None

                    # mudança de grupo
                    if grupo_ant != grupo:
                        grupo_ant = grupo
                        ordem1 = 1 + (cnttrest % 13)
                        ordem2 = 1 + (cnttrest // 13)
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"GRUPO=\tTRAFOS-{estacao}\n")
                        fp.write(f"PNT=\t{grupo}\n")
                        fp.write(f"TPPNT=\tGRUPO\n")
                        fp.write(f"ORDEM1=\t{ordem1}\n")
                        fp.write(f"ORDEM2=\t{ordem2}\n")
                        fp.write(f"CORTXT=\tPRETO\n")
                        fp.write(f"TPTXT=\tID\n")
                        cnttrest += 1
                        cntpntgrp = 0
                        cntmodgrp = 0
                        cntpntmod = 0
                        cod_modant = None

                    # mudança de módulo (colunas)
                    if cod_modant != cod_modulo:
                        cod_modant = cod_modulo
                        if cntmodgrp <= 1:
                            cntmodgrp += 1
                            cntpntmod = 0

                    cntpntmod += 1

                    if cntpntmod <= 35:
                        # monta TXT
                        if cod_origem == 7:
                            txt = f"{traducao_id} {cmd_0}/{cmd_1}"
                            txt = txt.replace(estacao, "").replace(mod, "").strip(" -")
                            tppnt = "CGS"
                            extra = ""
                        elif tipo == "D":
                            txt = traducao_id.replace(estacao, "").replace(mod, "").strip(" -")
                            tppnt = "PDS"
                            extra = "TPSIMB=\tESTADO\n"
                        else:
                            cleaned = traducao_id.replace(estacao, "").replace(mod, "").strip(" -")
                            txt = f"{unidade} {cleaned}".strip()
                            tppnt = "PAS"
                            extra = ""

                        if cntpntmod == 35:
                            txt = "..."

                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"GRUPO=\t{grupo}\n")
                        fp.write(f"PNT=\t{ponto_id}\n")
                        fp.write(f"TPPNT=\t{tppnt}\n")
                        if extra:
                            fp.write(extra)
                        fp.write(f"TXT=\t{txt}\n")
                        fp.write(f"ORDEM1=\t{cntpntmod}\n")
                        fp.write(f"ORDEM2=\t{cntmodgrp}\n")
                        fp.write("CORTXT=\tPRETO\n")
                        fp.write("TPTXT=\tTXT\n")

                        logging.info(f"{ent.upper()}={cntpntgrp:05d} {ponto_id}")

                    cntpntgrp += 1

                except Exception:
                    logging.exception(f"[{ent}] erro processando linha: {pt}")
                    continue

            # rodapé
            fp.write("\n")
            fp.write(f"// {'=' * 70}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros lidos: {len(rows)}\n")
            fp.write(f"// {'=' * 70}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {len(rows)} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO GRCMP.DAT
# GRCMP de TRANSFORMADORES
def generate_grcmp_tr_dat(paths, conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    """
    ARQUIVO GRCMP.DAT : Composição dos Grupos de Transformadores
    Saída: <automaticos>/grcmp-tr.dat
    """
    import logging
    from pathlib import Path
    from datetime import datetime as dt

    ent = "grcmp"
    destino = Path(paths["dats_unir"]) / f"{ent}-tr.dat"
    first_write = not destino.exists() or force
    sql = f"""
select 
  e.estacao as estacao, 
  m.id as modulo, 
  m.descricao as descr_mod, 
  e.descricao as descr_est, 
  i.id as id, 
  tpnt.tipo as tipo, 
  i.cod_origem as cod_origem,
  tpnt.unidade as unidade,
  i.traducao_id as traducao_id,
  m.cod_tpmodulo as cod_tpmodulo,
  n.cod_modulo as cod_modulo,
  tpnt.abrev_0 as pres_0,
  tpnt.abrev_1 as pres_1,
  tpnt.cmd_0 as cmd_0,
  tpnt.cmd_1 as cmd_1,
  tp.descricao as tpdescr,
  pr.prot as prot,
  fs.fases as fases,
  i.nponto as nponto,
  coalesce(ik.nponto, 0) as nponto_cmd,
  coalesce(ik.id, '') as id_cmd,
  tpnt.cod_tipopnt as cod_tipopnt,
  case 
  when t.tipo_eq = 'ZTCO' then 0  
  when t.tipo_eq like 'RB%%' then 1  
  when t.tipo_eq = 'ATCC' then 2
  when t.tipo_eq = 'YPAR' then 3
  when ik.id is not null then 10
  -- when tpnt.tipo = 'A' then 910
  else 800 end as sord
from id_ponto i
join id_nops n on i.cod_nops=n.cod_nops
join id_modulos m on m.cod_modulo=n.cod_modulo
join id_ptlog_noh l on i.nponto=l.nponto
join id_tpeq t on t.cod_tpeq=i.cod_tpeq
join id_info f on f.cod_info=i.cod_info
join id_estacao e on e.cod_estacao=m.cod_estacao
join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
join id_prot pr on pr.cod_prot=i.cod_prot
join id_fases fs on fs.cod_fases=i.cod_fases
left outer join id_ponto ik on i.nponto = ik.nponto_sup and ik.nponto in (select nponto from id_ptlog_noh where COD_NOHSUP=%s)
where   
(select 'S' from id_ponto x where x.cod_origem=1 and i.cod_origem!=1 and x.cod_tpeq=i.cod_tpeq and x.cod_info=i.cod_info and x.cod_nops=i.cod_nops limit 1) is null -- tira os que tem calculados em duplicidade
and l.cod_nohsup=%s
and tpnt.tipo = 'D'
and i.evento!='S' and i.cod_origem not in (5, 6, 7, 11, 24, 16, 17)
and m.cod_tpmodulo in (3)
and tpnt.cod_tipopnt not in (96,97)
and (f.info not in ('SynF','Sprv','InFl','InHt','CmHt','CmFl','MDsj','Fail') )
and (f.info not in ('InFl','InHt','Fail') or t.tipo_eq in ('ATCC'))
and (f.info not in ('LoDC', 'LoAC', 'PwFl'))
and t.tipo_eq not in ('PGRP')
and ( 
  (ik.nponto is not null and t.tipo_eq not in ('YTAP','XCBR')) 
  OR t.tipo_eq in ('YVAL','ZTCO','ATCC','YPBH','YCDC','YCBH','YPBX','YCCM','YPAR','YCLV','PTTI','PTTR','YCOM','YCVA','YDTP','YCPR','YIMC','YPMB','YPVC','YVAS','YXBH','RCLR')
  OR t.tipo_eq like ('RB%%')
  OR (t.tipo_eq like ('YF%%') and  tpnt.cod_tipopnt not in (10,11)) -- inclui VF falhas 
  OR (t.tipo_eq like ('YF%%') and  tpnt.cod_tipopnt in (10,11) and ik.nponto is not null) -- inclui estado da VF somente se tiver comando
  OR t.tipo_eq like ('YI%%')
  OR t.tipo_eq like ('YL%%')
)
order by
e.estacao, m.id, sord, i.id
    """

    logging.info(f"[{ent}-tr] Executando SQL de grcmp transformadores…")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (cod_noh, cod_noh))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}-tr] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}-tr] Dry-run: {len(rows)} linhas retornadas (nenhum arquivo gerado).")
        return

    # ===== Estado de layout =====
    cntgrptr = 0   # contador para blocos TRAFOS-{SE}
    cnttrest = 0   # contador de subgrupos por SE
    cntpntgrp = 0
    cntmodgrp = 0
    cntpntmod = 0
    cod_modant = None

    se_ant = None
    grupo_ant = None

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()} (TRAFOS)  {timestamp}\n")
            fp.write(f"// Nó: {cod_noh}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                try:
                    estacao     = (pt.get("estacao") or "").strip()
                    modulo_raw  = (pt.get("modulo") or "").strip()
                    descr_mod   = (pt.get("descr_mod") or "").strip()
                    descr_est   = (pt.get("descr_est") or "").strip()
                    ponto_id    = (pt.get("id") or "").strip()
                    tipo        = (pt.get("tipo") or "").strip()  # 'D' esperado
                    cod_origem  = pt.get("cod_origem")
                    unidade     = (pt.get("unidade") or "").strip()
                    traducao_id = (pt.get("traducao_id") or "").strip()
                    cmd_0       = (pt.get("cmd_0") or "").strip()
                    cmd_1       = (pt.get("cmd_1") or "").strip()
                    cod_modulo  = str(pt.get("cod_modulo") or "").strip()
                    id_cmd      = (pt.get("id_cmd") or "").strip()
                    nponto_cmd  = pt.get("nponto_cmd") or 0
                    tpdescr     = (pt.get("tpdescr") or "").strip()
                    prot        = (pt.get("prot") or "").strip()
                    fases       = (pt.get("fases") or "").strip()
                    cod_tipopnt = pt.get("cod_tipopnt")

                    # derivação de grupo (4 primeiros chars do módulo, sem traços/esp.)
                    mod = modulo_raw[:4].strip(" -")
                    grupo = f"{estacao}-{mod}"

                    # >>> Mudou a estação? abre o bloco TRAFOS-{SE}
                    if se_ant != estacao:
                        se_ant = estacao
                        ordem1 = 1 + (cntgrptr // 6)
                        ordem2 = 1 + (cntgrptr % 6)
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write("GRUPO=\tTRAFOS\n")
                        fp.write(f"PNT=\tTRAFOS-{estacao}\n")
                        fp.write("TPPNT=\tGRUPO\n")
                        fp.write(f"ORDEM1=\t{ordem1}\n")
                        fp.write(f"ORDEM2=\t{ordem2}\n")
                        fp.write("TPTXT=\tID\n")
                        fp.write("CORTXT=\tPRETO\n")
                        cntgrptr += 1
                        cnttrest = 0
                        cntpntgrp = 0
                        cntmodgrp = 0
                        cntpntmod = 0
                        cod_modant = None
                        grupo_ant = None

                    # >>> Mudou o subgrupo (SE-MOD)?
                    if grupo_ant != grupo:
                        grupo_ant = grupo
                        ordem1 = 1 + (cnttrest % 13)
                        ordem2 = 1 + (cnttrest // 13)
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"GRUPO=\tTRAFOS-{estacao}\n")
                        fp.write(f"PNT=\t{grupo}\n")
                        fp.write("TPPNT=\tGRUPO\n")
                        fp.write(f"ORDEM1=\t{ordem1}\n")
                        fp.write(f"ORDEM2=\t{ordem2}\n")
                        fp.write("CORTXT=\tPRETO\n")
                        fp.write("TPTXT=\tID\n")
                        cnttrest += 1
                        cntpntgrp = 0
                        cntmodgrp = 0
                        cntpntmod = 0
                        cod_modant = None

                    # >>> Controle de colunas por módulo (máx. 2 colunas)
                    if cod_modant != cod_modulo:
                        cod_modant = cod_modulo
                        if cntmodgrp <= 1:
                            cntmodgrp += 1
                            cntpntmod = 0

                    # Próxima linha no módulo
                    cntpntmod += 1
                    if cntpntmod > 70:
                        continue  # corta após 70 linhas por coluna, igual ao PHP

                    # ====== Texto base (limpando estação e prefixo do módulo) ======
                    def limpa_txt(txt: str) -> str:
                        txt = txt.replace(estacao, "").replace(mod, "").strip(" -")
                        return txt

                    # ====== Monta paind_dcr (descrição do painel) ======
                    paind_dcr = ""
                    if tpdescr:
                        paind_dcr = tpdescr
                        p1 = traducao_id.find("-DJ")
                        p2 = traducao_id.find(":estado")
                        if p1 > 3 and p2 > p1:
                            paind_dcr += " " + traducao_id[p1+3:p2]
                        if prot.startswith("P"):
                            paind_dcr += " (P)"
                        elif prot.startswith("A"):
                            paind_dcr += " (A)"
                        if fases.startswith("01"):
                            paind_dcr += " Ind1"
                        elif fases.startswith("02"):
                            paind_dcr += " Ind2"

                    # ====== Se existe ponto de comando associado, gera antes na coluna 1 ======
                    if id_cmd:
                        fp.write("\n")
                        fp.write(f"; NPONTO={nponto_cmd} {id_cmd}\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"GRUPO=\t{grupo}\n")
                        fp.write(f"PNT=\t{id_cmd}\n")
                        fp.write("TPPNT=\tCGS\n")
                        fp.write(f"ORDEM1=\t{cntpntmod}\n")  # linha
                        fp.write("ORDEM2=\t1\n")             # coluna 1

                    # ====== Bloco principal do ponto ======
                    txt = traducao_id
                    if cod_origem == 7:
                        # comando (em tese não entra aqui por filtro, mas mantém lógica)
                        txt = f"{traducao_id} {cmd_0}/{cmd_1}"
                        txt = limpa_txt(txt)
                        if paind_dcr:
                            txt = paind_dcr
                        fp.write("\n")
                        fp.write(f"; NPONTO={pt.get('nponto')} {ponto_id} {traducao_id}\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"GRUPO=\t{grupo}\n")
                        fp.write(f"PNT=\t{ponto_id}\n")
                        fp.write("TPPNT=\tCGS\n")
                        fp.write(f"TXT=\t{txt.upper()}\n")
                        fp.write(f"ORDEM1=\t{cntpntmod}\n")
                        fp.write("ORDEM2=\t2\n")  # coluna 2 para texto
                        fp.write("CORTXT=\tPRETO\n")
                        fp.write("TPTXT=\tTXT\n")
                    else:
                        # Digital/Analógico
                        base_txt = limpa_txt(txt)
                        if tipo == "D":
                            if paind_dcr:
                                base_txt = paind_dcr
                            fp.write("\n")
                            fp.write(f"; NPONTO={pt.get('nponto')} {ponto_id} {traducao_id}\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"GRUPO=\t{grupo}\n")
                            fp.write(f"PNT=\t{ponto_id}\n")
                            fp.write("TPPNT=\tPDS\n")
                            # Símbolo (subset do PHP; mapeamentos mais comuns)
                            simb = None
                            if cod_tipopnt in (74, 36, 28):
                                simb = "CHECK"
                            elif cod_tipopnt in (37, 29):
                                simb = "CHECK_INV"
                            elif cod_tipopnt in (64, 65, 126, 128, 130):
                                simb = "CIRC"
                            elif cod_tipopnt in (8, 23, 25, 26):
                                simb = "CIRC_SIMPLES"
                            elif cod_tipopnt in (9,):
                                simb = "CIRC_SIMPLES_INV"
                            if simb:
                                fp.write(f"TPSIMB=\t{simb}\n")
                            fp.write(f"TXT=\t{base_txt.upper()}\n")
                            fp.write(f"ORDEM1=\t{cntpntmod}\n")
                            fp.write("ORDEM2=\t2\n")
                            fp.write("CORTXT=\tPRETO\n")
                            fp.write("TPTXT=\tTXT\n")

                            # bloco do ESTADO (coluna 5)
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"GRUPO=\t{grupo}\n")
                            fp.write("TPPNT=\tPDS\n")
                            fp.write(f"PNT=\t{ponto_id}\n")
                            fp.write("TPSIMB=\tESTADO\n")
                            fp.write(f"ORDEM1=\t{cntpntmod}\n")
                            fp.write("ORDEM2=\t5\n")
                        else:
                            # Analógico – não deveria aparecer pela cláusula tpnt.tipo='D',
                            # mas mantemos compatibilidade
                            if paind_dcr:
                                base_txt = paind_dcr
                            else:
                                base_txt = f"{unidade} {base_txt}".strip()
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"GRUPO=\t{grupo}\n")
                            fp.write(f"PNT=\t{ponto_id}\n")
                            fp.write("TPPNT=\tPAS\n")
                            fp.write(f"TXT=\t{base_txt.upper()}\n")
                            fp.write(f"ORDEM1=\t{cntpntmod}\n")
                            fp.write("ORDEM2=\t2\n")
                            fp.write("CORTXT=\tPRETO\n")
                            fp.write("TPTXT=\tTXT\n")
                            # bloco PAS (coluna 5)
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"GRUPO=\t{grupo}\n")
                            fp.write(f"PNT=\t{ponto_id}\n")
                            fp.write("TPPNT=\tPAS\n")
                            fp.write(f"ORDEM1=\t{cntpntmod}\n")
                            fp.write("ORDEM2=\t5\n")

                    cntpntgrp += 1

                except Exception:
                    logging.exception(f"[{ent}-tr] Falha processando linha: {pt}")
                    continue

            # rodapé
            fp.write("\n")
            fp.write(f"// {'=' * 70}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()}-TR  total lidos: {len(rows)}\n")
            fp.write(f"// Arquivo: {destino}\n")
            fp.write(f"// {'=' * 70}\n")

        logging.info(f"[{ent}-tr] gerado em '{destino}' (modo={mode}), {len(rows)} registros lidos.")
    except Exception as e:
        logging.error(f"[{ent}-tr] Erro escrevendo '{destino}': {e}")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO GRCMP.DAT
# GRCMP de BARRAS
def generate_grcmp_barras_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ses_grps_440_525: List[str],
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo GRCMP-barras.dat, seguindo a lógica do script PHP original.
    """
    ent = 'grcmp'
    destino = Path(paths["dats_unir"]) / f"{ent}-barras.dat"
    first_write = not destino.exists() or force
    
    # Prepara os placeholders para a cláusula IN
    ph_ses = ",".join(f"'{s}'" for s in ses_grps_440_525)

    sql = f"""
  select 
e.estacao as estacao, 
m.id as modulo, 
m.descricao as descr_mod, 
e.descricao as descr_est, 
i.id as id, 
tpnt.tipo as tipo, 
i.cod_origem as cod_origem,
tpnt.unidade as unidade,
i.traducao_id as traducao_id,
m.cod_tpmodulo as cod_tpmodulo,
n.cod_modulo as cod_modulo,
tpnt.abrev_0 as pres_0,
tpnt.abrev_1 as pres_1,
tpnt.cmd_0 as cmd_0,
tpnt.cmd_1 as cmd_1,
tp.descricao as tpdescr,
pr.prot as prot,
fs.fases as fases,
i.nponto as nponto,
coalesce(ik.nponto, 0) as nponto_cmd,
coalesce(ik.id, '') as id_cmd,
tpnt.cod_tipopnt as cod_tipopnt,
case 
when t.tipo_eq = 'ZTCO' then 0  
when t.tipo_eq like 'RB%%' then 1  
when t.tipo_eq = 'ATCC' then 2
when t.tipo_eq = 'YPAR' then 3
when ik.id is not null then 10
-- when tpnt.tipo = 'A' then 910
else 800 end as sord
from id_ponto i
join id_nops n on i.cod_nops=n.cod_nops
join id_modulos m on m.cod_modulo=n.cod_modulo
join id_ptlog_noh l on i.nponto=l.nponto
join id_tpeq t on t.cod_tpeq=i.cod_tpeq
join id_info f on f.cod_info=i.cod_info
join id_estacao e on e.cod_estacao=m.cod_estacao
join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
join id_prot pr on pr.cod_prot=i.cod_prot
join id_fases fs on fs.cod_fases=i.cod_fases
left outer join id_ponto ik on i.nponto = ik.nponto_sup and ik.nponto in (select nponto from id_ptlog_noh where COD_NOHSUP=%s)
where 
l.cod_nohsup=%s
and tpnt.tipo = 'D'
and i.evento!='S' and  i.cod_origem not in (5, 6, 7, 11, 24, 16, 17)
and ( m.cod_nivtensao not in (0,3,4,5,7,8,9) or (e.estacao in ({ph_ses}) and m.cod_nivtensao in (4,5)) )
and m.cod_tpmodulo in (8)
and tpnt.cod_tipopnt not in (96,97)
and (f.info not in ('SynF','Sprv','InFl','InHt','CmHt','CmFl','Fail') )
and (f.info not in ('InFl','InHt','Fail'))
and (f.info not in ('LoDC', 'LoAC', 'PwFl'))
and t.tipo_eq not in ('PGRP')
order by
e.estacao, m.id, sord, i.id
    """
    
    logging.info(f"[{ent}] Executando SQL para GRCMP BARRAS.")
    try:
        with conn.cursor() as cur:
            params = (cod_noh, cod_noh) # Duas vezes pois a query usa duas vezes
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # Inicia contadores
    se_ant = None
    grupo_ant = None
    cod_modant = None
    cntgrptr = 0
    cnttrest = 0
    cntpntgrp = 0
    cntmodgrp = 0
    cntpntmod = 0
    cnt = 0
    num_reg = defaultdict(int)

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")
            
            # Cabeçalho padrão de arquivo gerado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} BARRAS {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                modulo_raw = str(pt.get("modulo", "")).strip()
                mod = modulo_raw[:5].strip(" -")
                grupo = f"{pt['estacao']}-{mod}"

                # Lógica para criação do grupo de estação
                if se_ant != pt["estacao"]:
                    se_ant = pt["estacao"]
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write("GRUPO=\tBARRAS\n")
                    fp.write(f"PNT=\tBARRAS-{pt['estacao']}\n")
                    fp.write("TPPNT=\tGRUPO\n")
                    fp.write(f"ORDEM1=\t{1 + cntgrptr // 6}\n")
                    fp.write(f"ORDEM2=\t{1 + cntgrptr % 6}\n")
                    fp.write("TPTXT=\tID\n")
                    fp.write("CORTXT=\tPRETO\n")

                    cntgrptr += 1
                    cnttrest = 0
                    num_reg[ent] += 1
                    cnt += 1

                # Lógica para criação do grupo de módulo
                if grupo_ant != grupo:
                    grupo_ant = grupo
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"GRUPO=\tBARRAS-{pt['estacao']}\n")
                    fp.write(f"PNT=\t{grupo}\n")
                    fp.write("TPPNT=\tGRUPO\n")
                    fp.write(f"ORDEM1=\t{1 + cnttrest % 13}\n")
                    fp.write(f"ORDEM2=\t{1 + cnttrest // 13}\n")
                    fp.write("CORTXT=\tPRETO\n")
                    fp.write("TPTXT=\tID\n")

                    cnttrest += 1
                    cntpntgrp = 0
                    cntmodgrp = 0
                    num_reg[ent] += 1
                    cnt += 1

                # Lógica para contagem de módulos (colunas)
                if cod_modant != pt["cod_modulo"]:
                    cod_modant = pt["cod_modulo"]
                    if cntmodgrp <= 1:
                        cntmodgrp += 1
                        cntpntmod = 0
                
                cntpntmod += 1

                # Início do bloco de pontos individuais
                if cntpntmod <= 70:
                    # Lógica para Ponto de Comando (CGS)
                    if pt["id_cmd"]:
                        fp.write("\n")
                        fp.write(f"; NPONTO={pt['nponto_cmd']} {pt['id_cmd']}\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"GRUPO=\t{grupo}\n")
                        fp.write(f"PNT=\t{pt['id_cmd']}\n")
                        fp.write("TPPNT=\tCGS\n")
                        fp.write(f"ORDEM1=\t{cntpntmod}\n")
                        fp.write(f"ORDEM2=\t{1}\n")

                    # Lógica para o ponto de estado ou analógico (PDS/PAS)
                    paind_dcr = ""
                    if pt["tpdescr"]:
                        paind_dcr = pt["tpdescr"]
                        # Busca o sufixo -DJ do id_traducao
                        p1 = pt["traducao_id"].find("-DJ")
                        p2 = pt["traducao_id"].find(":estado")
                        if p1 > 3 and p2 > p1:
                            paind_dcr += " " + pt["traducao_id"][p1 + 3:p2]
                        # Adiciona sufixo de proteção ou automação
                        if pt["prot"].startswith("P"):
                            paind_dcr += " (P)"
                        elif pt["prot"].startswith("A"):
                            paind_dcr += " (A)"
                        # Adiciona sufixo de fases
                        if pt["fases"].startswith("01"):
                            paind_dcr += " Ind1"
                        elif pt["fases"].startswith("02"):
                            paind_dcr += " Ind2"

                    fp.write("\n")
                    fp.write(f"; NPONTO={pt['nponto']} {pt['id']} {pt['traducao_id']}\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"GRUPO=\t{grupo}\n")

                    if pt["cod_origem"] == 7: # Ponto de controle
                        txt = pt["traducao_id"] + " " + pt["cmd_0"] + "/" + pt["cmd_1"]
                        txt = txt.replace(pt["estacao"], "").replace(mod, "").strip(" -")
                        if paind_dcr:
                            txt = paind_dcr
                        
                        fp.write(f"PNT=\t{pt['id']}\n")
                        fp.write("TPPNT=\tCGS\n")
                        fp.write(f"TXT=\t{txt.upper()}\n")
                        fp.write(f"ORDEM1=\t{cntpntmod}\n")
                        fp.write(f"ORDEM2=\t{2}\n")
                        fp.write("CORTXT=\tPRETO\n")
                        fp.write("TPTXT=\tTXT\n")

                    else: # Ponto de estado ou analógico
                        if pt["tipo"] == "D":
                            txt = pt["traducao_id"].replace(pt["estacao"], "").replace(mod, "").strip(" -")
                            if paind_dcr:
                                txt = paind_dcr

                            # Lógica para o TPSIMB com base no cod_tipopnt
                            cod_tipopnt = pt["cod_tipopnt"]
                            if cod_tipopnt in (74, 36, 28):
                                fp.write("TPSIMB=\tCHECK\n")
                            elif cod_tipopnt in (37, 29):
                                fp.write("TPSIMB=\tCHECK_INV\n")
                            elif cod_tipopnt in (64, 65, 126, 128, 130):
                                fp.write("TPSIMB=\tCIRC\n")
                            elif cod_tipopnt in (8, 23, 25, 26):
                                fp.write("TPSIMB=\tCIRC_SIMPLES\n")
                            elif cod_tipopnt == 9:
                                fp.write("TPSIMB=\tCIRC_SIMPLES_INV\n")
                            
                            fp.write(f"PNT=\t{pt['id']}\n")
                            fp.write("TPPNT=\tPDS\n")
                            fp.write(f"TXT=\t{txt.upper()}\n")
                            fp.write(f"ORDEM1=\t{cntpntmod}\n")
                            fp.write(f"ORDEM2=\t{2}\n")
                            fp.write("CORTXT=\tPRETO\n")
                            fp.write("TPTXT=\tTXT\n")
                            
                            num_reg[ent] += 1
                            fp.write(f"\n{ent.upper()}\n")
                            fp.write(f"GRUPO=\t{grupo}\n")
                            fp.write("TPPNT=\tPDS\n")
                            fp.write(f"PNT=\t{pt['id']}\n")
                            fp.write("TPSIMB=\tESTADO\n")
                            fp.write(f"ORDEM1=\t{cntpntmod}\n")
                            fp.write(f"ORDEM2=\t{5}\n")
                        else: # Ponto analógico
                            txt = pt["traducao_id"].replace(pt["estacao"], "").replace(mod, "").strip(" -")
                            txt = f"{pt['unidade']} {txt}"
                            if paind_dcr:
                                txt = paind_dcr

                            fp.write(f"TXT=\t{txt.upper()}\n")
                            fp.write(f"ORDEM1=\t{cntpntmod}\n")
                            fp.write(f"ORDEM2=\t{2}\n")
                            fp.write("CORTXT=\tPRETO\n")
                            fp.write("TPTXT=\tTXT\n")

                            num_reg[ent] += 1
                            fp.write(f"\n{ent.upper()}\n")
                            fp.write(f"GRUPO=\t{grupo}\n")
                            fp.write(f"PNT=\t{pt['id']}\n")
                            fp.write("TPPNT=\tPAS\n")
                            fp.write(f"ORDEM1=\t{cntpntmod}\n")
                            fp.write(f"ORDEM2=\t{5}\n")

                cntpntgrp += 1
                logging.info(f"{ent.upper()}={cnt+1:05d} {pt['id']}")
                num_reg[ent] += 1
                cnt += 1
            
            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} BARRAS\n")
            fp.write(f"// Total de registros processados: {len(rows)}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)

''' # COMENTADO CONFORME CÓDIGO ORIGINAL

def generate_grcmp_telecomando_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo GRCMP-cmd.dat (Grupos de Telecomando), seguindo a lógica do script PHP original.
    """
    ent = 'grcmp'
    destino = Path(paths["automaticos"]) / f"{ent}-cmd.dat"
    first_write = not destino.exists() or force

    # Consulta principal para comandos
    sql_principal = """
    select
     e.estacao as estacao,
     m.id as modulo,
     m.descricao as descr_mod,
     e.descricao as descr_est,
     i.id as id,
     ktpnt.tipo as tipo,
     i.cod_origem as cod_origem,
     ktpnt.unidade as unidade,
     i.traducao_id as traducao_id,
     m.cod_tpmodulo as cod_tpmodulo,
     n.cod_modulo as cod_modulo,
     ktpnt.cmd_0 as cmd_0,
     ktpnt.cmd_1 as cmd_1,
     s.nponto as sup_nponto,
     s.id as sup_id,
     s.traducao_id as sup_traducao_id,
     i.cod_estacao
    from id_ponto i
    join id_nops n on i.cod_nops=n.cod_nops
    join id_modulos m on m.cod_modulo=n.cod_modulo
    join id_ptlog_noh l on i.nponto=l.nponto
    join id_tpeq t on t.cod_tpeq=i.cod_tpeq
    join id_estacao e on e.cod_estacao=m.cod_estacao
    join id_tipos as ktp on ktp.cod_tpeq=i.cod_tpeq and ktp.cod_info=i.cod_info
    join id_tipopnt as ktpnt on ktpnt.cod_tipopnt=ktp.cod_tipopnt
    left join id_ponto s on s.nponto=i.nponto_sup
    left join id_tipos as tp on tp.cod_tpeq=s.cod_tpeq and tp.cod_info=s.cod_info
    left join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
    where
    l.cod_nohsup=%s
    and i.cod_origem=7
    order by
    e.estacao, m.id, tpnt.tipo, t.tipo_eq
    """
    
    # Consulta secundária para comandos de TAP de transformadores
    sql_tap = """
    select
     e.estacao as estacao,
     m.id as modulo,
     m.descricao as descr_mod,
     e.descricao as descr_est,
     i.id as id,
     ktpnt.tipo as tipo,
     i.cod_origem as cod_origem,
     ktpnt.unidade as unidade,
     i.traducao_id as traducao_id,
     m.cod_tpmodulo as cod_tpmodulo,
     n.cod_modulo as cod_modulo,
     ktpnt.cmd_0 as cmd_0,
     ktpnt.cmd_1 as cmd_1,
     s.nponto as sup_nponto,
     s.id as sup_id,
     s.traducao_id as sup_traducao_id,
     i.cod_estacao
    from
     id_ponto i
     join id_nops n on i.cod_nops=n.cod_nops
     join id_modulos m on m.cod_modulo=n.cod_modulo
     join id_ptlog_noh l on i.nponto=l.nponto
     join id_tpeq t on t.cod_tpeq=i.cod_tpeq
     join id_estacao e on e.cod_estacao=m.cod_estacao
     join id_tipos as ktp on ktp.cod_tpeq=i.cod_tpeq and ktp.cod_info=i.cod_info
     join id_tipopnt as ktpnt on ktpnt.cod_tipopnt=ktp.cod_tipopnt
     left join id_ponto s on s.nponto=i.nponto_sup
     left join id_tipos as tp on tp.cod_tpeq=s.cod_tpeq and tp.cod_info=s.cod_info
     left join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
    where
      l.cod_nohsup=%s
      and i.cod_origem=7
      and i.cod_estacao = %s
      and n.cod_modulo != %s
      and i.cod_tpeq=16
      and m.id like %s
    order by
      i.id
    """

    logging.info(f"[{ent}-cmd] Executando SQL para GRCMP TELECOMANDO.")
    try:
        with conn.cursor() as cur:
            cur.execute(sql_principal, (cod_noh,))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}-cmd] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}-cmd] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # Inicia contadores
    se_ant = None
    grupo_ant = None
    cod_modant = None
    cntgrptr = 0
    cnttrest = 0
    cntpntgrp = 0
    cntmodgrp = 0
    cntpntmod = 0
    cnt = 0
    num_reg = defaultdict(int)

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")
            
            # Cabeçalho padrão de arquivo gerado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} TELECOMANDO {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                mod = str(pt.get("modulo", "")).strip(" -")
                grupo = f"CMD-{pt['estacao']}-{mod}"

                # Lógica para criação do grupo de estação
                if se_ant != pt["estacao"]:
                    se_ant = pt["estacao"]
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write("GRUPO=\tCOMANDOS\n")
                    fp.write(f"PNT=\tCMD-{pt['estacao']}\n")
                    fp.write("TPPNT=\tGRUPO\n")
                    fp.write(f"ORDEM1=\t{1 + cntgrptr // 6}\n")
                    fp.write(f"ORDEM2=\t{1 + cntgrptr % 6}\n")
                    fp.write("TPTXT=\tID\n")
                    
                    cntgrptr += 1
                    cnttrest = 0
                    num_reg[ent] += 1
                    cnt += 1
                
                # Lógica para criação do grupo de módulo
                if grupo_ant != grupo:
                    grupo_ant = grupo
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"GRUPO=\tCMD-{pt['estacao']}\n")
                    fp.write(f"PNT=\t{grupo}\n")
                    fp.write("TPPNT=\tGRUPO\n")
                    fp.write(f"ORDEM1=\t{1 + cnttrest % 13}\n")
                    fp.write(f"ORDEM2=\t{1 + cnttrest // 13}\n")
                    fp.write("CORTXT=\tPRETO\n")
                    fp.write("TPTXT=\tID\n")
                    
                    cnttrest += 1
                    cntpntgrp = 0
                    cntmodgrp = 0
                    num_reg[ent] += 1
                    cnt += 1

                    # Lógica para comandos de TAP em transformadores
                    if pt["cod_tpmodulo"] == 3:
                        try:
                            with conn.cursor() as cur_tap:
                                params_tap = (cod_noh, pt['cod_estacao'], pt['cod_modulo'], f"{mod}%")
                                cur_tap.execute(sql_tap, params_tap)
                                tap_rows = cur_tap.fetchall()
                                for tap_pt in tap_rows:
                                    # Chamada para processar o ponto de TAP, simulando a recursão do PHP
                                    # Os contadores são compartilhados
                                    process_telecomando_point(fp, ent, grupo, tap_pt, mod, cntpntmod, cntmodgrp)
                                    cntpntmod += 1
                                    cntpntgrp += 1
                                    cnt += 1
                        except Exception as e:
                            logging.error(f"[{ent}-cmd] Erro na sub-query de TAP: {e}", exc_info=True)


                # Lógica para contagem de módulos (colunas)
                if cod_modant != pt["cod_modulo"]:
                    cod_modant = pt["cod_modulo"]
                    if cntmodgrp <= 1:
                        cntmodgrp += 1
                        cntpntmod = 0
                
                cntpntmod += 1

                # Lógica para o ponto de comando e seu associado (se houver)
                process_telecomando_point(fp, ent, grupo, pt, mod, cntpntmod, cntmodgrp)

                cntpntgrp += 1
                logging.info(f"{ent.upper()}={cnt+1:05d} {pt['id']}")
                num_reg[ent] += 1
                cnt += 1

            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()} TELECOMANDO\n")
            fp.write(f"// Total de registros processados: {len(rows)}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}-cmd] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}-cmd] Erro escrevendo '{destino}': {e}", exc_info=True)


def process_telecomando_point(fp, ent, grupo, pt, mod, cntpntmod, cntmodgrp):
    """Função auxiliar para encapsular a lógica de escrita de um ponto de telecomando."""
    txt = pt["traducao_id"] + " : " + pt["cmd_0"] + "/" + pt["cmd_1"]
    txt = txt.replace(pt["estacao"], "").replace(mod, "").replace(pt["descr_mod"], "").strip(" -")

    fp.write("\n")
    fp.write(f"{ent.upper()}\n")
    fp.write(f"GRUPO=\t{grupo}\n")
    fp.write(f"PNT=\t{pt['id']}\n")
    fp.write("TPPNT=\tCGS\n")
    fp.write(f"TXT=\t{txt}\n")
    fp.write(f"ORDEM1=\t{cntpntmod}\n")
    fp.write(f"ORDEM2=\t{1}\n")
    fp.write("CORTXT=\tPRETO\n")
    fp.write("TPTXT=\tTXT\n")

    # Se tem ponto associado, mostra
    if pt["sup_id"]:
        fp.write("\n")
        fp.write(f"{ent.upper()}\n")
        fp.write(f"GRUPO=\t{grupo}\n")
        fp.write(f"PNT=\t{pt['sup_id']}\n")

        sup_traducao_id = pt["sup_traducao_id"]
        sup_traducao_id = sup_traducao_id.replace(pt["estacao"], "").replace(pt["descr_mod"], "").replace(mod, "").strip(" -")
        
        if pt["tipo"] == "D":
            fp.write("TPPNT=\tPDS\n")
            fp.write("TPSIMB=\tESTADO\n")
            fp.write(f"TXT=\t{sup_traducao_id}\n")
        else: # Analógico
            fp.write("TPPNT=\tPAS\n")
            fp.write(f"TXT=\t{pt['unidade']} {sup_traducao_id}\n")

        fp.write(f"ORDEM1=\t{cntpntmod}\n")
        fp.write(f"ORDEM2=\t{2}\n")
        fp.write("CORTXT=\tPRETO\n")
        fp.write("TPTXT=\tTXT\n")
'''
#---------------------------------------------------------------------------------------------------------
# ARQUIVO TCTL.DAT
def generate_tctl_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "tctl"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select
            tpnt.tctl,
            tpnt.nome as nome,
            tpnt.cmd_0,
            tpnt.cmd_1
    from
            id_tipopnt as tpnt
    where
    tctl like 'USR%%'
    union (select  'USRx41', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx42', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx43', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx44', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx45', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx46', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx47', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx48', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx49', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx50', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx51', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx52', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx53', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx54', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx55', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx56', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx57', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx58', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx59', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx60', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx61', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx62', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx63', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx64', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx65', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx66', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx67', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx68', 'USR', 'TRIP', 'CLOSE')
    union (select  'USRx69', 'USR', 'TRIP', 'CLOSE')
    order by 1
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    cnt = 0  # inicia contador para calcular ID = USR{cnt+42}
    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho decorado comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                try:
                    nome = str(pt.get("nome", "") or "").strip()
                    cmd_0 = str(pt.get("cmd_0", "") or "").strip()
                    cmd_1 = str(pt.get("cmd_1", "") or "").strip()

                    if cnt + 42 > 69:
                        break  # não gera além do limite

                    # ajustes se ambos vazios
                    if not cmd_0 and not cmd_1:
                        cmd_0 = "TRIP"
                        cmd_1 = "CLOSE"

                    seq = cnt + 42
                    usr_id = f"USR{seq}"

                    # comentário de seção
                    fp.write(f";---------------- {nome}\n")
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"ID=\t{usr_id}\n")
                    fp.write(f"TIP=\tCTL\n")

                    # ALR_CLOSE
                    if cmd_1:
                        fp.write(f"ALR_CLOSE=\t{cmd_1}\n")
                    else:
                        fp.write(f"ALR_CLOSE=\t{cmd_0}\n")

                    # ALR_TRIP
                    if cmd_0:
                        fp.write(f"ALR_TRIP=\t{cmd_0}\n")
                    else:
                        fp.write(f"ALR_TRIP=\t{cmd_1}\n")

                    fp.write(f"DLG_CLOSE=\t{cmd_1}\n")
                    fp.write(f"DLG_TRIP=\t{cmd_0}\n")
                    fp.write(f"NSEQ=\t{seq}\n")

                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} {nome}")
                except Exception:
                    logging.exception(f"[{ent}] erro processando linha: {pt}")
                    continue

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} entradas processadas.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CNF.DAT
def generate_cnf_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "cnf"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    # carrega conexões de origem/destino
    info = load_conexoes(conn, cod_noh)
    conexoes_org = info["conexoes_org"]
    conexoes_dst = info["conexoes_dst"]
    DescrNoh = info["descr_noh"]
    lia_bidirecional = info["lia_bidirecional"]
    all_conexoes = [89, 1]

    if not all_conexoes:
        logging.error(f"[{ent}] sem conexões válidas para montar CNF. Abortando.")
        return

    # 1. checagem de duplicidade
    dup_sql = """
SELECT
    c.cod_conexao,
    c.descricao as nome,
    c.id_sage_aq,
    c.placa_princ,
    c.linha_princ,
    c.placa_resrv,
    c.linha_resrv,
    cpl.descricao as dup_nome,
    cpl.placa_princ as dup_placa_princ,
    cpl.linha_princ as dup_linha_princ,
    cpl.placa_resrv as dup_placa_resrv,
    cpl.linha_resrv as dup_linha_resrv,
    p.nome as pnome
FROM
    id_conexoes c
    join id_protocolos p on p.cod_protocolo=c.cod_protocolo
    left join id_conexoes cpl
      on
        cpl.cod_noh_dst=c.cod_noh_dst and
        cpl.cod_conexao!=c.cod_conexao and
        cpl.cod_protocolo=c.cod_protocolo and
       ( cpl.placa_princ=c.placa_princ and
        cpl.linha_princ=c.linha_princ  
        or 
        cpl.placa_resrv=c.placa_resrv and
        cpl.linha_resrv=c.linha_resrv and c.placa_resrv!=0
        or 
        cpl.placa_princ=c.placa_resrv and
        cpl.linha_princ=c.linha_resrv and c.placa_resrv!=0 
        or 
        cpl.placa_resrv=c.placa_princ and
        cpl.linha_resrv=c.linha_princ and c.placa_resrv!=0
        ) and
        cpl.cod_conexao in (select cod_conexao from id_conexoes where cod_noh_dst = %s)
WHERE
    c.cod_conexao in (select cod_conexao from id_conexoes where cod_noh_dst = %s)
    and p.cod_protocolo not in (0, 10)
    and c.end_org not in (-1,0) and cpl.end_org not in (-1,0)
    and cpl.descricao is not null
    """
    logging.info(f"[{ent}] Iniciando checagem de duplicidades de placa/linha.")
    try:
        with conn.cursor() as cur:
            cur.execute(dup_sql, (cod_noh, cod_noh))
            dup_rows = cur.fetchall()
            for registro in dup_rows:
                nome = registro.get("nome", "")
                dup_nome = registro.get("dup_nome", "")
                logging.error(f"ERRO: CNF placa e linha: {nome} *DUPLICADA COM* {dup_nome}")
    except Exception as e:
        logging.error(f"[{ent}] Erro ao rodar checagem de duplicidade: {e}", exc_info=True)

    # 2. query principal de CNF
    placeholders = ",".join(["%s"] * len(all_conexoes))
    cnf_sql = f"""
    SELECT
        c.cod_protocolo,
        p.grupo_protoc,
        if (c.cod_noh_org=%s, 'D', 'A') as aq_dt, 
        c.nsrv1,
        c.nsrv2,   
        c.placa_princ,    
        c.linha_princ,    
        c.placa_resrv,    
        c.linha_resrv, 
        c.cod_conexao,
        c.end_org,
        c.end_dst,
        c.descricao as nome,
        c.id_sage_aq,
        c.id_sage_dt,
        p.nome as pnome,
        c.cod_noh_org,
        c.cod_noh_dst,
        c.vel_enl1,    
        c.vel_enl2,
        c.param_cnf
    FROM 
        id_conexoes c
        join id_protocolos p on p.cod_protocolo=c.cod_protocolo 
    WHERE
        c.cod_conexao in ({placeholders})
        and p.cod_protocolo not in (0)    
    ORDER BY 
        p.cod_protocolo desc,
        aq_dt,
        c.nsrv1,
        c.nsrv2,
        c.placa_princ,      
        c.linha_princ,      
        c.placa_resrv,      
        c.linha_resrv,      
        c.cod_conexao
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            params = [cod_noh] + all_conexoes  # primeiro %s é para aq_dt, depois para IN (...)
            cur.execute(cnf_sql, tuple(params))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados do CNF: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query CNF.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar no CNF. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n\n")

            cnt = 0
            id_iccpant = None
            for pt in rows:
                try:
                    cod_protocolo = pt.get("cod_protocolo")
                    grupo_protoc = pt.get("grupo_protoc")
                    aq_dt = pt.get("aq_dt")
                    placa_princ = pt.get("placa_princ")
                    linha_princ = pt.get("linha_princ")
                    placa_resrv = pt.get("placa_resrv")
                    linha_resrv = pt.get("linha_resrv")
                    nome = str(pt.get("nome", "") or "").strip()
                    pnome = str(pt.get("pnome", "") or "").strip()
                    id_sage_aq = str(pt.get("id_sage_aq", "") or "").strip()
                    id_sage_dt = str(pt.get("id_sage_dt", "") or "").strip()
                    cod_noh_org = pt.get("cod_noh_org")
                    param_cnf = str(pt.get("param_cnf", "") or "").strip()

                    # determina tipo e IDs
                    if str(cod_noh) == str(cod_noh_org):
                        tipo = "DD"
                        _id = f"{id_sage_dt}-DT"
                        id_iccp = id_sage_dt
                    else:
                        tipo = "AA"
                        _id = f"{id_sage_aq}-AQ"
                        id_iccp = id_sage_aq

                    pula = False
                    if cod_protocolo == 10 and id_sage_dt:  # ICCP e distribuição
                        pula = True

                    if pula:
                        continue

                    # escreve bloco
                    fp.write(f"\n\n; >>>>>> {nome} - {pnome} <<<<<<\n")
                    fp.write(f"{ent.upper()}\n")

                    if cod_protocolo == 10:
                        fp.write(f"ID =\t{id_iccp}\n")
                        fp.write(f"CONFIG= {param_cnf}\n")
                        fp.write(f"LSC =\t{id_iccp}\n")
                    else:
                        fp.write(f"ID =\t{_id}\n")
                        if grupo_protoc == 8:
                            fp.write("CONFIG= ")
                            fp.write(f" PlPr= {placa_princ} LiPr= {linha_princ} PlRe= {placa_resrv} LiRe= {linha_resrv} {param_cnf}\n")
                        else:
                            fp.write(f"CONFIG= {param_cnf}")
                            fp.write(f" PlPr= {placa_princ} LiPr= {linha_princ} PlRe= {placa_resrv} LiRe= {linha_resrv}\n")
                        fp.write(f"LSC =\t{_id}\n")

                    id_iccpant = id_iccp
                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} {nome}")
                except Exception:
                    logging.exception(f"[{ent}] erro processando linha: {pt}")
                    continue

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")
        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO UTR.DAT
def generate_utr_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "utr"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    # carrega conexões (origem/destino) como em CNF
    info = load_conexoes(conn, cod_noh)
    conexoes_org = info["conexoes_org"]
    conexoes_dst = info["conexoes_dst"]
    all_conexoes = conexoes_org + conexoes_dst

    if not all_conexoes:
        logging.error(f"[{ent}] sem conexões válidas para montar UTR. Abortando.")
        return

    # monta placeholders para IN
    in_placeholders = ",".join(["%s"] * len(all_conexoes))

    sql = f"""
    SELECT DISTINCT
        c.cod_protocolo,
        if (c.cod_noh_org=%s, 'D', 'A') as aq_dt, 
        c.nsrv1,
        c.nsrv2,   
        c.placa_princ,    
        c.linha_princ,    
        c.placa_resrv,    
        c.linha_resrv, 
        cpl.placa_princ as pl_placa_princ,
        c.cod_conexao,
        c.end_org,
        c.end_dst,
        c.descricao as nome,
        c.id_sage_aq,
        c.id_sage_dt,
        p.nome as pnome,
        c.cod_noh_org,
        c.cod_noh_dst,
        c.vel_enl1,    
        c.vel_enl2,
        c.param_utr,
        c.param_cxu
    FROM 
        id_conexoes c
        join id_protocolos p on p.cod_protocolo=c.cod_protocolo 
        left join id_conexoes cpl
          on
            cpl.cod_noh_dst=c.cod_noh_dst and
            cpl.cod_conexao!=c.cod_conexao and
            cpl.cod_protocolo=c.cod_protocolo and
            cpl.placa_princ=c.placa_princ and
            cpl.linha_princ=c.linha_princ and
            cpl.placa_resrv=c.placa_resrv and
            cpl.linha_resrv=c.linha_resrv and
            cpl.cod_conexao in ({','.join(['%s'] * len(conexoes_dst))})
    WHERE
        c.cod_conexao in ({in_placeholders})
        and p.cod_protocolo not in (0, 10)    
    ORDER BY 
        c.cod_protocolo,
        aq_dt,
        c.nsrv1,
        c.nsrv2,
        c.placa_princ,      
        c.linha_princ,      
        c.placa_resrv,      
        c.linha_resrv,
        c.cod_conexao
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            # parâmetros: primeiro para aq_dt (%s), depois todos de all_conexoes, e para o cpl.cod_conexao IN (...) os conexoes_dst
            params = [cod_noh] + all_conexoes + conexoes_dst
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de UTR: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query UTR.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em UTR. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n\n")

            placa_princ_ant = None
            linha_princ_ant = None
            cnt_party_line = 0
            cxu_pl = ""
            cnt = 0

            for pt in rows:
                try:
                    cod_protocolo = pt.get("cod_protocolo")
                    aq_dt = pt.get("aq_dt")
                    placa_princ = pt.get("placa_princ")
                    linha_princ = pt.get("linha_princ")
                    placa_resrv = pt.get("placa_resrv")
                    linha_resrv = pt.get("linha_resrv")
                    pl_placa_princ = pt.get("pl_placa_princ") or ""
                    cod_conexao = pt.get("cod_conexao")
                    end_org = pt.get("end_org")
                    nome = str(pt.get("nome", "") or "").strip()
                    pnome = str(pt.get("pnome", "") or "").strip()
                    id_sage_aq = str(pt.get("id_sage_aq", "") or "").strip()
                    id_sage_dt = str(pt.get("id_sage_dt", "") or "").strip()
                    cod_noh_org = pt.get("cod_noh_org")
                    param_utr = str(pt.get("param_utr", "") or "")
                    param_cxu = str(pt.get("param_cxu", "") or "")

                    # normaliza param_utr como no PHP
                    param_utr = param_utr.replace(" ", "\n")
                    param_utr = param_utr.replace("=", " = ")

                    # determina tipo e id
                    if str(cod_noh) == str(cod_noh_org):
                        tipo = "DD"
                        _id = f"{id_sage_dt}-DT"
                    else:
                        tipo = "AA"
                        _id = f"{id_sage_aq}-AQ"

                    # party-line: zera se mudou placa/linha principal
                    if placa_princ != placa_princ_ant or linha_princ != linha_princ_ant:
                        cnt_party_line = 0
                        cxu_pl = ""
                    placa_princ_ant = placa_princ
                    linha_princ_ant = linha_princ

                    ordem_pl = ""
                    if pl_placa_princ and int(pl_placa_princ) > 0:
                        cnt_party_line += 1
                        ordem_pl = f"{cnt_party_line:02d}"
                        if cnt_party_line == 1:
                            cxu_pl = f"{id_sage_aq}-AQ"

                    # escreve bloco principal
                    fp.write(f"\n\n; >>>>>> {nome} - {pnome} <<<<<<\n")
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")

                    if "NTENT" not in param_utr:
                        fp.write("NTENT = 3\n")
                    if "RESPT" not in param_utr:
                        fp.write("RESPT = 1000\n")

                    if tipo == "AA":
                        fp.write(f"ID =\t{_id}_P\n")
                    else:
                        fp.write(f"ID =\t{_id}\n")

                    fp.write(f"CNF =\t{_id}\n")

                    if cxu_pl:
                        fp.write(f"CXU =\t{cxu_pl}\n")
                    else:
                        fp.write(f"CXU =\t{_id}\n")

                    fp.write(f"ENUTR =\t{end_org}\n")
                    fp.write(f"ORDEM =\t{ordem_pl}PRI\n")

                    if param_utr:
                        fp.write(f"{param_utr}\n")

                    # bloco de reserva para AA
                    if tipo == "AA":
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        if "NTENT" not in param_utr:
                            fp.write("NTENT = 3\n")
                        if "RESPT" not in param_utr:
                            fp.write("RESPT = 1000\n")
                        fp.write(f"ID =\t{_id}_R\n")
                        fp.write(f"CNF =\t{_id}\n")
                        if cxu_pl:
                            fp.write(f"CXU =\t{cxu_pl}\n")
                        else:
                            fp.write(f"CXU =\t{_id}\n")
                        # condição para ENUTR de reserva
                        if cod_protocolo == 3 or ("NFAIL=0" in param_cxu and "SFAIL=0" in param_cxu):
                            fp.write(f"ENUTR =\t{end_org}\n")
                        else:
                            fp.write(f"ENUTR =\t0\n")
                        fp.write(f"ORDEM =\t{ordem_pl}REV\n")
                        if param_utr:
                            fp.write(f"{param_utr}\n")

                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} {nome}")
                except Exception:
                    logging.exception(f"[{ent}] erro processando linha: {pt}")
                    continue

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CXU.DAT
def generate_cxu_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "cxu"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    # carrega conexões de origem/destino
    info = load_conexoes(conn, cod_noh)
    conexoes_org = info["conexoes_org"]
    conexoes_dst = info["conexoes_dst"]
    all_conexoes = conexoes_org + conexoes_dst

    if not all_conexoes:
        logging.error(f"[{ent}] sem conexões válidas para montar CXU. Abortando.")
        return

    # monta placeholders
    in_placeholders = ",".join(["%s"] * len(all_conexoes))
    dst_placeholders = ",".join(["%s"] * len(conexoes_dst))

    sql = f"""
    SELECT
        c.cod_protocolo,
        if (c.cod_noh_org=%s, 'D', 'A') as aq_dt, 
        c.nsrv1,
        c.nsrv2,   
        c.placa_princ,    
        c.linha_princ,    
        c.placa_resrv,    
        c.linha_resrv, 
        cpl.placa_princ as pl_placa_princ,
        c.cod_conexao,
        c.descricao as nome,
        c.id_sage_aq,
        c.id_sage_dt,
        p.nome as pnome,
        c.cod_noh_org,
        c.cod_noh_dst,
        c.vel_enl1,    
        c.vel_enl2,
        c.param_cxu 
    FROM 
        id_conexoes c
        join id_protocolos p on p.cod_protocolo=c.cod_protocolo 
        left join id_conexoes cpl
          on
            cpl.cod_noh_dst=c.cod_noh_dst and
            cpl.cod_conexao!=c.cod_conexao and
            cpl.cod_protocolo=c.cod_protocolo and
            cpl.placa_princ=c.placa_princ and
            cpl.linha_princ=c.linha_princ and
            cpl.placa_resrv=c.placa_resrv and
            cpl.linha_resrv=c.linha_resrv and
            cpl.cod_conexao in ({dst_placeholders})
    WHERE
        c.cod_conexao in ({in_placeholders})
        and p.cod_protocolo not in (0, 10)
    ORDER BY 
        p.cod_protocolo,
        aq_dt,
        c.nsrv1,
        c.nsrv2,
        c.placa_princ,      
        c.linha_princ,      
        c.placa_resrv,      
        c.linha_resrv,      
        c.cod_conexao
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            params = [cod_noh] + all_conexoes + conexoes_dst  # primeiro para aq_dt, depois IN(...) e cpl.IN(...)
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de CXU: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query CXU.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em CXU. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n\n")

            placa_princ_ant = None
            linha_princ_ant = None
            cnt_party_line = 0
            cxu_pl = ""
            cnt = 0

            for pt in rows:
                try:
                    cod_protocolo = pt.get("cod_protocolo")
                    aq_dt = pt.get("aq_dt")
                    placa_princ = pt.get("placa_princ")
                    linha_princ = pt.get("linha_princ")
                    placa_resrv = pt.get("placa_resrv")
                    linha_resrv = pt.get("linha_resrv")
                    pl_placa_princ = pt.get("pl_placa_princ") or ""
                    cod_conexao = pt.get("cod_conexao")
                    nome = str(pt.get("nome", "") or "").strip()
                    pnome = str(pt.get("pnome", "") or "").strip()
                    id_sage_aq = str(pt.get("id_sage_aq", "") or "").strip()
                    id_sage_dt = str(pt.get("id_sage_dt", "") or "").strip()
                    cod_noh_org = pt.get("cod_noh_org")
                    param_cxu = str(pt.get("param_cxu", "") or "")

                    # normaliza param_cxu
                    param_cxu = param_cxu.replace(" ", "\n")
                    param_cxu = param_cxu.replace("=", " = ")

                    # determina ID
                    if str(cod_noh) == str(cod_noh_org):
                        _id = f"{id_sage_dt}-DT"
                    else:
                        _id = f"{id_sage_aq}-AQ"

                    # party-line: zera se mudou placa/linha principal
                    if placa_princ != placa_princ_ant or linha_princ != linha_princ_ant:
                        cnt_party_line = 0
                        cxu_pl = ""
                    placa_princ_ant = placa_princ
                    linha_princ_ant = linha_princ

                    if pl_placa_princ and int(pl_placa_princ) > 0:
                        cnt_party_line += 1
                        if cnt_party_line == 1:
                            cxu_pl = f"{id_sage_aq}-AQ"

                    if cnt_party_line >= 2:
                        continue  # só a primeira do party-line é gerada

                    # escreve bloco
                    fp.write(f"\n\n; >>>>>> {nome} - {pnome} <<<<<<\n")
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")

                    # defaults se ausentes
                    if "AQANL" not in param_cxu:
                        fp.write("AQANL = 0\n")
                    if "AQPOL" not in param_cxu:
                        fp.write("AQPOL = 500\n")
                    if "AQTOT" not in param_cxu:
                        fp.write("AQTOT = 0\n")
                    if "FAILP" not in param_cxu:
                        fp.write("FAILP = 0\n")
                    if "FAILR" not in param_cxu:
                        fp.write("FAILR = 0\n")
                    if "INTGR" not in param_cxu:
                        fp.write("INTGR = 60000\n")
                    if "NFAIL" not in param_cxu:
                        fp.write("NFAIL = 10\n")
                    if "SFAIL" not in param_cxu:
                        fp.write("SFAIL = 400\n")

                    fp.write(f"GSD =\tGT_SCD_1\n")
                    fp.write(f"ID =\t{_id}\n")
                    fp.write(f"ORDEM =\t{cnt + 1}\n")

                    if param_cxu:
                        fp.write(f"{param_cxu}\n")

                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} {nome}")
                except Exception:
                    logging.exception(f"[{ent}] erro processando linha: {pt}")
                    continue

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO ENU.DAT
def generate_enu_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo ENU.dat a partir das conexões de aquisição e distribuição.
    """
    ent = 'enu'
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force
    
    # Prepara os placeholders para as cláusulas IN
    ph_dst = ",".join(["%s"] * len(conexoes_dst))
    ph_all = ",".join(["%s"] * (len(conexoes_org) + len(conexoes_dst)))

    sql = f"""
    SELECT
        c.cod_conexao,
        c.descricao as nome,
        if (c.cod_noh_org=%s, 'D', 'A') as aq_dt,
        c.id_sage_aq,
        c.id_sage_dt,
        c.placa_princ,
        c.linha_princ,
        cpl.placa_princ as pl_placa_princ,
        c.cod_protocolo,
        p.nome as pnome,
        c.cod_noh_org,
        c.cod_noh_dst,
        c.vel_enl1,
        c.vel_enl2,
        c.param_enu
    FROM
        id_conexoes c
        join id_protocolos p on p.cod_protocolo=c.cod_protocolo
        left join id_conexoes cpl
          on
            cpl.cod_noh_dst=c.cod_noh_dst and
            cpl.cod_conexao!=c.cod_conexao and
            cpl.cod_protocolo=c.cod_protocolo and
            cpl.placa_princ=c.placa_princ and
            cpl.linha_princ=c.linha_princ and
            cpl.placa_resrv=c.placa_resrv and
            cpl.linha_resrv=c.linha_resrv and
            cpl.cod_conexao in ({ph_dst})
    WHERE
        c.cod_conexao in ({ph_all})
        and p.cod_protocolo not in (0, 10)
    ORDER BY
        p.cod_protocolo,
        aq_dt,
        c.nsrv1,
        c.nsrv2,
        c.placa_princ,
        c.linha_princ,
        c.placa_resrv,
        c.linha_resrv,
        c.cod_conexao
    """

    logging.info(f"[{ent}] Executando SQL para ENU.")
    try:
        with conn.cursor() as cur:
            # Junta os parâmetros na ordem correta: cod_noh, conexoes_dst, conexoes_org, conexoes_dst
            params = (cod_noh,) + tuple(conexoes_dst) + tuple(conexoes_org) + tuple(conexoes_dst)
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # Inicia contadores e variáveis de estado
    placa_princ_ant = None
    linha_princ_ant = None
    cnt_party_line = 0
    cxu_pl = ""
    cnt = 0
    num_reg = defaultdict(int)

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")
            
            # Cabeçalho padrão de arquivo gerado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                # Lógica para party-line
                if placa_princ_ant != pt["placa_princ"] or linha_princ_ant != pt["linha_princ"]:
                    cnt_party_line = 0
                    cxu_pl = ""
                
                placa_princ_ant = pt["placa_princ"]
                linha_princ_ant = pt["linha_princ"]

                if pt["pl_placa_princ"] and pt["pl_placa_princ"] > 0:
                    cnt_party_line += 1
                    if cnt_party_line == 1:
                        cxu_pl = f"{pt['id_sage_aq']}-AQ"
                
                # Só gera se não for o segundo ponto de um party-line
                if cnt_party_line < 2:
                    if int(cod_noh) == pt["cod_noh_org"]:
                        id_enu = f"{pt['id_sage_dt']}-DT"
                    else:
                        id_enu = f"{pt['id_sage_aq']}-AQ"
                    
                    # Print de console
                    logging.info(f"{ent.upper()}={id_enu} conex={pt['cod_conexao']} {pt['nome']}" + 
                                  (" Party-line" if cxu_pl else ""))

                    # Escreve o bloco de dados no arquivo
                    fp.write(f"\n\n; >>>>>> {pt['nome']} - {pt['pnome']} - Conex={pt['cod_conexao']} <<<<<<\n")
                    
                    # Bloco principal
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"\tCXU =\t{id_enu}\n")
                    fp.write(f"\tID =\t{id_enu}_P\n")
                    fp.write(f"\tORDEM =\tPRI\n")
                    fp.write(f"\tVLUTR =\t{pt['vel_enl1']}\n")
                    if pt["cod_protocolo"] == 18:
                        fp.write("\tTDESC =\t15\n")
                        fp.write("\tTRANS =\t12\n")
                    if pt["param_enu"]:
                        fp.write(f"{pt['param_enu']}\n")
                    
                    # Bloco de reserva
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"CXU =\t{id_enu}\n")
                    fp.write(f"ID =\t{id_enu}_P\n")
                    fp.write(f"ORDEM =\tPRI\n")
                    fp.write(f"VLUTR =\t{pt['vel_enl1']}\n")
                    if pt["cod_protocolo"] == 18:
                        fp.write("\tTDESC =\t15\n")
                        fp.write("\tTRANS =\t12\n")
                    if pt["param_enu"]:
                        fp.write(f"{pt['param_enu']}\n")
                    
                    num_reg[ent] += 2
                    cnt += 2

            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}\n")
            fp.write(f"// Total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO MAP.DAT
def generate_map_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "map"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    SELECT 
        e.estacao,
        e.descricao,
        e.nohs_map
    FROM 
        id_estacao e
    WHERE
        e.nohs_map != ''
    ORDER BY 
        e.estacao desc
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de MAP: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query MAP.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em MAP. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n\n")

            cnt = 0
            total = len(rows)

            for idx, pt in enumerate(rows, start=1):
                try:
                    estacao = str(pt.get("estacao", "") or "").strip()
                    descricao = str(pt.get("descricao", "") or "").strip()
                    nohs_map_raw = str(pt.get("nohs_map", "") or "")
                    # explode e trim
                    arr_noh_map = [x.strip() for x in nohs_map_raw.split(",") if x.strip()]
                    if str(cod_noh) in arr_noh_map:
                        # escreve bloco
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"\tID =\t{estacao}\n")
                        fp.write(f"\tNARRT =\t{descricao}\n")
                        fp.write(f"\tORDEM =\t{25 + cnt}\n")
                        cnt += 1
                        logging.info(f"{ent.upper()} entrada {estacao} incluída (ORDEM={25+cnt-1})")
                except Exception:
                    logging.exception(f"[{ent}] erro processando linha: {pt}")
                    continue

            # bloco GERAL como no final do original
            fp.write("\n")
            fp.write(f"{ent.upper()}\n")
            fp.write(f"\tID =\tGERAL\n")
            fp.write(f"\tNARRT =\tLista Geral do Sistema Eletrico\n")
            fp.write(f"\tORDEM =\t{25 + cnt}\n")
            cnt += 1
            logging.info(f"{ent.upper()} entrada GERAL incluída (ORDEM={25+cnt-1})")

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO LSC.DAT
def generate_lsc_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo LSC.dat com base nas conexões de origem e destino.
    """
    ent = 'lsc'
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force
    
    # Combina as conexões para a cláusula IN
    all_conexoes = conexoes_org + conexoes_dst
    ph_all = ",".join(["%s"] * len(all_conexoes))
    
    sql = f"""
    SELECT
        c.cod_conexao,
        c.descricao as nome,
        c.id_sage_aq,
        c.id_sage_dt,
        c.cod_protocolo,
        c.end_org,
        c.end_dst,
        p.nome as pnome,
        p.descricao as pdescr,
        c.cod_noh_org,
        c.cod_noh_dst,
        p.balanceado,
        p.tcv as tcv,
        p.ttp as ttp,
        c.nsrv1,
        c.nsrv2,
        c.verbd,
        e.estacao,
        e.nohs_map
    FROM
        id_conexoes c
        join id_protocolos p on p.cod_protocolo=c.cod_protocolo
        join id_nohsup norg on norg.cod_nohsup=c.cod_noh_org
        join id_estacao e on e.cod_estacao=norg.cod_estacao
    WHERE
        c.cod_conexao in ({ph_all})
        and p.cod_protocolo!=0
    ORDER BY
        c.cod_conexao
    """

    logging.info(f"[{ent}] Executando SQL para LSC.")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(all_conexoes))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return
    
    # Inicia contadores
    cnt = 0
    num_reg = defaultdict(int)

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")
            
            # Cabeçalho padrão de arquivo gerado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                pula = False
                tipo = ""
                _id = ""
                _map = ""

                # Lógica para determinar TIPO, ID e MAP
                if pt["balanceado"] == 1:
                    tipo = "AD"
                    _id = pt["id_sage_aq"] if pt["id_sage_aq"] else pt["id_sage_dt"]
                    _map = "GERAL"
                    if pt["cod_protocolo"] == 10 and pt["id_sage_dt"]:
                        pula = True
                elif int(cod_noh) == pt["cod_noh_org"]:
                    tipo = "DD"
                    _map = "GERAL"
                    _id = f"{pt['id_sage_dt']}-DT"
                else:
                    tipo = "AA"
                    arr_noh_map = [x.strip() for x in str(pt["nohs_map"]).split(",") if x.strip()]
                    _map = pt["estacao"] if str(cod_noh) in arr_noh_map else "GERAL"
                    _id = f"{pt['id_sage_aq']}-AQ"
                
                # Valores padrão se nulos
                verbd = pt["verbd"] if pt["verbd"] else "NOV-04"
                nsrv1 = pt["nsrv1"] if pt["nsrv1"] else "localhost"
                nsrv2 = pt["nsrv2"] if pt["nsrv2"] else "localhost"

                if not pula:
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    
                    fp.write(f"\tGSD =\tGT_SCD_1\n")
                    fp.write(f"\tID =\t{_id}\n")
                    fp.write(f"\tMAP =\t{_map}\n")
                    fp.write(f"\tNOME =\t{pt['nome']}\n")
                    fp.write(f"\tTCV =\t{pt['tcv']}\n")
                    fp.write(f"\tTTP =\t{pt['ttp']}\n")
                    fp.write(f"\tNSRV1 =\t{nsrv1}\n")
                    fp.write(f"\tNSRV2 =\t{nsrv2}\n")
                    fp.write(f"\tTIPO =\t{tipo}\n")
                    fp.write(f"\tVERBD =\t{verbd}\n")
                    
                    num_reg[ent] += 1
                    cnt += 1

            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}\n")
            fp.write(f"// Total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO TCL.DAT
def generate_tcl_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    lia_bidirec: List[str],
    versao_num_base: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo TCL.dat a partir das fórmulas e conexões.
    """
    ent = 'tcl'
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force
    
    # A consulta SQL é formatada para ser compatível com placeholders do Python
    sql = f"""
    SELECT
        cod_formula as nseq,
        id as id,
        descricao as descr,
        formula as formula,
        nparcelas as nparcelas,
        tipo_calc as tcl,
        case when (id='G_LIA' or id='G_ENU')
          then (select group_concat(id_sage_aq separator ',')
                from id_conexoes
                where cod_noh_dst=%s and end_org not in ('0', '-1')
               )
          else ''
          end as idaq_list,
        case when (id='G_LID')
        then (select group_concat(id_sage_dt separator ',')
              from id_conexoes
              where cod_noh_org=%s and end_org not in ('0', '-1')
             )
        else ''
        end as iddt_list
    FROM
        id_formulas
    WHERE id not like 'PENG%%'
    ORDER BY
        tipo_calc desc, cod_formula
    """

    logging.info(f"[{ent}] Executando SQL para TCL.")
    try:
        with conn.cursor() as cur:
            params = (cod_noh, cod_noh)
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return
    
    # Inicia contadores
    cnt = 0
    num_reg = defaultdict(int)
    
    # Lógica para tratamento de fórmulas
    formula_max_len = 132

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")
            
            # Cabeçalho padrão de arquivo gerado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                if pt["id"] == "G_ENU":
                    if pt["idaq_list"]:
                        for idaq in pt["idaq_list"].split(","):
                            idaq = idaq.strip()
                            if idaq not in lia_bidirec:
                                fp.write("\n")
                                fp.write(f"{ent.upper()}\n")
                                fp.write(f"\tDESCR =\tMonitoracao do canal principal {idaq}\n")
                                fp.write(f"\tID =\t{idaq[:5]}-AQ_P\n")
                                fp.write(f"\tFORMULA = enu[{idaq}-AQ_P].e_falha\n")
                                fp.write(f"\tNSEQ =\t250\n")

                                fp.write("\n")
                                fp.write(f"{ent.upper()}\n")
                                fp.write(f"\tDESCR =\tMonitoracao do canal reserva {idaq}\n")
                                fp.write(f"\tID =\t{idaq[:5]}-AQ_R\n")
                                fp.write(f"\tFORMULA = enu[{idaq}-AQ_R].e_falha\n")
                                fp.write(f"\tNSEQ =\t250\n")
                                
                                num_reg[ent] += 2
                                cnt += 2
                
                elif pt["id"] == "G_LIA":
                    if pt["idaq_list"]:
                        for idaq in pt["idaq_list"].split(","):
                            idaq = idaq.strip()
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"\tDESCR =\tMonitoracao da ligacao de aquisicao {idaq}\n")
                            fp.write(f"\tID =\t{idaq[:5]}-AQ\n")
                            if idaq in lia_bidirec:
                                fp.write(f"\tFORMULA = NOT(lia[{idaq}].opera)\n")
                            else:
                                fp.write(f"\tFORMULA = NOT(lia[{idaq}-AQ].opera)\n")
                            fp.write(f"\tNSEQ =\t250\n")
                            
                            num_reg[ent] += 1
                            cnt += 1

                elif pt["id"] == "G_LID":
                    if pt["iddt_list"]:
                        for iddt in pt["iddt_list"].split(","):
                            iddt = iddt.strip()
                            if iddt not in lia_bidirec:
                                fp.write("\n")
                                fp.write(f"{ent.upper()}\n")
                                fp.write(f"\tDESCR =\tMonitoracao da ligacao de distribuicao {iddt}\n")
                                fp.write(f"\tID =\t{iddt[:5]}-DT\n")
                                fp.write(f"\tFORMULA = NOT(lid[{iddt}-DT].estad)\n")
                                fp.write(f"\tNSEQ =\t250\n")
                                
                                num_reg[ent] += 1
                                cnt += 1
                
                else: # Fórmulas genéricas
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"\tDESCR =\t{pt['descr']}\n")
                    fp.write(f"\tID =\t{pt['id']}\n")
                    
                    if pt["tcl"] == "I":
                        if len(str(pt["formula"])) > formula_max_len:
                            logging.error(f"Erro em TCL: campo fórmula com tamanho maior que {formula_max_len} em {pt['id']}")
                            # Slay no PHP geralmente para a execução, em Python é melhor registrar o erro e continuar se possível
                            # Ou levantar uma exceção
                            raise ValueError(f"Fórmula muito longa para {pt['id']}")

                        if pt["id"] == "I_VERSAO":
                            fp.write(f"\tFORMULA ={versao_num_base}+0*P1\n")
                        else:
                            fp.write(f"\tFORMULA ={pt['formula']}\n")
                        
                        fp.write(f"\tNSEQ =\t255\n")
                    else:
                        fp.write(f"\tNSEQ =\t{pt['nseq']}\n")
                    
                    num_reg[ent] += 1
                    cnt += 1

            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}\n")
            fp.write(f"// Total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO TAC.DAT
def generate_tac_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_dst: List[int],
    no_cos: bool,
    no_cor: bool,
    no_cps: bool,
    conex_ons_cos: int,
    conex_ons_cor: int,
    conex_cor_cos: int,
    ems: int,
    max_pontos_dig_por_tac: int,
    gestao_da_comunicacao: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo TAC.dat a partir das conexões de destino e estações.
    """
    ent = 'tac'
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force
    
    # Prepara os placeholders para a cláusula IN
    ph_dst = ",".join(["%s"] * len(conexoes_dst))

    # Consulta principal
    sql_principal = f"""
    select
    distinct e.estacao,
    e.cod_estacao,
    e.descricao as descricao,
    c.id_sage_aq as id_conex_aq,
    c.cod_protocolo as cod_protocolo,
    c.cod_conexao as cod_conexao,
    e.ems_modela
    from
      id_ptfis_conex as f,
      id_conexoes as c,
      id_ptlog_noh as l,
      id_ponto as i
      join id_nops n on n.cod_nops=i.cod_nops
      join id_modulos m on m.cod_modulo=n.cod_modulo
      join id_estacao e on e.cod_estacao=m.cod_estacao
    where
      f.cod_conexao in ({ph_dst}) and
      f.cod_conexao = c.cod_conexao and
      f.id_dst=i.nponto and
      l.nponto=i.nponto and l.cod_nohsup=%s and
      i.cod_tpeq!=95 and
      i.nponto not in (0, 9991, 9992) and
      !(e.cod_estacao!=76 and i.cod_origem=16) and
      !(e.cod_estacao!=67 and i.cod_origem=17)
    order by
    c.cod_conexao,
    e.estacao
    """

    logging.info(f"[{ent}] Executando SQL principal para TAC.")
    try:
        with conn.cursor() as cur:
            params_principal = tuple(conexoes_dst) + (cod_noh,)
            cur.execute(sql_principal, params_principal)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # Variáveis de estado e contadores
    tac_conex: Dict[int, str] = {}
    tac_estacao: List[str] = []
    taccomant = ""
    cnt = 0
    num_reg = defaultdict(int)

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")
            
            # Cabeçalho padrão
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                estacao = pt.get("estacao", "")
                cod_estacao = pt.get("cod_estacao")
                id_conex_aq = pt.get("id_conex_aq", "")
                cod_conexao = pt.get("cod_conexao")
                
                # Sub-query 1: conta conexões distintas para a mesma estação
                sql_numconx = f"""
                select
                distinct e.estacao
                from
                  id_ptfis_conex as f,
                  id_conexoes as c,
                  id_ptlog_noh as l,
                  id_ponto as i
                  join id_nops n on n.cod_nops=i.cod_nops
                  join id_modulos m on m.cod_modulo=n.cod_modulo
                  join id_estacao e on e.cod_estacao=m.cod_estacao
                where
                  f.cod_conexao in ({ph_dst}) and
                  f.cod_conexao = c.cod_conexao and
                  f.id_dst=i.nponto and
                  l.nponto=i.nponto and l.cod_nohsup=%s and
                  i.cod_tpeq!=95 and
                  e.cod_estacao = %s and
                  i.nponto not in (0, 9991, 9992) and
                  f.cod_conexao not in ( %s, %s, 86 )
                """
                with conn.cursor() as cur_numconx:
                    params_numconx = tuple(conexoes_dst) + (cod_noh, cod_estacao, conex_ons_cos, conex_ons_cor)
                    cur_numconx.execute(sql_numconx, params_numconx)
                    numconx = len(cur_numconx.fetchall())
                
                # Sub-query 2: conta estações para a mesma conexão
                sql_numest = f"""
                select
                distinct
                 e.estacao
                from
                  id_ptfis_conex as f,
                  id_conexoes as c,
                  id_ptlog_noh as l,
                  id_ponto as i
                  join id_nops n on n.cod_nops=i.cod_nops
                  join id_modulos m on m.cod_modulo=n.cod_modulo
                  join id_estacao e on e.cod_estacao=m.cod_estacao
                where
                  f.cod_conexao in ({ph_dst}) and
                  f.cod_conexao = c.cod_conexao and
                  f.id_dst=i.nponto and
                  l.nponto=i.nponto and l.cod_nohsup=%s and
                  i.cod_tpeq!=95 and
                  i.nponto not in (0, 9991, 9992) and
                  c.cod_conexao=%s
                """
                with conn.cursor() as cur_numest:
                    params_numest = tuple(conexoes_dst) + (cod_noh, cod_conexao)
                    cur_numest.execute(sql_numest, params_numest)
                    numest = len(cur_numest.fetchall())

                # Sub-query 3: conta pontos digitais
                sql_numdig = """
                select i.nponto
                from
                        id_ptlog_noh as l
                        join id_ponto as i on l.nponto=i.nponto
                        join id_nops n on n.cod_nops=i.cod_nops
                        join id_modulos m on m.cod_modulo=n.cod_modulo
                        join id_estacao e on e.cod_estacao=m.cod_estacao
                        join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
                        join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
                where
                        l.cod_nohsup=%s and
                        tpnt.tipo='D' and
                        i.cod_origem!=7 and
                        i.cod_tpeq!=95 and
                        e.cod_estacao=%s
                """
                with conn.cursor() as cur_numdig:
                    params_numdig = (cod_noh, cod_estacao)
                    cur_numdig.execute(sql_numdig, params_numdig)
                    num_pts_dig = len(cur_numdig.fetchall())

                # Lógica de exclusão ("bacalhau")
                skip_tac = False
                if (no_cos and cod_conexao == conex_ons_cos) or \
                   ((no_cor or no_cps) and cod_conexao == conex_ons_cor):
                    logging.info(f"Ignorando TAC para {cod_conexao} {estacao} (condição ONS).")
                    skip_tac = True
                elif no_cos and cod_conexao == conex_cor_cos and estacao not in ("CORX", "ECEZ", "ECEY"):
                    logging.info(f"Ignorando TAC para {cod_conexao} {estacao} (condição COR_COS).")
                    skip_tac = True
                
                if skip_tac:
                    continue

                # Início da escrita do bloco
                if numconx > 1:
                    if cod_conexao not in tac_conex:
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"\tID =\t{id_conex_aq}\n")
                        fp.write(f"\tNOME =\t{id_conex_aq} - {pt['descricao']}\n")
                        if pt["cod_protocolo"] == 10:
                            fp.write(f"\tLSC =\t{id_conex_aq}\n")
                        else:
                            fp.write(f"\tLSC =\t{id_conex_aq}-AQ\n")
                        fp.write(f"\tTPAQS =\tASAC\n")
                        if ems and pt["ems_modela"] == "S" and numest == 1:
                            fp.write(f"\tINS =\t{estacao}\n")
                        else:
                            fp.write(f"\tINS =\t\n")
                        num_reg[ent] += 1
                    tac_conex[cod_conexao] = id_conex_aq
                else:
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"\tID =\t{estacao}\n")
                    tac_estacao.append(estacao)
                    fp.write(f"\tNOME =\t{estacao} - {pt['descricao']}\n")
                    if pt["cod_protocolo"] == 10:
                        fp.write(f"\tLSC =\t{id_conex_aq}\n")
                    else:
                        fp.write(f"\tLSC =\t{id_conex_aq}-AQ\n")
                    fp.write(f"\tTPAQS =\tASAC\n")
                    if ems and pt["ems_modela"] == "S":
                        fp.write(f"\tINS =\t{estacao}\n")
                    else:
                        fp.write(f"\tINS =\t\n")
                    num_reg[ent] += 1

                    # Lógica para TACs adicionais se o número de pontos digitais exceder o limite
                    extra_tacs = int(num_pts_dig / max_pontos_dig_por_tac)
                    for i in range(1, extra_tacs):
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"\tID =\t{estacao}_{i}\n")
                        tac_estacao.append(estacao)
                        fp.write(f"\tNOME =\t{estacao} - {pt['descricao']}\n")
                        if pt["cod_protocolo"] == 10:
                            fp.write(f"\tLSC =\t{id_conex_aq}\n")
                        else:
                            fp.write(f"\tLSC =\t{id_conex_aq}-AQ\n")
                        fp.write(f"\tTPAQS =\tASAC\n")
                        if ems and pt["ems_modela"] == "S":
                            fp.write(f"\tINS =\t{estacao}\n")
                        else:
                            fp.write(f"\tINS =\t\n")
                        num_reg[ent] += 1
                
                # TAC para gestão da comunicação
                if taccomant != id_conex_aq:
                    taccomant = id_conex_aq
                    if gestao_da_comunicacao:
                        fp.write(f"\n{ent.upper()}\n")
                        fp.write(f"\tID =\t{id_conex_aq}-COM\n")
                        fp.write(f"\tLSC =\t{id_conex_aq}-AQ\n")
                        fp.write(f"\tNOME =\tContrl.Comunic.- {pt['descricao']}\n")
                        fp.write(f"\tTPAQS =\tASAC\n")
                        num_reg[ent] += 1

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={estacao}")

            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}\n")
            fp.write(f"// Total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
        return {
            "tac_conex": tac_conex,
            "tac_estacao": tac_estacao
        }
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO TDD.DAT
def generate_tdd_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    max_pontos_ana_por_tdd: int,
    max_pontos_dig_por_tdd: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo TDD.dat a partir das conexões de origem.
    """
    ent = "tdd"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force
    in_placeholders = ",".join(["%s"] * len(conexoes_org))
    sql = f"""
    select
    distinct (c.id_sage_dt) as id_conex,
    c.descricao as nome,
    c.cod_conexao,
    count(*) as cnt,
    tpnt.tipo,
    c.cod_protocolo
    from
      id_ptfis_conex as f,
      id_conexoes as c,
      id_ponto as i
      join id_ptlog_noh as l on l.nponto=i.nponto
      join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
      join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
    where
      f.cod_conexao in ({in_placeholders}) and
      f.cod_conexao = c.cod_conexao and
      f.id_org=i.nponto and
      c.cod_noh_org=%s and
      l.cod_nohsup=%s and
      i.cod_tpeq!=95
    group by cod_conexao, tpnt.tipo
    order by
    c.cod_conexao
    """

    logging.info(f"[{ent}] Executando SQL para TDD.")
    try:
        with conn.cursor() as cur:
            params = tuple(conexoes_org + [cod_noh, cod_noh])
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de TDD: {e}", exc_info=True)
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # Cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n\n")

            cnt = 0
            for pt in rows:
                id_conex = str(pt.get("id_conex", "") or "").strip()
                nome = str(pt.get("nome", "") or "").strip()
                tipo_pts = str(pt.get("tipo", "") or "").strip()
                cod_protocolo = pt.get("cod_protocolo")
                cnt_raw = pt.get("cnt", 0) or 0

                if tipo_pts == "A":
                    fimtdd = int(1 + cnt_raw / max_pontos_ana_por_tdd)
                else:
                    fimtdd = int(1 + cnt_raw / max_pontos_dig_por_tdd)

                for i in range(1, fimtdd + 1):
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")

                    fp.write(f"\tID =\t{id_conex}{tipo_pts}{i}\n")
                    fp.write(f"\tNOME =\t{nome}-{tipo_pts}-Parte {i}\n")

                    if cod_protocolo == 10:
                        fp.write(f"\tLSC =\t{id_conex}\n")
                    else:
                        fp.write(f"\tLSC =\t{id_conex}-DT\n")

                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} ID={id_conex}{tipo_pts}{i}")

            # Rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO NV1.DAT
def generate_nv1_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    gestao_da_comunicacao: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo NV1.dat (Nível 1 de Comunicação) a partir das conexões.
    """
    ent = 'nv1'
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    # Combina as conexões para a cláusula IN
    all_conexoes = conexoes_org + conexoes_dst
    ph_all = ",".join(["%s"] * len(all_conexoes))
    
    sql = f"""
    select
        c.cod_conexao as cod_conexao,
        c.cod_noh_org as cod_noh_org,
        c.cod_noh_dst as cod_noh_dst,
        p.sufixo_sage as sufixo_sage,
        c.id_sage_aq as id_conex_aq,
        c.id_sage_dt as id_conex_dt,
        c.cod_protocolo as cod_protocolo,
        c.descricao as descricao,
        p.grupo_protoc as grupo_protoc
    from
        id_conexoes as c
        join id_protocolos p on p.cod_protocolo=c.cod_protocolo
    where
        (c.cod_conexao in ({ph_all}))
    union
        (select 999999 as cod_conexao, 0, 0, '', '', '', 0, '', 0)
    order by
        cod_conexao
    """

    logging.info(f"[{ent}] Executando SQL para NV1.")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(all_conexoes))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}", exc_info=True)
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return
    
    # Dicionários para armazenar as ordens de NV1
    ordemnv1_sage_gc = {}
    ordemnv1_sage_aq = {}
    ordemnv1_sage_ct = {}
    ordemnv1_sage_dt = {}

    # Variáveis de estado e contadores
    cod_conexant = None
    ordem = 0
    cnt = 0
    num_reg = defaultdict(int)
    id_conex_aq_ant = ""
    sufixo_sage_ant = ""
    cod_conexao_ant = None
    cod_noh_dst_ant = None
    cod_protocolo_ant = None
    
    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")
            
            # Cabeçalho padrão
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                cod_conexao = pt.get("cod_conexao")
                
                if cod_conexao is None:
                    continue
                
                # Se mudou a conexão
                if cod_conexant != cod_conexao:
                    # Lógica para gestão da comunicação da conexão anterior
                    if gestao_da_comunicacao and cnt > 0 and cod_noh_dst_ant == int(cod_noh) and cod_protocolo_ant != 10:
                        ordem += 1
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"\tCNF =\t{id_conex_aq_ant}-AQ\n")
                        nv1_gc = f"{id_conex_aq_ant}_G{sufixo_sage_ant}_{ordem}"
                        fp.write(f"\tCONFIG =\t(Gestão da Comunic. {id_conex_aq_ant}-AQ)\n")
                        fp.write(f"\tTN1 =\tG{sufixo_sage_ant}\n")
                        ordemnv1_sage_gc[cod_conexao_ant] = ordem
                        fp.write(f"\tORDEM =\t{ordem}\n")
                        fp.write(f"\tID =\t{nv1_gc}\n")
                    
                    ordem = 1
                    cod_conexant = cod_conexao
                else:
                    ordem += 1
                
                # Armazena os valores da conexão atual para a próxima iteração
                id_conex_aq_ant = pt.get("id_conex_aq", "")
                sufixo_sage_ant = pt.get("sufixo_sage", "")
                cod_conexao_ant = pt.get("cod_conexao", None)
                cod_noh_dst_ant = pt.get("cod_noh_dst", None)
                cod_protocolo_ant = pt.get("cod_protocolo", None)

                # Pula o placeholder final
                if cod_conexao >= 999999:
                    continue
                
                # Lógica para aquisição
                if pt["cod_noh_dst"] == int(cod_noh):
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    
                    if pt["cod_protocolo"] == 10: # ICCP
                        fp.write(f"\tCNF =\t{pt['id_conex_aq']}\n")
                    else:
                        fp.write(f"\tCNF =\t{pt['id_conex_aq']}-AQ\n")
                    
                    if pt["cod_protocolo"] == 10:
                        nv1 = f"{pt['id_conex_aq']}_NV1"
                    else:
                        nv1 = f"{pt['id_conex_aq']}_A{pt['sufixo_sage']}_{ordem}"
                    
                    fp.write(f"\tCONFIG =\t(Aquisição de Dados {pt['id_conex_aq']}-AQ)\n")
                    if pt["cod_protocolo"] == 10:
                        fp.write(f"\tTN1 =\tNLN1\n")
                    else:
                        fp.write(f"\tTN1 =\tA{pt['sufixo_sage']}\n")
                    
                    ordemnv1_sage_aq[pt["cod_conexao"]] = ordem
                    fp.write(f"\tORDEM =\t{ordem}\n")
                    fp.write(f"\tID =\t{nv1}\n")

                    # Lógica para comandos na mesma conexão
                    sql_cmd = f"select 1 from id_ptfis_conex f join id_ponto i on f.id_dst=i.nponto where i.cod_origem=7 and f.cod_conexao={pt['cod_conexao']} limit 1"
                    with conn.cursor() as cur_cmd:
                        cur_cmd.execute(sql_cmd)
                        has_cmd = cur_cmd.fetchone() is not None
                    
                    if has_cmd and pt["cod_protocolo"] != 10:
                        ordem += 1
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"\tCNF =\t{pt['id_conex_aq']}-AQ\n")
                        nv1_ct = f"{pt['id_conex_aq']}_C{pt['sufixo_sage']}_{ordem}"
                        fp.write(f"\tCONFIG =\t(Controle Supervisório {pt['id_conex_aq']}-AQ)\n")
                        fp.write(f"\tTN1 =\tC{pt['sufixo_sage']}\n")
                        ordemnv1_sage_ct[pt["cod_conexao"]] = ordem
                        fp.write(f"\tORDEM =\t{ordem}\n")
                        fp.write(f"\tID =\t{nv1_ct}\n")

                # Lógica para distribuição
                else:
                    if pt["cod_protocolo"] != 10: # Não é ICCP
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"\tCNF =\t{pt['id_conex_dt']}-DT\n")
                        
                        if pt["grupo_protoc"] == 8: # DNP
                            fp.write(f"\tCONFIG= Classe= 1 \t(Distrib.Dados {pt['id_conex_dt']}-DT)\n")
                        else:
                            fp.write(f"\tCONFIG =\t(Distribuição de Dados {pt['id_conex_dt']}-DT)\n")
                        
                        fp.write(f"\tTN1 =\tD{pt['sufixo_sage']}\n")
                        nv1_dt = f"{pt['id_conex_dt']}_D{pt['sufixo_sage']}_{ordem}"
                        ordemnv1_sage_dt[pt["cod_conexao"]] = ordem
                        fp.write(f"\tORDEM =\t{ordem}\n")
                        fp.write(f"\tID =\t{nv1_dt}\n")
                    else: # ICCP - apenas armazena a ordem
                        ordemnv1_sage_dt[pt["cod_conexao"]] = ordem

                num_reg[ent] += 1
                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={pt['descricao']}")

            # Lógica para a gestão da comunicação na última conexão
            if gestao_da_comunicacao and cnt > 0 and cod_noh_dst_ant == int(cod_noh) and cod_protocolo_ant != 10:
                ordem += 1
                fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"\tCNF =\t{id_conex_aq_ant}-AQ\n")
                nv1_gc = f"{id_conex_aq_ant}_G{sufixo_sage_ant}_{ordem}"
                fp.write(f"\tCONFIG =\t(Gestão da Comunic. {id_conex_aq_ant}-AQ)\n")
                fp.write(f"\tTN1 =\tG{sufixo_sage_ant}\n")
                ordemnv1_sage_gc[cod_conexao_ant] = ordem
                fp.write(f"\tORDEM =\t{ordem}\n")
                fp.write(f"\tID =\t{nv1_gc}\n")
            
            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}\n")
            fp.write(f"// Total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
        # Retorna os dicionários de ordem para uso em outras funções (como NV2, CGF)
        return {
            "ordemnv1_sage_gc": ordemnv1_sage_gc,
            "ordemnv1_sage_aq": ordemnv1_sage_aq,
            "ordemnv1_sage_ct": ordemnv1_sage_ct,
            "ordemnv1_sage_dt": ordemnv1_sage_dt,
        }
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO NV2.DAT
def generate_nv2_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    gestao_da_comunicacao: int,
    ordemnv1_sage_aq: Dict[int, int],
    ordemnv1_sage_ct: Dict[int, int],
    ordemnv1_sage_dt: Dict[int, int],
    ordemnv1_sage_gc: Dict[int, int],
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo NV2.dat (Nível 2 de Comunicação) a partir das conexões.
    """
    ent = 'nv2'
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force
    
    # Prepara placeholders para as consultas UNION
    ph_dst = ",".join(["%s"] * len(conexoes_dst))
    ph_org = ",".join(["%s"] * len(conexoes_org))

    sql = f"""
select
distinct
a.tn2_aq as tipo,
a.tipo as tipoad,
c.cod_noh_org as cod_noh_org,
c.cod_noh_dst as cod_noh_dst,
p.sufixo_sage as sufixo_sage,
c.id_sage_aq as id_conex_aq,
c.id_sage_dt as id_conex_dt,
c.cod_protocolo as cod_protocolo,
c.cod_conexao as cod_conexao,
if (c.id_sage_aq!='', c.id_sage_aq, c.id_sage_dt) as id_conex,
if (c.cod_noh_org=$s, 'D', 'A') as aq_dt
from
  id_ptfis_conex as f
  join id_protoc_asdu as a on a.cod_asdu=f.cod_asdu,
  id_ptlog_noh as l,
  id_ponto as i
  join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
  join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt 
  join id_nops n on n.cod_nops=i.cod_nops
  join id_modulos m on m.cod_modulo=n.cod_modulo
  join id_estacao e on e.cod_estacao=m.cod_estacao, 
  id_conexoes as c
  join id_protocolos p on p.cod_protocolo=c.cod_protocolo
where
  (f.cod_conexao in ({ph_dst}))
  and
  f.cod_conexao = c.cod_conexao and
  f.id_dst=i.nponto and
  l.nponto=i.nponto and l.cod_nohsup=%s and
  i.cod_tpeq!=95

union

select
distinct
a.tn2_dt as tipo,
a.tipo as tipoad,
c.cod_noh_org as cod_noh_org,
c.cod_noh_dst as cod_noh_dst,
p.sufixo_sage as sufixo_sage,
c.id_sage_aq as id_conex_aq,
c.id_sage_dt as id_conex_dt,
c.cod_protocolo as cod_protocolo,
c.cod_conexao as cod_conexao,
if (c.id_sage_aq!='', c.id_sage_aq, c.id_sage_dt) as id_conex,
if (c.cod_noh_org=%s, 'D', 'A') as aq_dt
from
  id_ptfis_conex as f
  join id_protoc_asdu as a on a.cod_asdu=f.cod_asdu,
  id_ptlog_noh as l,
  id_ponto as i
  join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
  join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt 
  join id_nops n on n.cod_nops=i.cod_nops
  join id_modulos m on m.cod_modulo=n.cod_modulo
  join id_estacao e on e.cod_estacao=m.cod_estacao, 
  id_conexoes as c
  join id_protocolos p on p.cod_protocolo=c.cod_protocolo
where
  ( f.cod_conexao in ({ph_org}) )
  and
  f.cod_conexao = c.cod_conexao and
  f.id_org=i.nponto and
  l.nponto=i.nponto and l.cod_nohsup=%s and
  i.cod_tpeq!=95

union (select '','',0,0,/*0,0,0,*/'','','',999999,999999,'','')  

order by
cod_protocolo,
id_conex,
aq_dt,
cod_conexao,
tipo
    """
    logging.info(f"[{ent}] Executando SQL para NV2.")
    try:
        with conn.cursor() as cur:
            params = tuple(conexoes_dst) + (cod_noh, ) + tuple(conexoes_org) + (cod_noh,)
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}", exc_info=True)
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return
    
    # Variáveis de estado e contadores
    nv1ant = "@#^%"
    cod_conexant = None
    ordem = 0
    cnt = 0
    num_reg = defaultdict(int)
    id_conex_aq_ant = ""
    sufixo_sage_ant = ""
    cod_conexao_ant = None
    cod_noh_dst_ant = None
    cod_protocolo_ant = None
    
    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")
            
            # Cabeçalho padrão
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                cod_conexao = pt.get("cod_conexao")
                
                if cod_conexao is None:
                    continue
                
                # Se mudou a conexão
                if cod_conexant != cod_conexao:
                    # Lógica para gestão da comunicação da conexão anterior
                    if gestao_da_comunicacao and cnt > 0 and cod_noh_dst_ant == int(cod_noh) and cod_protocolo_ant != 10:
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"\tCONFIG =\t(Gestão da Comunic. {id_conex_aq_ant}-AQ)\n")
                        nv1 = f"{id_conex_aq_ant}_G{sufixo_sage_ant}_{ordemnv1_sage_gc[cod_conexao_ant]}"
                        fp.write(f"\tID =\t{nv1}_CGCD\n")
                        fp.write(f"\tNV1 =\t{nv1}\n")
                        fp.write(f"\tORDEM =\t1\n")
                        fp.write(f"\tTN2 =\tCGCD\n")
                        fp.write(f"\tTPPNT =\tCGF\n")
                    
                    cod_conexant = cod_conexao
                
                # Armazena os valores da conexão atual para a próxima iteração
                id_conex_aq_ant = pt.get("id_conex_aq", "")
                sufixo_sage_ant = pt.get("sufixo_sage", "")
                cod_conexao_ant = pt.get("cod_conexao", None)
                cod_noh_dst_ant = pt.get("cod_noh_dst", None)
                cod_protocolo_ant = pt.get("cod_protocolo", None)

                # Pula o placeholder final
                if cod_conexao >= 999999:
                    continue
                
                # Lógica para aquisição
                fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                
                nv1 = ""
                if pt["cod_noh_dst"] == int(cod_noh): # AQUISIÇÃO
                    fp.write(f"\tCONFIG =\t{pt['tipo']} {pt['id_conex_aq']}-AQ\n")
                    if str(pt["tipo"]).startswith("C") or str(pt["tipo"]).startswith("S"):
                        nv1 = f"{pt['id_conex_aq']}_C{pt['sufixo_sage']}_{ordemnv1_sage_ct[pt['cod_conexao']]}"
                    else:
                        nv1 = f"{pt['id_conex_aq']}_A{pt['sufixo_sage']}_{ordemnv1_sage_aq[pt['cod_conexao']]}"
                    
                    if pt["cod_protocolo"] == 10:
                        nv1 = f"{pt['id_conex_aq']}_NV1"
                    
                else: # DISTRIBUIÇÃO
                    fp.write(f"\tCONFIG =\t{pt['tipo']} {pt['id_conex_dt']}-DT\n")
                    nv1 = f"{pt['id_conex_dt']}_D{pt['sufixo_sage']}_{ordemnv1_sage_dt[pt['cod_conexao']]}"
                    
                    if pt["cod_protocolo"] == 10:
                        nv1 = f"{pt['id_conex_dt']}_NV1"
                
                # Conta ordem para o mesmo nv1
                if nv1ant != nv1:
                    ordem = 1
                    nv1ant = nv1
                else:
                    ordem += 1

                if pt["cod_protocolo"] == 10:
                    fp.write(f"\tID =\t{pt['id_conex']}_{pt['tipo']}_NV2\n")
                else:
                    fp.write(f"\tID =\t{nv1}_{pt['tipo']}\n")
                
                fp.write(f"\tNV1 =\t{nv1}\n")
                fp.write(f"\tORDEM =\t{ordem}\n")
                fp.write(f"\tTN2 =\t{pt['tipo']}\n")
                
                if pt["tipoad"] == "C" or pt["tipoad"] == "S":
                    fp.write(f"\tTPPNT =\tCGF\n")
                elif pt["tipoad"] == "A":
                    fp.write(f"\tTPPNT =\tPAF\n")
                else:
                    fp.write(f"\tTPPNT =\tPDF\n")

                num_reg[ent] += 1
                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={nv1}_{pt['tipo']}")

            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}\n")
            fp.write(f"// Total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO TELA.DAT
def generate_tela_dat(paths: Dict[str, Path], conn, cod_noh: str, ems: bool, dry_run: bool = False, force: bool = False):
    if not ems:
        logging.info("[tela] EMS desabilitado, pulando geração de TELAS.")
        return

    ent = "tela"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
    distinct 
        i.estacao as id, 
        i.descricao as nome, 
        i.tipo as tipo, 
        i.cia as cia,
        i.param_ems_ins as param_ems
    from 
        id_emsestacao e 
        join id_estacao i on i.cod_estacao=e.cod_estacao 
    where 
        i.tipo <= 2 and
        i.cod_estacao > 0
        and i.ems_modela='S'
    order by i.estacao
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de TELAS: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query TELAS.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em TELAS. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                estacao_id = str(pt.get("id", "") or "").strip()
                cia = str(pt.get("cia", "") or "").strip()
                # monta o nome da tela conforme a lógica
                if cia == "" or cia == "CE":
                    tela = f"Unifilares/Tela_{estacao_id}"
                else:
                    tela = f"Unifilares/Tela_{estacao_id}_{cia}"

                # separa blocos com uma linha em branco (exceto primeiro)
                if cnt > 0:
                    fp.write("\n")

                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {tela}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={estacao_id}")

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO INS.DAT
def generate_ins_dat(paths: Dict[str, Path], conn, cod_noh: str, ems: bool, dry_run: bool = False, force: bool = False):
    if not ems:
        logging.info("[ins] EMS desabilitado, pulando geração de INS.")
        return

    ent = "ins"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
    distinct 
        i.estacao as id, 
        i.descricao as nome, 
        i.tipo as tipo, 
        i.cia as cia,
        i.param_ems_ins as param_ems
    from 
        id_emsestacao e 
        join id_estacao i on i.cod_estacao=e.cod_estacao 
    where 
        i.tipo <= 3 and
        i.cod_estacao > 0
        and i.ems_modela='S'
    order by i.estacao
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de INS: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query INS.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em INS. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                raw_id = str(pt.get("id", "") or "").strip()
                raw_nome = str(pt.get("nome", "") or "").strip()
                tipo_raw = pt.get("tipo")
                cia_raw = str(pt.get("cia", "") or "").strip()
                param_ems_raw = str(pt.get("param_ems") or "").strip()

                # fallback cia
                if cia_raw == "":
                    cia_raw = "CE"

                # monta tela
                if cia_raw == "" or cia_raw == "CE":
                    tela = f"Unifilares/Tela_{raw_id}"
                else:
                    tela = f"Unifilares/Tela_{raw_id}_{cia_raw}"

                # normaliza tipo
                tipo = ""
                if tipo_raw in (0, 3):
                    tipo = "SUB"
                else:
                    tipo = "USI"

                # normaliza nome com remoções sucessivas até caber em 13
                nome = raw_nome
                if len(nome) > 13:
                    nome = nome.replace("SE", "")
                if len(nome) > 13:
                    nome = nome.replace("U.T.", "")
                if len(nome) > 13:
                    nome = nome.replace("U.H.", "")
                if len(nome) > 13:
                    nome = nome.replace("ESUL", "")
                if len(nome) > 13:
                    nome = nome.replace(" do ", " ")
                nome = nome[:13]

                # formata param_ems: substitui espaços por quebras e coloca espaços ao redor de '='
                param_ems = param_ems_raw.replace("=", " = ")
                # mantém quebras de linha por espaço original substituído por nova linha
                param_ems = "\n".join(line for line in param_ems.split())

                # separa blocos
                if cnt > 0:
                    fp.write("\n")

                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {raw_id}\n")
                fp.write(f"NOME = {nome}\n")

                # escreve TIPO se não estiver em param_ems
                if "TIPO =" not in param_ems:
                    fp.write(f"TIPO = {tipo}\n")
                if "CIA =" not in param_ems:
                    fp.write(f"CIA = {cia_raw}\n")
                if "TELA =" not in param_ems:
                    fp.write(f"TELA = {tela}\n")

                if param_ems:
                    # já contém múltiplas linhas
                    fp.write(f"{param_ems}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={raw_id}")

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO USI.DAT
def generate_usi_dat(paths: Dict[str, Path], conn, cod_noh: str, ems: bool, dry_run: bool = False, force: bool = False):
    if not ems:
        logging.info("[usi] EMS desabilitado, pulando geração de USI.")
        return

    ent = "usi"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
    distinct 
        i.estacao as id, 
        i.descricao as nome, 
        i.tipo as tipo, 
        i.cia as cia,
        i.param_ems_usi as param_ems
    from 
        id_emsestacao e 
        join id_estacao i on i.cod_estacao=e.cod_estacao 
    where 
        i.tipo in (1, 2) and
        i.cod_estacao > 0
        and i.ems_modela='S'
    order by i.estacao
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de USI: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query USI.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em USI. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                raw_id = str(pt.get("id", "") or "").strip()
                tipo_raw = pt.get("tipo")
                param_ems_raw = str(pt.get("param_ems") or "").strip()

                # normaliza tipo
                if tipo_raw == 1:
                    tipo = "HIDRO"
                elif tipo_raw == 2:
                    tipo = "TERMICA"
                else:
                    tipo = str(tipo_raw)

                # formata param_ems conforme PHP: espaço -> newline, e adiciona espaço ao redor do '='
                param_ems = param_ems_raw.replace(" ", "\n")
                param_ems = param_ems.replace("=", " = ")

                # separa blocos
                if cnt > 0:
                    fp.write("\n")

                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {raw_id}\n")

                if "TIPO =" not in param_ems:
                    fp.write(f"TIPO = {tipo}\n")
                if "ORDFOLGA =" not in param_ems:
                    fp.write(f"ORDFOLGA = \n")

                if param_ems:
                    fp.write(f"{param_ems}\n")

                if "PMAX =" not in param_ems:
                    fp.write(f"PMAX = 10000\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={raw_id}")

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO AFP.DAT
def generate_afp_dat(paths: Dict[str, Path], conn, cod_noh: str, ems: bool, dry_run: bool = False, force: bool = False):
    if not ems:
        logging.info("[afp] EMS desabilitado, pulando geração de AFP.")
        return

    ent = "afp"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select
        distinct
        a.cod_areafp,
        a.nome
    from
        id_areafp a
    join id_emsestacao e on a.cod_areafp=e.cod_areafp
    where
        a.cod_areafp > 0
    order by a.cod_areafp
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de AFP: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query AFP.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em AFP. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                cod_areafp = str(pt.get("cod_areafp", "") or "").strip()
                nome = str(pt.get("nome", "") or "").strip()

                if cnt > 0:
                    fp.write("\n")

                fp.write(f"{ent.upper()}\n")
                fp.write(f"NOME = {nome}\n")
                fp.write(f"NUMERO = {cod_areafp}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={nome}")

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO EST.DAT
def generate_est_dat(paths: Dict[str, Path], conn, cod_noh: str, ems: bool, dry_run: bool = False, force: bool = False):
    if not ems:
        logging.info("[est] EMS desabilitado, pulando geração de EST.")
        return

    ent = "est"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select
        distinct e.id as id,
        i.estacao as ins,
        n.vnom as vnom,
        n.vbase as vbase,
        e.param_ems as param_ems,
        e.cod_areafp as cod_areafp,
        e.liami,
        e.liale,
        e.liame,
        e.liape,
        e.liama,
        e.liumi,
        e.liule,
        e.liume,
        e.liupe,
        e.liuma,
        e.lsami,
        e.lsale,
        e.lsame,
        e.lsape,
        e.lsama,
        e.lsumi,
        e.lsule,
        e.lsume,
        e.lsupe,
        e.lsuma
    from
        id_emsestacao e
        join id_estacao i on i.cod_estacao = e.cod_estacao
        join id_nivtensao n on n.cod_nivtensao = e.cod_nivtensao
        join id_modulos m on m.cod_emsest = e.cod_emsest
    where
        e.cod_estacao > 0
        and i.ems_modela = 'S'
        and m.ems_lig1 != ''
    order by i.estacao 
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de EST: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em EST. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                raw_id = str(pt.get("id", "") or "").strip()
                ins = str(pt.get("ins", "") or "").strip()
                vnom = pt.get("vnom") or 0.0
                vbase = pt.get("vbase") or 0.0
                param_ems_raw = str(pt.get("param_ems") or "").strip()
                cod_areafp = pt.get("cod_areafp")

                liami = pt.get("liami")
                liale = pt.get("liale")
                liame = pt.get("liame")
                liape = pt.get("liape")
                liama = pt.get("liama")
                liumi = pt.get("liumi")
                liule = pt.get("liule")
                liume = pt.get("liume")
                liupe = pt.get("liupe")
                liuma = pt.get("liuma")
                lsami = pt.get("lsami")
                lsale = pt.get("lsale")
                lsame = pt.get("lsame")
                lsape = pt.get("lsape")
                lsama = pt.get("lsama")
                lsumi = pt.get("lsumi")
                lsule = pt.get("lsule")
                lsume = pt.get("lsume")
                lsupe = pt.get("lsupe")
                lsuma = pt.get("lsuma")

                # formata param_ems como no PHP: espaços viram quebras e '=' com espaços
                param_ems = param_ems_raw.replace(" ", "\n").replace("=", " = ")

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {raw_id}\n")
                fp.write(f"INS = {ins}\n")
                fp.write(f"VNOM = {float(vnom):.6f}\n")
                fp.write(f"VBASE = {float(vbase):.6f}\n")
                if cod_areafp is not None and str(cod_areafp).strip() != "" and int(cod_areafp) > 0:
                    fp.write(f"AFP = {int(cod_areafp)}\n")

                if param_ems:
                    fp.write(f"{param_ems}\n")

                # limites físicos (só se não estiverem explicitamente em param_ems)
                if "LIFI =" not in param_ems:
                    fp.write(f"LIFI = {float(vnom) * 0.8:.6f}\n")
                if "LIOP =" not in param_ems:
                    fp.write(f"LIOP = {float(vnom) * 0.9:.6f}\n")
                if "LSOP =" not in param_ems:
                    fp.write(f"LSOP = {float(vnom) * 1.1:.6f}\n")
                if "LSFI =" not in param_ems:
                    fp.write(f"LSFI = {float(vnom) * 1.2:.6f}\n")

                # patamares de carga
                if liami is not None and liami > -999999:
                    fp.write(f"LIAMI = {float(liami):.6f}\n")
                    fp.write(f"LIEMI = {-999999:.6f}\n")
                if liale is not None and liale > -999999:
                    fp.write(f"LIALE = {float(liale):.6f}\n")
                    fp.write(f"LIELE = {-999999:.6f}\n")
                if liame is not None and liame > -999999:
                    fp.write(f"LIAME = {float(liame):.6f}\n")
                    fp.write(f"LIEME = {-999999:.6f}\n")
                if liape is not None and liape > -999999:
                    fp.write(f"LIAPE = {float(liape):.6f}\n")
                    fp.write(f"LIEPE = {-999999:.6f}\n")
                if liama is not None and liama > -999999:
                    fp.write(f"LIAMA = {float(liama):.6f}\n")
                    fp.write(f"LIEMA = {-999999:.6f}\n")

                if liumi is not None and liumi > -999999:
                    fp.write(f"LIUMI = {float(liumi):.6f}\n")
                if liule is not None and liule > -999999:
                    fp.write(f"LIULE = {float(liule):.6f}\n")
                if liume is not None and liume > -999999:
                    fp.write(f"LIUME = {float(liume):.6f}\n")
                if liupe is not None and liupe > -999999:
                    fp.write(f"LIUPE = {float(liupe):.6f}\n")
                if liuma is not None and liuma > -999999:
                    fp.write(f"LIUMA = {float(liuma):.6f}\n")

                if lsami is not None and lsami < 999999:
                    fp.write(f"LSAMI = {float(lsami):.6f}\n")
                    fp.write(f"LSEMI = {999999:.6f}\n")
                if lsale is not None and lsale < 999999:
                    fp.write(f"LSALE = {float(lsale):.6f}\n")
                    fp.write(f"LSELE = {999999:.6f}\n")
                if lsame is not None and lsame < 999999:
                    fp.write(f"LSAME = {float(lsame):.6f}\n")
                    fp.write(f"LSEME = {999999:.6f}\n")
                if lsape is not None and lsape < 999999:
                    fp.write(f"LSAPE = {float(lsape):.6f}\n")
                    fp.write(f"LSEPE = {999999:.6f}\n")
                if lsama is not None and lsama < 999999:
                    fp.write(f"LSAMA = {float(lsama):.6f}\n")
                    fp.write(f"LSEMA = {999999:.6f}\n")

                if lsumi is not None and lsumi < 999999:
                    fp.write(f"LSUMI = {float(lsumi):.6f}\n")
                if lsule is not None and lsule < 999999:
                    fp.write(f"LSULE = {float(lsule):.6f}\n")
                if lsume is not None and lsume < 999999:
                    fp.write(f"LSUME = {float(lsume):.6f}\n")
                if lsupe is not None and lsupe < 999999:
                    fp.write(f"LSUPE = {float(lsupe):.6f}\n")
                if lsuma is not None and lsuma < 999999:
                    fp.write(f"LSUMA = {float(lsuma):.6f}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={raw_id}")

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO BCP.DAT
def generate_bcp_dat(paths: Dict[str, Path], conn, cod_noh: str, ems: bool, dry_run: bool = False, force: bool = False):
    if not ems:
        logging.info("[bcp] EMS desabilitado, pulando geração de BCP.")
        return

    ent = "bcp"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
        m.ems_id as id, 
        e.id as est, 
        m.param_ems as param_ems  
    from 
        id_modulos m 
        join id_emsestacao e on e.cod_emsest=m.cod_emsest 
        join id_estacao i on i.cod_estacao=e.cod_estacao
    where 
        m.cod_tpmoduloems=4 and 
        m.cod_emsest!=0 and
        m.ems_id !='' and m.ems_lig1!='' 
        and i.ems_modela='S'
    order by
        m.ems_id    
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de BCP: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em BCP. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                bcp_id = str(pt.get("id", "") or "").strip()
                est = str(pt.get("est", "") or "").strip()
                param_raw = str(pt.get("param_ems") or "").strip()

                # formata param_ems como no PHP: espaços viram quebras e '=' com espaços
                param_ems = param_raw.replace(" ", "\n").replace("=", " = ")

                # remove explicitamente "INVSN = SIM" se existir
                cleaned_lines = []
                for line in param_ems.splitlines():
                    if line.strip().upper() == "INVSN = SIM":
                        continue
                    cleaned_lines.append(line)
                param_ems = "\n".join(cleaned_lines)

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {bcp_id}\n")
                fp.write(f"EST = {est}\n")
                if param_ems:
                    fp.write(f"{param_ems}\n")
                # se não tem "NOME =" em param, adiciona
                if "NOME =" not in param_ems.upper():
                    fp.write(f"NOME = {bcp_id}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={bcp_id}")

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CAR.DAT
def generate_car_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    cargas_eramltr: list,
    dry_run: bool = False,
    force: bool = False,
):
    if not ems:
        logging.info("[car] EMS desabilitado, pulando geração de CAR.")
        return cargas_eramltr

    ent = "car"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
        m.ems_id as id, 
        e.id as est, 
        m.param_ems as param_ems,
        i.cia as cia, 
        0 as era_ltr
    from 
        id_modulos m 
        join id_emsestacao e on e.cod_emsest=m.cod_emsest 
        join id_estacao i on i.cod_estacao=e.cod_estacao
    where 
        m.cod_tpmoduloems=18 and 
        m.cod_emsest!=0 and
        m.ems_id !='' and m.ems_lig1!='' 
        and i.ems_modela='S'

    -- acrescenta as LT's cujo outro lado está fora das estacoes do modelo
    union
    select
        md.ems_id as id,
        e.id as est,
        '' as param_ems,
        i.cia as cia,
        1 as era_ltr
    from
        id_modulos md
        join id_emsestacao e on e.cod_emsest=md.cod_emsest
        join id_estacao i on i.cod_estacao=e.cod_estacao
        left outer join id_modulos mp on mp.ems_id = md.ems_id
        left outer join id_estacao ip on mp.cod_estacao=ip.cod_estacao
        left outer join id_estacao id on md.cod_estacao=id.cod_estacao
    where
        md.cod_tpmoduloems=1
        and md.ems_id !=''
        and md.ems_lig1 !=''
        and id.ems_modela = 'S'
        and ip.ems_modela != 'S'
        and md.cod_emsest>0
        and mp.cod_emsest>0
        and md.cod_emsest!=0 
    order by
        id    
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de CAR: {e}", exc_info=True)
        return cargas_eramltr

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em CAR. Saindo.")
        return cargas_eramltr

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return cargas_eramltr

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                car_id = str(pt.get("id", "") or "").strip()
                est = str(pt.get("est", "") or "").strip()
                cia = str(pt.get("cia", "") or "").strip()
                if not cia:
                    cia = "CE"
                era_ltr = bool(pt.get("era_ltr", 0))
                param_raw = str(pt.get("param_ems") or "").strip()

                # formata param_ems como no PHP: espaços viram quebras e '=' com espaços
                param_ems = param_raw.replace(" ", "\n").replace("=", " = ")

                # remove linha "INVSN = SIM" se presente
                cleaned_lines = []
                for line in param_ems.splitlines():
                    if line.strip().upper() == "INVSN = SIM":
                        continue
                    cleaned_lines.append(line)
                param_ems = "\n".join(cleaned_lines)

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {car_id}\n")
                fp.write(f"EST = {est}\n")
                fp.write(f"GCA = {cia}_GERAL\n")
                if param_ems:
                    fp.write(f"{param_ems}\n")
                # se não tem NOME explicitamente
                if "NOME =" not in param_ems.upper():
                    fp.write(f"NOME = {car_id}\n")
                if "LSFI =" not in param_ems.upper():
                    fp.write("LSFI = 8000\n")
                if "LSOP =" not in param_ems.upper():
                    fp.write("LSOP = 7000\n")

                if era_ltr:
                    cargas_eramltr.append(car_id)

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={car_id}")

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)

    return cargas_eramltr
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CSI.DAT
def generate_csi_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    dry_run: bool = False,
    force: bool = False,
):
    if not ems:
        logging.info("[csi] EMS desabilitado, pulando geração de CSI.")
        return

    ent = "csi"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
        m.ems_id as id, 
        e.id as est, 
        m.param_ems as param_ems  
    from 
        id_modulos m 
        join id_emsestacao e on e.cod_emsest=m.cod_emsest 
        join id_estacao i on i.cod_estacao=e.cod_estacao
    where 
        m.cod_tpmoduloems=11 and 
        m.cod_emsest!=0 and
        m.ems_id !='' and m.ems_lig1!='' 
        and i.ems_modela='S'
    order by
        m.ems_id    
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de CSI: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em CSI. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                csi_id = str(pt.get("id", "") or "").strip()
                est = str(pt.get("est", "") or "").strip()
                param_raw = str(pt.get("param_ems") or "").strip()

                # formata param_ems como no PHP
                param_ems = param_raw.replace(" ", "\n").replace("=", " = ")

                # remove explicitamente INVSN = SIM, se existir
                lines = [ln for ln in param_ems.splitlines() if ln.strip().upper() != "INVSN = SIM"]
                param_ems = "\n".join(lines)

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {csi_id}\n")
                fp.write(f"EST = {est}\n")
                if param_ems:
                    fp.write(f"{param_ems}\n")
                if "NOME =" not in param_ems.upper():
                    fp.write(f"NOME = {csi_id}\n")
                if "LIFI =" not in param_ems.upper():
                    fp.write("LIFI = -500\n")
                if "LIOP =" not in param_ems.upper():
                    fp.write("LIOP = -400\n")
                if "LSFI =" not in param_ems.upper():
                    fp.write("LSFI = 500\n")
                if "LSOP =" not in param_ems.upper():
                    fp.write("LSOP = 400\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={csi_id}")

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO LTR.DAT
def generate_ltr_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    dry_run: bool = False,
    force: bool = False,
):
    if not ems:
        logging.info("[ltr] EMS desabilitado, pulando geração de LTR.")
        return

    ent = "ltr"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select
      md.ems_id as ltr,
      md.param_ems as param_ems1,
      mp.param_ems as param_ems2,
      ed.id as estde,
      ed.cod_emsest as codestde,
      ep.id as estpara,
      ep.cod_emsest as codestpara,
      n.vbase as vbase
    from 
      id_modulos md
      join id_emsestacao ed on ed.cod_emsest = md.cod_emsest
      join id_modulos mp on mp.ems_id = md.ems_id
      join id_emsestacao ep on ep.cod_emsest = mp.cod_emsest
      join id_nivtensao n on ed.cod_nivtensao = n.cod_nivtensao
      left outer join id_estacao ip on mp.cod_estacao = ip.cod_estacao
      left outer join id_estacao id on md.cod_estacao = id.cod_estacao
    where
      md.cod_tpmoduloems = 1
      and mp.cod_tpmoduloems = 1
      and md.cod_emsest > 0
      and md.ems_id != ''
      and md.ems_lig1 != ''
      and mp.cod_emsest > 0
      and mp.ems_id != ''
      and mp.ems_lig1 != ''
      and md.cod_emsest != mp.cod_emsest
      and ed.id < ep.id
      and id.ems_modela = 'S'
      and ip.ems_modela = 'S'
    order by
      md.ems_id
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de LTR: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em LTR. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                ltr_id = str(pt.get("ltr", "") or "").strip()
                estde = str(pt.get("estde", "") or "").strip()
                estpara = str(pt.get("estpara", "") or "").strip()
                vbase = pt.get("vbase", "")
                raw1 = str(pt.get("param_ems1") or "")
                raw2 = str(pt.get("param_ems2") or "")
                combined = (raw1 + raw2).strip()

                # formata param_ems
                param_ems = combined.replace(" ", "\n").replace("=", " = ")
                # remove "INVSN = SIM"
                lines = [ln for ln in param_ems.splitlines() if ln.strip().upper() != "INVSN = SIM"]
                param_ems = "\n".join(lines)

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {ltr_id}\n")
                fp.write(f"DE = {estde}\n")
                fp.write(f"PARA = {estpara}\n")
                fp.write(f"VBASE = {vbase}\n")
                if param_ems:
                    fp.write(f"{param_ems}\n")
                if "NOME =" not in param_ems.upper():
                    fp.write(f"NOME = {ltr_id}\n")
                if "CIA =" not in param_ems.upper():
                    fp.write("CIA = CE\n")
                if "LSFI =" not in param_ems.upper():
                    fp.write("LSFI = 5000\n")
                if "LSOP =" not in param_ems.upper():
                    fp.write("LSOP = 4000\n")
                if "S =" not in param_ems.upper():
                    fp.write("S = 1\n")
                if "R =" not in param_ems.upper():
                    fp.write("R = 0.01\n")
                if "X =" not in param_ems.upper():
                    fp.write("X = 0.01\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={ltr_id}")

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO RAM.DAT
def generate_ram_dat(
    paths: Dict[str, Path],
    conn,
    ems: bool,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo RAM.dat (Ramais) se EMS estiver habilitado.
    """
    if not ems:
        logging.info("[ram] EMS desabilitado, pulando geração de RAM.")
        return

    ent = "ram"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select
    md.ems_id as ram,
    md.param_ems as param_ems,
    ed.id as estde,
    ed.cod_emsest codestde,
    n.vbase as vbase
    from
    id_modulos md
    join id_emsestacao ed on ed.cod_emsest=md.cod_emsest
    join id_nivtensao n on ed.cod_nivtensao=n.cod_nivtensao
    left outer join id_estacao id on md.cod_estacao=id.cod_estacao
    where
    md.cod_tpmoduloems=19 and
    md.cod_emsest>0 and
    md.ems_id !='' and md.ems_lig1!=''
    and id.ems_modela = 'S'
    order by md.ems_id
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de RAM: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em RAM. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                ram_id = str(pt.get("ram", "") or "").strip()
                estde = str(pt.get("estde", "") or "").strip()
                vbase = str(pt.get("vbase", "") or "").strip()
                raw_params = str(pt.get("param_ems") or "").strip()
                
                # formata param_ems
                param_ems = raw_params.replace(" ", "\n").replace("=", " = ")
                
                # remove "INVSN = SIM"
                lines = [line for line in param_ems.splitlines() if line.strip().upper() != "INVSN = SIM"]
                param_ems = "\n".join(lines)
                
                if cnt > 0:
                    fp.write("\n")
                
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {ram_id}\n")
                fp.write(f"EST = {estde}\n")
                fp.write(f"VBASE = {vbase}\n")

                if param_ems:
                    fp.write(f"{param_ems}\n")

                if "NOME =" not in param_ems.upper():
                    fp.write(f"NOME = {ram_id}\n")
                if "CIA =" not in param_ems.upper():
                    fp.write("CIA = CE\n")
                if "LSFI =" not in param_ems.upper():
                    fp.write("LSFI = 5000\n")
                if "LSOP =" not in param_ems.upper():
                    fp.write("LSOP = 4000\n")
                if "S =" not in param_ems.upper():
                    fp.write("S = 1\n")
                if "R =" not in param_ems.upper():
                    fp.write("R = 0.01\n")
                if "X =" not in param_ems.upper():
                    fp.write("X = 0.01\n")
                    
                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={ram_id}")

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO REA.DAT
def generate_rea_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    dry_run: bool = False,
    force: bool = False,
):
    if not ems:
        logging.info("[rea] EMS desabilitado, pulando geração de REA.")
        return

    ent = "rea"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
        m.ems_id as id, 
        e.id as est, 
        m.param_ems as param_ems  
    from 
        id_modulos m 
        join id_emsestacao e on e.cod_emsest = m.cod_emsest 
        join id_estacao i on i.cod_estacao = e.cod_estacao
    where 
        m.cod_tpmoduloems = 5 
        and m.cod_emsest != 0
        and m.ems_id != '' 
        and m.ems_lig1 != '' 
        and i.ems_modela = 'S'
    order by
        m.ems_id
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de REA: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em REA. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                rea_id = str(pt["id"]).strip()
                est = str(pt["est"]).strip()
                raw = str(pt.get("param_ems") or "").strip()

                # formata param_ems
                param_ems = raw.replace(" ", "\n").replace("=", " = ")
                # remove "INVSN = SIM" se existir
                lines = [ln for ln in param_ems.splitlines() if ln.strip().upper() != "INVSN = SIM"]
                param_ems = "\n".join(lines)

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {rea_id}\n")
                fp.write(f"EST = {est}\n")
                if param_ems:
                    fp.write(f"{param_ems}\n")
                if "NOME =" not in param_ems.upper():
                    fp.write(f"NOME = {rea_id}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={rea_id}")

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO SBA.DAT
def generate_sba_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    dry_run: bool = False,
    force: bool = False,
):
    if not ems:
        logging.info("[sba] EMS desabilitado, pulando geração de SBA.")
        return

    ent = "sba"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
        m.ems_id as id, 
        e.id as est, 
        m.param_ems as param_ems  
    from 
        id_modulos m 
        join id_emsestacao e on e.cod_emsest = m.cod_emsest 
        join id_estacao i on i.cod_estacao = e.cod_estacao
    where 
        m.cod_tpmoduloems = 8 
        and m.cod_emsest != 0
        and m.ems_id != '' 
        and m.ems_lig1 != '' 
        and i.ems_modela = 'S'
    order by
        m.ems_id
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de SBA: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em SBA. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                sba_id = str(pt["id"]).strip()
                est = str(pt["est"]).strip()
                raw = str(pt.get("param_ems") or "").strip()

                # formata param_ems
                param_ems = raw.replace(" ", "\n").replace("=", " = ")

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {sba_id}\n")
                fp.write(f"EST = {est}\n")
                if param_ems:
                    fp.write(f"{param_ems}\n")
                if "NOME =" not in param_ems.upper():
                    fp.write(f"NOME = {sba_id}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={sba_id}")

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO TR2.DAT
def generate_tr2_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    dry_run: bool = False,
    force: bool = False,
):
    if not ems:
        logging.info("[tr2] EMS desabilitado, pulando geração de TR2.")
        return

    ent = "tr2"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select
        m1.ems_id as id1,
        e1.id as est1,
        e2.id as est2,
        m1.param_ems as param_ems1,
        m2.param_ems as param_ems2,
        n1.vbase as vbase1,
        n2.vbase as vbase2
    from
        id_modulos m1
        join id_emsestacao e1 on e1.cod_emsest = m1.cod_emsest
        join id_nivtensao n1 on e1.cod_nivtensao = n1.cod_nivtensao
        left join id_estacao i1 on i1.cod_estacao = m1.cod_estacao,
        id_modulos m2
        join id_emsestacao e2 on e2.cod_emsest = m2.cod_emsest
        join id_nivtensao n2 on e2.cod_nivtensao = n2.cod_nivtensao
        left join id_estacao i2 on i2.cod_estacao = m2.cod_estacao
    where
        m1.ems_id = m2.ems_id
        and m1.cod_modulo != m2.cod_modulo
        and n1.vnom > n2.vnom
        and m1.cod_tpmoduloems = 52
        and m1.cod_tpmodulo = 2
        and m1.cod_emsest != 0
        and m1.ems_id != ''
        and m1.ems_lig1 != ''
        and m2.cod_tpmoduloems = 52
        and m2.cod_tpmodulo = 2
        and m2.cod_emsest != 0
        and m2.ems_id != ''
        and m2.ems_lig1 != ''
        and i1.ems_modela = 'S'
        and i2.ems_modela = 'S'
    order by
        m1.ems_id
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de TR2: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em TR2. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                id1 = str(pt["id1"]).strip()
                est1 = str(pt["est1"]).strip()
                est2 = str(pt["est2"]).strip()
                raw1 = str(pt.get("param_ems1") or "").strip()
                raw2 = str(pt.get("param_ems2") or "").strip()
                vbase1 = pt["vbase1"]

                # formata param_ems1 e param_ems2
                p1 = raw1.replace(" ", "\n").replace("=", " = ")
                p2 = raw2.replace(" ", "\n").replace("=", " = ")
                # remove INVSN = SIM
                p1 = "\n".join([ln for ln in p1.splitlines() if ln.upper().strip() != "INVSN = SIM"])
                p2 = "\n".join([ln for ln in p2.splitlines() if ln.upper().strip() != "INVSN = SIM"])

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {id1}\n")
                fp.write(f"PRIM = {est1}\n")
                fp.write(f"SEC = {est2}\n")
                if p1:
                    fp.write(f"{p1}\n")
                if p2 and p2 != p1:
                    fp.write(f"{p2}\n")
                if "NOME =" not in (p1+p2).upper():
                    fp.write(f"NOME = {id1}\n")
                if "VBPR =" not in (p1+p2).upper():
                    fp.write(f"VBPR = {vbase1}\n")
                if "RPS =" not in (p1+p2).upper():
                    fp.write("RPS = 0.01\n")
                if "XPS =" not in (p1+p2).upper():
                    fp.write("XPS = 1.00\n")
                if "TTERMP =" not in (p1+p2).upper():
                    fp.write("TTERMP = S\n")
                if "TTERMS =" not in (p1+p2).upper():
                    fp.write("TTERMS = S\n")
                if "LSFI =" not in (p1+p2).upper():
                    fp.write("LSFI = 5000\n")
                if "LSOP =" not in (p1+p2).upper():
                    fp.write("LSOP = 4000\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={id1}")

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO TR3.DAT
def generate_tr3_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    dry_run: bool = False,
    force: bool = False,
):
    if not ems:
        logging.info("[tr3] EMS desabilitado, pulando geração de TR3.")
        return

    ent = "tr3"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select
        m1.ems_id as id1,
        e1.id    as est1,
        e2.id    as est2,
        e3.id    as est3,
        m1.param_ems as param_ems1,
        m2.param_ems as param_ems2,
        m3.param_ems as param_ems3,
        n1.vbase as vbase1,
        n2.vbase as vbase2,
        n3.vbase as vbase3
    from
        id_modulos m1
        join id_emsestacao e1 on e1.cod_emsest = m1.cod_emsest
        join id_nivtensao n1 on e1.cod_nivtensao = n1.cod_nivtensao
        left join id_estacao i1 on i1.cod_estacao = m1.cod_estacao,
        id_modulos m2
        join id_emsestacao e2 on e2.cod_emsest = m2.cod_emsest
        join id_nivtensao n2 on e2.cod_nivtensao = n2.cod_nivtensao
        left join id_estacao i2 on i2.cod_estacao = m2.cod_estacao,
        id_modulos m3
        join id_emsestacao e3 on e3.cod_emsest = m3.cod_emsest
        join id_nivtensao n3 on e3.cod_nivtensao = n3.cod_nivtensao
        left join id_estacao i3 on i3.cod_estacao = m3.cod_estacao
    where
        m1.ems_id = m2.ems_id
        and m2.ems_id = m3.ems_id
        and m1.cod_modulo != m2.cod_modulo
        and m2.cod_modulo != m3.cod_modulo
        and m1.cod_modulo != m3.cod_modulo
        and not (n2.vnom = n3.vnom and e2.id > e3.id)
        and n1.vnom >= n2.vnom
        and n2.vnom >= n3.vnom
        and m1.cod_tpmoduloems = 53
        and m1.cod_tpmodulo     = 2
        and m1.cod_emsest != 0
        and m1.ems_id    != ''
        and m1.ems_lig1  != ''
        and m2.cod_tpmoduloems = 53
        and m2.cod_tpmodulo     = 2
        and m2.cod_emsest != 0
        and m2.ems_id    != ''
        and m2.ems_lig1  != ''
        and m3.cod_tpmoduloems = 53
        and m3.cod_tpmodulo     = 2
        and m3.cod_emsest != 0
        and m3.ems_id    != ''
        and m3.ems_lig1  != ''
        and i1.ems_modela = 'S'
        and i2.ems_modela = 'S'
        and i3.ems_modela = 'S'
    order by
        m1.ems_id
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de TR3: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em TR3. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                id1 = str(pt["id1"]).strip()
                est1 = str(pt["est1"]).strip()
                est2 = str(pt["est2"]).strip()
                est3 = str(pt["est3"]).strip()
                raw1 = (pt.get("param_ems1") or "").strip()
                raw2 = (pt.get("param_ems2") or "").strip()
                raw3 = (pt.get("param_ems3") or "").strip()
                v1 = pt["vbase1"]
                v2 = pt["vbase2"]

                # format params
                def fmt(p):
                    lines = p.replace(" ", "\n").replace("=", " = ").splitlines()
                    return "\n".join(ln for ln in lines if ln.upper().strip() != "INVSN = SIM")
                p1, p2, p3 = fmt(raw1), fmt(raw2), fmt(raw3)

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {id1}\n")
                fp.write(f"PRIM = {est1}\n")
                fp.write(f"SEC  = {est2}\n")
                fp.write(f"TERC = {est3}\n")
                if p1:
                    fp.write(f"{p1}\n")
                if p2 and p2 != p1:
                    fp.write(f"{p2}\n")
                if p3 and p3 not in (p1, p2):
                    fp.write(f"{p3}\n")
                combined = (p1 + p2 + p3).upper()
                if "NOME =" not in combined:
                    fp.write(f"NOME = {id1}\n")
                if "VBPR =" not in combined:
                    fp.write(f"VBPR = {v1}\n")
                if "VBSE =" not in combined:
                    fp.write(f"VBSE = {v2}\n")
                if "LSFP =" not in combined:
                    fp.write("LSFP = 5000\n")
                if "LSFT =" not in combined:
                    fp.write("LSFT = 1500\n")
                if "LSOP =" not in combined:
                    fp.write("LSOP = 4000\n")
                if "LSOT =" not in combined:
                    fp.write("LSOT = 1400\n")
                if "TTERMP =" not in combined:
                    fp.write("TTERMP = S\n")
                if "TTERMS =" not in combined:
                    fp.write("TTERMS = S\n")
                if "TTERMT =" not in combined:
                    fp.write("TTERMT = S\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={id1}")

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO UGE.DAT
def generate_uge_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo uge.dat (Unidades Geradoras) se EMS estiver habilitado.
    """
    if not ems:
        logging.info("[uge] EMS desabilitado, pulando geração de UGE.")
        return

    ent = "uge"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
        m.ems_id   as id, 
        e.id       as est, 
        m.param_ems as param_ems,
        i.estacao  as ins
    from 
        id_modulos m 
        join id_emsestacao e on e.cod_emsest = m.cod_emsest 
        join id_estacao i    on i.cod_estacao   = e.cod_estacao
    where 
        m.cod_tpmoduloems = 6 
        and m.cod_emsest   != 0
        and m.ems_id       != ''
        and m.ems_lig1     != ''
        and i.ems_modela   = 'S'
    order by
        m.ems_id
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de UGE: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            cnt = 0
            for pt in rows:
                uid = str(pt["id"]).strip()
                est = str(pt["est"]).strip()
                ins = str(pt["ins"]).strip()
                raw = (pt.get("param_ems") or "").strip()
                # formata param_ems em linhas, padroniza " = "
                param = "\n".join(line for line in raw.replace(" ", "\n").replace("=", " = ").splitlines())

                if cnt > 0:
                    fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {uid}\n")
                fp.write(f"EST = {est}\n")
                if param:
                    fp.write(f"{param}\n")
                up = param.upper()
                if "NOME =" not in up:
                    fp.write(f"NOME = {uid}\n")
                if "USI =" not in up:
                    fp.write(f"USI = {ins}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={uid}")

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CNC.DAT
def generate_cnc_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo cnc.dat (Conectores) se EMS estiver habilitado.
    """
    if not ems:
        logging.info("[cnc] EMS desabilitado, pulando geração de CNC.")
        return

    ent = "cnc"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select
        n.ems_id      as cnc,
        e.id          as est,
        n.tipo_nops   as tipo_nops
    from id_nops n
    join id_modulos m on n.cod_modulo = m.cod_modulo
    join id_emsestacao e on e.cod_emsest = m.cod_emsest
    join id_estacao i on i.cod_estacao = m.cod_estacao
    where
        n.ems_id     != '' 
        and n.ems_lig1 != '' 
        and n.ems_lig2 != '' 
        and (m.ems_id != '' or m.cod_tpmodulo = 9) 
        and m.cod_emsest != 0
        and n.tipo_nops in ('S','D')
        and i.ems_modela = 'S'
    order by est
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de CNC: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            estant = None
            cnt = 0

            for pt in rows:
                est = str(pt["est"]).strip()
                if estant != est:
                    estant = est
                    # cabeçalho por estação
                    fp.write(f"\n; --- ESTACAO: {est} ------------------------------- \n")

                cnc_id = str(pt["cnc"]).strip()
                tipo_nops = pt["tipo_nops"]
                tipo = "CHAVE" if tipo_nops == "S" else "DISJ"

                fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {cnc_id}\n")
                fp.write(f"EST = {est}\n")
                fp.write(f"TIPO = {tipo}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={cnc_id}")

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO LIG.DAT
def generate_lig_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ems: bool,
    cargas_eramltr: List[str] = None,
    dry_run: bool = False,
    force: bool = False,
):
    if cargas_eramltr is None:
        cargas_eramltr = []
    """
    Gera o arquivo lig.dat (Ligações) se EMS estiver habilitado.
    """
    if not ems:
        logging.info("[lig] EMS desabilitado, pulando geração de LIG.")
        return

    ent = "lig"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select
        e.id       as est,
        n.ems_id   as eqp,
        'CNC'      as tpeqp,
        n.ems_lig1 as lig
    from id_nops n
    join id_modulos m  on m.cod_modulo = n.cod_modulo
    join id_emsestacao e on e.cod_emsest = m.cod_emsest
    join id_tpmodulo t on t.cod_tpmodulo = m.cod_tpmoduloems
    join id_estacao i  on i.cod_estacao = m.cod_estacao
    where (m.ems_id != '' or m.cod_tpmodulo = 9)
      and n.ems_lig1 != '' and n.ems_lig2 != ''
      and i.ems_modela = 'S'

    union

    select
        e.id       as est,
        n.ems_id   as eqp,
        'CNC'      as tpeqp,
        n.ems_lig2 as lig
    from id_nops n
    join id_modulos m  on m.cod_modulo = n.cod_modulo
    join id_emsestacao e on e.cod_emsest = m.cod_emsest
    join id_tpmodulo t on t.cod_tpmodulo = m.cod_tpmoduloems
    join id_estacao i  on i.cod_estacao = m.cod_estacao
    where (m.ems_id != '' or m.cod_tpmodulo = 9)
      and n.ems_lig2 != ''
      and i.ems_modela = 'S'

    union

    select
        e.id       as est,
        m.ems_id   as eqp,
        t.ent_ems  as tpeqp,
        m.ems_lig1 as lig
    from id_modulos m
    join id_emsestacao e on e.cod_emsest = m.cod_emsest
    join id_tpmodulo t  on t.cod_tpmodulo = m.cod_tpmoduloems
    join id_estacao i  on i.cod_estacao = m.cod_estacao
    where m.ems_id != '' and m.ems_lig1 != ''
      and i.ems_modela = 'S'

    order by est, lig, eqp
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de LIG: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}  {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{top}\n")

            estant = None
            cnt = 0

            for pt in rows:
                est = str(pt["est"]).strip()
                if estant != est:
                    estant = est
                    fp.write(f"\n; --- ESTACAO: {est} ------------------------------- \n")

                eqp   = str(pt["eqp"]).strip()
                lig   = str(pt["lig"]).strip()
                tpeqp = pt["tpeqp"]
                # se for carga que era LTR, renomeia
                if eqp in cargas_eramltr:
                    tpeqp = "CAR"

                fp.write("\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ID = {lig}\n")
                fp.write(f"EQP = {eqp}\n")
                fp.write(f"TPEQP = {tpeqp}\n")
                fp.write(f"EST = {est}\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} ID={lig}")

            # rodapé comentado
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO RCA.DAT
def generate_rca_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo rca.dat (Cálculos com parcelas) para o SAGE.
    """
    ent = "rca"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    SELECT
        c.ordem           AS ordem,
        ic.nponto         AS nponto,
        ic.cod_origem     AS cod_origem,
        ic.id             AS id_calculado,
        ip.id             AS id_parcela,
        ip.nponto         AS npt_parc,
        ip.cod_tpeq       AS ctpeq_parc,
        tpntc.tipo        AS tpc,
        tpntp.tipo        AS tpp
    FROM id_calculos c
    JOIN id_ponto ic  ON c.nponto = ic.nponto
    JOIN id_formulas f ON f.cod_formula = ic.cod_formula
    JOIN id_ptlog_noh n ON n.nponto=ic.nponto AND n.cod_nohsup=%s
    JOIN id_ponto ip  ON c.parcela = ip.nponto
    JOIN id_tipos   tpc ON tpc.cod_tpeq=ic.cod_tpeq AND tpc.cod_info=ic.cod_info
    JOIN id_tipopnt tpntc ON tpntc.cod_tipopnt=tpc.cod_tipopnt
    JOIN id_tipos   tpp ON tpp.cod_tpeq=ip.cod_tpeq AND tpp.cod_info=ip.cod_info
    JOIN id_tipopnt tpntp ON tpntp.cod_tipopnt=tpp.cod_tipopnt
    WHERE f.tipo_calc NOT IN ('F')
    ORDER BY ic.nponto, c.ordem
    """

    logging.info(f"[{ent}] Executando SQL de cálculos.")
    with conn.cursor() as cur:
        cur.execute(sql, (cod_noh,))
        rows = cur.fetchall()

    logging.info(f"[{ent}] {len(rows)} linhas retornadas.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar. Abortando.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # cabeçalho comentado
            ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha = "// " + "=" * 70
            fp.write(f"{linha}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10}  {ts}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha}\n\n")

            for pt in rows:
                ordem        = pt["ordem"]
                nponto       = pt["nponto"]
                cod_origem   = pt["cod_origem"]
                id_calc      = pt["id_calculado"]
                id_parc      = pt["id_parcela"]
                npt_parc     = pt["npt_parc"]
                ctpeq_parc   = pt["ctpeq_parc"]
                tpc          = pt["tpc"]
                tpp          = pt["tpp"]

                if cod_origem == 1:
                    # TPPNT
                    tppnt = "PDS" if tpc == "D" else "PAS"
                    # TPPARC e TIPOP
                    if tpp == "D":
                        tipop  = "EDC"
                        tpparc = "PDS"
                    else:
                        tipop  = "VAC"
                        tpparc = "PAS"

                    # ponto futuro?
                    if ctpeq_parc == 95:
                        logging.error(f"[{ent}] Ponto futuro em parcela: nponto={nponto}, parcela={npt_parc}")
                        sys.exit(1)

                    # escreve bloco
                    fp.write("\n")
                    fp.write(f"; NPONTO CALCULADO: {nponto} - PARCELA: {npt_parc}\n")
                    fp.write("RCA\n")
                    fp.write(f"ORDEM= {ordem}\n")
                    fp.write(f"PARC= {id_parc}\n")
                    fp.write(f"PNT= {id_calc}\n")
                    fp.write(f"TIPOP= {tipop}\n")
                    fp.write(f"TPPARC= {tpparc}\n")
                    fp.write(f"TPPNT= {tppnt}\n\n")

                    logging.info(f"[{ent}] PNT={id_calc} (nponto={nponto})")
                else:
                    logging.warning(f"[{ent}] Ponto não calculado: nponto={nponto}, id={id_calc}")

            # rodapé comentado
            fp.write(f"{linha}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros lidos: {len(rows)}\n")
            fp.write(f"{linha}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {len(rows)} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CGS_GCOM.DAT
# CGS GESTÃO DE COMUNICAÇÃO
def generate_cgs_gcom_dat(
    paths: Dict[str, Path],
    conn,
    conexoes_dst: List[int],
    gestao_com: bool,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo cgs.gcom.dat (CGS – Gestão da Comunicação), se gestao_com estiver habilitado.
    """
    if not gestao_com:
        logging.info("[cgs_gcom] Gestão da comunicação desabilitada. Pulando.")
        return

    ent = "cgs"
    destino = Path(paths["dats_unir"]) / f"{ent}.gcom.dat"
    first_write = not destino.exists() or force
    ph = ",".join(["%s"] * len(conexoes_dst))
    sql = f"""
    SELECT
      c.cod_protocolo,
      c.nsrv1,
      c.nsrv2,
      c.placa_princ,
      c.linha_princ,
      c.placa_resrv,
      c.linha_resrv,
      c.cod_conexao,
      c.end_org,
      c.end_dst,
      c.descricao AS nome,
      c.id_sage_aq,
      c.id_sage_dt,
      p.nome AS pnome,
      c.cod_noh_org,
      c.cod_noh_dst
    FROM
      id_conexoes c
      JOIN id_protocolos p ON p.cod_protocolo = c.cod_protocolo
    WHERE
      c.cod_conexao in ({ph})
      AND p.cod_protocolo not in (0, 10)
    ORDER BY
      p.cod_protocolo,
      c.nsrv1,
      c.nsrv2,
      c.placa_princ,
      c.linha_princ,
      c.placa_resrv,
      c.linha_resrv
    """
    logging.info(f"[{ent}_gcom] Executando SQL para CGS GCOM.")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(conexoes_dst))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}_gcom] Erro ao buscar dados: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}_gcom] Nenhum registro para processar. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}_gcom] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # Definição dos blocos a gerar: (ID_SUFIXO, NOME_DESCRICAO, TIPO_EVENTO)
    blocos = [
        ("_DESAB_ENUP", "Desabilitacao do Enlace Principal", "PDCAN"),
        ("_HABIL_ENUP", "Habilitacao do Enlace Principal", "PHCAN"),
        ("_DESAB_ENUR", "Desabilitacao do Enlace Reserva", "PDCAN"),
        ("_HABIL_ENUR", "Habilitacao do Enlace Reserva", "PHCAN"),
        ("_DESAB_FSECN", "Desabil da Func Secund nos Enlaces", "PDSEC"),
        ("_HABIL_FSECN", "Habilit da Func Secund nos Enlaces", "PHSEC"),
        ("_HABIL_UTRP", "Habilitacao da UTR Principal", "PHUTR"),
        ("_DESAB_UTRP", "Desabilitacao da UTR Principal", "PDUTR"),
        ("_HABIL_UTRR", "Habilitacao da UTR Reserva", "PHUTR"),
        ("_DESAB_UTRR", "Desabilitacao da UTR Reserva", "PDUTR"),
        ("_PFAIL_ENUP", "Failover do Enlace Principal", "PFCAN"),
        ("_PFAIL_ENUR", "Failover do Enlace Reserva", "PFCAN"),
    ]

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # Cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} GESTÃO DA COMUNICAÇÃO {timestamp}\n")
            fp.write(f"// Versão: {VersaoBase}\n")
            fp.write(f"{top}\n\n")

            cnt = 0
            for pt in rows:
                nome = str(pt.get("nome", "")).strip()
                pnome = str(pt.get("pnome", "")).strip()
                id_sage_aq = str(pt.get("id_sage_aq", "")).strip()
                
                fp.write(f"\n\n; >>>>>> {nome} - {pnome} <<<<<<\n")

                for sufixo, nome_bloco, tipoe in blocos:
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"ID =\t{id_sage_aq}{sufixo}\n")
                    fp.write("LMI1C =\t0\n")
                    fp.write("LMI2C =\t0\n")
                    fp.write("LMS1C =\t0\n")
                    fp.write("LMS2C =\t0\n")
                    fp.write(f"NOME =\t{nome_bloco} {id_sage_aq}\n")
                    fp.write("PAC =\tCOM_SAGE\n")
                    fp.write("PINT =\t\n")
                    fp.write(f"TAC =\t{id_sage_aq}-COM\n")
                    fp.write("TIPO =\tPDS\n")
                    fp.write(f"TIPOE =\t{tipoe}\n")
                    fp.write("TPCTL =\tCSCD\n")

                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} {id_sage_aq}{sufixo}")

            # Rodapé final
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CGS_GCOM.DAT
# CGS Pontos de controle lógicos de aquisição
def generate_cgs_logico_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_dst: List[int],
    tac_conex: Dict[int, str],
    tac_estacao: List[str],
    no_cor: bool,
    com_flag: bool,
    max_id_size: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo cgs.dat (CGS – pontos de controle lógicos de aquisição).
    """
    ent = 'cgs'
    destino = Path(paths["dats_unir"]) / f"{ent}-logico.dat"
    first_write = not destino.exists() or force
    
    # Prepara placeholders
    ph_dst = ",".join(["%s"] * len(conexoes_dst))

    sql = f"""
    SELECT
      m.descricao as entidade,
      i.id,
      e.estacao as estacao,
      i.traducao_id as traducao_id,
      isup.id as supervisao,
      isup.nponto as sup_nponto,
      tpnt.tctl as tipo2,
      cx.cod_conexao as cod_conexao,
      '' as inter,
      tpnt.tipo as tipo3,
      tpnt.cmd_1,
      tpnt.cmd_0,
      i.nponto as objeto,
      i.cod_tpeq,
      i.cod_info,
      i.cod_origem,
      f.cod_asdu,
      case when l.lia < -99999 then 0 else l.lia end as lmi1c,
      case when l.liu < -99999 then 0 else l.liu end as lmi2c,
      case when l.lsa > 99999 then 0 else l.lsa end as lms1c,
      case when l.lsu > 99999 then 0 else l.lsu end as lms2c,
      a.tipo as tipo_asdu
    FROM
      id_ptlog_noh as l
      left outer join id_ptfis_conex f on f.id_dst=l.nponto and f.cod_conexao in ({ph_dst})
      left outer join id_protoc_asdu a on a.cod_asdu=f.cod_asdu
      left outer join id_conexoes cx on cx.cod_conexao=f.cod_conexao,
      id_ponto as i
      join id_ponto as isup on i.nponto_sup=isup.nponto
      join id_nops n on n.cod_nops=i.cod_nops
      join id_modulos m on m.cod_modulo=n.cod_modulo
      join id_estacao e on e.cod_estacao=m.cod_estacao
      join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
      join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
    where
      l.cod_nohsup = %s and
      l.nponto = i.nponto and
      (i.cod_origem=7 or i.cod_origem=15) and
      i.cod_tpeq!=95
    order by
      i.nponto, cx.cod_conexao desc
    """

    logging.info(f"[{ent}] Executando SQL para CGS.")
    try:
        with conn.cursor() as cur:
            params = tuple(conexoes_dst) + (cod_noh,)
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em CGS. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    ptant = None
    cnt = 0
    num_reg = defaultdict(int)

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # Cabeçalho padrão
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                if ptant == pt["objeto"]:
                    continue
                ptant = pt["objeto"]
                
                # Início da lógica
                cod_conexao = pt.get("cod_conexao")
                if cod_conexao is None:
                    logging.warning(f"[{ent}] Comando CGS sem ponto físico CGF associado para nponto={pt['objeto']} id={pt['id']}")
                    continue

                nome = f"{pt['estacao']}-{pt['traducao_id']}".strip()
                if len(nome) > max_id_size:
                    raise ValueError(f"Nome muito longo ({len(nome)}) para nponto={pt['objeto']} id={pt['id']}")

                inter = pt["inter"]
                pac = pt["supervisao"]
                tac = pt["estacao"]

                if pt["cod_origem"] == 15 and (pt["cod_info"] == 185 or pt["cod_info"] == 42):
                    inter = pt["id"]
                    pac = pt["id"]
                    tac = "LOCAL"
                else:
                    if cod_conexao in tac_conex:
                        if cod_conexao == 1 and pt["estacao"] in tac_estacao:
                            tac = pt["estacao"]
                        else:
                            tac = tac_conex[cod_conexao]
                    else:
                        tac = pt["estacao"]

                tipo = "PDS" if pt["tipo3"] == "D" else "PAS"
                if pt["sup_nponto"] == 0 or pt["sup_nponto"] == 9991:
                    tipo = "PDS"
                    if pt["sup_nponto"] == 9991:
                        inter = "COM_SAGE"
                    pac = "COM_SAGE"

                tipoe = pt["tipo2"]
                if tipoe == "PULS" and (pt["cod_asdu"] == 45 or pt["cod_asdu"] == 46):
                    tipoe = "AUMD"
                if pt["tipo_asdu"] == "S":
                    tipoe = "STPT"

                fp.write("\n")
                if com_flag:
                    fp.write(f"; NPONTO= {pt['objeto']:05d}\n")
                
                fp.write(f"ID= {pt['id']}\n")
                fp.write(f"NOME= {nome}\n")
                if not no_cor:
                    fp.write(f"AOR= CPFLT\n")
                fp.write(f"LMI1C= {pt['lmi1c']:.5f}\n")
                fp.write(f"LMI2C= {pt['lmi2c']:.5f}\n")
                fp.write(f"LMS1C= {pt['lms1c']:.5f}\n")
                fp.write(f"LMS2C= {pt['lms2c']:.5f}\n")
                fp.write(f"TIPO= {tipo}\n")
                fp.write(f"TPCTL= CSAC\n")
                fp.write(f"TAC= {tac}\n")
                fp.write(f"PAC= {pac}\n")
                fp.write(f"PINT= {inter}\n")
                fp.write(f"TIPOE= {tipoe}\n")
                fp.write(f"IDOPER= {pt['objeto']}\n")
                
                num_reg[ent] += 1
                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} PONTO={pt['objeto']:5d} ID={pt['id']}")

            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}\n")
            fp.write(f"// Total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CGF_GCOM.DAT
# CGF GESTÃO DE COMUNICAÇÃO
def generate_cgf_gcom_dat(
    paths: Dict[str, Path],
    conn,
    conexoes_dst: List[int],
    gestao_com: bool,
    ordemnv1_sage_gc: Dict[int, int],
    dry_run: bool = False,
    force: bool = False,
) -> int:
    """
    Gera o arquivo cgf.gcom.dat (CGF – gestão da comunicação), se gestao_com=True.
    Retorna o contador final de comandos (end_gcom), para uso posterior.
    """
    ent = 'cgf'
    destino = Path(paths["dats_unir"]) / f"{ent}-gcom.dat"
    first_write = not destino.exists() or force

    if not gestao_com:
        logging.info("[cgf_gcom] Gestão da comunicação desabilitada. Pulando.")
        return 0
    
    ph = ",".join(["%s"] * len(conexoes_dst))
    sql = f"""
    SELECT
        c.cod_protocolo,
        c.nsrv1,
        c.nsrv2,
        c.placa_princ,
        c.linha_princ,
        c.placa_resrv,
        c.linha_resrv,
        c.cod_conexao,
        c.end_org,
        c.end_dst,
        c.descricao as nome,
        c.id_sage_aq,
        c.id_sage_dt,
        p.nome as pnome,
        p.sufixo_sage,
        c.cod_noh_org,
        c.cod_noh_dst
    FROM
        id_conexoes c
        join id_protocolos p on p.cod_protocolo=c.cod_protocolo
    WHERE
        c.cod_conexao in ({ph})
        and p.cod_protocolo not in (0, 10)
    ORDER BY
        p.cod_protocolo,
        c.nsrv1,
        c.nsrv2,
        c.placa_princ,
        c.linha_princ,
        c.placa_resrv,
        c.linha_resrv
    """
    logging.info(f"[{ent}_gcom] Executando SQL para CGF GCOM.")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(conexoes_dst))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}_gcom] Erro ao buscar dados: {e}", exc_info=True)
        return 0

    if not rows:
        logging.warning(f"[{ent}_gcom] Nenhum registro para processar. Saindo.")
        return 0

    if dry_run:
        logging.info(f"[{ent}_gcom] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return 0

    end_gcom = 0
    cnt = 0
    num_reg = defaultdict(int)

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")
            
            # Cabeçalho
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} GESTÃO DA COMUNICAÇÃO {timestamp}\n")
            fp.write(f"// Versão: {VersaoBase}\n")
            fp.write(f"{top}\n\n")

            for pt in rows:
                nome = str(pt.get("nome", "")).strip()
                pnome = str(pt.get("pnome", "")).strip()
                id_sage_aq = str(pt.get("id_sage_aq", "")).strip()
                cod_conexao = pt.get("cod_conexao")
                sufixo_sage = str(pt.get("sufixo_sage", "")).strip()
                
                # nv1 é criado fora do loop, dentro do generate_nv1_dat. Aqui só recuperamos a ordem
                # O formato do NV1 é `id_conexao_G<sufixo_prot>_<ordem>`.
                try:
                    ordem_gc = ordemnv1_sage_gc[cod_conexao]
                    nv1 = f"{id_sage_aq}_G{sufixo_sage}_{ordem_gc}"
                except KeyError:
                    logging.warning(f"[{ent}] Não foi possível encontrar a ordem NV1_GC para a conexão {cod_conexao}. Pulando este item.")
                    continue
                
                fp.write(f"\n\n; >>>>>> {nome} - {pnome} <<<<<<\n")

                blocos = [
                    ("Desabilitacao do Enlace Principal", "_DESAB_ENUP", "PRI"),
                    ("Habilitacao do Enlace Principal", "_HABIL_ENUP", "PRI"),
                    ("Desabilitacao do Enlace Reserva", "_DESAB_ENUR", "REV"),
                    ("Habilitacao do Enlace Reserva", "_HABIL_ENUR", "REV"),
                    ("Desabil da Func Secund nos Enlaces", "_DESAB_FSECN", ""),
                    ("Habilit da Func Secund nos Enlaces", "_HABIL_FSECN", ""),
                    ("Desabilitacao da UTR Principal", "_DESAB_UTRP", "PRI"),
                    ("Habilitacao da UTR Principal", "_HABIL_UTRP", "PRI"),
                    ("Desabilitacao da UTR Reserva", "_DESAB_UTRR", "REV"),
                    ("Habilitacao da UTR Reserva", "_HABIL_UTRR", "REV"),
                    ("Failover do Enlace Principal", "_PFAIL_ENUP", "PRI"),
                    ("Failover do Enlace Reserva", "_PFAIL_ENUR", "REV"),
                ]
                
                for nome_bloco, sufixo, kconv in blocos:
                    end_gcom += 1
                    fp.write("\n")
                    fp.write(f";{nome_bloco}\n")
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"CGS =\t{id_sage_aq}{sufixo}\n")
                    fp.write(f"ID =\t{id_sage_aq}{sufixo}_{end_gcom}\n")
                    fp.write(f"KCONV =\t{kconv}\n")
                    fp.write(f"NV2 =\t{nv1}_CGCD\n")
                    fp.write(f"ORDEM =\t{end_gcom}\n")
                    
                cnt += 12
                num_reg[ent] += 12
                logging.info(f"{ent.upper()}={cnt:05d} {id_sage_aq}")
                
            # Rodapé final
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
        return end_gcom
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
        return end_gcom
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CGF_ROTEAMENTO.DAT
# CGF ROTEAMENTO DE COMUNICAÇÃO
def generate_cgf_routing_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    ordemnv1_sage_ct: Dict[int, int],
    com_flag: bool,
    max_id_size: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo cgf.dist.dat (CGF – pontos físicos para roteamento),
    que representa o roteamento de comandos.
    """
    ent = 'cgf'
    destino = Path(paths["dats_unir"]) / f"{ent}-routing.dat"
    first_write = not destino.exists() or force
    
    # Combina as listas de conexões
    all_conex = conexoes_org + conexoes_dst
    ph_all = ",".join(["%s"] * len(all_conex))

    sql = f"""
SELECT
    m.descricao as entidade, 
    i.id, 
    f.kconv1 as kconv1,
    f.kconv2 as kconv2,
    f.kconv as kconv,
    f.endereco,
    i.nponto as objeto,
    c.id_sage_dt as id_conex_dt,
    c.id_sage_aq as id_conex,
    c.cod_noh_org as cod_noh_org,
    p.sufixo_sage as suf_prot,
    f.cod_conexao as cod_conexao,
    c.descricao as descr_conex,
    p.descricao as descr_protocolo,
    c.cod_protocolo as cod_protocolo,
    p.grupo_protoc as grupo_protoc,
    a.tn2_aq as tn2,

    -- Novo campo: id_conex_dt da distribuição
    (
      select c2.id_sage_dt
      from id_ptfis_conex f2
      join id_conexoes c2 on f2.cod_conexao = c2.cod_conexao
      where f2.id_dst = i.nponto
        and c2.cod_noh_org = %s
      limit 1
    ) as id_conex_dt_dst,

    -- para encontrar mesmo pf em outra conexão que não a 1 
    f2.cod_conexao as con2,
    c2.end_org as org2      
FROM
    id_ptfis_conex as f
    join id_protoc_asdu as a on a.cod_asdu=f.cod_asdu
    left outer join id_ptfis_conex f2 on f.endereco=f2.endereco and f.cod_conexao!=f2.cod_conexao and f.cod_conexao=1 and f2.id_dst not in (9991,9992)
    left outer join id_conexoes c2 on f2.cod_conexao=c2.cod_conexao,      
    id_conexoes as c
    join id_protocolos as p on c.cod_protocolo = p.cod_protocolo,
    id_ponto as i
    join id_ptlog_noh l on l.nponto=i.nponto
    join id_nops n on n.cod_nops=i.cod_nops
    join id_modulos m on m.cod_modulo=n.cod_modulo
    join id_estacao e on e.cod_estacao=m.cod_estacao
WHERE
    f.cod_conexao in ({ph_all}) and
    f.cod_conexao = c.cod_conexao and
    f.id_dst = i.nponto and
    i.cod_origem = 7  and
    i.cod_tpeq!=95 and
    l.cod_nohsup = %s and
    exists (
      select 1
      from id_ptfis_conex f2
      join id_conexoes c2 on f2.cod_conexao = c2.cod_conexao
      where f2.id_dst = i.nponto
        and c2.cod_noh_org = %s
    )
ORDER BY
    f.cod_conexao, i.nponto
    """

    logging.info(f"[{ent}_dist] Executando SQL para CGF DIST.")
    try:
        with conn.cursor() as cur:
            # A ordem dos parâmetros deve ser: all_conex, cod_noh, cod_noh
            params = tuple(all_conex) + (cod_noh, cod_noh, cod_noh)
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}_dist] Erro ao buscar dados: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}_dist] Nenhum registro para processar. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}_dist] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    conexant = None
    cntconxant = 0
    cnt = 0
    num_reg = defaultdict(int)

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # Cabeçalho padrão
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} DISTRIBUICAO {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")
            
            for pt in rows:
                cod_conexao = pt.get("cod_conexao")

                if conexant != cod_conexao:
                    conexant = cod_conexao
                    if cnt != 0:
                        fp.write(f"\n; Pontos nesta conexão: {cnt - cntconxant} \n\n")
                    fp.write("\n; --------------------------------------------------------------------------------")
                    fp.write(f"\n; {pt['descr_conex']} ( {pt['descr_protocolo']} )\n\n")
                    cntconxant = cnt
                
                # Regra de bacalhau para pular pontos da conexão 1 se houverem em outra conexão válida
                if pt["cod_conexao"] == 1 and pt["con2"] and pt["org2"]:
                    logging.info(f"[{ent}_dist] Ignorando ponto {pt['id']} da conexão 1, pois existe na conexão {pt['con2']}.")
                    continue
                
                # Lógica para determinar KCONV
                kconv = pt["kconv"]
                if not kconv and pt["grupo_protoc"] == 1:
                    kconv = "NO_S" if pt["kconv1"] == 1 else "NO"
                
                # Lógica para determinar ID e NV2
                if pt["cod_noh_org"] == int(cod_noh): # Distribuição
                    id_cgf = pt["endereco"]
                    nv2 = f"{pt['id_conex_dt']}_{pt['tn2']}_NV2"
                    cgs = f"CGS= {pt['id']}-{pt['id_conex_dt_dst']}"
                else: # Aquisição
                    id_cgf = f"{pt['id_conex']}_C{pt['suf_prot']}_{ordemnv1_sage_ct[pt['cod_conexao']]}_{pt['tn2']}_{pt['endereco']}"
                    nv2 = f"{pt['id_conex']}_C{pt['suf_prot']}_{ordemnv1_sage_ct[pt['cod_conexao']]}_{pt['tn2']}"
                    cgs = f"CGS= {pt['id']}"

                fp.write("\n")
                if com_flag:
                    fp.write(f"; NPONTO= {pt['objeto']:05d}\n")
                
                fp.write(f"ID= {id_cgf}\n")
                fp.write(f"KCONV= {kconv}\n")
                fp.write(f"ORDEM= {pt['endereco']}\n")
                fp.write(f"{cgs}\n")
                fp.write(f"NV2= {nv2}\n")
                
                cnt += 1
                num_reg[ent] += 1
                logging.info(f"{ent.upper()}={cnt:05d} PONTO={pt['objeto']:5d} ID={id_cgf}")

            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
        return
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO CGF_FISICO.DAT
# CGF PONTO DE CONTROLE FÍSICO
def generate_cgf_fisico_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    ordemnv1_sage_ct: Dict[int, int],
    com_flag: bool,
    max_id_size: int,
    dry_run: bool = False,
    force: bool = False,
    start_gcom: int = 0
):
    """
    Gera o arquivo cgf.dat (CGF – pontos de controle físicos).
    """
    ent = 'cgf'
    destino = Path(paths["dats_unir"]) / f"{ent}-fisico.dat"
    first_write = not destino.exists() or force
    
    # Combina as listas de conexões
    all_conex = conexoes_org + conexoes_dst
    ph_all = ",".join(["%s"] * len(all_conex))

    sql = f"""
    SELECT
        m.descricao as entidade,
        i.id,
        f.kconv1 as kconv1,
        f.kconv2 as kconv2,
        f.kconv as kconv,
        f.endereco,
        i.nponto as objeto,
        c.id_sage_dt as id_conex_dt,
        c.id_sage_aq as id_conex,
        c.cod_noh_org as cod_noh_org,
        p.sufixo_sage as suf_prot,
        f.cod_conexao as cod_conexao,
        c.descricao as descr_conex,
        p.descricao as descr_protocolo,
        c.cod_protocolo as cod_protocolo,
        p.grupo_protoc as grupo_protoc,
        a.tn2_aq as tn2,
        f2.cod_conexao as con2,
        c2.end_org as org2
    FROM
        id_ptfis_conex as f
        join id_protoc_asdu as a on a.cod_asdu=f.cod_asdu
        left outer join id_ptfis_conex f2 on f.endereco=f2.endereco and f.cod_conexao!=f2.cod_conexao and f.cod_conexao=1 and f2.id_dst not in (9991,9992)
        left outer join id_conexoes c2 on f2.cod_conexao=c2.cod_conexao,
        id_conexoes as c
        join id_protocolos as p on c.cod_protocolo = p.cod_protocolo,
        id_ponto as i
        join id_ptlog_noh l on l.nponto=i.nponto
        join id_nops n on n.cod_nops=i.cod_nops
        join id_modulos m on m.cod_modulo=n.cod_modulo
        join id_estacao e on e.cod_estacao=m.cod_estacao
    WHERE
        f.cod_conexao in ({ph_all}) and
        f.cod_conexao = c.cod_conexao and
        f.id_dst = i.nponto and
        i.cod_origem = 7 and
        i.cod_tpeq!=95 and
        l.cod_nohsup = %s
    ORDER BY
        f.cod_conexao, i.nponto
    """

    logging.info(f"[{ent}] Executando SQL para CGF.")
    try:
        with conn.cursor() as cur:
            params = tuple(all_conex) + (cod_noh,)
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao buscar dados: {e}", exc_info=True)
        return

    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em CGF. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    conexant = None
    cntconxant = 0
    cnt = 0
    num_reg = defaultdict(int)

    try:
        mode = "w" if first_write else "a"
        with open(destino, mode, encoding="utf-8") as fp:
            if not first_write:
                fp.write("\n")

            # Cabeçalho padrão
            timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            linha_top = "// " + "=" * 70
            fp.write(f"{linha_top}\n")
            fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10} {timestamp}\n")
            fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
            fp.write(f"{linha_top}\n\n")

            for pt in rows:
                cod_conexao = pt.get("cod_conexao")
                
                if conexant != cod_conexao:
                    conexant = cod_conexao
                    if cnt != 0:
                        fp.write(f"\n; Pontos nesta conexão: {cnt - cntconxant} \n\n")
                    fp.write("\n; --------------------------------------------------------------------------------")
                    fp.write(f"\n; {pt['descr_conex']} ( {pt['descr_protocolo']} )\n\n")
                    cntconxant = cnt
                
                # Regra de bacalhau para pular pontos da conexão 1 se houverem em outra conexão válida
                if pt["cod_conexao"] == 1 and pt["con2"] and pt["org2"]:
                    logging.info(f"[{ent}] Ignorando ponto {pt['id']} da conexão 1, pois existe na conexão {pt['con2']}.")
                    continue
                
                # Lógica para determinar KCONV
                kconv = pt["kconv"]
                if not kconv and pt["grupo_protoc"] == 1:
                    if pt["kconv1"] == 1:
                        kconv = "NO_S"
                    else:
                        kconv = "NO"
                
                if pt["cod_noh_org"] == int(cod_noh):  # Distribuição
                    if pt["cod_protocolo"] == 10:  # ICCP
                        id_cgf = f"{pt['id_conex_dt']}{str(pt['id']).replace('-', '_').upper()}"
                        cgs = f"CGS= {pt['id']}"
                        nv2 = f"{pt['id_conex_dt']}_{pt['tn2']}_NV2"
                    else:  # Outros protocolos
                        # Sua lógica PHP original tem um erro aqui, `id_conex_dt_dst` não existe nesta query
                        # Revertendo para uma lógica mais simples.
                        id_cgf = f"{pt['endereco']}"
                        cgs = f"CGS= {pt['id']}"
                        nv2 = f"{pt['id_conex_dt']}_{pt['tn2']}_NV2"
                else:  # Aquisição
                    if pt["cod_protocolo"] == 10:  # ICCP
                        id_cgf = f"{pt['id_conex']}_{str(pt['id']).replace('-', '_').upper()}"
                        cgs = f"CGS= {pt['id']}"
                        nv2 = f"{pt['id_conex']}_C{pt['suf_prot']}_{ordemnv1_sage_ct[cod_conexao]}_{pt['tn2']}"
                    else:  # Outros protocolos
                        id_cgf = f"{pt['id_conex']}_C{pt['suf_prot']}_{ordemnv1_sage_ct[cod_conexao]}_{pt['tn2']}_{pt['endereco']}"
                        cgs = f"CGS= {pt['id']}"
                        nv2 = f"{pt['id_conex']}_C{pt['suf_prot']}_{ordemnv1_sage_ct[cod_conexao]}_{pt['tn2']}"

                fp.write("\n")
                if com_flag:
                    fp.write(f"; NPONTO= {pt['objeto']:05d}\n")
                
                fp.write(f"ID= {id_cgf}\n")
                fp.write(f"KCONV= {kconv}\n")
                
                if pt["cod_protocolo"] != 10:
                    fp.write(f"ORDEM= {pt['endereco']}\n")
                
                fp.write(f"{cgs}\n")
                fp.write(f"NV2= {nv2}\n")
                
                cnt += 1
                num_reg[ent] += 1
                logging.info(f"{ent.upper()}={cnt:05d} PONTO={pt['objeto']:5d} ID={id_cgf}")

            # Rodapé final
            fp.write("\n")
            fp.write(f"{linha_top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper()}\n")
            fp.write(f"// Total de registros escritos: {cnt}\n")
            fp.write(f"{linha_top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
        return
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)
#---------------------------------------------------------------------------------------------------------
# ARQUIVO PDD.DAT
def generate_pdd_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    com_flag: bool,
    max_pts_por_tdd: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo pdd.dat (Pontos digitais lógicos de distribuição).
    Usa apenas conexoes_org.
    """
    ent = "pdd"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    # placeholders para IN (...)
    ph = ",".join("%s" for _ in conexoes_org)
    sql = f"""
    SELECT
        m.descricao      AS entidade,
        i.id             AS id_pt,
        c.id_sage_dt     AS id_conex,
        e.estacao        AS estacao,
        i.nponto         AS objeto,
        i.cod_origem     AS cod_origem,
        f.cod_conexao    AS cod_conexao
    FROM id_ptfis_conex f
    JOIN id_conexoes c  ON f.cod_conexao = c.cod_conexao
    JOIN id_ponto i     ON f.id_org = i.nponto
    JOIN id_ptlog_noh l ON l.nponto = i.nponto
    JOIN id_nops n      ON n.cod_nops = i.cod_nops
    JOIN id_modulos m   ON m.cod_modulo = n.cod_modulo
    JOIN id_estacao e   ON e.cod_estacao = m.cod_estacao
    JOIN id_tipos tp    ON tp.cod_tpeq = i.cod_tpeq
                       AND tp.cod_info = i.cod_info
    JOIN id_tipopnt tpn ON tpn.cod_tipopnt = tp.cod_tipopnt
    WHERE f.cod_conexao IN ({ph})
      AND l.cod_nohsup = %s
      AND tpn.tipo = 'D'
      AND i.cod_origem != 7
      AND i.cod_tpeq != 95
    ORDER BY f.cod_conexao, i.nponto
    """

    if dry_run:
        logging.info(f"[{ent}] dry-run, não escreve nada em {destino}")
        return

    # faz a query
    params = tuple(conexoes_org) + (cod_noh,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros.")
        return

    # escreve o arquivo
    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = 0

    # contador por conexão
    contagem_por_conex: Dict[int,int] = {}

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO PDD       {ts}\n")
        fp.write(f"// NOH={cod_noh}\n")
        fp.write(f"{top}\n")

        for pt in rows:
            conex = pt["cod_conexao"]
            # incrementa contador para essa conexão
            contagem_por_conex[conex] = contagem_por_conex.get(conex, 0) + 1
            seq = contagem_por_conex[conex]

            # calcula em qual bloco TDD esse ponto cai
            bloco = (seq - 1) // max_pts_por_tdd + 1
            tdd = f"{pt['id_conex']}D{bloco}"

            fp.write("\n")
            fp.write("PDD\n")
            if com_flag:
                fp.write(f"; NPONTO= {pt['objeto']:05d}\n")
            fp.write(f"ID= {pt['id_conex']}_{pt['id_pt']}\n")
            fp.write(f"PDS= {pt['id_pt']}\n")
            fp.write(f"TDD= {tdd}\n")

            cnt += 1

        fp.write(f"\n{top}\n")
        fp.write(f"// FIM PDD – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros.")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO PAD.DAT
def generate_pad_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    coment: bool,
    max_points_ana: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo pad.dat (Pontos analógicos lógicos de distribuição).
    """
    ent = "pad"
    auto      = Path(paths["automaticos"])
    destino   = auto / f"{ent}.dat"
    first_write = not destino.exists() or force

    # monta placeholder para IN (%s,%s,…) em conexoes_org
    ph = ",".join("%s" for _ in conexoes_org)

    sql = f"""
select
  m.descricao as entidade,
  c.id_sage_dt as id_conex,
  i.id as id,
  e.estacao as estacao,  
  form.id as tcl, 
  i.nponto as objeto, 
  i.cod_origem as cod_origem,
  f.cod_conexao as cod_conexao
from
  id_ptfis_conex as f, 
  id_conexoes as c,
  id_ponto as i
  join id_ptlog_noh as l on l.nponto=i.nponto
  join id_nops n on n.cod_nops=i.cod_nops
  join id_modulos m on m.cod_modulo=n.cod_modulo
  join id_estacao e on e.cod_estacao=m.cod_estacao        
  join id_formulas as form on i.cod_formula=form.cod_formula
  join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
  join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
where
  f.cod_conexao in ({ph}) and
  f.cod_conexao = c.cod_conexao and
  f.id_org=i.nponto and
  l.cod_nohsup= %s and
  i.cod_origem!=7 and
  tpnt.tipo='A' and
  i.cod_tpeq!=95
order by
  f.cod_conexao, i.nponto
    """
    if dry_run:
        logging.info(f"[{ent}] dry-run, não escreve nada em {destino}")
        return

    # executa a query
    params = tuple(conexoes_org) + (cod_noh,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros para gerar.")
        return

    # contagem de pontos por conexão (para montar o TDD)
    pad_count: Dict[int,int] = defaultdict(int)

    # abre o arquivo
    mode = "w" if first_write else "a"
    top  = "// " + "=" * 70
    ts   = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt  = 0

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO PAD       {ts}\n")
        fp.write(f"// Código NOH: {cod_noh}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            conex = pt["cod_conexao"]
            pad_count[conex] += 1

            # bacalhau para TDD
            idx = int(1 + pad_count[conex] / max_points_ana)
            tdd = f"{pt['id_conex']}A{idx}"

            fp.write("\n")
            fp.write(f"{ent.upper()}\n")
            if coment:
                fp.write(f"; NPONTO= {pt['objeto']:05d}\n")
            fp.write(f"ID= {pt['id_conex']}_{pt['id']}\n")
            fp.write(f"PAS= {pt['id']}\n")
            fp.write(f"TDD= {tdd}\n")

            cnt += 1

        fp.write(f"\n{top}\n")
        fp.write(f"// FIM PAD – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros.")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO PDS_ROTEAMENTO.DAT
# PDS ROTEAMENTO DE COMUNICAÇÃO
def generate_pds_gcom_dat(
    paths: Dict[str, Path],
    conn,
    conexoes_dst: List[int],
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera pds.gcom.dat (PDS – gestão da comunicação),
    se houver conexoes_dst e dry_run=False.
    """
    ent = "pds"
    destino = Path(paths["dats_unir"]) / f"{ent}-gcom.dat"
    first_write = not destino.exists() or force

    # 1) monta placeholders e SQL
    ph = ",".join("%s" for _ in conexoes_dst)
    sql = f"""
    SELECT
    c.cod_protocolo,
    c.nsrv1,
    c.nsrv2,   
    c.placa_princ,    
    c.linha_princ,    
    c.placa_resrv,    
    c.linha_resrv, 
    c.cod_conexao,
    c.end_org,
    c.end_dst,
    c.descricao as nome,
    c.id_sage_aq,
    c.id_sage_dt,
    p.nome as pnome,
    c.cod_noh_org,
    c.cod_noh_dst
FROM 
    id_conexoes c
    join id_protocolos p on p.cod_protocolo=c.cod_protocolo 
WHERE
    c.cod_conexao in ({ph})
    and p.cod_protocolo not in (0, 10)    
ORDER BY 
    p.cod_protocolo,
    c.nsrv1,
    c.nsrv2,
    c.placa_princ,      
    c.linha_princ,      
    c.placa_resrv,      
    c.linha_resrv      
    """

    if dry_run:
        logging.info(f"[{ent}_gcom] dry-run, não escreve nada em {destino}")
        return

    # 2) busca no banco
    with conn.cursor() as cur:
        cur.execute(sql, tuple(conexoes_dst))
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}_gcom] sem registros para gerar.")
        return

    # 3) escreve no arquivo
    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    # os sufixos, legendas e KCONV são sempre estes 8 blocos:
    blocos = [
        ("DESAB_ENUP", "Desabilitacao do Enlace Principal"),
        ("HABIL_ENUP", "Habilitacao do Enlace Principal"),
        ("DESAB_ENUR", "Desabilitacao do Enlace Reserva"),
        ("HABIL_ENUR", "Habilitacao do Enlace Reserva"),
        ("DESAB_FSECN", "Desabil da Func Secund nos Enlaces"),
        ("HABIL_FSECN", "Habilit da Func Secund nos Enlaces"),
        ("HABIL_UTRP", "Habilitacao da UTR Principal"),
        ("DESAB_UTRP", "Desabilitacao da UTR Principal"),
        ("HABIL_UTRR", "Habilitacao da UTR Reserva"),
        ("DESAB_UTRR", "Desabilitacao da UTR Reserva"),
        ("PFAIL_ENUP", "Failover do Enlace Principal"),
        ("PFAIL_ENUR", "Failover do Enlace Reserva"),
    ]

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO {ent.upper()}.GCOM  {ts}\n")
        fp.write(f"{top}\n")

        cnt = 0
        for pt in rows:
            nome   = pt["nome"]
            pnome  = pt["pnome"]
            id_aq  = pt["id_sage_aq"]

            # cabeçalho de contexto
            fp.write(f"\n\n; >>>>>> {nome} - {pnome} <<<<<<\n")
            for sufixo, legend in blocos:
                cnt += 1
                fp.write("\n")
                fp.write("PDS\n")
                fp.write(f"ID    =\t{id_aq}_{sufixo}\n")
                fp.write(f"NOME  =\t{legend} {id_aq}\n")
                fp.write("TIPO  =\tOUTROS\n")
                fp.write("TAC   =\tTAC-NAOSUP1\n")
                fp.write("OCR   =\tOCR_HAB01\n")
                fp.write("ALRIN =\tSIM\n")
                fp.write("ALINT =\tNAO\n")
                fp.write("STINI =\tA\n")
                fp.write("STNOR =\tA\n")
                fp.write("TPFIL =\tNLFL\n")
                fp.write("TCL   =\tNLCL\n")
                fp.write("SELSD =\tNAO\n")

        # rodapé
        fp.write(f"\n\n{top}\n")
        fp.write(f"// FIM {ent.upper()}.GCOM – total de registros escritos: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}_gcom] gerado em '{destino}' (modo={mode}), {cnt} registros.")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO PDS_SIMBOLICO.DAT
# PDS PONTO DIGITAL SIMBÓLICO
def generate_pds_simb_dat(
    paths: Dict[str, Path],
    conn: Any,
    cod_noh: str,
    conexoes_dst: List[int],
    tac_conex: Dict[int, str],
    tac_estacao: List[str],
    constants: Dict[str, Any],
    dry_run: bool = False,
    force: bool = False
):
    """
    Gera o arquivo pds.dat (Pontos digitais lógicos de aquisição).
    
    VERSÃO OTIMIZADA: Utiliza uma única consulta SQL para buscar todos os dados,
    eliminando o problema de N+1 queries para um ganho massivo de performance.
    """
    ent = "pds"
    destino = Path(paths["dats_unir"]) / f"{ent}-simb.dat"
    first_write = not destino.exists() or force

    if dry_run:
        logging.info(f"[{ent.upper()}] Dry-run ativo. {len(rows)} registros seriam processados para '{destino}'.")
        return
    conexoes_dst_placeholders = ",".join(["%s"] * len(conexoes_dst))
    
    sql = f"""
    SELECT
        main.*,
        -- Coluna para a lógica do filtro (substitui a função get_filter_tac_suffix)
        p_filtro.sufixo_sage AS filter_sufixo_sage,

        -- Coluna para a lógica do ponto EMS primário (substitui a função is_primary_ems_point)
        -- Rankeia os pontos dentro de cada grupo de ems_id. O primeiro (rank=1) é o primário.
        ROW_NUMBER() OVER (
            PARTITION BY main.ems_id 
            ORDER BY main.cod_origem, main.objeto
        ) AS ems_rank
    FROM (
        -- A consulta original está encapsulada aqui como uma subquery.
        SELECT
            m.descricao AS entidade, i.id, i.traducao_id, i.cod_tpeq, i.cod_info,
            i.cod_origem, i.cod_prot, i.cod_fases, e.estacao, i.nponto AS objeto,
            m.id AS mid, m.cod_tpmodulo, tpm.ent_ems, l.alrin, tpnt.cod_tipopnt,
            p.cod_tipopnt AS prot_cod_tipopnt, pt_ocr.ocr AS ptocr, tpnt.ocr,
            tpnt.casa_decimal AS estalm, tpnt.pres_1, tpnt.pres_0, pt_ocr.pres_1 AS ppres_1,
            pt_ocr.pres_0 AS ppres_0, m.ems_id AS ems_id_mod, n.ems_id, n.ems_lig1,
            n.ems_lig2, cx.cod_conexao,
            CASE i.cod_tpeq
                WHEN 28 THEN IF(i.cod_info=0 AND i.cod_prot=0, 'CHAVE', 'OUTROS')
                WHEN 27 THEN IF(i.cod_info=0 AND i.cod_prot=0, 'DISJ', 'OUTROS')
                ELSE
                    CASE
                        WHEN tpnt.casa_decimal < 2 THEN 'ALRP'
                        WHEN SUBSTRING(i.id, 15, 1) = 'O' THEN 'PTIP'
                        WHEN SUBSTRING(i.id, 15, 1) IN ('S','T','P','R') THEN 'PTNI'
                        ELSE 'OUTROS'
                    END
            END AS tipo_pds,
            'NAO' AS selsd, 'NLFL' AS tpfil, form.id AS tcl, form.tipo_calc,
            i.vlinic, i.evento AS eh_evento, e.ems_modela = 'S' AS pres_ems
        FROM
            id_ptlog_noh AS l
            JOIN id_ponto AS i ON l.nponto=i.nponto
            JOIN id_nops n ON n.cod_nops=i.cod_nops
            JOIN id_modulos m ON m.cod_modulo=n.cod_modulo
            JOIN id_tpmodulo tpm ON tpm.cod_tpmodulo=m.cod_tpmoduloems
            JOIN id_estacao e ON e.cod_estacao=m.cod_estacao
            JOIN id_formulas AS form ON i.cod_formula=form.cod_formula
            JOIN id_prot p ON i.cod_prot=p.cod_prot
            JOIN id_tipopnt AS pt_ocr ON p.cod_tipopnt=pt_ocr.cod_tipopnt
            JOIN id_tipos AS tp ON tp.cod_tpeq=i.cod_tpeq AND tp.cod_info=i.cod_info
            JOIN id_tipopnt AS tpnt ON tpnt.cod_tipopnt=tp.cod_tipopnt
            LEFT OUTER JOIN id_ptfis_conex f ON f.id_dst=l.nponto AND f.cod_conexao IN ({conexoes_dst_placeholders})
            LEFT OUTER JOIN id_conexoes cx ON cx.cod_conexao=f.cod_conexao
        WHERE
            l.cod_nohsup = %s
            AND tpnt.tipo = 'D' AND i.cod_origem != 7 AND i.cod_tpeq != 95
            AND i.nponto NOT IN (0, 9991, 9992)
    ) AS main
    -- JOINs adicionados para buscar o sufixo do filtro de uma só vez.
    LEFT JOIN id_calculos calc_filtro ON main.objeto = calc_filtro.nponto AND main.tipo_calc = 'F'
    LEFT JOIN id_ptfis_conex f_filtro ON calc_filtro.parcela = f_filtro.id_dst
    LEFT JOIN id_conexoes cx_filtro ON f_filtro.cod_conexao = cx_filtro.cod_conexao AND cx_filtro.cod_noh_dst = %s
    LEFT JOIN id_protocolos p_filtro ON cx_filtro.cod_protocolo = p_filtro.cod_protocolo
    ORDER BY
        main.objeto, main.cod_conexao DESC
    """
    
    logging.info(f"[{ent.upper()}] Executando consulta OTIMIZADA para Pontos Digitais.")
    try:
        with conn.cursor() as cur:
            # Parâmetros para a query: lista de conexões, cod_noh para o WHERE, cod_noh para o JOIN do filtro
            params = tuple(conexoes_dst) + (cod_noh, cod_noh)
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent.upper()}] Erro ao buscar dados com a query otimizada: {e}")
        return

    # Variáveis de estado para o processamento em Python
    ptant = None
    cntcalccomp = 0
    cntnaosup = 0
    ptosDigTacEst = {}
    num_reg_gerados = 0

    try:
        with open(destino, "w" if first_write else "a", encoding="utf-8") as fp:
            fp.write(f"// --- Arquivo gerado via script otimizado ---\n")
            
            for pt in rows:
                if ptant == pt["objeto"]:
                    continue
                ptant = pt["objeto"]

                # Lógica de OCR (inalterada)
                if pt["prot_cod_tipopnt"] != 0:
                    if pt["prot_cod_tipopnt"] == 23:
                        if pt["cod_tipopnt"] in {8, 23, 25}:
                            pt["ocr"], pt["pres_0"], pt["pres_1"] = pt["ptocr"], pt["ppres_0"], pt["ppres_1"]
                        elif pt["cod_tipopnt"] in {7,20,22,26,31,34,42,54,57,103}:
                            pt["ocr"] = "OCR_OPE1"
                        elif pt["cod_tipopnt"] in {36,38,49,64,65,69,85,95,107}:
                            pt["ocr"] = "OCR_OPE2"
                    else:
                        pt["ocr"], pt["pres_0"], pt["pres_1"] = pt["ptocr"], pt["ppres_0"], pt["ppres_1"]

                # Lógica de definição da TAC (agora sem consultas aninhadas)
                tac = pt["estacao"]
                cod_conexao = pt.get("cod_conexao")
                if cod_conexao and cod_conexao > 0:
                    if (NO_COS and cod_conexao == CONEX_ONS_COS) or \
                       ((NO_COR or NO_CPS) and cod_conexao == CONEX_ONS_COR):
                        tac = "CEEE_S_1"
                    elif cod_conexao in tac_conex:
                        if cod_conexao in {1, 100, 120, 72} and pt["estacao"] in tac_estacao:
                            tac = pt["estacao"]
                        else:
                            tac = tac_conex[cod_conexao]
                    else:
                        count = ptosDigTacEst.get(pt["estacao"], 0) + 1
                        ptosDigTacEst[pt["estacao"]] = count
                        if count > MaxPontosDigPorTAC:
                            tac = f'{pt["estacao"]}_{math.floor(count / MaxPontosDigPorTAC)}'
                else:
                    if pt["cod_origem"] == 1:
                        if pt["tipo_calc"] == "C":
                            cntcalccomp += 1
                            tac = f"CALC-COMP{1 + cntcalccomp // MaxPontosPorTAC_Calc}"
                        elif pt["tipo_calc"] == "I":
                            tac = "CALC-INTER"
                        elif pt["tipo_calc"] == "F":
                            pt["tpfil"], pt["tcl"] = pt["tcl"], "NLCL"
                            sufixo = pt.get("filter_sufixo_sage") or "101" # Usa o valor da query
                            tac = f"FILC{sufixo}"
                    elif pt["cod_origem"] == 15:
                        tac = "LOCAL"
                    else:
                        cntnaosup += 1
                        if pt["cod_origem"] != 6:
                            logging.warning(f"Ponto {pt['objeto']} ({pt['id']}) sem ponto físico associado.")
                        tac = f"TAC-NAOSUP{1 + cntnaosup // MaxPontosPorTAC}"
                        pt["tcl"] = "NLCL"
                
                if (NO_COS or NO_COR or NO_CPS) and pt["cod_origem"] == 17 and pt["estacao"] != "ECEY": tac = "ECEY"
                if (NO_COS or NO_COR or NO_CPS) and pt["cod_origem"] == 16 and pt["estacao"] != "ECEZ": tac = "ECEZ"

                # --- Escrita no arquivo ---
                fp.write(f"\n{ent.upper()}\n")
                if COMENT: fp.write(f'; NPONTO= {pt["objeto"]:05d}\n')

                nome = f'{pt["estacao"]}-{pt["traducao_id"]}'.strip()
                if not pt["id"]: logging.error(f"Ponto com id vazio! nponto={pt['objeto']}")
                if not pt["traducao_id"]: logging.error(f"Ponto com descritivo vazio! nponto={pt['objeto']}")
                if len(nome) > MaxIdSize: logging.error(f'Nome muito longo ({len(nome)}) para o ponto {pt["objeto"]}: {nome}')

                fp.write(f'ID= {pt["id"]}\n')
                fp.write(f'NOME= {nome}\n')
                if not NO_COR: fp.write("AOR= CPFLT\n")
                fp.write(f'TIPO= {pt["tipo_pds"]}\n')
                fp.write(f'TAC= {tac}\n')

                if EMS and pt["pres_ems"]:
                    if pt["ems_id_mod"] and pt["tipo_pds"] not in {"DISJ", "CHAVE"}:
                        fp.write(f'EQP= {pt["ems_id_mod"]}\n')
                        fp.write(f'TPEQP= {pt["ent_ems"]}\n')
                    elif pt["ems_id"] and pt["tipo_pds"] in {"DISJ", "CHAVE"}:
                        if pt["ems_rank"] == 1: # Verifica o rank da query, sem nova consulta
                            fp.write(f'EQP= {pt["ems_id"]}\n')
                            fp.write(f'TPEQP= CNC\n')
                
                if pt["cod_tpeq"] in {181, 237, 182, 199} and pt["cod_prot"] in {2, 6, 8}: pt["ocr"] = "OCR_OPB"

                fp.write(f'OCR= {pt["ocr"]}01\n')
                fp.write(f'ALRIN= {"SIM" if pt["alrin"] != "N" else "NAO"}\n')
                fp.write("ALINT= SIM\n")

                if pt["eh_evento"] == "S":
                    fp.write("STNOR= A\nSTINI= A\n")
                elif pt["estalm"] <= 1:
                    fp.write("ALRP= SIM\n")
                    st = "F" if pt["estalm"] == 0 else "A"
                    fp.write(f"STNOR= {st}\nSTINI= {st}\n")
                else:
                    st = "F" if int(pt.get("vlinic", 0)) else "A"
                    fp.write(f"STNOR= {st}\nSTINI= {st}\n")
                
                fp.write(f'TPFIL= {pt["tpfil"]}\n')

                if pt["cod_info"] == 42 and pt["tcl"] == "G_LIA": fp.write(f'TCL= {pt["mid"]}-AQ\n')
                elif pt["cod_info"] == 42 and pt["tcl"] == "G_LID": fp.write(f'TCL= {pt["mid"]}-DT\n')
                elif pt["cod_info"] == 189 and pt["tcl"] == "G_ENU" and pt["cod_fases"] == 14: fp.write(f'TCL= {pt["mid"]}-AQ_P\n')
                elif pt["cod_info"] == 189 and pt["tcl"] == "G_ENU" and pt["cod_fases"] == 15: fp.write(f'TCL= {pt["mid"]}-AQ_R\n')
                else: fp.write(f'TCL= {pt["tcl"]}\n')
                
                fp.write(f'SELSD= {pt["selsd"]}\n')
                fp.write(f'IDOPER= {pt["objeto"]}\n')
                
                if pt["cod_tipopnt"] in {32, 33, 42, 43}: fp.write("TMP_ANORM= 300\n")

                num_reg_gerados += 1
                logging.info(f"{ent.upper()}={num_reg_gerados:05d} PONTO={pt['objeto']:5d} ID={pt['id']}")

            fp.write(f"\n// --- FIM DA GERAÇÃO OTIMIZADA ---\n")
            
        logging.info(f"[{ent.upper()}] Geração OTIMIZADA concluída. Total: {num_reg_gerados} registros.")

    except Exception as e:
        # MUDANÇA: Captura e loga o erro completo (traceback)
        logging.error(f"[{ent.upper()}] Erro ao processar dados para o arquivo '{destino}'. Causa: {e}")
        # A linha abaixo vai imprimir no log o local exato do erro no código.
        logging.error(traceback.format_exc())
#---------------------------------------------------------------------------------------------------------
# ARQUIVO PAS.DAT
# PAS PONTO ANALÓGICO
def generate_pas_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_dst: List[int],
    tac_conex: Dict[int, str],
    tac_estacao: List[str],
    no_cor: bool,
    com_flag: bool,
    max_id_size: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo pas.dat (Pontos analógicos lógicos de aquisição).
    """
    ent     = "pas"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    if dry_run:
        logging.info(f"[{ent}] dry-run, não grava em {destino}")
        return

    # placeholders e SQL
    ph = ",".join("%s" for _ in conexoes_dst)
    sql = f"""
select
  m.descricao as entidade,
  i.id as id,
  i.traducao_id as traducao_id,
  i.cod_tpeq as cod_tpeq,
  i.cod_origem as cod_origem,
  i.cod_info as cod_info,
  e.estacao as estacao,
  i.nponto as objeto, 
  cx.cod_conexao,
  m.id as mid,
  m.cod_tpmodulo as cod_tpmodulo,
  m.ems_id as ems_id,
  m.param_ems as param_ems,
  m.cod_modulo as cod_modulo,
  m.ems_lig1 as ems_lig1,
  em.id as ems_est,
  tpm.ent_ems as ent_ems,
  coalesce(p.lie, l.lie) as lie,
  coalesce(p.liu, l.liu) as liu,
  coalesce(p.lia, l.lia) as lia,
  coalesce(p.lsa, l.lsa) as lsa,
  coalesce(p.lsu, l.lsu) as lsu,
  coalesce(p.lse, l.lse) as lse,
  coalesce(l.htris, 0) as htris,
  form.id as tcl,
  form.tipo_calc as tipo_calc,
  'NLFL' as tpfil,
  l.alrin as alrin,  
  i.vlinic as vlinic,
  i.evento as eh_evento,
  tpnt.unidade as unidade
  , e.ems_modela = 'S' as pres_ems 
  , h.periodo as periodo
  , h.nponto as hnponto
  , v.valor as valor_atual
  , i.excl_ems as excl_ems
  , coalesce(p.liemi, '') as liemi
  , coalesce(p.liele, '') as liele
  , coalesce(p.lieme, '') as lieme
  , coalesce(p.liepe, '') as liepe
  , coalesce(p.liema, '') as liema
  , coalesce(p.lsemi, '') as lsemi
  , coalesce(p.lsele, '') as lsele
  , coalesce(p.lseme, '') as lseme
  , coalesce(p.lsepe, '') as lsepe
  , coalesce(p.lsema, '') as lsema
  , coalesce(p.liumi, '') as liumi
  , coalesce(p.liule, '') as liule
  , coalesce(p.liume, '') as liume
  , coalesce(p.liupe, '') as liupe
  , coalesce(p.liuma, '') as liuma
  , coalesce(p.lsumi, '') as lsumi
  , coalesce(p.lsule, '') as lsule
  , coalesce(p.lsume, '') as lsume
  , coalesce(p.lsupe, '') as lsupe
  , coalesce(p.lsuma, '') as lsuma
  , coalesce(p.liami, '') as liami
  , coalesce(p.liale, '') as liale
  , coalesce(p.liame, '') as liame
  , coalesce(p.liape, '') as liape
  , coalesce(p.liama, '') as liama
  , coalesce(p.lsami, '') as lsami
  , coalesce(p.lsale, '') as lsale
  , coalesce(p.lsame, '') as lsame
  , coalesce(p.lsape, '') as lsape
  , coalesce(p.lsama, '') as lsama
  , tpnt.ocr as ocr

from
  id_ptlog_noh as l
  join id_ponto as i on i.nponto=l.nponto
  join id_nops n on n.cod_nops=i.cod_nops
  join id_modulos m on m.cod_modulo=n.cod_modulo
  join id_estacao e on e.cod_estacao=m.cod_estacao        
  join id_formulas as form on i.cod_formula=form.cod_formula
  join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
  join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
  left outer join id_tpmodulo tpm on tpm.cod_tpmodulo=m.cod_tpmoduloems
  left outer join id_emsestacao em on m.cod_emsest=em.cod_emsest
  left outer join val_tr v on v.nponto=i.nponto
  left outer join cnf_hist_tr h on h.nponto=i.nponto
  left outer join id_ptfis_conex f on f.id_dst=l.nponto and f.cod_conexao in ({ph})
  left outer join id_conexoes cx on cx.cod_conexao=f.cod_conexao
  left outer join id_limites_ptc p on p.nponto=i.nponto and p.cod_nohsup=0
where
  l.cod_nohsup=%s and
  l.nponto=i.nponto and
  i.cod_origem!=7 and
  tpnt.tipo='A' and
  i.cod_tpeq!=95 and 
  i.nponto not in (0, 9991, 9992)
order by
  i.nponto, cx.cod_conexao desc
    """

    params = tuple(conexoes_dst) + (cod_noh,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros para gerar.")
        return

    # contadores
    ptant      = None
    cntnaosup  = 0
    cntcalccomp = 0

    # mapeamento de tipo_pas
    tipo_map = {
        1:"KV", 3:"AMP", 6:"MW", 7:"MVAR", 8:"MVA",
        33:"MWH", 99:"BIAS", 98:"ECA", 32:"DIST",
        9:"FREQ", 149:"NIVEL",150:"NIVEL",151:"NIVEL",
        16:"TAP",97:"TEMPO",132:"TEMPO",
        17:"TMP",19:"TMP",36:"TMP"
    }

    mode = "w" if first_write else "a"
    top  = "// " + "="*70
    ts   = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt  = 0

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO PAS       {ts}\n")
        fp.write(f"// Código NOH: {cod_noh}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            if pt["objeto"] == ptant:
                continue
            ptant = pt["objeto"]

            # 1) tipo_pas
            tipo_pas = tipo_map.get(pt["cod_tpeq"], "OUTROS")

            # 2) descobrir TAC
            conex = pt.get("cod_conexao") or 0
            if pt["cod_origem"] != 1:
                if conex > 0:
                    if (NO_COS and conex == CONEX_ONS_COS) or ((NO_COR or NO_CPS) and conex == CONEX_ONS_COR):
                        tac = "CEEE_S_1"
                    elif conex in tac_conex:
                        tac = (pt["estacao"] if (conex in (1,100,120,72) and pt["estacao"] in tac_estacao)
                               else tac_conex[conex])
                    else:
                        tac = pt["estacao"]
                else:
                    if pt["cod_origem"] == 11:
                        tac = "ESTIMADOS"
                    else:
                        cntnaosup += 1
                        tac = f"TAC-NAOSUP{1 + cntnaosup//MaxPontosPorTAC}"
                    pt["tcl"] = "NLCL"
            else:
                if pt["tipo_calc"] == "C":
                    cntcalccomp += 1
                    tac = f"CALC-COMP{1 + cntcalccomp//MaxPontosPorTAC_Calc}"
                elif pt["tipo_calc"] == "I":
                    tac = "CALC-INTER"
                else:
                    tac = pt["estacao"]

            # 3) escreve bloco
            fp.write("\nPAS\n")
            if com_flag:
                fp.write(f"; NPONTO= {pt['objeto']:05d}\n")

            fp.write(f"ID= {pt['id']}\n")
            nome = f"{pt['estacao']}-{pt['traducao_id']}".strip()
            fp.write(f"NOME= {nome}\n")
            if not no_cor:
                fp.write("AOR= CPFLT\n")

            if len(nome) > max_id_size:
                raise RuntimeError(f"Nome longo demais ({len(nome)}) em PAS nponto={pt['objeto']}")

            fp.write(f"TIPO= {tipo_pas}\n")
            fp.write(f"TAC= {tac}\n")
            # limites principais
            for fld in ("lie","liu","lia","lsa","lsu","lse","lsemi"):
                raw = pt.get(fld)
                try:
                    num = float(raw)
                except (TypeError, ValueError):
                    continue
                fp.write(f"{fld.upper()}= {num:.2f}\n")

            fp.write(f"TCL= {pt['tcl']}\n")
            fp.write(f"TPFIL= {pt['tpfil']}\n")
            fp.write(f"IDOPER= {pt['objeto']}\n")

            cnt += 1

        # rodapé
        fp.write(f"\n{top}\n")
        fp.write(f"// FIM PAS – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros.")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO PDF.DAT
# PDF PONTO DIGITAL FISICO
def generate_pdf_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    ordemnv1_sage_aq: Dict[int, int],
    ordemnv1_sage_dt: Dict[int, int],
    com_flag: bool,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo pdf.dat (PDF – pontos digitais físicos).
    """
    ent = "pdf"
    auto = Path(paths["automaticos"])
    destino = auto / f"{ent}.dat"
    first_write = not destino.exists() or force

    # 1) placeholders para IN (%s,…) em todas as conexões
    all_conex = conexoes_org + conexoes_dst
    ph_all = ",".join("%s" for _ in all_conex)
    ph_dst = ",".join("%s" for _ in conexoes_dst)

    # 2) SQL
    sql = f"""
   select   
        m.descricao as entidade, 
        m.descricao as moddescr, 
        i.id as id, 
        pntorg.id as id_pnt_org,
        pntdst.id as id_pnt_dst,
        c.id_sage_dt as id_conex_dt,
        c.id_sage_aq as id_conex_aq,
        c.cod_noh_org as cod_noh_org,
        c.cod_noh_dst as cod_noh_dst,
        p.sufixo_sage as suf_prot,
        a.tn2_aq as tn2_aq, 
        a.tn2_dt as tn2_dt,
        a.tipo as tipoasdu, 
        f.id_org as id_org,
        f.id_dst as id_dst,
        f.kconv1 as kconv1,
        f.kconv2 as kconv2,
        f.kconv as kconv,
        i.nponto as objeto, 
        i.cod_origem as cod_origem,
        f.cod_conexao as cod_conexao,
        f.endereco as endereco,
        c.descricao as descr_conex,
        p.cod_protocolo as cod_protocolo,
        p.grupo_protoc as grupo_protoc,
        p.descricao as descr_protocolo,
        tpnt.tipo as tipolog,
        tpntorg.tipo as tipoorg,
        tpntdst.tipo as tipodst,
        i.traducao_id as traducao_id,
        
        /* para encontrar mesmo pf em outra conexão que não a 1 */         
        f2.cod_conexao as con2,
        c2.end_org as org2      
from    
        id_ptfis_conex as f
        join id_protoc_asdu as a on a.cod_asdu=f.cod_asdu
        join id_ponto pntorg on pntorg.nponto=f.id_org
        join id_tipos as tporg on tporg.cod_tpeq=pntorg.cod_tpeq and tporg.cod_info=pntorg.cod_info
        join id_tipopnt as tpntorg on tpntorg.cod_tipopnt=tporg.cod_tipopnt
        join id_ponto pntdst on pntdst.nponto=f.id_dst
        join id_tipos as tpdst on tpdst.cod_tpeq=pntdst.cod_tpeq and tpdst.cod_info=pntdst.cod_info
        join id_tipopnt as tpntdst on tpntdst.cod_tipopnt=tpdst.cod_tipopnt
        /* para encontrar mesmo pf (não dummy) em outra conexão que não a 1 */
        left outer join id_ptfis_conex f2 on f.id_dst=f2.id_dst and f.cod_conexao!=f2.cod_conexao and f.cod_conexao=1 and f2.id_dst not in (9991,9992) and f2.cod_conexao in ({ph_dst})
        left outer join id_conexoes c2 on f2.cod_conexao=c2.cod_conexao and c2.cod_noh_dst=%s /*and c2.end_org!=0*/,      
        id_conexoes as c
        join id_protocolos as p on c.cod_protocolo = p.cod_protocolo,
        id_ponto as i
        join id_ptlog_noh l on l.nponto=i.nponto
        join id_nops n on n.cod_nops=i.cod_nops
        join id_modulos m on m.cod_modulo=n.cod_modulo
        join id_estacao e on e.cod_estacao=m.cod_estacao        
        join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
        join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
where       
        f.cod_conexao in ({ph_all}) and
        f.cod_conexao = c.cod_conexao and
        f.id_org=i.nponto and
        l.cod_nohsup=%s and 
        tpnt.tipo='D' and
        i.cod_origem!=7 and 
        i.cod_tpeq!=95
order by 
        f.cod_conexao, i.nponto 
"""

    params = tuple(all_conex) + tuple(conexoes_dst) + (cod_noh, cod_noh)

    if dry_run:
        logging.info(f"[{ent}] dry-run, não grava em {destino}")
        return

    # 3) executa consulta
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros para gerar.")
        return

    # 4) escreve o arquivo
    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = 0
    conex_ant = None
    cnt0 = 0
    ptoaqfis: Dict[int, str] = {}

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO PDF       {ts}\n")
        fp.write(f"// NOH={cod_noh}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            # -- Consistência de tipo e ASDU digital
            if pt["tipoasdu"] != "D" or pt["tipoorg"] != "D" or pt["tipodst"] != "D":
                msg = f"{pt['endereco']} {pt['objeto']} {pt['id_org']} {pt['id_dst']} {pt['id']}"
                raise ValueError(f"Ponto com tipo ou ASDU não digital em PDF. {msg}")

            # -- Validação do endereço conforme protocolo/grupo
            grupo = pt["grupo_protoc"]
            protocolo = pt["cod_protocolo"]
            end_raw = pt["endereco"]

            # protocolos numéricos (Conitel, DNP, PCEE, PCTR, IEC-101)
            if grupo in (6, 8, 7, 4, 1):
                try:
                    end = int(end_raw)
                except ValueError:
                    raise ValueError(f"Endereço não numérico para protocolo {protocolo!r}: {end_raw!r}")
                if end < 0 or (end > 65535 and protocolo != 18):
                    msg = f"{end} {pt['objeto']} {pt['id_org']} {pt['id_dst']} {pt['id']}"
                    raise ValueError(f"Endereco inválido. {msg}")
            # ICCP
            elif protocolo == 10:
                s = end_raw
                if not s or s.upper() != s or any(c in s for c in "-?."):
                    msg = f"{s!r} {pt['objeto']} {pt['id_org']} {pt['id_dst']} {pt['id']}"
                    raise ValueError(f"Endereco ICCP inválido. {msg}")
            # Modbus e GOOSE não precisam de validação

            # -- Aquisição vs Distribuição
            AqDt = "A"
            AqDtTxt = "Aquisição"
            PxD = "PDS"
            IdDt = ""
            IdConex = pt["id_conex_aq"]
            IdIccp = end_raw
            IdPnt = pt["id_pnt_dst"]
            TN2 = pt["tn2_aq"]

            if pt["cod_noh_org"] == cod_noh:
                AqDt = "D"
                AqDtTxt = "Distribuição"
                PxD = "PDD"
                IdDt = f"{pt['id_conex_dt']}_"
                IdConex = pt["id_conex_dt"]
                ordem_nv1 = ordemnv1_sage_dt[pt["cod_conexao"]]
                IdPnt = pt["id_pnt_org"]
                TN2 = pt["tn2_dt"]
            else:
                ordem_nv1 = ordemnv1_sage_aq.get(pt["cod_conexao"], 1)
                # dummy skip
                if pt["id_dst"] == 9991:
                    IdPnt = ""
                    PxD = ""
                # skip migrated
                if pt["cod_conexao"] == 1 and pt["con2"] and pt["org2"]:
                    continue

            # -- Monta ID, NV2 e Ordem
            if protocolo == 10:
                Id = IdIccp.upper()
                Ordem = ""
                NV2 = f"{IdConex}_{TN2}_NV2"
            else:
                Id = f"{IdConex}_{AqDt}{pt['suf_prot']}_{ordem_nv1}_{TN2}_{end_raw}"
                NV2 = f"{IdConex}_{AqDt}{pt['suf_prot']}_{ordem_nv1}_{TN2}"
                Ordem = end_raw

            # -- KCONV
            if pt["kconv2"] == 0:
                KConv = "SQI" if pt["kconv1"] < 0 else "SQN"
            else:
                KConv = "INV" if pt["kconv1"] < 0 else "NOR"
            if pt["kconv"] in ("NOR", "INV", "SQN", "SQI"):
                KConv = pt["kconv"]

            # -- Cabeçalho por conexão
            conx = pt["cod_conexao"]
            if conx != conex_ant:
                conex_ant = conx
                if cnt != 0:
                    fp.write(f"\n; Pontos nesta conexão: {cnt - cnt0}\n\n")
                fp.write("\n; " + "-"*55 + "\n")
                fp.write(f"; {pt['descr_conex']} ({AqDtTxt} - {pt['descr_protocolo']})\n\n")
                cnt0 = cnt

            # -- Guarda ponto de aquisição para uso posterior
            if AqDt == "A":
                ptoaqfis[int(pt["objeto"])] = Id

            # -- Escreve o bloco
            fp.write("\n")
            fp.write(f"{ent.upper()}\n")
            if com_flag:
                fp.write(f"; NPONTO= {pt['objeto']:05d}\n")
            fp.write(f"ID= {Id}\n")
            fp.write(f"KCONV= {KConv}\n")
            if Ordem:
                fp.write(f"ORDEM= {Ordem}\n")
            fp.write(f"TPPNT= {PxD}\n")
            fp.write(f"PNT= {IdDt}{IdPnt}\n")
            fp.write(f"NV2= {NV2}\n")

            # -- DESC1 e DESC2
            moddescr = pt["moddescr"]
            traduz = pt["traducao_id"]
            if traduz.startswith(moddescr + "-"):
                pointdescr = traduz[len(moddescr) + 1:]
            else:
                parts = traduz.split("-", 1)
                pointdescr = parts[1] if len(parts) > 1 else traduz
            fp.write(f"DESC1= {moddescr}\n")
            fp.write(f"DESC2= {pointdescr}\n")

            cnt += 1

        # rodapé
        fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// FIM PDF – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={'w' if first_write else 'a'}), {cnt} registros.")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO PAF.DAT
# PAF PONTO ANALOGICO FISICO
def generate_paf_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    ordemnv1_sage_aq: Dict[int, int],
    ordemnv1_sage_dt: Dict[int, int],
    com_flag: bool,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo paf.dat (PAF – pontos analógicos físicos).
    """
    ent = "paf"
    auto = Path(paths["automaticos"])
    destino = auto / f"{ent}.dat"
    first_write = not destino.exists() or force

    all_conex = conexoes_org + conexoes_dst
    ph_all = ",".join("%s" for _ in all_conex)
    ph_dst = ",".join("%s" for _ in conexoes_dst)

    sql = f"""
select
  m.descricao as entidade,
  m.descricao as moddescr, 
  i.id as id,
  pntorg.id as id_pnt_org,
  pntdst.id as id_pnt_dst,
  c.id_sage_dt as id_conex_dt,
  c.id_sage_aq as id_conex_aq,
  c.cod_noh_org as cod_noh_org,
  c.cod_noh_dst as cod_noh_dst,
  p.sufixo_sage as suf_prot,
  a.tn2_aq as tn2_aq, 
  a.tn2_dt as tn2_dt,
  a.tipo as tipoasdu, 
  f.id_org as id_org,
  f.id_dst as id_dst,
  f.kconv1 as kconv1,
  f.kconv2 as kconv2,
  i.nponto as objeto, 
  i.cod_origem as cod_origem,
  f.cod_conexao as cod_conexao,
  f.endereco as endereco,
  c.descricao as descr_conex,
  p.cod_protocolo as cod_protocolo,
  p.grupo_protoc as grupo_protoc,
  p.descricao as descr_protocolo,
  tpnt.tipo as tipolog,
  tpntorg.tipo as tipoorg,
  tpntdst.tipo as tipodst,         
  i.traducao_id as traducao_id,

  /* para encontrar mesmo pf em outra conexão que não a 1 */         
  f2.cod_conexao as con2,
  c2.end_org as org2      
from
  id_ptfis_conex as f
  join id_protoc_asdu as a on a.cod_asdu=f.cod_asdu
  join id_ponto pntorg on pntorg.nponto=f.id_org
  join id_tipos as tporg on tporg.cod_tpeq=pntorg.cod_tpeq and tporg.cod_info=pntorg.cod_info
  join id_tipopnt as tpntorg on tpntorg.cod_tipopnt=tporg.cod_tipopnt
  join id_ponto pntdst on pntdst.nponto=f.id_dst
  join id_tipos as tpdst on tpdst.cod_tpeq=pntdst.cod_tpeq and tpdst.cod_info=pntdst.cod_info
  join id_tipopnt as tpntdst on tpntdst.cod_tipopnt=tpdst.cod_tipopnt
  /* para encontrar mesmo pf (não dummy) em outra conexão que não a 1 */
  left outer join id_ptfis_conex f2 on f.id_dst=f2.id_dst and f.cod_conexao!=f2.cod_conexao and f.cod_conexao=1 and f2.id_dst not in (9991,9992) and f2.cod_conexao in ({ph_dst})
  left outer join id_conexoes c2 on f2.cod_conexao=c2.cod_conexao and c2.cod_noh_dst=%s /*and c2.end_org!=0*/,      
  id_conexoes as c
  join id_protocolos as p on c.cod_protocolo = p.cod_protocolo,
  id_ptlog_noh as l,
  id_ponto as i
  join id_nops n on n.cod_nops=i.cod_nops
  join id_modulos m on m.cod_modulo=n.cod_modulo
  join id_estacao e on e.cod_estacao=m.cod_estacao        
  join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
  join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
where
  f.cod_conexao in ({ph_all}) and
  f.cod_conexao = c.cod_conexao and
  f.id_org=i.nponto and
  l.nponto=i.nponto and l.cod_nohsup=%s and
  i.cod_origem!=7 and
  tpnt.tipo='A' and
  i.cod_tpeq!=95
order by
  f.cod_conexao, i.nponto
    """

    params = tuple(all_conex) + tuple(conexoes_dst) + (cod_noh, cod_noh)

    if dry_run:
        logging.info(f"[{ent}] dry-run, não grava em {destino}")
        return

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros para gerar.")
        return

    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = 0
    conexant = None
    cntconxant = 0

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO PAF       {ts}\n")
        fp.write(f"// NOH={cod_noh}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            # Consistência de tipo e ASDU analógica
            if pt["tipoasdu"] != "A" or pt["tipoorg"] != "A" or pt["tipodst"] != "A":
                msg = f'{pt["endereco"]} {pt["objeto"]} {pt["id_org"]} {pt["id_dst"]} {pt["id"]}'
                raise ValueError(f"Ponto com tipo ou ASDU não analógica em PAF. {msg}")

            # Validação do endereço conforme protocolo/grupo
            grupo = pt["grupo_protoc"]
            protocolo = pt["cod_protocolo"]
            end_raw = pt["endereco"]

            if grupo in (6, 8, 7, 4, 1):
                try:
                    end = int(end_raw)
                except ValueError:
                    raise ValueError(f"Endereço não numérico para protocolo {protocolo!r}: {end_raw!r}")
                if end < 0 or (end > 65535 and protocolo != 18):
                    msg = f"{end} {pt['objeto']} {pt['id_org']} {pt['id_dst']} {pt['id']}"
                    raise ValueError(f"Endereco inválido. {msg}")
            elif protocolo == 10:
                s = end_raw
                if not s or s.upper() != s or any(c in s for c in "-?."):
                    msg = f"{s!r} {pt['objeto']} {pt['id_org']} {pt['id_dst']} {pt['id']}"
                    raise ValueError(f"Endereco ICCP inválido. {msg}")
            # Modbus e GOOSE não precisam de validação

            # Aquisição vs Distribuição
            AqDt = "A"
            AqDtTxt = "Aquisição"
            PxD = "PAS"
            IdDt = ""
            IdConex = pt["id_conex_aq"]
            IdIccp = end_raw
            IdPnt = pt["id_pnt_dst"]
            TN2 = pt["tn2_aq"]

            if pt["cod_noh_org"] == cod_noh:
                AqDt = "D"
                AqDtTxt = "Distribuição"
                PxD = "PAD"
                IdDt = f"{pt['id_conex_dt']}_"
                ordem_nv1 = ordemnv1_sage_dt.get(pt["cod_conexao"], 1)
                IdConex = pt["id_conex_dt"]
                IdPnt = pt["id_pnt_org"]
                TN2 = pt["tn2_dt"]
            else:
                ordem_nv1 = ordemnv1_sage_aq.get(pt["cod_conexao"], 1)
                if pt["id_dst"] == 9992:
                    IdPnt = ""
                    PxD = ""
                if pt["cod_conexao"] == 1 and pt["con2"] and pt["org2"]:
                    continue

            # Monta ID, NV2 e Ordem
            if protocolo == 10:
                Id = IdIccp.upper()
                Ordem = ""
                NV2 = f"{IdConex}_{TN2}_NV2"
                pt["id"] = pt["id"].upper()
            else:
                Id = f"{IdConex}_{AqDt}{pt['suf_prot']}_{ordem_nv1}_{TN2}_{end_raw}"
                NV2 = f"{IdConex}_{AqDt}{pt['suf_prot']}_{ordem_nv1}_{TN2}"
                Ordem = end_raw

            # Cabeçalho por conexão
            if conexant != pt["cod_conexao"]:
                conexant = pt["cod_conexao"]
                if cnt != 0:
                    fp.write(f"\n; Pontos nesta conexão: {cnt - cntconxant}\n\n")
                fp.write("\n; " + "-"*80 + "\n")
                fp.write(f"; {pt['descr_conex']} ({AqDtTxt} - {pt['descr_protocolo']})\n\n")
                cntconxant = cnt

            fp.write("\n")
            fp.write(f"{ent.upper()}\n")
            if com_flag:
                fp.write(f"; NPONTO= {pt['objeto']:05d}\n")
            fp.write(f"ID= {Id}\n")
            fp.write(f"KCONV1= {pt['kconv1']:.9f}\n")
            fp.write(f"KCONV2= {pt['kconv2']:.9f}\n")
            fp.write(f"KCONV3= \n")
            if Ordem:
                fp.write(f"ORDEM= {Ordem}\n")
            fp.write(f"TPPNT= {PxD}\n")
            fp.write(f"PNT= {IdDt}{IdPnt}\n")
            fp.write(f"NV2= {NV2}\n")

            # DESC1 e DESC2
            moddescr = pt["moddescr"]
            traduz = pt["traducao_id"]
            if traduz.startswith(moddescr + "-"):
                pointdescr = traduz[len(moddescr) + 1:]
            else:
                parts = traduz.split("-", 1)
                pointdescr = parts[1] if len(parts) > 1 else traduz
            fp.write(f"DESC1= {moddescr}\n")
            fp.write(f"DESC2= {pointdescr}\n")

            cnt += 1
            logging.info(f"{ent.upper()}={cnt:05d} PONTO={pt['objeto']:5d} ID={pt['id']}")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={'w' if first_write else 'a'}), {cnt} registros.")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO RFC.DAT
# RFC PONTO FILTRADO
def generate_rfc_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    ptoaqfis: Dict[int, str],
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo rfc.dat (RFC – pontos filtrados).
    """
    ent = "rfc"
    auto = Path(paths["automaticos"])
    destino = auto / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = f"""
    select
        distinct
        c.ordem,
        ic.nponto as nponto,
        ic.cod_origem as cod_origem,
        ic.id as id_calculado,
        ip.id as id_parcela,
        ip.nponto as npt_parc,
        ip.cod_tpeq as ctpeq_parc,
        tpntc.tipo as tpc,
        tpntp.tipo as tpp
    from
        id_calculos c
        join id_ponto ic on c.nponto = ic.nponto
        join id_formulas f on f.cod_formula = ic.cod_formula
        join id_ptlog_noh n on n.nponto=ic.nponto and n.cod_nohsup=%s
        join id_ponto ip on c.parcela = ip.nponto
        join id_tipos as tpc on tpc.cod_tpeq=ic.cod_tpeq and tpc.cod_info=ic.cod_info
        join id_tipopnt as tpntc on tpntc.cod_tipopnt=tpc.cod_tipopnt
        join id_tipos as tpp on tpp.cod_tpeq=ip.cod_tpeq and tpp.cod_info=ip.cod_info
        join id_tipopnt as tpntp on tpntp.cod_tipopnt=tpp.cod_tipopnt
        left outer join id_ptfis_conex fis on fis.id_dst=ic.nponto
        left outer join id_conexoes cx on cx.cod_conexao=fis.cod_conexao and cx.cod_noh_dst=%s
    where f.tipo_calc = 'F'
    order by
        ic.nponto, c.ordem
    """

    params = (cod_noh, cod_noh)

    if dry_run:
        logging.info(f"[{ent}] dry-run, não grava em {destino}")
        return

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros para gerar.")
        return

    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = 0

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO RFC       {ts}\n")
        fp.write(f"// NOH={cod_noh}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            if pt["cod_origem"] == 1:
                tppnt = "PDS" if pt["tpc"] == "D" else "PAS"
                tpparc = "PDF" if pt["tpp"] == "D" else "PAF"

                if pt["ctpeq_parc"] == 95:
                    fp.close()
                    raise RuntimeError(f"Erro em RFC {pt['nponto']} {pt['id_calculado']}: Ponto futuro em parcela: {pt['npt_parc']}")

                fp.write("\n")
                fp.write(f"; NPONTO FILTRADO: {pt['nponto']} - PARCELA: {pt['npt_parc']}\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"ORDEM= {pt['ordem']}\n")
                fp.write(f"PARC= {ptoaqfis.get(int(pt['npt_parc']), '')}\n")
                fp.write(f"PNT= {pt['id_calculado']}\n")
                fp.write(f"TPPARC= {tpparc}\n")
                fp.write(f"TPPNT= {tppnt}\n\n")

                cnt += 1
                logging.info(f"{ent.upper()}={cnt:05d} PNT={pt['id_calculado']}")
            else:
                logging.error(f">>> ERRO <<< Ponto [{pt['nponto']}] [{pt['id_calculado']}] não é calculado/filtrado")

        fp.write(f"{top}\n")
        fp.write(f"// FIM RFC – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={'w' if first_write else 'a'}), {cnt} registros.")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO OCR.DAT
# OCR OCORRENCIAS
def generate_ocr_dat(paths: Dict[str, Path], conn, dry_run: bool = False, force: bool = False):
    ent = "ocr"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    sql = """
    select 
        tpnt.nome as nome, 
        tpnt.ocr as ocr, 
        tpnt.unidade as especial, 
        tpnt.pres_0 as texto0, 
        tpnt.pres_1 as texto1,
        tpnt.tpsom as tpsom,
        'ADVER' as sever,
        tpnt.casa_decimal as casa_decimal
    from 
        id_tipopnt as tpnt 
    where   
        tipo='D' 
    order by 
        ocr
    """

    if dry_run:
        logging.info(f"[{ent}] dry-run, não grava em {destino}")
        return

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros para gerar.")
        return

    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = 0

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO OCR       {ts}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            nome = pt["nome"]
            ocr = pt["ocr"]
            especial = pt["especial"] or ""
            texto0 = pt["texto0"] or ""
            texto1 = pt["texto1"] or ""
            tpsom = pt["tpsom"] or ""
            sever = pt["sever"]
            casa_decimal = pt["casa_decimal"]

            tpsons = tpsom.split("/") if tpsom else [""] * 6
            if len(tpsons) < 6:
                tpsons = [tpsom] * 6

            tipoe = "NORML"
            tipoeOn = tipoe
            tipoeOff = tipoe

            if casa_decimal == 1:
                tipoeOn = "NELES"
            if casa_decimal == 0:
                tipoeOff = "NELES"

            if ocr in ("OCR_DIS", "OCR_CHV", "OCR_OPE"):
                tipoe = "NSUPO"
                tipoeOn = tipoe
                tipoeOff = tipoe

            # Bloco 01
            fp.write(f"\n;---------------- {nome}\n")
            fp.write(f"\n{ent.upper()}\n")
            fp.write(f"ID=\t\t{ocr}01\n")
            fp.write(f"SEVER=\t\t{'NORML' if casa_decimal == 0 else sever}\n")
            fp.write(f"TEXTO=\t\tReservado ({texto1})\n")
            fp.write(f"TPSOM=\t\t{tpsons[0]}\n")
            fp.write(f"TIPOE=\t\tNORML\n\n")

            # Bloco 02
            fp.write(f"\n{ent.upper()}\n")
            fp.write(f"ID=\t\t{ocr}02\n")
            fp.write(f"SEVER=\t\t{sever}\n")
            fp.write(f"TEXTO=\t\t{especial}Reservado ({texto0})\n")
            fp.write(f"TPSOM=\t\t{tpsons[1]}\n")
            fp.write(f"TIPOE=\t\tNORML\n\n")

            # Bloco 03
            if ocr == "OCR_DIS":
                sever = "URGEN"
            fp.write(f"\n{ent.upper()}\n")
            fp.write(f"ID=\t\t{ocr}03\n")
            fp.write(f"SEVER=\t\t{sever}\n")
            if ocr == "OCR_CHV":
                fp.write(f"TEXTO=\t\t*LOG*Transitando para {texto0}\n")
            else:
                fp.write(f"TEXTO=\t\t{texto1}/{texto0}\n")
            fp.write(f"TPSOM=\t\t{tpsons[2]}\n")
            fp.write(f"TIPOE=\t\t{tipoeOff}\n\n")

            # Bloco 04
            fp.write(f"\n{ent.upper()}\n")
            fp.write(f"ID=\t\t{ocr}04\n")
            fp.write(f"SEVER=\t\t{'NORML' if casa_decimal == 0 else sever}\n")
            fp.write(f"TEXTO=\t\t{texto1}\n")
            fp.write(f"TPSOM=\t\t{tpsons[3]}\n")
            fp.write(f"TIPOE=\t\t{tipoeOn}\n\n")

            # Bloco 05
            fp.write(f"\n{ent.upper()}\n")
            fp.write(f"ID=\t\t{ocr}05\n")
            fp.write(f"SEVER=\t\t{'NORML' if casa_decimal == 1 else sever}\n")
            fp.write(f"TEXTO=\t\t{especial}{texto0}\n")
            fp.write(f"TPSOM=\t\t{tpsons[4]}\n")
            fp.write(f"TIPOE=\t\t{tipoeOff}\n\n")

            # Bloco 06
            fp.write(f"\n{ent.upper()}\n")
            fp.write(f"ID=\t\t{ocr}06\n")
            fp.write(f"SEVER=\t\t{sever}\n")
            if especial:
                fp.write(f"TEXTO=\t\t{texto1}\n")
            elif ocr == "OCR_CHV":
                fp.write(f"TEXTO=\t\t*LOG*Transitando para {texto1}\n")
            else:
                fp.write(f"TEXTO=\t\t{texto0}/{texto1}\n")
            fp.write(f"TPSOM=\t\t{tpsons[5]}\n")
            fp.write(f"TIPOE=\t\t{tipoeOn}\n\n")

            cnt += 1
            logging.info(f"{ent.upper()}={cnt:05d} ID={ocr}")

        fp.write(f"{top}\n")
        fp.write(f"// FIM OCR – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={'w' if first_write else 'a'}), {cnt} registros.")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO E2M.DAT
# OCR x Macro Alarme
def generate_e2m_dat(paths: Dict[str, Path], conn, dry_run: bool = False, force: bool = False):
    ent = "e2m"
    destino = Path(paths["dats_unir"]) / f"{ent}1.dat"
    first_write = not destino.exists() or force

    sql = """
    select   
        tpnt.ocr as ocr,
        tpnt.prioridade as prioridade
    from    
        id_tipopnt as tpnt
    where
        tpnt.tipo = 'D'
        and prioridade not in (2, 3, 4)
    order by
        tpnt.ocr
    """

    if dry_run:
        logging.info(f"[{ent}] dry-run, não grava em {destino}")
        return

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros para gerar.")
        return

    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = 0

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO E2M       {ts}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            ocr = pt["ocr"]
            prioridade = pt["prioridade"]

            # Blocos PRIORIDADE
            for i in range(1, 7):
                fp.write(f"\n{ent.upper()}\n")
                fp.write(f"IDPTO = {ocr}{i:02d}\n")
                fp.write(f"MAP = PRIOR{prioridade}\n")
                fp.write("TIPO = OCR\n")

            # Blocos OPERAÇÃO
            for i in range(1, 7):
                fp.write(f"\n{ent.upper()}\n")
                fp.write(f"IDPTO = {ocr}{i:02d}\n")
                fp.write("MAP = OPERACAO\n")
                fp.write("TIPO = OCR\n")

            cnt += 1
            logging.info(f"{ent.upper()}={cnt:05d} ID={ocr}")

        fp.write(f"{top}\n")
        fp.write(f"// FIM E2M – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={'w' if first_write else 'a'}), {cnt} registros.")
#---------------------------------------------------------------------------------------------------------
# ARQUIVO E2M.DAT
# Ponto x Macro Alarme
def generate_e2m2_dat(paths: Dict[str, Path], conn, cod_noh: str, com_flag: bool = True, dry_run: bool = False, force: bool = False):
    ent = "e2m"
    destino = Path(paths["dats_unir"]) / f"{ent}2.dat"
    first_write = not destino.exists() or force

    sql = f"""
  select   
        i.id as id,
        i.nponto as objeto,
        tp.prioridade as prioridade,
        tpnt.prioridade as ocr_prioridade,
        pt_ocr.prioridade as ptocr_prioridade,
        tpnt.tipo as tipo,
        i.cod_tpeq as cod_tpeq, 
        i.cod_info as cod_info,
        i.cod_prot as cod_prot,
        i.cod_origem as cod_origem,
        l.alrin as alrin,
        tpnt.ocr as ocr,
        pt_ocr.ocr as pocr,
        m.cod_tpmodulo as cod_tpmodulo,
        e.cod_estacao as cod_estacao,
        p.cod_tipopnt as pt_cod_tipopnt
from    id_ptlog_noh as l,
        id_ponto as i 
        join id_nops n on n.cod_nops=i.cod_nops
        join id_modulos m on m.cod_modulo=n.cod_modulo
        join id_estacao e on e.cod_estacao=m.cod_estacao        
        join id_formulas as form on i.cod_formula=form.cod_formula
        join id_prot p on i.cod_prot=p.cod_prot
        join id_tipopnt as pt_ocr on p.cod_tipopnt=pt_ocr.cod_tipopnt
        join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
        join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
where       
        l.nponto=i.nponto and 
        l.cod_nohsup=%s and
        i.cod_origem not in (7,6) and
        i.cod_tpeq!=95 and
        i.nponto > 0        
order by
        i.nponto 
    """

    params = (cod_noh,)

    if dry_run:
        logging.info(f"[{ent}2] dry-run, não grava em {destino}")
        return

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}2] sem registros para gerar.")
        return

    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = 0

    def is_protec(ocr):
        return ocr in ("OCR_OPE", "OCR_OPE1", "OCR_PAR", "OCR_POP")

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO E2M2      {ts}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            id = pt["id"]
            prioridade = pt["prioridade"]
            ocr_prioridade = str(pt["ocr_prioridade"])
            ptocr_prioridade = str(pt["ptocr_prioridade"])
            tipo = pt["tipo"]
            ocr = pt["ocr"]
            pocr = pt["pocr"]
            pt_cod_tipopnt = pt["pt_cod_tipopnt"]
            objeto = pt["objeto"]

            # Ajuste de prioridade para partida
            if len(id) >= 15 and id[14] == "S" and prioridade < 3:
                prioridade = 3

            # Ajuste para medida elétrica
            if tipo == "A" and len(id) > 9 and id[9] == "M" and prioridade < 1:
                prioridade = 1

            baixou_pri = 0
            if pt_cod_tipopnt != 0:
                if prioridade < int(ptocr_prioridade):
                    baixou_pri = 1
            else:
                if prioridade < int(ocr_prioridade):
                    baixou_pri = 1

            # PRIORIDADE 4
            if (prioridade == 4 and not is_protec(ocr) and not is_protec(pocr)
                and ocr_prioridade not in ("0", "1")):
                fp.write("\n")
                if com_flag:
                    fp.write(f"; NPONTO= {objeto:06d}\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"IDPTO = {id}\n")
                fp.write("MAP = PRIOR4\n")
                fp.write(f"TIPO = P{tipo}S\n")
                cnt += 1

            # PRIORIDADE 3
            if (prioridade == 3 and not is_protec(ocr) and not is_protec(pocr)
                and ocr_prioridade not in ("0", "1")):
                fp.write("\n")
                if com_flag:
                    fp.write(f"; NPONTO= {objeto:06d}\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"IDPTO = {id}\n")
                fp.write("MAP = PRIOR3\n")
                fp.write(f"TIPO = P{tipo}S\n")
                cnt += 1

            # PRIORIDADE 2
            if (prioridade == 2 and not is_protec(ocr) and not is_protec(pocr)
                and ocr_prioridade not in ("0", "1")):
                fp.write("\n")
                if com_flag:
                    fp.write(f"; NPONTO= {objeto:06d}\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"IDPTO = {id}\n")
                fp.write("MAP = PRIOR2\n")
                fp.write(f"TIPO = P{tipo}S\n")
                cnt += 1

            # DIAGNOSTICO (PRIORIDADE 2 ou 3)
            if (prioridade in (2, 3) and not is_protec(ocr) and not is_protec(pocr)
                and ocr_prioridade not in ("0", "1")):
                fp.write("\n")
                if com_flag:
                    fp.write(f"; NPONTO= {objeto:06d}\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"IDPTO = {id}\n")
                fp.write("MAP = DIAGNOSTICO\n")
                fp.write(f"TIPO = P{tipo}S\n")
                cnt += 1

            # ENGENHARIA (PRIORIDADE 4)
            if (prioridade == 4 and not is_protec(ocr) and not is_protec(pocr)
                and ocr_prioridade not in ("0", "1")):
                fp.write("\n")
                if com_flag:
                    fp.write(f"; NPONTO= {objeto:06d}\n")
                fp.write(f"{ent.upper()}\n")
                fp.write(f"IDPTO = {id}\n")
                fp.write("MAP = ENGENHARIA\n")
                fp.write(f"TIPO = P{tipo}S\n")
                cnt += 1

            logging.info(f"{ent.upper()}={cnt:05d} PONTO={objeto:5d} ID={id}")

        fp.write(f"{top}\n")
        fp.write(f"// FIM E2M2 – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}2] gerado em '{destino}' (modo={'w' if first_write else 'a'}), {cnt} registros.")

## FUNÇÃO PARA CONCATENAR ARQUIVOS GRUPO.DAT
def concat_grupo_dats(paths: Dict[str, Path]):
    arquivos = [
        paths["dats_unir"] / "grupo-tr.dat",
        paths["dats_unir"] / "grupo-barras.dat",
        paths["dats_unir"] / "grupo-dj.dat"
    ]
    destino = paths["automaticos"] / "grupo.dat"
    with open(destino, "w", encoding="utf-8") as outfile:
        for arq in arquivos:
            if arq.exists():
                with open(arq, "r", encoding="utf-8") as infile:
                    outfile.write(infile.read())
                    outfile.write("\n")
    logging.info(f"[grupo] Arquivo concatenado salvo em: {destino}")

def concat_grcmp_dats(paths: Dict[str, Path]):
    arquivos = [
        paths["dats_unir"] / "grcmp-tr.dat",
        paths["dats_unir"] / "grcmp-barras.dat",
        paths["dats_unir"] / "grcmp-dj.dat"
    ]
    destino = paths["automaticos"] / "grcmp.dat"
    with open(destino, "w", encoding="utf-8") as outfile:
        for arq in arquivos:
            if arq.exists():
                with open(arq, "r", encoding="utf-8") as infile:
                    outfile.write(infile.read())
                    outfile.write("\n")
    logging.info(f"[grcmp] Arquivo concatenado salvo em: {destino}")

def concat_cgs_dats(paths: Dict[str, Path]):
    arquivos = [
        paths["dats_unir"] / "cgs-gcom.dat",
        paths["dats_unir"] / "cgs-logico.dat",
    ]
    destino = paths["automaticos"] / "cgs.dat"
    with open(destino, "w", encoding="utf-8") as outfile:
        for arq in arquivos:
            if arq.exists():
                with open(arq, "r", encoding="utf-8") as infile:
                    outfile.write(infile.read())
                    outfile.write("\n")
    logging.info(f"[CGS] Arquivo concatenado salvo em: {destino}")

def concat_cgf_dats(paths: Dict[str, Path]):
    arquivos = [
        paths["dats_unir"] / "cgf.fisico.dat",
        paths["dats_unir"] / "cgf.gcom.dat",
        paths["dats_unir"] / "cgf.routing.dat",
    ]
    destino = paths["automaticos"] / "cgf.dat"
    with open(destino, "w", encoding="utf-8") as outfile:
        for arq in arquivos:
            if arq.exists():
                with open(arq, "r", encoding="utf-8") as infile:
                    outfile.write(infile.read())
                    outfile.write("\n")
    logging.info(f"[CGF] Arquivo concatenado salvo em: {destino}")

def concat_pds_dats(paths: Dict[str, Path]):
    arquivos = [
        paths["dats_unir"] / "pds-simb.dat",
        paths["dats_unir"] / "pds-gcom.dat",
    ]
    destino = paths["automaticos"] / "pds.dat"
    with open(destino, "w", encoding="utf-8") as outfile:
        for arq in arquivos:
            if arq.exists():
                with open(arq, "r", encoding="utf-8") as infile:
                    outfile.write(infile.read())
                    outfile.write("\n")
    logging.info(f"[PDS] Arquivo concatenado salvo em: {destino}")

def concat_e2m_dats(paths: Dict[str, Path]):
    arquivos = [
        paths["dats_unir"] / "e2m1.dat",
        paths["dats_unir"] / "e2m22-gcom.dat",
    ]
    destino = paths["automaticos"] / "e2m.dat"
    with open(destino, "w", encoding="utf-8") as outfile:
        for arq in arquivos:
            if arq.exists():
                with open(arq, "r", encoding="utf-8") as infile:
                    outfile.write(infile.read())
                    outfile.write("\n")
    logging.info(f"[E2M] Arquivo concatenado salvo em: {destino}")


def parse_args():
    parser = argparse.ArgumentParser(description="Gerador de arquivos .dat para SAGE")
    parser.add_argument("--dry-run", action="store_true", help="Não grava, apenas simula.")
    parser.add_argument("--force", action="store_true", help="Regrava mesmo se o arquivo existir.")

    # arquivos principais
    parser.add_argument("--grupo_transformadores", action="store_true", help="Gera grupo-tr.dat")
    parser.add_argument("--grupo_barras", action="store_true", help="Gera grupo-barras.dat")
    parser.add_argument("--grupo_disjuntor", action="store_true", help="Gera grupo-dj.dat")
    parser.add_argument("--grcmp-dj", action="store_true", help="Gera grcmp-dj.dat")
    parser.add_argument("--grcmp-barras", action="store_true", help="Gera grcmp-barras.dat")
    parser.add_argument("--grcmp-tr", action="store_true", help="Gera grcmp-tr.dat")
    parser.add_argument("--tctl", action="store_true", help="Gera tctl.dat")
    parser.add_argument("--cnf", action="store_true", help="Gera cnf.dat")
    parser.add_argument("--utr", action="store_true", help="Gera utr.dat")
    parser.add_argument("--cxu", action="store_true", help="Gera cxu.dat")
    parser.add_argument("--map", action="store_true", help="Gera map.dat")
    parser.add_argument("--lsc", action="store_true", help="Gera lsc.dat")
    parser.add_argument("--tcl", action="store_true", help="Gera tcl.dat")
    parser.add_argument("--tac", action="store_true", help="Gera tac.dat")
    parser.add_argument("--tdd", action="store_true", help="Gera tdd.dat")
    parser.add_argument("--nv1", action="store_true", help="Gera nv1.dat")
    parser.add_argument("--nv2", action="store_true", help="Gera nv2.dat")
    parser.add_argument("--tela", action="store_true", help="Gera tela.dat (se EMS habilitado)")
    parser.add_argument("--ins", action="store_true", help="Gera ins.dat")
    parser.add_argument("--usi", action="store_true", help="Gera usi.dat (se EMS habilitado)")
    parser.add_argument("--est", action="store_true", help="Gera est.dat (se EMS habilitado)")
    parser.add_argument("--afp", action="store_true", help="Gera afp.dat (se EMS habilitado)")
    parser.add_argument("--bcp", action="store_true", help="Gera bcp.dat (se EMS habilitado)")
    parser.add_argument("--car", action="store_true", help="Gera car.dat (se EMS habilitado)")
    parser.add_argument("--csi", action="store_true", help="Gera csi.dat (se EMS habilitado)")
    parser.add_argument("--ltr", action="store_true", help="Gera ltr.dat (se EMS habilitado)")
    parser.add_argument("--rea", action="store_true", help="Gera rea.dat (se EMS habilitado)")
    parser.add_argument("--sba", action="store_true", help="Gera sba.dat (se EMS habilitado)")
    parser.add_argument("--tr2", action="store_true", help="Gera tr2.dat (se EMS habilitado)")
    parser.add_argument("--tr3", action="store_true", help="Gera tr3.dat (se EMS habilitado)")
    parser.add_argument("--uge", action="store_true", help="Gera uge.dat (se EMS habilitado)")
    parser.add_argument("--cnc", action="store_true", help="Gera cnc.dat (se EMS habilitado)")
    parser.add_argument("--lig", action="store_true", help="Gera lig.dat (se EMS habilitado)")
    parser.add_argument("--rca", action="store_true", help="Gera rca.dat (se EMS habilitado)")

    # arquivos de controle lógico e físico
    parser.add_argument("--cgs_gcom", action="store_true", help="Gera cgs.gcom.dat")
    parser.add_argument("--cgs", action="store_true", help="Gera cgs.dat")
    parser.add_argument("--cgf_gcom", action="store_true", help="Gera cgf.gcom.dat")
    parser.add_argument("--cgf_dist", action="store_true", help="Gera cgf.dist.dat")
    parser.add_argument("--cgf", action="store_true", help="Gera cgf.dat")

    # arquivos de pontos digitais/analógicos
    parser.add_argument("--pdd", action="store_true", help="Gera pdd.dat")
    parser.add_argument("--pad", action="store_true", help="Gera pad.dat")
    parser.add_argument("--pds_gcom", action="store_true", help="Gera pds.gcom.dat")
    parser.add_argument("--pds", action="store_true", help="Gera pds.dat")
    parser.add_argument("--pas", action="store_true", help="Gera pas.dat")
    parser.add_argument("--pdf", action="store_true", help="Gera pdf.dat")
    parser.add_argument("--paf", action="store_true", help="Gera paf.dat")
    parser.add_argument("--rfc", action="store_true", help="Gera rfc.dat")
    parser.add_argument("--ocr", action="store_true", help="Gera ocr.dat")
    parser.add_argument("--e2m", action="store_true", help="Gera e2m.dat")
    parser.add_argument("--e2m2", action="store_true", help="Gera e2m2.dat")

    return parser.parse_args()


def main():
    args = parse_args()

    log_file = BASE_ROOT / "gerador_dat.log"
    setup_logging(log_file)

    logging.info("Iniciando geração de .dat.")
    paths = build_paths()

    try:
        conn = connect_db()
    except Exception:
        sys.exit(1)

    # Flags globais
    NO_COS    = bool(globals().get("NO_COS", False))
    NO_COR    = bool(globals().get("NO_COR", False))
    NO_CPS    = bool(globals().get("NO_CPS", False))
    EMS       = NO_COS or NO_COR or NO_CPS
    COMENT    = bool(globals().get("COMENT", True))
    MaxIdSize = globals().get("MaxIdSize", 63)

    # Carrega conexões e variáveis
    cx = load_conexoes(conn, CodNoh)
    conexoes_org    = cx["conexoes_org"]
    conexoes_dst    = cx["conexoes_dst"]
    descr_noh       = cx["descr_noh"]
    lia_bidirec     = cx["lia_bidirecional"]
    gestao_com      = EMS
    no_cor          = NO_COR
    com_flag        = COMENT
    max_id_size     = MaxIdSize
    versao_num_base = globals().get("versao_num_base", "1.0")

    info = load_conexoes(conn, CodNoh)
    ordemnv1_sage_gc = info.get("ordemnv1_sage_gc", {})
    ordemnv1_sage_ct = info.get("ordemnv1_sage_ct", {})
    ordemnv1_sage_aq = info.get("ordemnv1_sage_aq", {})
    ordemnv1_sage_dt = info.get("ordemnv1_sage_dt", {})
    ses_grps_440_525 = info.get("ses_grps_440_525", {})
    constants = globals().get("constants", {})

    # Controle de execução
    run_all = not any([
        args.grupo_transformadores, args.grupo_barras, args.grupo_disjuntor, args.grcmp_dj, args.tctl, args.cnf, args.utr, args.cxu, args.map, args.lsc,
        args.tcl, args.tac, args.tdd, args.nv1, args.nv2, args.tela, args.ins, args.usi, args.est, args.afp,
        args.bcp, args.car, args.csi, args.ltr, args.rea, args.sba, args.tr2, args.tr3, args.uge, args.cnc,
        args.lig, args.rca, args.cgs_gcom, args.cgs, args.cgf_gcom, args.cgf_dist, args.cgf, args.pdd, args.pad,
        args.pds_gcom, args.pds, args.pas, args.pdf, args.paf, args.rfc, args.ocr, args.e2m, args.e2m2
    ])

    # ---- GRUPOS E CONTROLE ----
    if run_all or args.grupo_transformadores:
        generate_grupo_transformadores_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.grupo_barras:
        generate_grupo_barras_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.grupo_disjuntor:
        generate_grupo_disjuntor_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.grcmp_dj:
        generate_grcmp_dj_dat(paths, conn, cod_noh=CodNoh, ses_grps_440_525=ses_grps_440_525, dry_run=args.dry_run, force=args.force)
    if run_all or args.tctl:
        generate_grcmp_tr_dat(paths=paths, conn=conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.tctl:
        generate_tctl_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.cnf:
        generate_cnf_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.utr:
        generate_utr_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.cxu:
        generate_cxu_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.map:
        generate_map_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.lsc:
        generate_lsc_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, conexoes_org=conexoes_org, conexoes_dst=conexoes_dst, force=args.force)
    if run_all or args.tcl:
        generate_tcl_dat(paths, conn, cod_noh=CodNoh, lia_bidirec=lia_bidirec, versao_num_base=versao_num_base, dry_run=args.dry_run, force=args.force)
    if run_all or args.tac:
        tac_info = generate_tac_dat(
            paths, conn,
            cod_noh=CodNoh,
            conexoes_dst=conexoes_dst,
            no_cos=NO_COS,
            no_cor=NO_COR,
            no_cps=NO_CPS,
            conex_ons_cos=CONEX_ONS_COS,
            conex_ons_cor=CONEX_ONS_COR,
            conex_cor_cos=CONEX_COR_COS,
            ems=EMS,
            max_pontos_dig_por_tac=MaxPontosDigPorTAC,
            gestao_da_comunicacao=GestaoDaComunicacao,
            dry_run=args.dry_run,
            force=args.force
        )
        if tac_info:
            tac_conex = tac_info["tac_conex"]
            tac_estacao = tac_info["tac_estacao"]
    else:
        tac_conex = {}
        tac_estacao = []
    if run_all or args.tdd:
        generate_tdd_dat(paths, conn, cod_noh=CodNoh, conexoes_org=conexoes_org, max_pontos_ana_por_tdd=MaxPontosAnaPorTDD, max_pontos_dig_por_tdd=MaxPontosDigPorTDD, dry_run=args.dry_run, force=args.force)
    if run_all or args.nv1:
        ordens_nv1 = generate_nv1_dat(
            paths, conn,
            cod_noh=CodNoh,
            conexoes_org=conexoes_org,
            conexoes_dst=conexoes_dst,
            gestao_da_comunicacao=GestaoDaComunicacao,
            dry_run=args.dry_run,
            force=args.force
        )
        if ordens_nv1:
            ordemnv1_sage_gc = ordens_nv1.get("ordemnv1_sage_gc", {})
            ordemnv1_sage_ct = ordens_nv1.get("ordemnv1_sage_ct", {})
            ordemnv1_sage_aq = ordens_nv1.get("ordemnv1_sage_aq", {})
            ordemnv1_sage_dt = ordens_nv1.get("ordemnv1_sage_dt", {})
    
    if run_all or args.nv2:
        generate_nv2_dat(
            paths, conn,
            cod_noh=CodNoh,
            conexoes_org=conexoes_org,
            conexoes_dst=conexoes_dst,
            gestao_da_comunicacao=GestaoDaComunicacao,
            ordemnv1_sage_aq=ordemnv1_sage_aq,
            ordemnv1_sage_ct=ordemnv1_sage_ct,
            ordemnv1_sage_dt=ordemnv1_sage_dt,
            ordemnv1_sage_gc=ordemnv1_sage_gc,
            dry_run=args.dry_run,
            force=args.force
        )
    if run_all or args.enu:
        generate_enu_dat(
            paths, conn,
            cod_noh=CodNoh,
            conexoes_org=conexoes_org,
            conexoes_dst=conexoes_dst,
            dry_run=args.dry_run,
            force=args.force
        )

    # ---- EMS ----
    EMS = bool(EMS)
    if run_all or args.tela:
        generate_tela_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.ins:
        generate_ins_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.usi:
        generate_usi_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.afp:
        generate_afp_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.est:
        generate_est_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.bcp:
        generate_bcp_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.car:
        cargas_eramltr = []
        cargas_eramltr = generate_car_dat(paths, conn, cod_noh=CodNoh, ems=EMS, cargas_eramltr=cargas_eramltr, dry_run=args.dry_run, force=args.force)
    else:
        cargas_eramltr = []
    if run_all or args.csi:
        generate_csi_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.ltr:
        generate_ltr_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.sba:
        generate_sba_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.tr2:
        generate_tr2_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.tr3:
        generate_tr3_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.uge:
        generate_uge_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.cnc:
        generate_cnc_dat(paths, conn, cod_noh=CodNoh, ems=EMS, dry_run=args.dry_run, force=args.force)
    if run_all or args.lig:
        generate_lig_dat(paths, conn, cod_noh=CodNoh, ems=EMS, cargas_eramltr=cargas_eramltr, dry_run=args.dry_run, force=args.force)
    if run_all or args.rca:
        generate_rca_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)

    # ---- CGS/CGF ----
    if run_all or args.cgs_gcom:
        generate_cgs_gcom_dat(
            paths=paths,
            conn=conn,
            conexoes_dst=conexoes_dst,
            gestao_com=GestaoDaComunicacao,
            dry_run=args.dry_run,
            force=args.force
        )
    if run_all or args.cgs:
        generate_cgs_logico_dat(
            paths=paths,
            conn=conn,
            cod_noh=CodNoh,
            conexoes_dst=conexoes_dst,
            tac_conex=tac_conex,
            tac_estacao=tac_estacao,
            no_cor=NO_COR,
            com_flag=COMENT,
            max_id_size=MaxIdSize,
            dry_run=args.dry_run,
            force=args.force
        )
    if run_all or args.cgf_gcom:
        # A função CGF_GCOM precisa da ordem do NV1 de gestão.
        end_gcom = generate_cgf_gcom_dat(
            paths, conn,
            conexoes_dst=conexoes_dst,
            gestao_com=GestaoDaComunicacao,
            ordemnv1_sage_gc=ordemnv1_sage_gc,
            dry_run=args.dry_run,
            force=args.force
        )
    else:
        # Inicializa a variável para evitar UnboundLocalError em chamadas futuras.
        end_gcom = 0

    if run_all or args.cgf_dist:
        generate_cgf_routing_dat(
            paths=paths,
            conn=conn,
            cod_noh=CodNoh,
            conexoes_org=conexoes_org,
            conexoes_dst=conexoes_dst,
            com_flag=COMENT,
            max_id_size=MaxIdSize,
            ordemnv1_sage_ct=ordemnv1_sage_ct, 
            dry_run=args.dry_run,
            force=args.force,
        )
    if run_all or args.cgf:
        generate_cgf_fisico_dat(
            paths, conn,
            cod_noh=CodNoh,
            conexoes_org=conexoes_org,
            conexoes_dst=conexoes_dst,
            ordemnv1_sage_ct=ordemnv1_sage_ct,
            com_flag=COMENT,
            max_id_size=MaxIdSize,
            start_gcom=end_gcom,
            dry_run=args.dry_run,
            force=args.force,
        )

    # ---- PONTOS LÓGICOS ----
    if run_all or args.pdd:
        generate_pdd_dat(
            paths      = paths,
            conn       = conn,
            cod_noh    = CodNoh,
            conexoes_org      = conexoes_org,
            com_flag   = COMENT,
            max_pts_por_tdd = globals().get("MaxPontosDigPorTDD", 2560),
            dry_run    = args.dry_run,
            force      = args.force,
        )
    if run_all or args.pad:
        generate_pad_dat(
            paths         = paths,
            conn          = conn,
            cod_noh       = CodNoh,
            conexoes_org  = conexoes_org,
            coment        = COMENT,
            max_points_ana= globals().get("MaxPontosAnaPorTDD", 1024),
            dry_run       = args.dry_run,
            force         = args.force,
        )
    if run_all or args.pds_gcom:
        generate_pds_gcom_dat(
            paths      = paths,
            conn       = conn,
            conexoes_dst = conexoes_dst,
            dry_run    = args.dry_run,
            force      = args.force,
        )
    if run_all or args.pds:
        generate_pds_simb_dat(
            paths=paths,
            conn=conn,
            cod_noh=CodNoh,
            conexoes_dst=conexoes_dst,
            tac_conex=tac_conex,
            tac_estacao=tac_estacao,
            constants=constants,
            dry_run  = args.dry_run,
            force    = args.force,
        )
    if run_all or args.pas:
        generate_pas_dat(
            paths         = paths,
            conn          = conn,
            cod_noh       = CodNoh,
            conexoes_dst  = conexoes_dst,
            tac_conex     = tac_conex,
            tac_estacao   = tac_estacao,
            no_cor        = NO_COR,
            com_flag      = COMENT,
            max_id_size   = MaxIdSize,
            dry_run       = args.dry_run,
            force         = args.force,
        )

    # ---- PONTOS FÍSICOS ----
    ptoaqfis = {}
    if run_all or args.pdf:
        generate_pdf_dat(
            paths             = paths,
            conn              = conn,
            cod_noh           = CodNoh,
            conexoes_org      = conexoes_org,
            conexoes_dst      = conexoes_dst,
            ordemnv1_sage_aq  = ordemnv1_sage_aq,
            ordemnv1_sage_dt  = ordemnv1_sage_dt,
            com_flag          = COMENT,
            dry_run           = args.dry_run,
            force             = args.force,
        )
    if run_all or args.paf:
        generate_paf_dat(
            paths             = paths,
            conn              = conn,
            cod_noh           = CodNoh,
            conexoes_org      = conexoes_org,
            conexoes_dst      = conexoes_dst,
            ordemnv1_sage_aq  = ordemnv1_sage_aq,
            ordemnv1_sage_dt  = ordemnv1_sage_dt,
            com_flag          = COMENT,
            dry_run           = args.dry_run,
            force             = args.force,
        )
    if run_all or args.rfc:
        generate_rfc_dat(
            paths      = paths,
            conn       = conn,
            cod_noh    = CodNoh,
            ptoaqfis   = ptoaqfis,
            dry_run    = args.dry_run,
            force      = args.force,
        )

    # ---- OUTROS ----
    if run_all or args.ocr:
        generate_ocr_dat(
            paths      = paths,
            conn       = conn,
            dry_run    = args.dry_run,
            force      = args.force,
        )
    if run_all or args.e2m:
        generate_e2m_dat(
            paths      = paths,
            conn       = conn,
            dry_run    = args.dry_run,
            force      = args.force,
        )
    if run_all or args.e2m2:
        generate_e2m2_dat(
            paths      = paths,
            conn       = conn,
            cod_noh    = CodNoh,
            com_flag   = COMENT,
            dry_run    = args.dry_run,
            force      = args.force,
        )

        if run_all or args.grcmp_barras:
            generate_grcmp_barras_dat(
            paths, conn, 
            cod_noh=CodNoh, 
            ses_grps_440_525=SES_GRPS_440_525, 
            dry_run=args.dry_run, 
            force=args.force
        )
    
    #CHAMADA DA CONCATENAÇÃO
    concat_grupo_dats(paths)
    concat_grcmp_dats(paths)
    concat_cgs_dats(paths)
    concat_cgf_dats(paths)
    concat_pds_dats(paths)

    logging.info("Geração concluída.")
    print("Entidade | Numero de Registros")
    print("-------- | -------------------")
    total = 0
    for ent, cont in NumReg.items():
        print(f"  {ent:>5}   |   {cont:6d}")
        total += cont
    print("-------- | -------------------")
    print(f"  Total  |   {total:6d}")

    elapsed = int(time.time() - TimeIni)
    print(f"\nTempo total de geração: {elapsed // 60} min {elapsed % 60} s")
    conn.close()
    logging.info("Conexão encerrada.")

if __name__ == "__main__":
    main()