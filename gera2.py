import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import date, datetime as dt
from typing import Any, Dict, List
import pymysql
import argparse
from dotenv import load_dotenv  # pip install python-dotenv
import time
from typing import Any, Dict, List
import logging
from collections import defaultdict


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

BASE_ROOT = Path(os.getenv("BASE_ROOT", r"D:\bancotr"))


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
def generate_grupo_transformadores_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
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

def generate_grcmp_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "grcmp"
    destino = Path(paths["automaticos"]) / f"{ent}-tr.dat"
    first_write = not destino.exists() or force

    sql = """
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
      tpnt.cmd_1 as cmd_1
    from id_ponto i
    join id_nops n on i.cod_nops=n.cod_nops
    join id_modulos m on m.cod_modulo=n.cod_modulo
    join id_ptlog_noh l on i.nponto=l.nponto
    join id_tpeq t on t.cod_tpeq=i.cod_tpeq
    join id_estacao e on e.cod_estacao=m.cod_estacao
    join id_tipos as tp on tp.cod_tpeq=i.cod_tpeq and tp.cod_info=i.cod_info
    join id_tipopnt as tpnt on tpnt.cod_tipopnt=tp.cod_tipopnt
    where 
      l.cod_nohsup=%s
      and i.evento!='S'
      and tpnt.cod_tipopnt not in (8,32,33,42,43,23)
      and i.cod_prot in (0,3,4)
      and i.cod_origem not in (6,7,11,21,22)
      and (t.tipo_eq not like 'Z%%')
      and (t.tipo_eq not like 'C%%')
      and (t.tipo_eq not like 'T%%')
      and (
        ( 
          m.cod_tpmodulo=3
          and (t.tipo_eq not like 'XC%%')
          and (t.tipo_eq not like 'XS%%')
        ) 
        or 
        (
          m.cod_tpmodulo=2
          and not (i.cod_tpeq=27 and i.cod_info=0 and i.cod_prot=0)
          and not (i.cod_tpeq=28 and i.cod_info=0 and i.cod_prot=0)
          and not (i.cod_tpeq=33)
          and (t.tipo_eq not like 'XC%%')
          and (t.tipo_eq not like 'XS%%')
          and (n.tipo_nops='O')
          and (t.tipo_eq not like 'R%%')
        )
      )
    order by
      e.estacao, m.id, tpnt.tipo, t.tipo_eq
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (cod_noh,))
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
                try:
                    estacao = str(pt.get("estacao", "") or "").strip()
                    modulo_raw = str(pt.get("modulo", "") or "").strip()
                    descr_mod = str(pt.get("descr_mod", "") or "").strip()
                    descr_est = str(pt.get("descr_est", "") or "").strip()
                    ponto_id = str(pt.get("id", "") or "").strip()
                    tipo = str(pt.get("tipo", "") or "").strip()
                    cod_origem = pt.get("cod_origem")
                    unidade = str(pt.get("unidade", "") or "").strip()
                    traducao_id = str(pt.get("traducao_id", "") or "").strip()
                    cmd_0 = str(pt.get("cmd_0", "") or "").strip()
                    cmd_1 = str(pt.get("cmd_1", "") or "").strip()
                    cod_modulo = str(pt.get("cod_modulo", "") or "").strip()

                    # derivações
                    mod = modulo_raw[:4]
                    mod = mod.strip(" -")
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
                        tppnt = ""
                        extra = ""
                        if cod_origem == 7:
                            txt = f"{traducao_id} {cmd_0}/{cmd_1}"
                            txt = txt.replace(estacao, "")
                            txt = txt.replace(mod, "")
                            txt = txt.strip(" -")
                            tppnt = "CGS"
                        elif tipo == "D":
                            txt = traducao_id
                            txt = txt.replace(estacao, "")
                            txt = txt.replace(mod, "")
                            txt = txt.strip(" -")
                            tppnt = "PDS"
                            extra = "TPSIMB=\tESTADO\n"
                        else:
                            cleaned = traducao_id.replace(estacao, "").replace(mod, "")
                            cleaned = cleaned.strip(" -")
                            txt = f"{unidade} {cleaned}"
                            tppnt = "PAS"

                        if cntpntmod == 35:
                            txt = "..."

                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"GRUPO=\t{grupo}\n")
                        fp.write(f"PNT=\t{ponto_id}\n")
                        fp.write(f"TPPNT=\t{tppnt}\n")
                        if tipo == "D":
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
                    continue  # não quebra o loop
            # rodapé comentado
            fp.write("\n")
            fp.write(f"// {'=' * 70}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros lidos: {len(rows)}\n")
            fp.write(f"// {'=' * 70}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {len(rows)} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)

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
    tctl like 'USR%'
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
    all_conexoes = conexoes_org + conexoes_dst

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
           (
             (cpl.placa_princ=c.placa_princ and cpl.linha_princ=c.linha_princ)
             or (cpl.placa_resrv=c.placa_resrv and cpl.linha_resrv=c.linha_resrv and c.placa_resrv!=0)
             or (cpl.placa_princ=c.placa_resrv and cpl.linha_princ=c.linha_resrv and c.placa_resrv!=0)
             or (cpl.placa_resrv=c.placa_princ and cpl.linha_resrv=c.linha_princ and c.placa_resrv!=0)
           )
           and cpl.cod_conexao in (SELECT cod_conexao FROM id_conexoes WHERE cod_noh_dst = %s)
    WHERE
        c.cod_conexao in (SELECT cod_conexao FROM id_conexoes WHERE cod_noh_dst = %s)
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

def generate_lsc_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "lsc"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    info = load_conexoes(conn, cod_noh)
    conexoes_org = info["conexoes_org"]
    conexoes_dst = info["conexoes_dst"]
    all_conexoes = conexoes_org + conexoes_dst

    if not all_conexoes:
        logging.error(f"[{ent}] sem conexões válidas para montar LSC. Abortando.")
        return

    in_placeholders = ",".join(["%s"] * len(all_conexoes))

    sql = f"""
    SELECT 
        c.cod_conexao,
        c.descricao as nome,
        if (c.cod_noh_org=%s, 'D', 'A') as aq_dt,
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
        c.cod_conexao in ({in_placeholders})
        and p.cod_protocolo != 0
    ORDER BY 
        c.cod_conexao
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            params = [cod_noh] + all_conexoes
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de LSC: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query LSC.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em LSC. Saindo.")
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
                try:
                    balanceado = pt.get("balanceado")
                    cod_protocolo = pt.get("cod_protocolo")
                    cod_noh_org = pt.get("cod_noh_org")
                    nome = str(pt.get("nome", "") or "").strip()
                    estacao = str(pt.get("estacao", "") or "").strip()
                    nohs_map_raw = str(pt.get("nohs_map", "") or "")
                    tcv = pt.get("tcv", "")
                    ttp = pt.get("ttp", "")
                    nsrv1 = pt.get("nsrv1") or "localhost"
                    nsrv2 = pt.get("nsrv2") or "localhost"
                    verbd = pt.get("verbd") or "NOV-04"
                    id_sage_aq = str(pt.get("id_sage_aq", "") or "").strip()
                    id_sage_dt = str(pt.get("id_sage_dt", "") or "").strip()

                    # decisão de pular e montagem de tipo/map/id
                    pula = False
                    tipo = ""
                    map_val = ""
                    _id = ""

                    if balanceado == 1:
                        tipo = "AD"
                        _id = pt.get("id_sage_aq") or pt.get("id_sage_dt") or ""
                        map_val = "GERAL"
                        if cod_protocolo == 10 and pt.get("id_sage_dt"):
                            pula = True
                    else:
                        if str(cod_noh) == str(cod_noh_org):
                            tipo = "DD"
                            map_val = "GERAL"
                            _id = f"{id_sage_dt}-DT"
                        else:
                            tipo = "AA"
                            arr_noh_map = [x.strip() for x in nohs_map_raw.split(",") if x.strip()]
                            map_val = estacao if str(cod_noh) in arr_noh_map else "GERAL"
                            _id = f"{id_sage_aq}-AQ"

                    if pula:
                        continue

                    # separa blocos: só uma linha em branco entre registros, não antes do primeiro
                    if cnt > 0:
                        fp.write("\n")

                    # escreve bloco sem indentação extra
                    fp.write(f"{ent.upper()}\n")
                    fp.write(f"GSD =\tGT_SCD_1\n")
                    fp.write(f"ID =\t{_id}\n")
                    if tipo == "DD":
                        fp.write(f"MAP =\n")
                    else:
                        fp.write(f"MAP =\t{map_val}\n")
                    fp.write(f"NOME =\t{nome}\n")
                    fp.write(f"TCV =\t{tcv}\n")
                    fp.write(f"TTP =\t{ttp}\n")
                    fp.write(f"NSRV1 =\t{nsrv1}\n")
                    fp.write(f"NSRV2 =\t{nsrv2}\n")
                    fp.write(f"TIPO =\t{tipo}\n")
                    fp.write(f"VERBD =\t{verbd}\n")

                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} {_id}")
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

def generate_tcl_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "tcl"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    # carrega conexões para pegar lia_bidirecional
    info = load_conexoes(conn, cod_noh)
    lia_bidirecional = info["lia_bidirecional"]  # lista de id_sage_aq de destino com protocolo 10

    sql = """
    SELECT 
        cod_formula as nseq,
        id as id,
        descricao as descr,
        formula as formula,
        nparcelas as nparcelas,
        tipo_calc as tcl,
        case when (id='G_LIA')
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

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (cod_noh, cod_noh))
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de TCL: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query TCL.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em TCL. Saindo.")
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
                try:
                    pt_id = str(pt.get("id", "") or "").strip()
                    descr = str(pt.get("descr", "") or "").strip()
                    formula = str(pt.get("formula", "") or "")
                    tipo_calc = str(pt.get("tcl", "") or "")
                    nseq = pt.get("nseq")
                    idaq_list_raw = str(pt.get("idaq_list", "") or "")
                    iddt_list_raw = str(pt.get("iddt_list", "") or "")

                    # G_ENU
                    if pt_id == "G_ENU":
                        idaqs = [i.strip() for i in idaq_list_raw.split(",") if i.strip()]
                        for idaq in idaqs:
                            if idaq in lia_bidirecional:
                                continue  # pula bidirecional aqui
                            short = idaq[:5]
                            # bloco principal
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"DESCR =\tMonitoracao do canal principal {idaq}\n")
                            fp.write(f"ID =\t{short}-AQ_P\n")
                            fp.write(f"FORMULA = enu[{idaq}-AQ_P].e_falha\n")
                            fp.write(f"NSEQ =\t250\n")
                            # bloco reserva
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"DESCR =\tMonitoracao do canal reserva {idaq}\n")
                            fp.write(f"ID =\t{short}-AQ_R\n")
                            fp.write(f"FORMULA = enu[{idaq}-AQ_R].e_falha\n")
                            fp.write(f"NSEQ =\t250\n")
                            cnt += 2
                            logging.info(f"{ent.upper()} G_ENU adicionou {short}-AQ_P / {short}-AQ_R")
                    elif pt_id == "G_LIA":
                        idaqs = [i.strip() for i in idaq_list_raw.split(",") if i.strip()]
                        for idaq in idaqs:
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"DESCR =\tMonitoracao da ligacao de aquisicao {idaq}\n")
                            short = idaq[:5]
                            fp.write(f"ID =\t{short}-AQ\n")
                            if idaq in lia_bidirecional:
                                fp.write(f"FORMULA = NOT(lia[{idaq}].opera)\n")
                            else:
                                fp.write(f"FORMULA = NOT(lia[{idaq}-AQ].opera)\n")
                            fp.write(f"NSEQ =\t250\n")
                            cnt += 1
                            logging.info(f"{ent.upper()} G_LIA adicionou {short}-AQ")
                    elif pt_id == "G_LID":
                        iddts = [i.strip() for i in iddt_list_raw.split(",") if i.strip()]
                        for iddt in iddts:
                            if iddt in lia_bidirecional:
                                continue
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"DESCR =\tMonitoracao da ligacao de distribuicao {iddt}\n")
                            short = iddt[:5]
                            fp.write(f"ID =\t{short}-DT\n")
                            fp.write(f"FORMULA = NOT(lid[{iddt}-DT].estad)\n")
                            fp.write(f"NSEQ =\t250\n")
                            cnt += 1
                            logging.info(f"{ent.upper()} G_LID adicionou {short}-DT")
                    else:
                        # caso genérico
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"DESCR =\t{descr}\n")
                        fp.write(f"ID =\t{pt_id}\n")
                        if tipo_calc == "I":
                            if len(formula) > 132:
                                logging.error(f"[{ent}] fórmula muito longa em {pt_id}, tamanho {len(formula)}")
                            if pt_id == "I_VERSAO":
                                fp.write(f"FORMULA ={VersaoNumBase}+0*P1\n")
                            else:
                                fp.write(f"FORMULA ={formula}\n")
                            fp.write(f"NSEQ =\t255\n")
                        else:
                            fp.write(f"NSEQ =\t{nseq}\n")
                        cnt += 1
                        logging.info(f"{ent.upper()} padrão adicionou {pt_id}")
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

def generate_tac_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "tac"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    info = load_conexoes(conn, cod_noh)
    conexoes_org = info["conexoes_org"]
    conexoes_dst = info["conexoes_dst"]
    all_conexoes = conexoes_org + conexoes_dst

    if not all_conexoes:
        logging.error(f"[{ent}] sem conexões válidas para montar TAC. Abortando.")
        return

    in_placeholders = ",".join(["%s"] * len(all_conexoes))
    dst_placeholders = ",".join(["%s"] * len(conexoes_dst))

    sql = f"""
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
      f.cod_conexao in ({dst_placeholders}) and
      f.cod_conexao = c.cod_conexao and
      f.id_dst=i.nponto and
      l.nponto=i.nponto and l.cod_nohsup=%s and
      i.cod_tpeq!=95 and
      i.nponto not in (0, 9991, 9992) and
      !(e.cod_estacao!=76 && i.cod_origem=16) and
      !(e.cod_estacao!=67 && i.cod_origem=17)
    order by
    c.cod_conexao,
    e.estacao
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            params = tuple(conexoes_dst + [cod_noh])
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro na query principal de TAC: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query TAC.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em TAC. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # estruturas auxiliares que o PHP usava
    tac_conex: dict = {}
    tac_estacao: list = []
    taccomant = None
    cnt = 0

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

            for pt in rows:
                try:
                    estacao = str(pt.get("estacao", "") or "").strip()
                    cod_estacao = pt.get("cod_estacao")
                    descricao = str(pt.get("descricao", "") or "").strip()
                    id_conex_aq = str(pt.get("id_conex_aq", "") or "").strip()
                    cod_protocolo = pt.get("cod_protocolo")
                    cod_conexao = pt.get("cod_conexao")
                    ems_modela = pt.get("ems_modela")

                    # subquery 1: número de conexões distintas para a mesma estação (numconx)
                    numconx_sql = """
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
                      f.cod_conexao in ({dst_placeholders}) and
                      f.cod_conexao = c.cod_conexao and
                      f.id_dst=i.nponto and
                      l.nponto=i.nponto and l.cod_nohsup=%s and
                      i.cod_tpeq!=95 and
                      e.cod_estacao = %s and
                      i.nponto not in (0, 9991, 9992) and
                      f.cod_conexao not in (%s, %s, 86)
                    order by
                    e.estacao,
                    c.cod_conexao
                    """.replace("{dst_placeholders}", dst_placeholders)

                    cur = conn.cursor()
                    cur.execute(numconx_sql, tuple(conexoes_dst + [cod_noh, cod_estacao, os.getenv("CONEX_ONS_COS", "21"), os.getenv("CONEX_ONS_COR", "55")]))
                    numconx = len(cur.fetchall())
                    cur.close()

                    # subquery 2: número de estações para mesma conexão (numest)
                    numest_sql = """
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
                      f.cod_conexao in ({dst_placeholders}) and
                      f.cod_conexao = c.cod_conexao and
                      f.id_dst=i.nponto and
                      l.nponto=i.nponto and l.cod_nohsup=%s and
                      i.cod_tpeq!=95 and
                      i.nponto not in (0, 9991, 9992)
                    and c.cod_conexao=%s
                    order by
                    e.estacao
                    """.replace("{dst_placeholders}", dst_placeholders)

                    cur = conn.cursor()
                    cur.execute(numest_sql, tuple(conexoes_dst + [cod_noh, cod_conexao]))
                    numest = len(cur.fetchall())
                    cur.close()

                    # subquery 3: pontos digitais
                    numdig_sql = """
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
                    cur = conn.cursor()
                    cur.execute(numdig_sql, (cod_noh, cod_estacao))
                    numPtsDig = len(cur.fetchall())
                    cur.close()

                    # lógica de "bacalhau" para não gerar TACs
                    skip_tac = False
                    NO_COS = globals().get("NO_COS", False)
                    NO_COR = globals().get("NO_COR", False)
                    NO_CPS = globals().get("NO_CPS", False)
                    CONEX_ONS_COS = int(os.getenv("CONEX_ONS_COS", "21"))
                    CONEX_ONS_COR = int(os.getenv("CONEX_ONS_COR", "55"))
                    CONEX_COR_COS = int(os.getenv("CONEX_COR_COS", "125"))

                    if ((NO_COS and cod_conexao == CONEX_ONS_COS) or (((NO_COR or NO_CPS) and cod_conexao == globals().get("CONEX_ONS_COR", CONEX_ONS_COR)))):
                        logging.info(f"[{ent}] Ignorando TAC para {cod_conexao} {estacao} (condição NO_*).")
                        skip_tac = True
                    elif NO_COS and cod_conexao == CONEX_COR_COS and estacao not in ("CORX", "ECEZ", "ECEY"):
                        logging.info(f"[{ent}] Ignorando TAC para {cod_conexao} {estacao} (condição COR_COS).")
                        skip_tac = True

                    if skip_tac:
                        continue

                    # começa a escrever os blocos
                    # mais de uma conexão para a estação
                    if numconx > 1:
                        if cod_conexao not in tac_conex:
                            # evita repetição
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"ID =\t{id_conex_aq}\n")
                            fp.write(f"NOME =\t{id_conex_aq} - {descricao}\n")
                            if cod_protocolo == 10:
                                fp.write(f"LSC =\t{id_conex_aq}\n")
                            else:
                                fp.write(f"LSC =\t{id_conex_aq}-AQ\n")
                            fp.write(f"TPAQS =\tASAC\n")
                            EMS = globals().get("EMS", 0)
                            if EMS and ems_modela == "S" and numest == 1:
                                fp.write(f"INS =\t{estacao}\n")
                            else:
                                fp.write("INS =\n")
                            # marca
                            tac_conex[cod_conexao] = id_conex_aq
                            # incrementa numreg
                            # não temos NumReg array aqui, apenas log
                        # não incrementa novamente tac_conex entry
                    else:
                        # uma conexão por estação ou múltiplas estações por conexão
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"ID =\t{estacao}\n")
                        tac_estacao.append(estacao)
                        fp.write(f"NOME =\t{estacao} - {descricao}\n")
                        if cod_protocolo == 10:
                            fp.write(f"LSC =\t{id_conex_aq}\n")
                        else:
                            fp.write(f"LSC =\t{id_conex_aq}-AQ\n")
                        fp.write(f"TPAQS =\tASAC\n")
                        EMS = globals().get("EMS", 0)
                        if EMS and ems_modela == "S":
                            fp.write(f"INS =\t{estacao}\n")
                        else:
                            fp.write("INS =\n")

                        # se excede o limite, cria tacs adicionais
                        MaxPontosDigPorTAC = globals().get("MaxPontosDigPorTAC", 2550)
                        extra = int(numPtsDig / MaxPontosDigPorTAC)
                        for i in range(1, extra):
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"ID =\t{estacao}_{i}\n")
                            tac_estacao.append(estacao)
                            fp.write(f"NOME =\t{estacao} - {descricao}\n")
                            if cod_protocolo == 10:
                                fp.write(f"LSC =\t{id_conex_aq}\n")
                            else:
                                fp.write(f"LSC =\t{id_conex_aq}-AQ\n")
                            fp.write(f"TPAQS =\tASAC\n")
                            if EMS and ems_modela == "S":
                                fp.write(f"INS =\t{estacao}\n")
                            else:
                                fp.write("INS =\n")

                    # TAC para gestão da comunicação
                    if taccomant != id_conex_aq:
                        taccomant = id_conex_aq
                        GestaoDaComunicacao = globals().get("GestaoDaComunicacao", 0)
                        if GestaoDaComunicacao:
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"ID =\t{id_conex_aq}-COM\n")
                            fp.write(f"LSC =\t{id_conex_aq}-AQ\n")
                            fp.write(f"NOME =\tContrl.Comunic.- {descricao}\n")
                            fp.write(f"TPAQS =\tASAC\n")

                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} ID={estacao}")
                except Exception:
                    logging.exception(f"[{ent}] erro processando linha: {pt}")
                    continue

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
        return {
            "tac_conex": tac_conex,
            "tac_estacao": tac_estacao
        }
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)

def generate_tdd_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "tdd"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    info = load_conexoes(conn, cod_noh)
    conexoes_org = info["conexoes_org"] or [-1]

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

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            params = tuple(conexoes_org + [cod_noh, cod_noh])
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de TDD: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query TDD.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em TDD. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    MaxPontosAnaPorTDD = globals().get("MaxPontosAnaPorTDD", 1024)
    MaxPontosDigPorTDD = globals().get("MaxPontosDigPorTDD", 2560)

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
                try:
                    id_conex = str(pt.get("id_conex", "") or "").strip()
                    nome = str(pt.get("nome", "") or "").strip()
                    tipo_pts = str(pt.get("tipo", "") or "").strip()
                    cod_protocolo = pt.get("cod_protocolo")
                    cnt_raw = pt.get("cnt", 0) or 0

                    if tipo_pts == "A":
                        fimtdd = int(1 + cnt_raw / MaxPontosAnaPorTDD)
                    else:
                        fimtdd = int(1 + cnt_raw / MaxPontosDigPorTDD)

                    for i in range(1, fimtdd + 1):
                        # separa blocos: só uma linha em branco entre registros
                        if cnt > 0:
                            fp.write("\n")

                        fp.write(f"{ent.upper()}\n")
                        fp.write(f"ID =\t{id_conex}{tipo_pts}{i}\n")
                        fp.write(f"NOME =\t{nome}-{tipo_pts}-Parte {i}\n")
                        if cod_protocolo == 10:
                            fp.write(f"LSC =\t{id_conex}\n")
                        else:
                            fp.write(f"LSC =\t{id_conex}-DT\n")
                        cnt += 1
                        logging.info(f"{ent.upper()}={cnt:05d} ID={id_conex}{tipo_pts}{i}")
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

def generate_nv1_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "nv1"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    info = load_conexoes(conn, cod_noh)
    conexoes_org = info["conexoes_org"]
    conexoes_dst = info["conexoes_dst"]
    all_conexoes = list(dict.fromkeys(conexoes_dst + conexoes_org))  # dedup

    if not all_conexoes:
        logging.error(f"[{ent}] sem conexões válidas. Abortando.")
        return

    in_placeholders = ",".join(["%s"] * len(all_conexoes))

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
        (c.cod_conexao in ({in_placeholders}) or c.cod_conexao in ({in_placeholders}))
    union
       (select 999999 as cod_conexao, 0 as cod_noh_org, 0 as cod_noh_dst, '' as sufixo_sage, '' as id_conex_aq,
               '' as id_conex_dt, 0 as cod_protocolo, '' as descricao, 0 as grupo_protoc)
    order by
        cod_conexao
    """

    # para simplificar parâmetros duplicados (dst + org)
    params = tuple(all_conexoes + all_conexoes)

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de NV1: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query NV1.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em NV1. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # variáveis de estado equivalentes ao PHP
    cod_conexao_ant = None
    ordem = 0
    id_conex_aq_ant = ""
    sufixo_sage_ant = ""
    cod_noh_dst_ant = None
    cod_protocolo_ant = None
    ordemnv1_sage_gc = {}
    ordemnv1_sage_aq = {}
    ordemnv1_sage_ct = {}
    ordemnv1_sage_dt = {}
    taccomant = None  # não usado diretamente aqui, mas mantém padrão

    # flags globais / bacalhos
    GestaoDaComunicacao = globals().get("GestaoDaComunicacao", 0)

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
                try:
                    cod_conexao = pt.get("cod_conexao")
                    cod_noh_dst = pt.get("cod_noh_dst")
                    cod_noh_org = pt.get("cod_noh_org")
                    sufixo_sage = str(pt.get("sufixo_sage") or "").strip()
                    id_conex_aq = str(pt.get("id_conex_aq") or "").strip()
                    id_conex_dt = str(pt.get("id_conex_dt") or "").strip()
                    cod_protocolo = pt.get("cod_protocolo")
                    descricao = str(pt.get("descricao") or "").strip()
                    grupo_protoc = pt.get("grupo_protoc")

                    # controla ordem por mudança de conexão
                    if cod_conexao_ant != cod_conexao:
                        # bloco de gestão da comunicação da conexão anterior (se aplicável)
                        if GestaoDaComunicacao and cod_conexao_ant is not None and cod_noh_dst_ant == cod_noh and cod_protocolo_ant != 10:
                            ordem += 1
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"CNF =\t{ id_conex_aq_ant }-AQ\n" if cod_protocolo_ant != 10 else f"CNF =\t{ id_conex_aq_ant }\n")
                            nv1_gc = f"{id_conex_aq_ant}_G{sufixo_sage_ant}_{ordem}"
                            fp.write(f"CONFIG =\t(Gestão da Comunic. {id_conex_aq_ant}-AQ)\n")
                            fp.write(f"TN1 =\tG{sufixo_sage_ant}\n")
                            fp.write(f"ORDEM =\t{ordem}\n")
                            fp.write(f"ID =\t{nv1_gc}\n")
                            ordemnv1_sage_gc[cod_conexao_ant] = ordem

                        ordem = 1
                        cod_conexao_ant = cod_conexao
                    else:
                        ordem += 1

                    # guarda antecedentes
                    id_conex_aq_ant = id_conex_aq
                    sufixo_sage_ant = sufixo_sage
                    cod_noh_dst_ant = cod_noh_dst
                    cod_protocolo_ant = cod_protocolo

                    # pula caso seja o placeholder “999999” (geralmente só gera algum bloco vazio? o PHP aceita <999999)
                    if cod_conexao >= 999999:
                        continue

                    # começa geração de blocos
                    if cod_noh_dst == int(cod_noh):  # AQUISIÇÃO
                        fp.write("\n")
                        fp.write(f"{ent.upper()}\n")
                        if cod_protocolo == 10:
                            fp.write(f"CNF =\t{id_conex_aq}\n")
                        else:
                            fp.write(f"CNF =\t{id_conex_aq}-AQ\n")

                        if cod_protocolo == 10:
                            nv1 = f"{id_conex_aq}_NV1"
                        else:
                            nv1 = f"{id_conex_aq}_A{sufixo_sage}_{ordem}"

                        fp.write(f"CONFIG =\t(Aquisição de Dados {id_conex_aq}-AQ)\n")
                        if cod_protocolo == 10:
                            fp.write(f"TN1 =\tNLN1\n")
                        else:
                            fp.write(f"TN1 =\tA{sufixo_sage}\n")
                        ordemnv1_sage_aq[cod_conexao] = ordem
                        fp.write(f"ORDEM =\t{ordem}\n")
                        fp.write(f"ID =\t{nv1}\n")

                        # testa se tem comando nessa conexão (verifica existência de ponto com cod_origem=7)
                        cmd_sql = """
                        select 1 from id_ptfis_conex f
                          join id_ponto i on f.id_dst=i.nponto
                        where i.cod_origem=7 and f.cod_conexao=%s
                        limit 1
                        """
                        with conn.cursor() as cur_cmd:
                            cur_cmd.execute(cmd_sql, (cod_conexao,))
                            has_cmd = cur_cmd.fetchone() is not None

                        if has_cmd and cod_protocolo != 10:
                            ordem += 1
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            if cod_protocolo == 10:
                                fp.write(f"CNF =\t{id_conex_aq}\n")
                            else:
                                fp.write(f"CNF =\t{id_conex_aq}-AQ\n")
                            nv1_ct = f"{id_conex_aq}_C{sufixo_sage}_{ordem}"
                            fp.write(f"CONFIG =\t(Controle Supervisório {id_conex_aq}-AQ)\n")
                            fp.write(f"TN1 =\tC{sufixo_sage}\n")
                            ordemnv1_sage_ct[cod_conexao] = ordem
                            fp.write(f"ORDEM =\t{ordem}\n")
                            fp.write(f"ID =\t{nv1_ct}\n")

                    else:  # DISTRIBUIÇÃO
                        if cod_protocolo != 10:
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"CNF =\t{id_conex_dt}-DT\n")
                            if grupo_protoc == 8:
                                fp.write(f"CONFIG= Classe= 1 \t(Distrib.Dados {id_conex_dt}-DT)\n")
                            else:
                                fp.write(f"CONFIG =\t(Distribuição de Dados {id_conex_dt}-DT)\n")
                            fp.write(f"TN1 =\tD{sufixo_sage}\n")
                            nv1_dt = f"{id_conex_dt}_D{sufixo_sage}_{ordem}"
                            ordemnv1_sage_dt[cod_conexao] = ordem
                            fp.write(f"ORDEM =\t{ordem}\n")
                            fp.write(f"ID =\t{nv1_dt}\n")
                        else:
                            # ICCP distribuição: só atualiza ordem de distribuição
                            ordemnv1_sage_dt[cod_conexao] = ordem

                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} ID={descricao}")
                except Exception:
                    logging.exception(f"[{ent}] erro processando linha NV1: {pt}")
                    continue

            # rodapé
            fp.write("\n")
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)

def generate_nv2_dat(paths: Dict[str, Path], conn, cod_noh: str, dry_run: bool = False, force: bool = False):
    ent = "nv2"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    info = load_conexoes(conn, cod_noh)
    conexoes_dst = info["conexoes_dst"] or [-1]
    conexoes_org = info["conexoes_org"] or [-1]

    dst_placeholders = ",".join(["%s"] * len(conexoes_dst))
    org_placeholders = ",".join(["%s"] * len(conexoes_org))
    ordemnv1_sage_gc: dict = {}
    ordemnv1_sage_aq: dict = {}
    ordemnv1_sage_ct: dict = {}
    ordemnv1_sage_dt: dict = {}

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
      (f.cod_conexao in ({dst_placeholders}))
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
      ( f.cod_conexao in ({org_placeholders}) )
      and
      f.cod_conexao = c.cod_conexao and
      f.id_org=i.nponto and
      l.nponto=i.nponto and l.cod_nohsup=%s and
      i.cod_tpeq!=95

    union (select '','',0,0,'' as sufixo_sage,'','',999999 as cod_protocolo,999999 as cod_conexao,'','')

    order by
    cod_protocolo,
    id_conex,
    aq_dt,
    cod_conexao,
    tipo
    """

    # parâmetros: para cada bloco é preciso cod_noh e cod_noh de novo
    params = tuple(conexoes_dst + [cod_noh, cod_noh] + conexoes_org + [cod_noh, cod_noh])

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        logging.error(f"[{ent}] Erro ao executar SQL de NV2: {e}", exc_info=True)
        return

    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query NV2.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar em NV2. Saindo.")
        return

    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # estado equivalente ao PHP
    nv1ant = "@#^%"
    cod_conexant = None
    ordem = 0
    cod_noh_dst_ant = None
    cod_protocolo_ant = None
    sufixo_sage_ant = ""
    id_conex_aq_ant = ""

    # flag global
    GestaoDaComunicacao = globals().get("GestaoDaComunicacao", 0)

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
                try:
                    tipo = str(pt.get("tipo", "") or "").strip()
                    tipoad = str(pt.get("tipoad", "") or "").strip()
                    cod_noh_dst = pt.get("cod_noh_dst")
                    cod_noh_org = pt.get("cod_noh_org")
                    sufixo_sage = str(pt.get("sufixo_sage") or "").strip()
                    id_conex_aq = str(pt.get("id_conex_aq") or "").strip()
                    id_conex_dt = str(pt.get("id_conex_dt") or "").strip()
                    cod_protocolo = pt.get("cod_protocolo")
                    cod_conexao = pt.get("cod_conexao")
                    id_conex = str(pt.get("id_conex") or "").strip()
                    aq_dt = str(pt.get("aq_dt") or "").strip()

                    # mudança de conexão
                    if cod_conexant != cod_conexao:
                        # blocos de gestão da comunicação se aplicável
                        if GestaoDaComunicacao and cnt > 0 and cod_noh_dst_ant == int(cod_noh) and cod_protocolo_ant != 10:
                            fp.write("\n")
                            fp.write(f"{ent.upper()}\n")
                            fp.write(f"CONFIG =\t(Gestão da Comunic. {id_conex_aq_ant}-AQ)\n")
                            nv1 = f"{id_conex_aq_ant}_G{sufixo_sage_ant}_{ordemnv1_sage_gc.get(cod_conexant, 0)}"
                            fp.write(f"ID =\t{nv1}_CGCD\n")
                            fp.write(f"NV1 =\t{nv1}\n")
                            fp.write(f"ORDEM =\t1\n")
                            fp.write(f"TN2 =\tCGCD\n")
                            fp.write(f"TPPNT =\tCGF\n")
                        cod_conexant = cod_conexao

                    # guarda antecedentes
                    cod_noh_dst_ant = cod_noh_dst
                    cod_protocolo_ant = cod_protocolo
                    sufixo_sage_ant = sufixo_sage
                    id_conex_aq_ant = id_conex_aq

                    if cod_conexao >= 999999:
                        continue  # pula placeholder final

                    # início do bloco principal
                    fp.write("\n")
                    fp.write(f"{ent.upper()}\n")

                    nv1 = ""
                    if int(cod_noh_dst or 0) == int(cod_noh):  # AQUISIÇÃO
                        fp.write(f"CONFIG =\t{tipo} {id_conex_aq}-AQ\n")
                        if tipo and tipo[0] not in ("C", "S"):
                            nv1 = f"{id_conex_aq}_A{sufixo_sage}_{ordemnv1_sage_aq.get(cod_conexao, 0)}"
                        else:
                            nv1 = f"{id_conex_aq}_C{sufixo_sage}_{ordemnv1_sage_ct.get(cod_conexao, 0)}"
                        if cod_protocolo == 10:
                            nv1 = f"{id_conex_aq}_NV1"
                        id_conex_atual = id_conex_aq
                    else:  # DISTRIBUIÇÃO
                        fp.write(f"CONFIG =\t{tipo} {id_conex_dt}-DT\n")
                        nv1 = f"{id_conex_dt}_D{sufixo_sage}_{ordemnv1_sage_dt.get(cod_conexao, 0)}"
                        if cod_protocolo == 10:
                            nv1 = f"{id_conex_dt}_NV1"
                        id_conex_atual = id_conex_dt

                    # controle de ordem para o mesmo nv1
                    if nv1ant != nv1:
                        ordem = 1
                        nv1ant = nv1
                    else:
                        ordem += 1

                    if cod_protocolo == 10:
                        fp.write(f"ID =\t{ id_conex }_{tipo}_NV2\n")
                    else:
                        fp.write(f"ID =\t{ nv1 }_{tipo}\n")

                    fp.write(f"NV1 =\t{nv1}\n")
                    fp.write(f"ORDEM =\t{ordem}\n")
                    fp.write(f"TN2 =\t{tipo}\n")

                    # TPPNT conforme tipoad
                    if tipoad in ("C", "S"):
                        fp.write(f"TPPNT =\tCGF\n")
                    elif tipoad == "A":
                        fp.write(f"TPPNT =\tPAF\n")
                    else:
                        fp.write(f"TPPNT =\tPDF\n")

                    cnt += 1
                    logging.info(f"{ent.upper()}={cnt:05d} ID={nv1}_{tipo}")
                except Exception:
                    logging.exception(f"[{ent}] erro processando linha NV2: {pt}")
                    continue

            # rodapé
            fp.write("\n")
            top = "// " + "=" * 70
            fp.write(f"{top}\n")
            fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
            fp.write(f"{top}\n")

        logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")
    except Exception as e:
        logging.error(f"[{ent}] Erro escrevendo '{destino}': {e}", exc_info=True)

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

def generate_cgs_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_dst: List[int],
    gestao_com: bool,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo cgs.gcom.dat (CGS – Gestão da Comunicação)
    se gestao_com estiver habilitado.
    """
    ent = "cgs"
    destino = Path(paths["automaticos"]) / f"{ent}.gcom.dat"

    if not gestao_com:
        logging.info(f"[{ent}] Gestão da Comunicação desabilitada, pulando geração de {ent.upper()}.")
        return

    first_write = not destino.exists() or force

    # Monta a cláusula IN com as conexoes_dst
    lista = ",".join(str(x) for x in conexoes_dst)
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
        c.descricao      AS nome,
        c.id_sage_aq,
        c.id_sage_dt,
        p.nome           AS pnome,
        c.cod_noh_org,
        c.cod_noh_dst
    FROM id_conexoes c
      JOIN id_protocolos p ON p.cod_protocolo = c.cod_protocolo
    WHERE c.cod_conexao IN ({lista})
      AND p.cod_protocolo NOT IN (0, 10)
    ORDER BY
      p.cod_protocolo,
      c.nsrv1,
      c.nsrv2,
      c.placa_princ,
      c.linha_princ,
      c.placa_resrv,
      c.linha_resrv
    """

    logging.info(f"[{ent}] === Iniciando generate_{ent}_dat (destino: {destino}) ===")
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    logging.info(f"[{ent}] {len(rows)} linhas retornadas pela query.")
    if not rows:
        logging.warning(f"[{ent}] Nenhum registro para processar. Saindo.")
        return
    if dry_run:
        logging.info(f"[{ent}] Dry-run ativo. {len(rows)} registros seriam processados em '{destino}'.")
        return

    # definição dos blocos a gerar (sufixo, texto no NOME, código TP)
    blocos = [
        ("_DESAB_ENUP", "Desabilitacao do Enlace Principal", "PDCAN"),
        ("_HABIL_ENUP", "Habilitacao do Enlace Principal",   "PHCAN"),
        ("_DESAB_ENUR", "Desabilitacao do Enlace Reserva",   "PDCAN"),
        ("_HABIL_ENUR", "Habilitacao do Enlace Reserva",     "PHCAN"),
        ("_DESAB_FSECN","Desabil da Func Secund nos Enlaces","PDSEC"),
        ("_HABIL_FSECN","Habilit da Func Secund nos Enlaces","PHSEC"),
        ("_HABIL_UTRP", "Habilitacao da UTR Principal",      "PHUTR"),
        ("_DESAB_UTRP","Desabilitacao da UTR Principal",     "PDUTR"),
        ("_HABIL_UTRR","Habilitacao da UTR Reserva",         "PHUTR"),
        ("_DESAB_UTRR","Desabilitacao da UTR Reserva",       "PDUTR"),
        ("_PFAIL_ENUP","Failover do Enlace Principal",       "PFCAN"),
        ("_PFAIL_ENUR","Failover do Enlace Reserva",         "PFCAN"),
    ]

    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        # cabeçalho
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO DA GERAÇÃO AUTOMÁTICA DA ENTIDADE {ent.upper():<10}  {timestamp}\n")
        fp.write(f"// Código NOH: {cod_noh} | Versão: {VersaoBase}\n")
        fp.write(f"{top}\n\n")

        cnt = 0
        for pt in rows:
            nome  = str(pt["nome"]).strip()
            pnome = str(pt["pnome"]).strip()
            id_aq = str(pt["id_sage_aq"]).strip()

            # comentário de grupo
            fp.write("\n\n")
            fp.write(f"; >>>>>> {nome} - {pnome} <<<<<<\n\n")

            # imprime cada bloco
            for suffix, desc, tp in blocos:
                fp.write("CGS\n")
                fp.write(f"ID =\t{id_aq}{suffix}\n")
                fp.write("LMI1C =\t0\n")
                fp.write("LMI2C =\t0\n")
                fp.write("LMS1C =\t0\n")
                fp.write("LMS2C =\t0\n")
                fp.write(f"NOME =\t{desc} {id_aq}\n")
                fp.write("PAC =\tCOM_SAGE\n")
                fp.write("PINT =\t\n")
                fp.write(f"TAC =\t{id_aq}-COM\n")
                fp.write("TIPO =\tPDS\n")
                fp.write(f"TIPOE =\t{tp}\n")
                fp.write("TPCTL =\tCSCD\n\n")
                cnt += 1

        # rodapé
        fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// TÉRMINO DA GERAÇÃO AUTOMÁTICA DE {ent.upper()} - total de registros escritos: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros processados.")

def generate_cgs_gcom_dat(
    paths: Dict[str, Path],
    conn,
    conexoes_dst: List[int],
    gestao_com: bool,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera cgs.gcom.dat (CGS – Gestão da Comunicação).
    Só roda se gestao_com=True.
    """
    if not gestao_com:
        logging.info("[cgs_gcom] Gestão da comunicação desabilitada. Pulando.")
        return

    ent, ext = "cgs", "gcom.dat"
    destino = Path(paths["automaticos"]) / f"{ent}.{ext}"
    first_write = not destino.exists() or force

    # placeholders dinâmicos para o IN
    ph = ",".join("%s" for _ in conexoes_dst)
    sql = f"""
    SELECT
      c.cod_protocolo, c.nsrv1, c.nsrv2,
      c.placa_princ, c.linha_princ,
      c.placa_resrv, c.linha_resrv,
      c.cod_conexao, c.end_org, c.end_dst,
      c.descricao AS nome,
      c.id_sage_aq, c.id_sage_dt,
      p.nome AS pnome
    FROM id_conexoes c
      JOIN id_protocolos p ON p.cod_protocolo = c.cod_protocolo
    WHERE c.cod_conexao IN ({ph})
      AND p.cod_protocolo NOT IN (0,10)
    ORDER BY
      p.cod_protocolo,
      c.nsrv1, c.nsrv2,
      c.placa_princ, c.linha_princ,
      c.placa_resrv, c.linha_resrv
    """
    logging.info(f"[{ent}_gcom] Iniciando (destino: {destino})")
    with conn.cursor() as cur:
        cur.execute(sql, tuple(conexoes_dst))
        rows = cur.fetchall()
    logging.info(f"[{ent}_gcom] {len(rows)} linhas retornadas.")
    if not rows:
        logging.warning(f"[{ent}_gcom] nada para gerar.")
        return
    if dry_run:
        logging.info(f"[{ent}_gcom] dry-run: {len(rows)} registros.")
        return

    # definição dos 12 blocos: (sufixo, descrição, tipo de evento)
    blocos = [
      ("_DESAB_ENUP", "Desabilitacao do Enlace Principal", "PDCAN"),
      ("_HABIL_ENUP", "Habilitacao do Enlace Principal", "PHCAN"),
      ("_DESAB_ENUR", "Desabilitacao do Enlace Reserva",  "PDCAN"),
      ("_HABIL_ENUR", "Habilitacao do Enlace Reserva",   "PHCAN"),
      ("_DESAB_FSECN","Desabil da Func Secund nos Enlaces","PDSEC"),
      ("_HABIL_FSECN","Habilit da Func Secund nos Enlaces","PHSEC"),
      ("_HABIL_UTRP","Habilitacao da UTR Principal",     "PHUTR"),
      ("_DESAB_UTRP","Desabilitacao da UTR Principal",   "PDUTR"),
      ("_HABIL_UTRR","Habilitacao da UTR Reserva",       "PHUTR"),
      ("_DESAB_UTRR","Desabilitacao da UTR Reserva",     "PDUTR"),
      ("_PFAIL_ENUP","Failover do Enlace Principal",     "PFCAN"),
      ("_PFAIL_ENUR","Failover do Enlace Reserva",       "PFCAN"),
    ]

    top = "// " + "=" * 70
    ts  = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    mode = "w" if first_write else "a"

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO CGS GCOM  {ts}\n")
        fp.write(f"{top}\n\n")

        cnt = 0
        for pt in rows:
            id_aq  = pt["id_sage_aq"].strip()
            nome   = pt["nome"].strip()
            pnome  = pt["pnome"].strip()

            fp.write(f"\n\n; >>>>>> {nome} - {pnome} <<<<<<\n\n")
            for suf, descr, tp in blocos:
                fp.write("CGS\n")
                fp.write(f"ID =\t{id_aq}{suf}\n")
                fp.write("LMI1C =\t0\nLMI2C =\t0\nLMS1C =\t0\nLMS2C =\t0\n")
                fp.write(f"NOME =\t{descr} {id_aq}\n")
                fp.write("PAC =\tCOM_SAGE\nPINT =\t\n")
                fp.write(f"TAC =\t{id_aq}-COM\n")
                fp.write("TIPO =\tPDS\n")
                fp.write(f"TIPOE =\t{tp}\nTPCTL =\tCSCD\n\n")
                cnt += 1

        fp.write(f"\n{top}\n")
        fp.write(f"// FIM CGS GCOM – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}_gcom] gerado em '{destino}' (modo={mode}), {cnt} registros.")


def generate_cgs_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    no_cor: bool,
    tac_conex: Dict[int,str],
    tac_estacao: List[str],
    com_flag: bool,
    max_id_size: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera cgs.dat (CGS – pontos de controle lógico de aquisição).
    """
    ent = "cgs"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    # placeholders para IN
    ph = ",".join("%s" for _ in conexoes_dst)
    sql = f"""
    SELECT 
        m.descricao   AS entidade,
        i.id          AS id,
        e.estacao     AS estacao,
        i.traducao_id AS traducao_id,
        isup.id       AS supervisao,
        isup.nponto   AS sup_nponto,
        tpnt.tctl     AS tipo2,
        cx.cod_conexao,
        ''            AS inter,
        tpnt.tipo     AS tipo3,
        tpnt.cmd_1, tpnt.cmd_0,
        i.nponto      AS objeto,
        i.cod_tpeq, i.cod_info, i.cod_origem,
        f.cod_asdu,
        CASE WHEN l.lia < -99999 THEN 0 ELSE l.lia END AS lmi1c,
        CASE WHEN l.liu < -99999 THEN 0 ELSE l.liu END AS lmi2c,
        CASE WHEN l.lsa >  99999 THEN 0 ELSE l.lsa END AS lms1c,
        CASE WHEN l.lsu >  99999 THEN 0 ELSE l.lsu END AS lms2c,
        a.tipo        AS tipo_asdu
    FROM id_ptlog_noh l
      LEFT JOIN id_ptfis_conex f
        ON f.id_dst = l.nponto
       AND f.cod_conexao IN ({ph})
      LEFT JOIN id_protoc_asdu a ON a.cod_asdu = f.cod_asdu
      LEFT JOIN id_conexoes cx  ON cx.cod_conexao = f.cod_conexao
      JOIN id_ponto i             ON l.nponto = i.nponto
      JOIN id_ponto isup          ON i.nponto_sup = isup.nponto
      JOIN id_nops n              ON n.cod_nops = i.cod_nops
      JOIN id_modulos m           ON m.cod_modulo = n.cod_modulo
      JOIN id_estacao e           ON e.cod_estacao = m.cod_estacao
      JOIN id_tipos tp            ON tp.cod_tpeq = i.cod_tpeq
                                  AND tp.cod_info = i.cod_info
      JOIN id_tipopnt tpnt        ON tpnt.cod_tipopnt = tp.cod_tipopnt
    WHERE l.cod_nohsup = %s
      AND (i.cod_origem IN (7,15))
      AND i.cod_tpeq != 95
    ORDER BY i.nponto, cx.cod_conexao DESC
    """

    logging.info(f"[{ent}] executando SQL (pontos lógicos)…")
    with conn.cursor() as cur:
        cur.execute(sql, tuple(conexoes_dst) + (cod_noh,))
        rows = cur.fetchall()

    logging.info(f"[{ent}] {len(rows)} linhas retornadas.")
    if not rows:
        logging.warning(f"[{ent}] nada para gerar.")
        return
    if dry_run:
        logging.info(f"[{ent}] dry-run: {len(rows)} registros.")
        return

    mode = "w" if first_write else "a"
    top  = "// " + "=" * 70

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO CGS       {ts}\n")
        fp.write(f"// Código NOH: {cod_noh}\n")
        fp.write(f"{top}\n\n")

        cnt = 0
        ptant = None
        for pt in rows:
            obj = pt["objeto"]
            if obj == ptant:
                continue
            ptant = obj

            est    = pt["estacao"].strip()
            nome   = f"{est}-{pt['traducao_id'].strip()}"
            if len(nome) > max_id_size:
                raise ValueError(f"ID muito longo ({len(nome)}): {nome}")
            sup_np = pt["sup_nponto"]
            inter  = pt["inter"] or ""
            pac    = pt["supervisao"]
            codc   = pt["cod_conexao"]
            orig   = pt["cod_origem"]
            tp2    = pt["tipo2"]
            tp3    = pt["tipo3"]
            lt1    = float(pt["lmi1c"])
            lt2    = float(pt["lmi2c"])
            ls1    = float(pt["lms1c"])
            ls2    = float(pt["lms2c"])

            # cabeçalho de estação
            fp.write(f"\n; --- ESTACAO: {est} ------------------------\n\n")

            # logica de TAC / PAC / PINT…
            if orig == 15 and pt["cod_info"] in (185,42):
                inter, pac, tac = pt["id"], pt["id"], "LOCAL"
            else:
                tac = tac_conex.get(codc, est)
                if codc == 1 and est in tac_estacao:
                    tac = est

            # verifica dummy
            if sup_np not in (0,9991):
                tipo = "PAS" if tp3=="A" else "PDS"
            else:
                tipo = "PDS"
                if sup_np == 9991:
                    inter = "COM_SAGE"
                pac = "COM_SAGE"

            # ajusta TIPOE
            tipoe = tp2
            if tipoe=="PULS" and pt["cod_asdu"] in (45,46):
                tipoe = "AUMD"
            if pt["tipo_asdu"]=="S":
                tipoe = "STPT"

            # escreve bloco
            fp.write("CGS\n")
            if com_flag:
                fp.write(f"; NPONTO= {obj:05d}\n")
            fp.write(f"ID= {pt['id']}\n")
            fp.write(f"NOME= {nome}\n")
            if not no_cor:
                fp.write("AOR= CPFLT\n")
            fp.write(f"LMI1C= {lt1:.5f}\n")
            fp.write(f"LMI2C= {lt2:.5f}\n")
            fp.write(f"LMS1C= {ls1:.5f}\n")
            fp.write(f"LMS2C= {ls2:.5f}\n")
            fp.write(f"TIPO= {tipo}\n")
            fp.write("TPCTL= CSAC\n")
            fp.write(f"TAC= {tac}\n")
            fp.write(f"PAC= {pac}\n")
            fp.write(f"PINT= {inter}\n")
            fp.write(f"TIPOE= {tipoe}\n")
            fp.write(f"IDOPER= {obj}\n\n")

            cnt += 1

        fp.write(f"{top}\n")
        fp.write(f"// FIM CGS – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros.")

def generate_cgf_gcom_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    gestao_com: bool,
    dry_run: bool = False,
    force: bool = False,
) -> int:
    """
    Gera o arquivo cgf.gcom.dat (CGF – gestão da comunicação), se gestao_com=True.
    Retorna o contador final de comandos (end_gcom), para uso posterior.
    """
    ent = "cgf"
    destino = Path(paths["automaticos"]) / f"{ent}.gcom.dat"
    first_write = not destino.exists() or force

    if not gestao_com:
        logging.info(f"[{ent}.gcom] gestão da comunicação desabilitada, pulando.")
        return 0
    if dry_run:
        logging.info(f"[{ent}.gcom] dry-run ativo, nada será escrito em {destino}")
        return 0

    # prepara SQL
    placeholders = ",".join("%s" for _ in conexoes_dst)
    sql = f"""
    SELECT
        c.id_sage_aq,
        p.sufixo_sage,
        c.cod_conexao
    FROM id_conexoes c
      JOIN id_protocolos p ON p.cod_protocolo = c.cod_protocolo
    WHERE c.cod_conexao IN ({placeholders})
      AND p.cod_protocolo NOT IN (0,10)
    ORDER BY p.cod_protocolo,
             c.nsrv1, c.nsrv2,
             c.placa_princ, c.linha_princ,
             c.placa_resrv, c.linha_resrv
    """

    # busca dados
    with conn.cursor() as cur:
        cur.execute(sql, tuple(conexoes_dst))
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}.gcom] sem registros para gerar.")
        return 0

    # gravação
    end_gcom = 0
    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO CGF.GCOM  {timestamp}\n")
        fp.write(f"// NOH = {cod_noh}\n")
        fp.write(f"{top}\n")

        for pt in rows:
            id_aq    = str(pt["id_sage_aq"])
            sufixo   = str(pt["sufixo_sage"])
            conexao  = pt["cod_conexao"]
            # ordemnv1_sage_gc deve vir de contexto anterior (dicionário global)
            ordem = globals().get("ordemnv1_sage_gc", {}).get(conexao, 1)
            nv2 = f"{id_aq}_G{sufixo}_{ord}_CGCD"

            # 1) Desabilitacao do Enlace Principal
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Desabilitacao do Enlace Principal\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_DESAB_ENUP\n")
            fp.write(f"ID  = {id_aq}_DESAB_ENUP_{end_gcom}\n")
            fp.write("KCONV = PRI\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 2) Habilitacao do Enlace Principal
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Habilitacao do Enlace Principal\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_HABIL_ENUP\n")
            fp.write(f"ID  = {id_aq}_HABIL_ENUP_{end_gcom}\n")
            fp.write("KCONV = PRI\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 3) Desabilitacao do Enlace Reserva
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Desabilitacao do Enlace Reserva\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_DESAB_ENUR\n")
            fp.write(f"ID  = {id_aq}_DESAB_ENUR_{end_gcom}\n")
            fp.write("KCONV = REV\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 4) Habilitacao do Enlace Reserva
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Habilitacao do Enlace Reserva\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_HABIL_ENUR\n")
            fp.write(f"ID  = {id_aq}_HABIL_ENUR_{end_gcom}\n")
            fp.write("KCONV = REV\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 5) Desabil da Func Secund nos Enlaces
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Desabil da Func Secund nos Enlaces\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_DESAB_FSECN\n")
            fp.write(f"ID  = {id_aq}_DESAB_FSECN_{end_gcom}\n")
            fp.write("KCONV = \n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 6) Habilit da Func Secund nos Enlaces
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Habilit da Func Secund nos Enlaces\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_HABIL_FSECN\n")
            fp.write(f"ID  = {id_aq}_HABIL_FSECN_{end_gcom}\n")
            fp.write("KCONV = \n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 7) Desabilitacao da UTR Principal
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Desabilitacao da UTR Principal\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_DESAB_UTRP\n")
            fp.write(f"ID  = {id_aq}_DESAB_UTRP_{end_gcom}\n")
            fp.write("KCONV = PRI\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 8) Habilitacao da UTR Principal
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Habilitacao da UTR Principal\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_HABIL_UTRP\n")
            fp.write(f"ID  = {id_aq}_HABIL_UTRP_{end_gcom}\n")
            fp.write("KCONV = PRI\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 9) Desabilitacao da UTR Reserva
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Desabilitacao da UTR Reserva\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_DESAB_UTRR\n")
            fp.write(f"ID  = {id_aq}_DESAB_UTRR_{end_gcom}\n")
            fp.write("KCONV = REV\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 10) Habilitacao da UTR Reserva
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Habilitacao da UTR Reserva\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_HABIL_UTRR\n")
            fp.write(f"ID  = {id_aq}_HABIL_UTRR_{end_gcom}\n")
            fp.write("KCONV = REV\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 11) Failover do Enlace Principal
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Failover do Enlace Principal\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_PFAIL_ENUP\n")
            fp.write(f"ID  = {id_aq}_PFAIL_ENUP_{end_gcom}\n")
            fp.write("KCONV = PRI\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

            # 12) Failover do Enlace Reserva
            end_gcom += 1
            fp.write("\n")
            fp.write(f"; Failover do Enlace Reserva\n")
            fp.write("CGF\n")
            fp.write(f"CGS = {id_aq}_PFAIL_ENUR\n")
            fp.write(f"ID  = {id_aq}_PFAIL_ENUR_{end_gcom}\n")
            fp.write("KCONV = REV\n")
            fp.write(f"NV2    = {nv2}\n")
            fp.write(f"ORDEM  = {end_gcom}\n")

        # rodapé
        fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// FIM CGF.GCOM – total de blocos: {end_gcom}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}.gcom] gerado em '{destino}' (modo={mode}), {end_gcom} blocos.")
    return end_gcom

def generate_cgf_routing_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    com_flag: bool,
    max_id_size: int,
    start_gcom: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo cgf.dist.dat (CGF – pontos físicos para roteamento),
    se EMS estiver habilitado. Usa também conexoes_org, conexoes_dst, com_flag e max_id_size.
    """
    ent = "cgf"
    destino = Path(paths["automaticos"]) / f"{ent}.dist.dat"
    first_write = not destino.exists() or force

    # placeholders para IN
    all_conex = conexoes_org
    ph = ",".join("%s" for _ in all_conex)

    sql = f"""
    SELECT
        m.descricao       AS entidade,
        i.id              AS id_pt,
        f.kconv1          AS kconv1,
        f.kconv2          AS kconv2,
        f.kconv           AS kconv,
        f.endereco        AS endereco,
        i.nponto          AS objeto,
        c.id_sage_dt      AS id_conex_dt,
        c.id_sage_aq      AS id_conex,
        c.cod_noh_org     AS cod_noh_org,
        p.sufixo_sage     AS suf_prot,
        f.cod_conexao     AS cod_conexao,
        c.descricao       AS descr_conex,
        p.descricao       AS descr_prot,
        p.cod_protocolo   AS cod_protocolo,
        p.grupo_protoc    AS grupo_protoc,
        a.tn2_aq          AS tn2,
        -- id_conex_dt da distribuição
        (
          SELECT c2.id_sage_dt
            FROM id_ptfis_conex f2
            JOIN id_conexoes c2 ON f2.cod_conexao=c2.cod_conexao
           WHERE f2.id_dst = i.nponto
             AND c2.cod_noh_org = %s
           LIMIT 1
        )                  AS id_conex_dt_dst,
        f2.cod_conexao     AS con2,
        c2.end_org         AS org2
    FROM id_ptfis_conex f
      JOIN id_protoc_asdu a ON a.cod_asdu=f.cod_asdu
      LEFT JOIN id_ptfis_conex f2
        ON f.endereco=f2.endereco
       AND f.cod_conexao<>f2.cod_conexao
       AND f.cod_conexao=1
       AND f2.id_dst NOT IN (9991,9992)
      LEFT JOIN id_conexoes c2 ON f2.cod_conexao=c2.cod_conexao
      JOIN id_conexoes c       ON f.cod_conexao=c.cod_conexao
      JOIN id_protocolos p      ON p.cod_protocolo=c.cod_protocolo,
      id_ponto i
      JOIN id_ptlog_noh l       ON l.nponto=i.nponto
                              AND l.cod_nohsup=%s
      JOIN id_nops n            ON n.cod_nops=i.cod_nops
      JOIN id_modulos m         ON m.cod_modulo=n.cod_modulo
      JOIN id_estacao e         ON e.cod_estacao=m.cod_estacao
    WHERE f.cod_conexao IN ({ph})
      AND f.cod_conexao=c.cod_conexao
      AND f.id_dst=i.nponto
      AND i.cod_origem = 7
      AND i.cod_tpeq != 95
      AND EXISTS (
        SELECT 1
          FROM id_ptfis_conex f2
          JOIN id_conexoes c2 ON f2.cod_conexao=c2.cod_conexao
         WHERE f2.id_dst = i.nponto
           AND c2.cod_noh_org = %s
      )
    ORDER BY f.cod_conexao, i.nponto
    """

    if dry_run:
        logging.info(f"[{ent}.dist] dry-run, não escreve nada em {destino}")
        return

    # busca
    params = tuple(all_conex + [cod_noh, cod_noh, cod_noh])
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}.dist] sem registros.")
        return

    # escreve
    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = 0
    conex_ant = None

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO CGF.DIST   {ts}\n")
        fp.write(f"// NOH={cod_noh}\n")
        fp.write(f"{top}\n")

        for pt in rows:
            conx = pt["cod_conexao"]
            # cabeçalho de conexão
            if conx != conex_ant:
                conex_ant = conx
                if cnt != 0:
                    fp.write(f"\n; Pontos nesta conexão: {cnt - cnt0}\n\n")
                fp.write(f"\n; -----------------------------------------------\n")
                fp.write(f"; {pt['descr_conex']} ({pt['descr_prot']})\n\n")
                cnt0 = cnt

            # pula duplicado migrado
            if conx == 1 and pt["con2"] and pt["org2"]:
                continue

            # escreve ponto
            fp.write("\n")
            fp.write("CGF\n")
            if com_flag:
                fp.write(f"; NPONTO= {pt['objeto']:05d}\n")

            # ID
            if pt["cod_noh_org"] == cod_noh:
                # distribuição (exemplo simples)
                fp.write(f"ID= {pt['endereco']}\n")
                fp.write(f"KCONV= CGS= {pt['id']}\n")
                fp.write(f"NV2= {pt['id_conex_dt']}_{pt['tn2']}_NV2\n")
            else:
                # aquisição
                ord_ct = globals().get("ordemnv1_sage_ct", {}).get(conx, 1)
                fp.write(f"ID= {pt['id_conex']}_C{pt['suf_prot']}_{ord_ct}_{pt['tn2']}_{pt['endereco']}\n")
                fp.write(f"KCONV= {pt['kconv']}\n")
                fp.write(f"CGS= {pt['id_pt']}\n")
                fp.write(f"NV2= {pt['id_conex']}_C{pt['suf_prot']}_{ord_ct}_{pt['tn2']}\n")

            cnt += 1

        # rodapé
        fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// FIM CGF.DIST – total: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}.dist] escrito em {destino}, {cnt} registros.")
    return

from pathlib import Path
from datetime import datetime as dt
import logging

def generate_cgf_dat(
    paths: Dict[str, Path],
    conn,
    cod_noh: str,
    conexoes_org: List[int],
    conexoes_dst: List[int],
    ordemnv1_sage_ct: Dict[int,int],
    com_flag: bool,
    max_id_size: int,
    start_gcom: int,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Gera o arquivo cgf.dat (pontos físicos) para o SAGE, 
    unindo conexoes_org + conexoes_dst e usando start_gcom como base de ORDEM/NV2.
    """
    ent = "cgf"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    # 1) junta todas as conexões
    all_conex = conexoes_org + conexoes_dst
    ph = ",".join("%s" for _ in all_conex)

    sql = f"""
    SELECT
        m.descricao   AS entidade,
        i.id          AS id,
        f.kconv1      AS kconv1,
        f.kconv2      AS kconv2,
        f.kconv       AS kconv,
        f.endereco    AS endereco,
        i.nponto      AS objeto,
        c.id_sage_dt  AS id_conex_dt,
        c.id_sage_aq  AS id_conex,
        c.cod_noh_org AS cod_noh_org,
        p.sufixo_sage AS suf_prot,
        f.cod_conexao AS cod_conexao,
        c.descricao   AS descr_conex,
        p.descricao   AS descr_protocolo,
        c.cod_protocolo AS cod_protocolo,
        p.grupo_protoc  AS grupo_protoc,
        a.tn2_aq      AS tn2,
        -- para encontrar mesmo pf em outra conexão
        f2.cod_conexao AS con2,
        c2.end_org     AS org2
    FROM id_ptfis_conex AS f
      JOIN id_protoc_asdu AS a 
        ON a.cod_asdu = f.cod_asdu
      LEFT JOIN id_ptfis_conex AS f2
        ON  f.endereco   = f2.endereco
        AND f.cod_conexao = 1
        AND f2.cod_conexao != 1
        AND f2.id_dst NOT IN (9991,9992)
      LEFT JOIN id_conexoes AS c2
        ON f2.cod_conexao = c2.cod_conexao
      JOIN id_conexoes AS c 
        ON f.cod_conexao = c.cod_conexao
      JOIN id_protocolos AS p 
        ON p.cod_protocolo = c.cod_protocolo
      JOIN id_ponto AS i 
        ON i.nponto = f.id_dst
      JOIN id_ptlog_noh AS l 
        ON l.nponto = i.nponto
      JOIN id_nops   AS n 
        ON n.cod_nops = i.cod_nops
      JOIN id_modulos AS m
        ON m.cod_modulo = n.cod_modulo
      JOIN id_estacao AS e
        ON e.cod_estacao = m.cod_estacao
    WHERE
        f.cod_conexao IN ({ph})
        AND l.cod_nohsup = %s
        AND i.cod_origem = 7
        AND i.cod_tpeq    != 95
    ORDER BY
        f.cod_conexao, i.nponto
    """

    if dry_run:
        logging.info(f"[{ent}] dry‐run, não escreveria nada em {destino}")
        return 0

    # 2) carrega dados
    with conn.cursor() as cur:
        cur.execute(sql, tuple(all_conex) + (cod_noh,))
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros para gerar.")
        return 0

    # 3) escreve o arquivo
    mode = "w" if first_write else "a"
    top  = "// " + "=" * 70
    cnt = 0
    conexant = None
    cntconxant = 0
    ordem_base = start_gcom

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO CGF      {ts}\n")
        fp.write(f"// NOH={cod_noh}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            conex = pt["cod_conexao"]
            obj   = pt["objeto"]

            # cabeçalho por conexão
            if conex != conexant:
                if cnt > 0:
                    npts = cnt - cntconxant
                    fp.write(f"\n; Pontos nesta conexão: {npts}\n\n")
                fp.write(f"; --------------------------------------------------------------------------------\n")
                fp.write(f"; {pt['descr_conex']} ({pt['descr_protocolo']})\n\n")
                conexant = conex
                cntconxant = cnt

            # pula mesmo pf em conexão 1 duplicada
            if conex == 1 and pt["con2"] and pt["org2"]:
                continue

            # comentário NPOINTO
            fp.write("\n")
            fp.write("CGF\n")
            if com_flag:
                fp.write(f"; NPONTO= {obj:05d}\n")

            # monta KCONV padrão
            kconv = pt["kconv"] or ""
            if not kconv and pt["grupo_protoc"] == 1:
                kconv = "NO_S" if pt["kconv1"] == 1 else "NO"

            # distribuição vs aquisição
            is_dist = (pt["cod_noh_org"] == cod_noh and pt["cod_protocolo"] == 10)
            if is_dist:
                # ICCP distribuído
                id_str = f"{pt['id_conex_dt']}{pt['id']}".upper().replace("-", "_")
                nv2    = f"{pt['id_conex_dt']}_{pt['tn2']}_NV2"
            else:
                # aquisição
                if pt["cod_protocolo"] == 10:
                    id_str = f"{pt['id_conex']}_{pt['id']}".upper().replace("-", "_")
                else:
                    ordemnv = ordemnv1_sage_ct.get(conex, 1)
                    id_str  = f"{pt['id_conex']}_C{pt['suf_prot']}_{ordemnv}_{pt['tn2']}_{pt['endereco']}"
                nv2 = f"{pt['id_conex']}_C{pt['suf_prot']}_{ordemnv1_sage_ct.get(conex,1)}_{pt['tn2']}"

            # ORDEM
            ordem_base += 1

            # escreve campos
            fp.write(f"ID= {id_str}\n")
            fp.write(f"KCONV= {kconv}\n")
            if not is_dist:
                fp.write(f"ORDEM= {pt['endereco']}\n")
            fp.write(f"CGS= {pt['id']}\n")
            fp.write(f"NV2= {nv2}\n")

            cnt += 1

        # rodapé
        fp.write(f"\n{top}\n")
        fp.write(f"// FIM CGF – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros.")
    return cnt

from pathlib import Path
from datetime import datetime as dt
import logging

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
    destino = Path(paths["automaticos"]) / f"{ent}.gcom.dat"
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

def generate_pds_dat(
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
    Gera o arquivo pds.dat (PDS – pontos digitais lógicos de aquisição).
    """
    ent = "pds"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
    first_write = not destino.exists() or force

    if dry_run:
        logging.info(f"[{ent}] dry-run, não grava em {destino}")
        return

    # 1) placeholders e SQL
    ph = ",".join("%s" for _ in conexoes_dst)
    sql = f"""
    SELECT
        m.descricao           AS entidade,
        i.id                  AS id,
        i.traducao_id         AS traducao_id,
        i.cod_tpeq            AS cod_tpeq,
        i.cod_info            AS cod_info,
        i.cod_origem          AS cod_origem,
        i.cod_prot            AS cod_prot,
        i.cod_fases           AS cod_fases,
        e.estacao             AS estacao,
        i.nponto              AS objeto,
        m.id                  AS mid,
        m.cod_tpmodulo        AS cod_tpmodulo,
        tpm.ent_ems           AS ent_ems,
        l.alrin               AS alrin,
        tpnt.cod_tipopnt      AS cod_tipopnt,
        p.cod_tipopnt         AS prot_cod_tipopnt,
        pt_ocr.ocr            AS ptocr,
        tpnt.ocr              AS ocr,
        tpnt.casa_decimal     AS estalm,
        tpnt.pres_1           AS pres_1,
        tpnt.pres_0           AS pres_0,
        pt_ocr.pres_1         AS ppres_1,
        pt_ocr.pres_0         AS ppres_0,
        m.ems_id              AS ems_id_mod,
        n.ems_id              AS ems_id,
        n.ems_lig1            AS ems_lig1,
        n.ems_lig2            AS ems_lig2,
        cx.cod_conexao        AS cod_conexao,
        CASE i.cod_tpeq
          WHEN 28 THEN IF(i.cod_info=0 AND i.cod_prot=0, 'CHAVE','OUTROS')
          WHEN 27 THEN IF(i.cod_info=0 AND i.cod_prot=0, 'DISJ','OUTROS')
          ELSE
            CASE
              WHEN tpnt.casa_decimal < 2 THEN 'ALRP'
              WHEN MID(i.id,15,1) = 'O' THEN 'PTIP'
              WHEN MID(i.id,15,1) IN ('S','T','P','R') THEN 'PTNI'
              ELSE 'OUTROS'
            END
        END                   AS tipo_pds,
        'NAO'                 AS selsd,
        'NLFL'                AS tpfil,
        form.id               AS tcl,
        form.tipo_calc        AS tipo_calc,
        i.vlinic              AS vlinic,
        i.evento              AS eh_evento,
        e.ems_modela = 'S'    AS pres_ems,
        v.valor               AS valor_atual
    FROM id_ptlog_noh l
    JOIN id_ponto i               ON l.nponto = i.nponto
    JOIN id_nops n                ON n.cod_nops = i.cod_nops
    JOIN id_modulos m             ON m.cod_modulo = n.cod_modulo
    JOIN id_tpmodulo tpm          ON tpm.cod_tpmodulo = m.cod_tpmoduloems
    JOIN id_estacao e             ON e.cod_estacao = m.cod_estacao
    JOIN id_formulas form         ON i.cod_formula = form.cod_formula
    JOIN id_prot p                ON i.cod_prot = p.cod_prot
    JOIN id_tipopnt pt_ocr        ON p.cod_tipopnt = pt_ocr.cod_tipopnt
    JOIN id_tipos tp              ON tp.cod_tpeq = i.cod_tpeq AND tp.cod_info = i.cod_info
    JOIN id_tipopnt tpnt          ON tpnt.cod_tipopnt = tp.cod_tipopnt
    LEFT JOIN val_tr v            ON v.nponto = i.nponto
    LEFT JOIN id_ptfis_conex f    ON f.id_dst = l.nponto AND f.cod_conexao IN ({ph})
    LEFT JOIN id_conexoes cx      ON cx.cod_conexao = f.cod_conexao
    WHERE
      l.cod_nohsup = %s
      AND tpnt.tipo = 'D'
      AND i.cod_origem != 7
      AND i.cod_tpeq != 95
      AND i.nponto NOT IN (0, 9991, 9992)
    ORDER BY
      i.nponto, cx.cod_conexao DESC
    """

    # 2) executa consulta
    params = tuple(conexoes_dst) + (cod_noh,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    if not rows:
        logging.warning(f"[{ent}] sem registros para gerar.")
        return

    # 3) prepara escrita
    mode = "w" if first_write else "a"
    top = "// " + "=" * 70
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    cnt = 0
    ptant = None
    ptosDigTacEst = {}
    cntcalccomp = 0
    cntnaosup = 0

    with open(destino, mode, encoding="utf-8") as fp:
        if not first_write:
            fp.write("\n")
        fp.write(f"{top}\n")
        fp.write(f"// INÍCIO PDS       {ts}\n")
        fp.write(f"// Código NOH: {cod_noh}\n")
        fp.write(f"{top}\n\n")

        for pt in rows:
            # evita repetir mesmo objeto
            if pt["objeto"] == ptant:
                continue
            ptant = pt["objeto"]

            # --- 1) Ajuste de OCR e presões a partir do prot_cod_tipopnt
            if pt["prot_cod_tipopnt"] != 0:
                if pt["prot_cod_tipopnt"] == 23:
                    if pt["cod_tipopnt"] in (8, 23, 25):
                        pt["ocr"] = pt["ptocr"]
                        pt["pres_0"] = pt["ppres_0"]
                        pt["pres_1"] = pt["ppres_1"]
                    elif pt["cod_tipopnt"] in (7,20,22,26,31,34,42,54,57,103):
                        pt["ocr"] = "OCR_OPE1"
                        pt["pres_0"] = pt["ppres_0"]
                        pt["pres_1"] = pt["ppres_1"]
                    elif pt["cod_tipopnt"] in (36,38,49,64,65,69,85,95,107):
                        pt["ocr"] = "OCR_OPE1"
                        pt["pres_0"] = pt["ppres_0"]
                        pt["pres_1"] = pt["ppres_1"]
            else:
                pt["ocr"] = pt["ptocr"]
                pt["pres_0"] = pt["ppres_0"]
                pt["pres_1"] = pt["ppres_1"]

            # --- 2) Descobrir TAC
            conex = pt.get("cod_conexao") or 0
            tac = pt["estacao"]

            if conex > 0:
                # supervisionado
                if (NO_COS and conex == CONEX_ONS_COS) \
                   or ((NO_COR or NO_CPS) and conex == CONEX_ONS_COR):
                    tac = "CEEE_S_1"
                else:
                    tac = tac_conex.get(conex, pt["estacao"])
                    if conex in (1,100,120,72) and pt["estacao"] in tac_estacao:
                        tac = pt["estacao"]
                    ptosDigTacEst.setdefault(pt["estacao"], 0)
                    ptosDigTacEst[pt["estacao"]] += 1
                    if ptosDigTacEst[pt["estacao"]] > MaxPontosDigPorTAC:
                        bloco = ptosDigTacEst[pt["estacao"]] // MaxPontosDigPorTAC
                        tac = f"{pt['estacao']}_{bloco}"
            else:
                # não supervisionado ou calculado
                if pt["cod_origem"] == 1:
                    if pt["tipo_calc"] == "C":
                        cntcalccomp += 1
                        tac = f"CALC-COMP{1 + cntcalccomp//MaxPontosPorTAC_Calc}"
                    elif pt["tipo_calc"] == "I":
                        tac = "CALC-INTER"
                    elif pt["tipo_calc"] == "F":
                        tac = "FILC101"
                        pt["tpfil"] = pt["tcl"]
                        pt["tcl"] = "NLCL"
                elif pt["cod_origem"] == 15:
                    tac = "LOCAL"
                else:
                    cntnaosup += 1
                    tac = f"TAC-NAOSUP{1 + cntnaosup//MaxPontosPorTAC}"
                    pt["tcl"] = "NLCL"

            # ajustes finais de ECEZ/ECEY
            if (NO_COS or NO_COR or NO_CPS) and pt["cod_origem"] == 17 and pt["estacao"] != "ECEY":
                tac = "ECEY"
            if (NO_COS or NO_COR or NO_CPS) and pt["cod_origem"] == 16 and pt["estacao"] != "ECEZ":
                tac = "ECEZ"

            pt["tac"] = tac

            # --- 3) Escrever o bloco no arquivo
            fp.write("\nPDS\n")
            if com_flag:
                fp.write(f"; NPONTO= {pt['objeto']:05d}\n")
            fp.write(f"ID    = {pt['id']}\n")
            fp.write(f"NOME  = {pt['estacao']}-{pt['traducao_id']}\n")
            if not no_cor:
                fp.write("AOR   = CPFLT\n")
            fp.write(f"TIPO  = {pt['tipo_pds']}\n")
            fp.write(f"TAC   = {pt['tac']}\n")
            fp.write(f"OCR   = {pt['ocr']}01\n")
            fp.write(f"ALRIN = {'SIM' if pt['alrin']=='S' else 'NAO'}\n")
            fp.write("ALINT = SIM\n")
            fp.write(f"STINI = {pt.get('stini','A')}\n")
            fp.write(f"STNOR = {pt.get('stnor','A')}\n")
            fp.write(f"TPFIL = {pt['tpfil']}\n")
            fp.write(f"TCL   = {pt['tcl']}\n")
            fp.write(f"SELSD = {pt['selsd']}\n")
            fp.write(f"IDOPER= {pt['objeto']}\n")
            if pt["cod_tipopnt"] in (32,33,42,43):
                fp.write("TMP_ANORM= 300\n")

            cnt += 1

        # rodapé
        fp.write(f"\n{top}\n")
        fp.write(f"// FIM PDS – total de registros: {cnt}\n")
        fp.write(f"{top}\n")

    logging.info(f"[{ent}] gerado em '{destino}' (modo={mode}), {cnt} registros.")

from pathlib import Path
from datetime import datetime as dt
import logging

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
  l.cod_nohsup= %s and
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
        left outer join id_conexoes c2 on f2.cod_conexao=c2.cod_conexao and c2.cod_noh_dst= %s /*and c2.end_org!=0*/,      
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
        l.cod_nohsup= %s and 
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
      left outer join id_ptfis_conex f2 on f.id_dst=f2.id_dst and f.cod_conexao!=f2.cod_conexao and f.cod_conexao=1 and f2.id_dst not in (9991,9992) and f2.cod_conexao in ({ph_dst})
      left outer join id_conexoes c2 on f2.cod_conexao=c2.cod_conexao and c2.cod_noh_dst=%s,
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

def generate_e2m_dat(paths: Dict[str, Path], conn, dry_run: bool = False, force: bool = False):
    ent = "e2m"
    destino = Path(paths["automaticos"]) / f"{ent}.dat"
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

def generate_e2m2_dat(paths: Dict[str, Path], conn, cod_noh: str, com_flag: bool = True, dry_run: bool = False, force: bool = False):
    ent = "e2m"
    destino = Path(paths["automaticos"]) / f"{ent}2.dat"
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
    from    
        id_ptlog_noh as l,
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

def parse_args():
    parser = argparse.ArgumentParser(description="Gerador de arquivos .dat para SAGE")
    parser.add_argument("--dry-run", action="store_true", help="Não grava, apenas simula.")
    parser.add_argument("--force", action="store_true", help="Regrava mesmo se o arquivo existir.")

    # arquivos principais
    parser.add_argument("--grupo_transformadores", action="store_true", help="Gera grupo-tr.dat")
    parser.add_argument("--grupo_barras", action="store_true", help="Gera grupo-barras.dat")
    parser.add_argument("--grupo_disjuntor", action="store_true", help="Gera grupo-dj.dat")
    parser.add_argument("--grcmp", action="store_true", help="Gera grcmp-tr.dat")
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

    info = load_conexoes(conn, CodNoh)
    ordemnv1_sage_ct = info.get("ordemnv1_sage_ct", {})
    ordemnv1_sage_aq = info.get("ordemnv1_sage_aq", {})
    ordemnv1_sage_dt = info.get("ordemnv1_sage_dt", {})

    # Controle de execução
    run_all = not any([
        args.grupo_transformadores, args.grupo_barras, args.grupo_disjuntor, args.grcmp, args.tctl, args.cnf, args.utr, args.cxu, args.map, args.lsc,
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
    if run_all or args.grcmp:
        generate_grcmp_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
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
        generate_lsc_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.tcl:
        generate_tcl_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.tac:
        tac_info = generate_tac_dat(paths, conn, CodNoh, dry_run=args.dry_run, force=args.force)
        tac_conex = tac_info["tac_conex"]
        tac_estacao = tac_info["tac_estacao"]
    else:
        tac_conex = {}
        tac_estacao = []
    if run_all or args.tdd:
        generate_tdd_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.nv1:
        generate_nv1_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)
    if run_all or args.nv2:
        generate_nv2_dat(paths, conn, cod_noh=CodNoh, dry_run=args.dry_run, force=args.force)

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
        generate_cgs_gcom_dat(paths, conn, conexoes_dst, gestao_com, dry_run=args.dry_run, force=args.force)
    if run_all or args.cgs:
        generate_cgs_dat(
            paths         = paths,
            conn          = conn,
            cod_noh       = CodNoh,
            conexoes_org  = conexoes_org,
            conexoes_dst  = conexoes_dst,
            no_cor        = NO_COR,
            tac_conex     = tac_conex,
            tac_estacao   = tac_estacao,
            com_flag      = COMENT,
            max_id_size   = MaxIdSize,
            dry_run       = args.dry_run,
            force         = args.force,
        )
    if run_all or args.cgf_gcom:
        end_gcom = generate_cgf_gcom_dat(paths, conn, cod_noh=CodNoh, conexoes_org=conexoes_org, conexoes_dst=conexoes_dst, gestao_com=gestao_com, dry_run=args.dry_run, force=args.force)
    else:
        end_gcom = 0
    if run_all or args.cgf_dist:
        generate_cgf_routing_dat(
            paths          = paths,
            conn           = conn,
            cod_noh        = CodNoh,
            conexoes_org   = conexoes_org,
            conexoes_dst   = conexoes_dst,
            com_flag       = COMENT,
            max_id_size    = MaxIdSize,
            start_gcom     = end_gcom,
            dry_run        = args.dry_run,
            force          = args.force,
        )
    if run_all or args.cgf:
        generate_cgf_dat(
            paths, conn,
            cod_noh       = CodNoh,
            conexoes_org  = conexoes_org,
            conexoes_dst  = conexoes_dst,
            ordemnv1_sage_ct = ordemnv1_sage_ct,
            com_flag      = COMENT,
            max_id_size   = MaxIdSize,
            start_gcom    = end_gcom,
            dry_run       = args.dry_run,
            force         = args.force,
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
        generate_pds_dat(
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
    concat_grupo_dats(paths)
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