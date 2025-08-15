import { NextRequest, NextResponse } from "next/server";
import mysql from "mysql2/promise";

export const dynamic = "force-dynamic";

export async function GET(
  _req: NextRequest,
  context: { params: { cod_estacao: string } }
) {
  const sigla = context.params.cod_estacao.trim().toUpperCase(); // ex.: BGO2

  try {
    const conn = await mysql.createConnection({
      host: "127.0.0.1",
      user: "root",
      password: "@M1ch43l52",
      database: "bancotr",
    });

    const [rows] = await conn.execute<
      { ID: string; TRADUCAO_ID: string; NPONTO: number; COD_ORIGEM: number | null }[]
    >(
      `SELECT
         p.ID           AS id,
         p.TRADUCAO_ID  AS descricao,
         p.NPONTO       AS nPonto,
         p.COD_ORIGEM   AS status
       FROM id_ponto p
       JOIN id_estacao e ON e.cod_estacao = p.cod_estacao
       WHERE UPPER(TRIM(e.estacao)) = ?
       ORDER BY p.NPONTO`,
      [sigla]
    );

    await conn.end();

    // RowDataPacket -> objeto literal
    const pontos = (rows as any[]).map((r) => ({ ...r }));
    return NextResponse.json(pontos);
  } catch (err) {
    console.error(err);
    return NextResponse.json({ message: "Erro interno" }, { status: 500 });
  }
}
