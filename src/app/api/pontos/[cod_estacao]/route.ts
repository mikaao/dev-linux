// src/app/api/pontos/[cod_estacao]/route.ts
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

    /* ── único SELECT com JOIN ─────────────────────────────────── */
    const [rows] = await conn.execute<
      {
        id:          string;
        descricao:   string;
        nPonto:      number;
        status:      number | null;
      }[]
    >(
      `SELECT
         p.ID            AS id,
         p.TRADUCAO_ID   AS descricao,
         p.NPONTO        AS nPonto,
         p.COD_ORIGEM    AS status
       FROM id_ponto   p
       JOIN id_estacao e ON e.cod_estacao = p.cod_estacao
       WHERE UPPER(TRIM(e.estacao)) = ?
       ORDER BY p.NPONTO`,
      [sigla]
    );

    await conn.end();

    /* se rows.length === 0 então não há pontos para essa sigla      */
    return NextResponse.json(rows);
  } catch (err) {
    console.error(err);
    return NextResponse.json(
      { message: "Erro interno" },
      { status: 500 }
    );
  }
}
