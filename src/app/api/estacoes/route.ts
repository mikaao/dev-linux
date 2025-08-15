import { NextResponse } from "next/server";
import mysql from "mysql2/promise";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const conn = await mysql.createConnection({
      host: "127.0.0.1",
      user: "root",
      password: "@M1ch43l52",
      database: "bancotr",
    });

    const [rows] = await conn.execute(
      `SELECT
         TRIM(ESTACAO)   AS codigo,
         TRIM(DESCRICAO) AS nome
       FROM id_estacao
       ORDER BY ESTACAO`
    );

    await conn.end();
    return NextResponse.json(rows); // [{codigo:"BGO2", nome:"SE Bento …"}]
  } catch (err) {
    console.error(err);
    return NextResponse.json(
      { message: "Erro ao buscar subestações" },
      { status: 500 }
    );
  }
}
