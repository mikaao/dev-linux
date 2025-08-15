// src/app/api/estacoes/route.ts
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

    /* -- SELECT já com aliases ------------------------------------ */
    const [rows] = await conn.execute(
      `SELECT
        ESTACAO   AS codigo,        -- sigla (string)
         DESCRICAO AS nome           -- nome da subestação
       FROM id_estacao
       ORDER BY ESTACAO`
    );

    await conn.end();

    /* rows já está no formato [{codigo:"ATL2", nome:"SE Atlântida 2"}] */
    return NextResponse.json(rows);

  } catch (err) {
    console.error(err);
    return NextResponse.json(
      { message: "Erro ao buscar subestações" },
      { status: 500 }
    );
  }
}
