// src/app/api/pontos/[cod_estacao]/route.ts
import { NextResponse } from 'next/server';
import mysql from 'mysql2/promise';

export async function GET(
  request: Request,
  { params }: { params: { cod_estacao: string } }
) {
  const codEstacao = params.cod_estacao;

  try {
    const connection = await mysql.createConnection({
      host: '127.0.0.1',
      user: 'root',
      password: '@M1ch43l52', // Sua senha
      database: 'bancotr',
    });

    const [rows] = await connection.execute(
      'SELECT nponto, id, traducao_id, cod_origem FROM id_ponto WHERE cod_estacao = ? ORDER BY id',
      [codEstacao]
    );

    await connection.end();

    return NextResponse.json(rows);

  } catch (error) {
    console.error(error);
    const errorMessage = error instanceof Error ? error.message : 'Erro desconhecido';
    return new NextResponse(
      JSON.stringify({ message: 'Erro ao buscar pontos no banco.', error: errorMessage }),
      { status: 500 }
    );
  }
}