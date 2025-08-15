// src/app/api/estacoes/route.ts
import { NextResponse } from 'next/server';
import mysql from 'mysql2/promise';

export async function GET() {
  try {
    // É uma boa prática usar variáveis de ambiente, mas por enquanto vamos usar os valores diretamente
    const connection = await mysql.createConnection({
      host: '127.0.0.1',
      user: 'root',
      password: '@M1ch43l52', // Coloque sua senha aqui
      database: 'bancotr',
    });

    // Executa a consulta para buscar as estações
    const [rows] = await connection.execute(
      'SELECT cod_estacao, estacao, descricao FROM id_estacao ORDER BY estacao'
    );

    await connection.end();

    // Retorna os dados como JSON
    return NextResponse.json(rows);

  } catch (error) {
    console.error(error);
    // Retorna um erro 500 se algo der errado
    const errorMessage = error instanceof Error ? error.message : 'Erro desconhecido';
    return new NextResponse(
      JSON.stringify({ message: 'Erro ao conectar ou buscar dados no banco.', error: errorMessage }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }
}