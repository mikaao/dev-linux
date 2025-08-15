"use client";

import { useEffect, useState } from "react";
import {
  Terminal,
  Search,
  Building2,
  CheckCircle2,
  AlertTriangle,
  XCircle,
} from "lucide-react";

import {
  ResizablePanelGroup,
  ResizablePanel,
  ResizableHandle,
} from "@/components/ui/resizable";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Alert,
  AlertTitle,
  AlertDescription,
} from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Input } from "@/components/ui/input";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

/* ------------------------------------------------------------------ */
/* TYPES                                                              */
/* ------------------------------------------------------------------ */
interface Estacao {
  codigo: string;
  nome: string;
}

interface Ponto {
  id: string;
  descricao: string;
  nPonto: number;
  status?: string; // ex.: "ok" | "alerta" | "falha"
}

/* ------------------------------------------------------------------ */
/* COMPONENT                                                          */
/* ------------------------------------------------------------------ */
export default function HomePage() {
  /* ---------------------- state ----------------------------------- */
  const [estacoes, setEstacoes] = useState<Estacao[]>([]);
  const [estacaoSelecionada, setEstacaoSelecionada] =
    useState<Estacao | null>(null);
  const [pontos, setPontos] = useState<Ponto[]>([]);

  const [filtro, setFiltro] = useState("");
  const [loadingEstacoes, setLoadingEstacoes] = useState(true);
  const [loadingPontos, setLoadingPontos] = useState(false);
  const [error, setError] = useState<string | null>(null);

  /* ---------------------- fetch estações -------------------------- */
  useEffect(() => {
    async function fetchEstacoes() {
      try {
        const res = await fetch("/api/estacoes");
        const raw: { estacao: string; descricao: string }[] =
          await res.json();

        const limpas: Estacao[] = raw
          .map((e) => ({
            codigo: (e.estacao ?? "").trim(),
            nome: (e.descricao ?? "").trim(),
          }))
          .filter((e) => e.codigo && e.nome);

        setEstacoes(limpas);
      } catch (err) {
        console.error(err);
        setError("Erro ao carregar subestações.");
      } finally {
        setLoadingEstacoes(false);
      }
    }
    fetchEstacoes();
  }, []);

  /* ---------------------- fetch pontos ---------------------------- */
useEffect(() => {
  async function fetchEstacoes() {
    try {
      const res = await fetch("/api/estacoes");
      const data: Estacao[] = await res.json();   // já vem {codigo,nome}

      // descarta vazios, se houver
      setEstacoes(
        data.filter((e) => e.codigo && e.nome)
      );
    } catch (err) {
      console.error(err);
      setError("Erro ao carregar subestações.");
    } finally {
      setLoadingEstacoes(false);
    }
  }
  fetchEstacoes();
}, []);
  /* ---------------------- helpers --------------------------------- */
  const estacoesFiltradas = estacoes.filter(
    (e) =>
      e.nome.toLowerCase().includes(filtro.toLowerCase()) ||
      e.codigo.toLowerCase().includes(filtro.toLowerCase())
  );

  const statusIcon = (st?: string) => {
    switch ((st ?? "").toLowerCase()) {
      case "ok":
        return (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <CheckCircle2 className="h-4 w-4 text-green-500" />
              </TooltipTrigger>
              <TooltipContent>Operacional</TooltipContent>
            </Tooltip>
          </TooltipProvider>
        );
      case "alerta":
        return (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <AlertTriangle className="h-4 w-4 text-yellow-500" />
              </TooltipTrigger>
              <TooltipContent>Alerta</TooltipContent>
            </Tooltip>
          </TooltipProvider>
        );
      case "falha":
        return (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <XCircle className="h-4 w-4 text-red-500" />
              </TooltipTrigger>
              <TooltipContent>Falha</TooltipContent>
            </Tooltip>
          </TooltipProvider>
        );
      default:
        return null;
    }
  };

  /* ---------------------------------------------------------------- */
  /* RENDER                                                           */
  /* ---------------------------------------------------------------- */
  return (
    <ResizablePanelGroup
      direction="horizontal"
      className="h-screen border"
    >
      {/* ---------------- Sidebar ---------------- */}
      <ResizablePanel defaultSize={25} className="bg-gray-50 border-r">
        <div className="p-3">
          <div className="relative">
            <Search className="absolute left-2 top-2.5 h-4 w-4 text-gray-400" />
            <Input
              placeholder="Buscar subestação..."
              value={filtro}
              onChange={(e) => setFiltro(e.target.value)}
              className="pl-8"
            />
          </div>
        </div>

        <ScrollArea className="h-[calc(100%-60px)] custom-scroll">
          <ul className="space-y-1 px-2">
            {loadingEstacoes
              ? Array(6)
                  .fill(0)
                  .map((_, i) => (
                    <Skeleton
                      key={i}
                      className="h-6 w-full"
                    />
                  ))
              : estacoesFiltradas.map((e) => (
                  <li
                    key={e.codigo}
                    onClick={() => setEstacaoSelecionada(e)}
                    className={`flex items-center gap-2 cursor-pointer rounded-md px-3 py-2 text-sm transition-colors ${
                      estacaoSelecionada?.codigo === e.codigo
                        ? "bg-blue-100 text-blue-700 font-medium"
                        : "hover:bg-gray-100 text-gray-700"
                    }`}
                  >
                    <Building2 className="h-4 w-4 text-gray-500" />
                    <span>
                      {e.codigo}: {e.nome}
                    </span>
                  </li>
                ))}
            {!loadingEstacoes && estacoesFiltradas.length === 0 && (
              <p className="px-4 py-2 text-sm text-gray-500">
                Nenhuma subestação encontrada.
              </p>
            )}
          </ul>
        </ScrollArea>
      </ResizablePanel>

      <ResizableHandle />

      {/* ---------------- Conteúdo ---------------- */}
      <ResizablePanel>
        <div className="p-4">
          {error && (
            <Alert className="mb-4">
              <AlertTitle>Erro</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {estacaoSelecionada ? (
            <>
              <h1 className="text-xl font-bold mb-2">
                {estacaoSelecionada.nome}
              </h1>
              <p className="text-sm text-gray-500 mb-4">
                Código: {estacaoSelecionada.codigo}
              </p>

              <div className="border rounded-lg overflow-hidden shadow-sm">
                <ScrollArea className="max-h-[75vh] custom-scroll">
                  <Table>
                    <TableHeader className="bg-gray-100 sticky top-0 z-10">
                      <TableRow>
                        <TableHead className="w-[250px]">
                          ID do Ponto
                        </TableHead>
                        <TableHead>Descrição</TableHead>
                        <TableHead className="w-24">
                          Status
                        </TableHead>
                        <TableHead className="text-right w-24">
                          Nº Ponto
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {loadingPontos ? (
                        <TableRow>
                          <TableCell colSpan={4}>
                            <Skeleton className="h-10 w-full" />
                          </TableCell>
                        </TableRow>
                      ) : pontos.length ? (
                        pontos.map((p, i) => (
                          <TableRow
                            key={p.id}
                            className={
                              i % 2 ? "bg-gray-50" : "bg-white"
                            }
                          >
                            <TableCell className="font-mono text-xs">
                              {p.id}
                            </TableCell>
                            <TableCell className="text-sm">
                              {p.descricao}
                            </TableCell>
                            <TableCell>{statusIcon(p.status)}</TableCell>
                            <TableCell className="text-right">
                              {p.nPonto}
                            </TableCell>
                          </TableRow>
                        ))
                      ) : (
                        <TableRow>
                          <TableCell
                            colSpan={4}
                            className="text-center py-8 text-sm text-gray-500"
                          >
                            Nenhum ponto para exibir
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </div>
            </>
          ) : (
            <div className="flex h-full items-center justify-center">
              <Alert className="max-w-md">
                <Terminal className="h-4 w-4" />
                <AlertTitle>Nenhuma subestação selecionada</AlertTitle>
                <AlertDescription>
                  Selecione uma subestação na barra lateral para visualizar seus pontos.
                </AlertDescription>
              </Alert>
            </div>
          )}
        </div>
      </ResizablePanel>
    </ResizablePanelGroup>
  );
}
