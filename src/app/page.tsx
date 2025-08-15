"use client";

import { useState, useEffect } from "react";
import { Terminal, Search, Zap, AlertTriangle, CheckCircle2, XCircle, Building2 } from "lucide-react";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Input } from "@/components/ui/input";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

export default function HomePage() {
  const [estacoes, setEstacoes] = useState([]);
  const [estacaoSelecionada, setEstacaoSelecionada] = useState(null);
  const [pontos, setPontos] = useState([]);
  const [loadingEstacoes, setLoadingEstacoes] = useState(true);
  const [loadingPontos, setLoadingPontos] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filtro, setFiltro] = useState("");

// 1ï¸