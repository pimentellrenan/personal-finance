# Finanças Pessoais (v0) — Guia do Usuário

App em Streamlit para acompanhar finanças pessoais (cartão de crédito, débitos/PIX, receitas e contas da casa), com categorização via Excel (`templates/financas.xlsx`) e dados persistidos em SQLite (`data/finance.sqlite`).

## Rodar o app

### Opção A (recomendado): Docker

Pré-requisito: Docker Desktop instalado.

```bash
docker compose up --build
```

Acesse:
- Neste computador: `http://localhost:8501`
- Em outro device na mesma rede: `http://<IP_DO_SEU_PC>:8501`

### Opção B: Python local

Pré-requisitos: Python 3.12+.

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Fluxo do dia a dia (o jeito “certo”)

O app gira em torno do arquivo `templates/financas.xlsx` (Excel unificado) e de imports de CSV de faturas.

### 1) Importar faturas de cartão (CSV)

Na sidebar, abra **“🔄 Atualização”**:

1. (Opcional) Em vez de copiar arquivos, você pode usar **“📤 Enviar faturas CSV (.csv)”**.
2. Ou coloque os CSVs em `raw_data/` (subpastas ok).  
   - `raw_data/old/` é ignorado (use como “arquivo morto”).
3. Clique em **“📥 Importar CSVs”**.

O que acontece:
- O app importa os lançamentos para o banco (`data/finance.sqlite`).
- Em seguida, ele **anexa** as novas linhas na aba de cartão do `templates/financas.xlsx` (sem sobrescrever o que você já categorizou).

Regras importantes (para não duplicar):
- Faturas “em aberto” são reprocessadas até passar o vencimento do arquivo (veja *Nome do arquivo* abaixo).
- Faturas antigas normalmente são puladas por hash do arquivo (se o conteúdo não mudou).
- Não edite a coluna **“Hash (oculto)”** no Excel: ela é o ID da linha.

### 2) Categorizar no Excel (`templates/financas.xlsx`)

No Excel, preencha principalmente:
- Aba **Cartão**: `Categoria`, `Subcategoria`, `Pago por Aline` (quando aplicável), `Reembolsável`, `Notas`.
- Abas **Débitos**, **Receitas**, **Contas da Casa**: lance/edite linhas manualmente quando necessário.

Em Docker você não “abre Excel” no servidor: use download/upload pelo app.

### 3) Subir o Excel categorizado de volta para o app

Na sidebar **“🔄 Atualização”**:

1. Baixe o arquivo atual em **“📊 Abrir Excel (baixar)”** (se quiser partir do último).
2. Depois de editar, envie em **“📤 Enviar Excel atualizado (.xlsx)”**.

O upload:
- Salva o arquivo como `templates/financas.xlsx`
- Faz backup versionado em `raw_data/backups/financas/`
- Sincroniza automaticamente as categorias para o banco

Importante: ao enviar o Excel, o app trata o Excel como “fonte da verdade” e pode remover do banco linhas que você apagou no arquivo.

## Nome do arquivo da fatura (muito importante)

Para o app entender o **vencimento da fatura**, o nome do CSV deve conter uma data `YYYY-MM-DD`.

Exemplos válidos:
- `Nubank_2026-02-19.csv`
- `Nubank_Aline_2026-02-05.csv`
- `XP_Fatura2026-02-05.csv`
- `C6_Fatura_2026-02-20.csv`
- `MercadoPago_2026-02-17.csv`
- `PortoBank_2026-02-22.csv`

Se o nome do arquivo não tiver `YYYY-MM-DD`, o app tenta um *fallback* com base na data da compra + configuração do cartão, mas você perde a precisão do “acerto” por vencimento.

## Formato do CSV por cartão (faturas)

O importador é flexível com:
- Separador: `;` ou `,`
- Encoding: `utf-8-sig`, `utf-8` ou `latin-1`
- Datas: `YYYY-MM-DD` ou `DD/MM/YYYY`

O mínimo que o CSV precisa ter é:
- uma coluna de **data**
- uma coluna de **descrição/estabelecimento**
- uma coluna de **valor**

Abaixo estão formatos reais suportados (exemplos do `raw_data/old/`):

### Nubank (cartão)

Cabeçalho típico:
```csv
date,title,amount
```

Exemplo de linha:
```csv
2026-01-25,Posto Leao de Juda,157.21
```

Observações:
- O valor vem numérico (ponto decimal). O app normaliza internamente para “despesa = negativo”.

### Nubank Aline

Mesmo formato do Nubank. Para o app identificar como Nubank Aline, o filename precisa conter `aline` (ex.: `Nubank_Aline_2026-02-05.csv`).

### XP (cartão)

Cabeçalho típico (separado por `;`):
```csv
Data;Estabelecimento;Portador;Valor;Parcela
```

Exemplo de linha:
```csv
01/07/2025;HOTEL FAZENDA M1;ALINE ANGELO;R$ 474,87;7 de 10
```

Observações:
- `Portador` é usado como “Pessoa” quando presente.
- `Parcela` é guardada em “Notas” e entra na identidade da linha.

### C6 (cartão)

Cabeçalho típico (separado por `;`):
```csv
Data de Compra;Nome no Cartão;Final do Cartão;Categoria;Descrição;Parcela;Valor (em US$);Cotação (em R$);Valor (em R$)
```

Observações:
- O app prefere `Valor (em R$)` quando existir (para evitar pegar US$).

### Mercado Pago

Sem um exemplo “canônico” no repo. Em geral funciona se tiver **Data + Descrição + Valor** e o filename tiver `MercadoPago_YYYY-MM-DD.csv` (ou pelo menos “mercado”/“mp” no nome).

### Porto Bank

Sem um exemplo “canônico” no repo. Em geral funciona se tiver **Data + Descrição + Valor** e o filename começar com `Porto_`/`PortoBank_` (ou contenha “porto”).

### Outros CSVs

Se o seu CSV tiver pelo menos **Data + Descrição + Valor**, normalmente funciona.  
Se der erro de “CSV não reconhecido”, veja as colunas esperadas em `pf/importers/credit_card_csv.py`.

## Regra do “Acerto Mensal” (resumo)

Regra fixa (início do mês), usada no Dashboard:
- Cartões usam **data de vencimento** (não a data da compra).
- XP e Nubank Aline entram no mês do acerto; demais cartões entram no mês anterior (por regra do app).
- Débitos/PIX entram no mês anterior completo.
- Contas da Casa entram no mês em que foram pagas (Data Pagamento).

## Estrutura de pastas

```
raw_data/           CSVs e backups
  old/              arquivo morto (ignorado pelo scan)
templates/          Excel principal (financas.xlsx)
config/             categorias, cartões, orçamento, regras
data/               banco SQLite (finance.sqlite)
```

## Dicas rápidas

- Se aparecer “cartão não identificado”, renomeie o CSV para começar com `XP_`, `Nubank_`, `C6_` ou `MercadoPago_` e inclua `YYYY-MM-DD`.
- Para remover um lançamento (cartão/débito/receita/contas): apague a linha no Excel e envie o arquivo atualizado.
