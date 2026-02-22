# Finanças Pessoais (v0) — README Técnico (para IA)

Objetivo: documentar como o app funciona **para outra IA conseguir manter/evoluir o repositório**, com foco em ingestão, persistência e deduplicação por **`row_hash`**.

## Visão geral

- UI: `streamlit_app.py` (Streamlit).
- Core (biblioteca): `pf/` (ingestão, importadores, Excel, DB, regras).
- Persistência: SQLite em `data/finance.sqlite`.
- Artefato de trabalho do usuário: `templates/financas.xlsx` (Excel unificado; o usuário edita e faz upload).

## Módulos chave (mapa rápido)

- `pf/db.py`: schema/migrations + funções de upsert/sync (inclui dedupe avançado de cartão).
- `pf/ingest.py`: ingestão de CSV/XLSX, scan de `raw_data/`, regras, sync do Excel unificado.
- `pf/importers/credit_card_csv.py`: parser de CSV de fatura + normalização de sinais + geração de `row_hash`.
- `pf/excel_unified.py`: cria/atualiza `financas.xlsx` e faz append/sync por `row_hash`.
- `pf/utils.py`: normalização, parsing e `sha256_text`/`sha256_file`.

## Modelo de dados (SQLite)

Criado por `pf/db.py:migrate`.

Tabela principal: `transactions`
- Chave lógica: `row_hash` (`UNIQUE INDEX idx_transactions_row_hash`).
- Campos importantes:
  - `txn_date`: data do evento (compra / lançamento).
  - `cash_date`: data de impacto no caixa (ex.: vencimento da fatura no cartão).
  - `amount`: **convenção interna**: despesa `< 0`, crédito/estorno `> 0`.
  - `payment_method`: `"credit_card" | "debit" | "income" | "household" | ..."`
  - `account`: nome do cartão/conta (ex.: `"XP"`).
  - `source`: id lógico do importador/cartão (ex.: `"xp"`, `"nubank"`).
  - `statement_due_date`/`statement_closing_date`: só para cartão.
  - `category`/`subcategory`/`person`/`reimbursable`/`notes`: campos “controlados pelo usuário”.
  - `source_file` + `source_hash`: rastreio e dedupe por arquivo.

Tabela `imports`
- Guarda `hash` (sha256 do arquivo) para pular reprocessamentos (exceto faturas “em aberto”).

Tabela `credit_card_statements`
- Metadados por cartão+vencimento (pago/fechado), usados na UI.

## Ingestão: CSV de cartão → DB → Excel

Fluxo disparado no sidebar de `streamlit_app.py`:

1) Descoberta de arquivos:
- `pf/ingest.py:scan_raw_data` varre `raw_data/**/*.(csv|xlsx)` e **ignora** `raw_data/old/`.
- Também aceita CSV via upload; uploads são salvos temporariamente e apagados após importar.

2) Identificação do cartão:
- `pf/importers/credit_card_csv.py:guess_card_id` usa heurística no nome do arquivo (`XP_`, `Nubank_`, `C6_`, `MercadoPago_`, `PortoBank_`).
- Caso especial: “Nubank Aline” vs “Nubank Renan” via substring `aline` no filename.

3) Datas da fatura por filename:
- `pf/importers/credit_card_csv.py:extract_statement_due_date_from_path` procura `YYYY-MM-DD` no nome.
- Se existe vencimento no filename:
  - `statement_due_date = vencimento`
  - `statement_closing_date` é derivado de `(vencimento, closing_day)` via `_compute_statement_closing_date_from_due`.
- Se não existe vencimento no filename:
  - cai no fallback por compra + config (`compute_card_closing_date`/`compute_card_due_date`).

4) “Fatura em aberto” e dedupe por hash do arquivo:
- `pf/ingest.py:ingest_credit_card_csv` calcula `file_hash = sha256_file(path)` e consulta `imports`.
- Se `due_dt` foi extraído do filename e `today <= due_dt`, a fatura é tratada como **em aberto** e o CSV é reprocessado sempre (permite re-download/substituição do arquivo até o vencimento).
- Caso contrário, se o hash já existir em `imports`, o ingest é pulado.
- Ao final de um ingest bem-sucedido, `pf/db.py:register_import` registra o `file_hash` em `imports`.

5) Leitura e mapeamento de colunas do CSV:
- `pf/importers/common.py:read_csv_flexible` tenta `utf-8-sig/utf-8/latin-1` e `; , \\t`.
- `pf/importers/credit_card_csv.py:import_credit_card_csv` exige encontrar:
  - data (`data|date|data da compra|...`)
  - descrição (`descrição|estabelecimento|title|merchant|...`)
  - valor (`valor|amount|valor (r$)|...`; prefere coluna que contenha `r$` quando há múltiplas)

6) Normalização de lançamentos:
- Ignora pagamentos de fatura (transferência interna) por `PAYMENT_PATTERNS`.
- Estornos/créditos:
  - Detecta por keywords (ex.: “estorno”, “reembolso”) e/ou `tipo` quando disponível.
  - Garante que crédito/estorno fique **positivo**.
- Compras/despesas ficam **negativas**.

7) Geração de `row_hash` (cartão)

Arquivo: `pf/importers/credit_card_csv.py`

O importador cria uma identidade por linha:

- `row_identity = card.id | txn_date | amount(2dp) | description | holder | installment | external_id`
- Para não colapsar compras “iguais” repetidas no mesmo CSV, ele mantém um contador determinístico por `row_identity` e adiciona `occurrence:N`.

Pseudocódigo:
```text
occurrence[row_identity] += 1
row_hash = sha256("credit_card|{row_identity}|occurrence:{occurrence}")
```

Além disso, ele cria `stable_key` (sem `description` e sem `installment`) para lidar com re-downloads em que só a descrição muda:
```text
stable_key = sha256("credit_card_stable|card.id|txn_date|amount|holder|external_id")
```
e marca `_stable_key_unique_in_file` para só usar esse fallback quando a chave for única dentro do arquivo.

## Deduplicação / upsert (o “porquê” do `row_hash`)

A função mais importante é `pf/db.py:upsert_credit_card_transactions`.

Ela tenta inserir/atualizar **sem duplicar lançamentos** e **sem apagar o que o usuário já categorizou**.

Pipeline (ordem lógica):

1) **Match por `row_hash` (novo formato)**
- Se já existe `transactions.row_hash == incoming.row_hash`:
  - atualiza campos “não do usuário” (datas, source, source_file, external_id etc.)
  - **nunca sobrescreve** categoria/subcategoria/pessoa/notas/reembolso; só preenche quando está em branco

2) **Migração de `row_hash` legado**
- Se o importador antigo usava outra fórmula, calcula hashes alternativos a partir de `_legacy_amount_file` e tenta migrar.

3) **Correção de bug histórico (amount=0)**
- Se achar linha antiga com `amount=0` por `(source_file, txn_date, description)`, corrige e atualiza.

4) **Dedupe por campos-chave (entre arquivos)**
- Procura candidatos por `(txn_date, cash_date, amount, account)` e `source` (com regras para aceitar `excel_credit_card`).
- Só faz merge quando há **um único** candidato (para não colapsar cobranças legítimas repetidas).
- Se achar, ele **atualiza o `row_hash`** do registro existente para o novo e aplica refresh.

5) **Fallback para “descrição mudou” (stable_key)**
- Quando `_stable_key_unique_in_file` é `True`, tenta achar uma linha no DB com mesmo `(source_file, txn_date, amount, source, person?, external_id?)`.
- Se achar e não for ambíguo, atualiza campos (sem inserir duplicata).

6) **Insert**
- Se nada casou, insere nova linha.

Invariantes desejadas (para manter consistência):
- `row_hash` deve ser tratado como ID imutável do lançamento para fins de categorização/sync.
- A coluna “Hash (oculto)” no Excel é esse `row_hash` e **não deve ser editada**.

## Excel unificado (`templates/financas.xlsx`)

O Excel é atualizado em dois sentidos:

### DB → Excel (append de novas faturas)
- Após importar CSVs, `streamlit_app.py` busca as linhas por `source_file` e chama:
  - `pf/excel_unified.py:append_credit_card_rows(...)`
- O append é dedupado por `row_hash` para não inserir linhas repetidas.

### Excel → DB (sync após upload)
- No upload do Excel, `streamlit_app.py` chama `pf/ingest.py:sync_unified_from_excel`.
- Para cada aba, o sync usa `row_hash` como chave:
  - Se `row_hash` existe: atualiza a transação correspondente.
  - Se `row_hash` falta: gera um hash determinístico (ex.: `debit_unified|date|amount|description`).
  - Para débito/receita/contas-casa: pode deletar no DB o que não está no Excel (`delete_missing=True`), tratando o Excel como “fonte da verdade”.

## Formatos de CSV (faturas) — o que o importador realmente usa

O importador é por *mapeamento de colunas*, não por “modelo fixo por banco”.

Mínimo necessário:
- **Data** (ex.: `date`, `Data`, `Data de Compra`)
- **Descrição** (ex.: `title`, `Estabelecimento`, `Descrição`)
- **Valor** (ex.: `amount`, `Valor`, `Valor (em R$)`)

Exemplos reais no repo:
- Nubank: `date,title,amount` (`,` + ISO dates)
- XP: `Data;Estabelecimento;Portador;Valor;Parcela` (`;` + PT-BR)
- C6: inclui `Valor (em R$)` e múltiplas colunas de moeda

## Onde mexer ao evoluir (guia para IA)

- Novo cartão:
  - adicionar em `config/cards.json` (id, name, closing_day, due_day, owner)
  - ajustar heurística em `pf/importers/credit_card_csv.py:_guess_card_id_from_path`
- Novo layout de CSV:
  - ampliar candidatos de coluna em `pf/importers/credit_card_csv.py` (date/desc/amount/tipo/id/parcela/portador)
- Ajustes de dedupe:
  - concentre mudanças em `pf/importers/credit_card_csv.py` (hashing) e `pf/db.py:upsert_credit_card_transactions` (merge)
  - qualquer mudança de hashing precisa considerar migração e impacto na coluna “Hash (oculto)” já existente em Excel/DB
