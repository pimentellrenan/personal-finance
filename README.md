# Personal Finance Dashboard

Sistema para acompanhar suas finanças pessoais (cartões de crédito, débito e receitas).

---

## 🐳 Rodando com Docker (para compartilhar na rede)

Pré-requisito: Docker Desktop instalado.

1) Suba o app:
```bash
docker compose up --build
```

2) Acesse o Streamlit:
- No mesmo computador: `http://localhost:8501`
- Em outro dispositivo na mesma rede (Wi‑Fi/lan): `http://<IP_DO_SEU_PC>:8501`

### Excel no Docker (baixar / enviar / sincronizar)

Em Docker não dá para “abrir o Excel” no servidor. O fluxo correto é:
1) Sidebar **🔄 Atualização** → **"📊 Abrir Excel (baixar)"** (faz download do `financas.xlsx`)
2) Edite no seu computador e salve
3) Volte no app → **"📤 Enviar Excel atualizado (.xlsx)"** → **"💾 Salvar upload"**
4) Clique em **"✅ Sincronizar Excel → DB"**

### Acesso fora de casa (opcional)

Se quiser acessar pela internet (fora da rede local), use uma VPN (ex.: Tailscale/ZeroTier) ou faça port‑forward do roteador para a porta `8501`.

## 📊 Dashboard Principal

O Dashboard mostra uma visão geral das suas finanças:

- **Fluxo de Caixa**: Gráfico mostrando entradas, saídas e saldo acumulado ao longo do tempo
- **Despesas por Categoria**: Visualização das suas despesas organizadas por categoria (Alimentação, Habitação, etc.)
- **Faturas de Cartão**: Lista das faturas de cartão de crédito seguindo a regra do **Acerto Mensal** (XP/Nubank Aline no mês; demais cartões no mês anterior)
- **Orçamento por Categoria**: Acompanhe quanto você gastou vs. quanto planejou para cada categoria

### Filtros

Use os filtros no topo da página para:
- Selecionar o período (mês/ano) que deseja visualizar
- Filtrar por tipo de transação (Despesas, Receitas, ou Ambos)

---

## 🔄 Rotina (Importação de Dados)

Aqui você importa e categoriza seus gastos de cartão de crédito.

### ⭐ Excel Unificado (recomendado)

O app usa o arquivo `templates/financas.xlsx` como planilha principal, com 4 abas:
**Cartão**, **Débitos**, **Receitas** e **Contas Casa**.

- Para abrir/criar: use o botão **"📊 Abrir Excel de Finanças"**
- Para baixar/abrir: use o botão **"📊 Abrir Excel (baixar)"** (e, fora do Docker, você também pode usar **"🖥️ Abrir Excel (no servidor)"**)
- Para aplicar no dashboard: use **"✅ Sincronizar Excel → DB"** (Excel → Banco)
- Para conferir os dados após edição no Excel: use **"✅ Sincronizar Excel → DB"** e valide no Dashboard.

### Passo 1: Importar CSVs do Cartão

1. Baixe os arquivos CSV da fatura dos seus cartões (Nubank, XP, etc.)
2. Coloque os arquivos na pasta `raw_data/` (organize por ano/mês se desejar)
3. Clique no botão **"🔄 Importar CSVs do raw_data"**
4. O Excel será gerado automaticamente e aberto para você

**Importante (sem duplicar / sem sobrescrever):**
- O app lê **todos os CSVs disponíveis em `raw_data/`** (incluindo subpastas), **exceto** `raw_data/old/`.
- Você pode manter CSVs antigos em `raw_data/`: faturas em aberto (hoje ≤ vencimento no nome do arquivo) são reprocessadas; faturas antigas são dedupadas por hash do arquivo.
- Para arquivar faturas **fechadas e já importadas** sem que sejam reprocessadas/escaneadas, mova para `raw_data/old/` (essa pasta é ignorada).
- A planilha `templates/financas.xlsx` só recebe lançamentos novos (dedupe pelo `Hash (oculto)`), então não sobrescreve linhas já categorizadas/ajustadas.
- Evite marcar **"Forçar reimport"** nas opções avançadas, a menos que você realmente queira reprocessar um CSV.

### Passo 2: Categorizar no Excel

1. O arquivo `templates/cartao_credito.xlsx` será aberto automaticamente
2. Preencha as colunas:
   - **Categoria**: Escolha a categoria principal (ex: Alimentação, Habitação)
   - **Subcategoria**: Ao selecionar a categoria, a subcategoria será filtrada automaticamente
   - **Portador**: Quem fez a compra (você, cônjuge, etc.)
   - **Reembolsável**: Se é um gasto que será reembolsado (Sim/Não)
3. Salve o arquivo Excel

### Passo 3: Aplicar as Categorias

1. Volte para o app
2. Clique em **"✅ Aplicar do Excel"**
3. Suas categorias serão salvas e o Dashboard será atualizado automaticamente

---

## 💵 Débitos e Receitas Manuais

Para lançar débitos (PIX, transferências) ou receitas manualmente:

### Débitos

1. Abra o arquivo `raw_data/debitos.xlsx`
2. Preencha as colunas:
   - **Data**: Data da transação
   - **Vencimento**: Data do impacto no caixa
   - **Descrição**: Descrição do gasto
   - **Valor (positivo)**: Valor em reais (sempre positivo, o sistema converte para negativo)
   - **Categoria** e **Subcategoria**: Classificação do gasto
3. Salve o arquivo
4. No app, clique em **"📥 Sincronizar Débitos"**

### Receitas

1. Abra o arquivo `raw_data/receitas.xlsx`
2. Preencha as colunas:
   - **Data**: Data da receita
   - **Vencimento**: Data do recebimento
   - **Descrição**: Origem da receita (salário, freelance, etc.)
   - **Valor (positivo)**: Valor em reais
   - **Categoria**: Tipo de receita
3. Salve o arquivo
4. No app, clique em **"📥 Sincronizar Receitas"**

---

## 💳 Transações

Nesta aba você pode:

- **Visualizar todas as transações** (cartão, débito, receitas)
- **Criar lançamentos manuais** diretamente no app
- **Editar categorias** de transações existentes usando dropdowns

### Filtros

- **Período**: Escolha o mês/ano
- **Tipo**: Despesas, Receitas ou Ambos
- **Método de Pagamento**: Cartão, Débito, PIX, etc.
- **Categoria**: Filtre por categoria específica

---

## 💰 Acerto Mensal

Regra fixa do acerto (início do mês):
- **Cartões** (usar a **data de vencimento**, não a data da compra):
  - **XP** e **Nubank Aline** → entram no mês do acerto (ex.: fatura 05/02 entra no acerto de Fevereiro)
  - **Nubank Renan**, **C6**, **Mercado Pago** (demais cartões) → entram com a fatura do **mês anterior**
- **Débitos/PIX**: mês anterior completo
- **Contas da Casa**: mês corrente pela **Data Pagamento**

Exemplo: no acerto de 05/Fevereiro, entram débitos de Janeiro, faturas de Nubank/C6/MP com vencimento em Janeiro e faturas de XP/Nubank Aline com vencimento até 05/Fevereiro, além das Contas da Casa pagas em Fevereiro.

**Nota:** o **Acerto** fica no **Dashboard** (com detalhes). A página separada **"Acerto Mensal"** não é necessária.

---

## ⚙️ Configurações

### Categorias

Você pode personalizar as categorias em:
- `config/categories_expenses.json` - Categorias de despesas
- `config/categories_income.json` - Categorias de receitas

### Orçamento

Defina limites mensais por categoria no arquivo:
- `config/budgets.json`

Exemplo:
```json
{
  "budgets": {
    "Alimentação": 2500,
    "Habitação": 8000,
    "Transporte": 1500
  },
  "total_monthly_budget": 20000
}
```

### Cartões de Crédito

Configure seus cartões em:
- `config/cards.json`

**Nota:** Os campos `closing_day` e `due_day` são mantidos para compatibilidade e lançamentos manuais, mas **não são mais usados na importação de CSVs**. As datas de fechamento e vencimento são extraídas automaticamente do nome dos arquivos CSV (ex: `XP_Fatura2026-02-05.csv` indica vencimento em 05/02/2026).

Exemplo:
```json
{
  "cards": [
    {
      "id": "nubank",
      "name": "Nubank",
      "closing_day": 12,
      "due_day": 19
    }
  ]
}
```

### Backup Automático

O sistema cria automaticamente um backup do banco de dados (SQLite) na pasta `raw_data/backup_db.zip` toda vez que o app é aberto, substituindo o backup anterior.

---

## 📋 Regras Automáticas (Opcional)

Para categorizar automaticamente transações recorrentes, edite o arquivo:
- `config/rules.json`

Exemplo:
```json
{
  "rules": [
    {
      "pattern": "IFOOD|RAPPI|UBER EATS",
      "category": "Alimentação",
      "subcategory": "Delivery"
    }
  ]
}
```

O sistema aplicará essas regras automaticamente ao importar novos CSVs.

---

## 🗂️ Estrutura de Pastas

```
raw_data/          → Coloque aqui seus CSVs de cartão
  2025/
    11/
      XP_Fatura2025-11-05.csv
      Nubank_2025-12-19.csv
  debitos.xlsx     → Arquivo para lançar débitos manuais
  receitas.xlsx    → Arquivo para lançar receitas manuais

templates/         → Planilhas geradas pelo app
  cartao_credito.xlsx  → Categorização do cartão
  saida_debitos.xlsx   → Consulta de débitos
  saida_receitas.xlsx  → Consulta de receitas

config/            → Configurações
  categories_expenses.json
  categories_income.json
  budgets.json
  cards.json
  rules.json

data/              → Banco de dados local (gerado automaticamente)
```

---

## 💡 Dicas

1. **Organize seus CSVs**: Crie subpastas por ano/mês em `raw_data/` para facilitar
2. **Defina orçamentos**: Configure limites em `config/budgets.json` para acompanhar seus gastos
3. **Use regras**: Configure regras em `config/rules.json` para categorizar automaticamente gastos recorrentes
4. **Reembolsos**: Marque gastos reembolsáveis para não contabilizá-los como despesa real
5. **Referências**: Use o campo "Referência" para ligar despesas reembolsáveis às receitas de reembolso
