# AGENTS.md

## Objetivo
Padronizar como o Codex deve operar este projeto, especialmente em relacao a atualizacoes do Docker apos mudancas de codigo.

## Regra Principal: Atualizacao do Docker
Sempre que houver alteracao em codigo executado pelo app, o Codex deve rebuildar e subir o container.

Arquivos que exigem rebuild (exemplos):
- `streamlit_app.py`
- `pf/**/*.py`
- `requirements.txt`
- `Dockerfile`
- `docker-compose.yml` (quando muda comportamento do servico)

Comando padrao:
```powershell
docker compose up -d --build
```

## Quando NAO precisa rebuild
Se a mudanca for somente em dados/arquivos montados por volume, normalmente nao precisa rebuild:
- `data/**`
- `templates/**`
- `raw_data/**`
- `config/**` (quando apenas conteudo de JSON)

Nesses casos, usar:
```powershell
docker compose up -d
```

## Verificacao minima apos subir
Executar e validar:
```powershell
docker ps
docker logs personal-finance --tail 80
```

Confirmar no log que o Streamlit subiu e informar URL:
- `http://localhost:8501`

## Regra de comunicacao
Ao finalizar mudancas de script, o Codex deve:
1. Informar que fez `up -d --build`.
2. Informar status do container.
3. Avisar se o usuario precisa refresh no navegador (`Ctrl+F5`).
