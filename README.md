# Displacement Analysis System

Sistema modular de análise de deslocamento de equipes operacionais.

## 📁 Estrutura do Projeto

```
src/
├── config/                 # Configurações e settings
│   ├── __init__.py
│   └── settings.py         # Configurações centralizadas
├── core/                   # Núcleo da aplicação
│   ├── __init__.py
│   ├── models.py           # Modelos de domínio (DTOs)
│   └── utils.py            # Utilitários (datetime, columns)
├── services/               # Serviços de negócio
│   ├── __init__.py
│   ├── data_loader.py      # Carregamento de dados CSV
│   ├── calculator.py       # Cálculo de métricas
│   ├── aggregator.py       # Agregação por equipe/dia
│   └── pipeline.py         # Orquestração do pipeline
├── reports/                # Geração de relatórios
│   ├── __init__.py
│   ├── docx_builder.py     # Builder para documentos Word
│   └── report_generator.py # Gerador de relatórios ABNT
├── data/                   # Dados de entrada
│   └── deslocamento.csv    # Arquivo de dados
├── __init__.py             # Package init
├── __main__.py             # Execução como módulo
└── main.py                 # Ponto de entrada principal

result/                     # Saída (gerado automaticamente)
├── deslocamento_calculado.csv
├── medias_por_equipe_dia.csv
├── medias_Improdutivas_por_equipe_dia.csv
└── relatorio_analise_equipes.docx
```

## 🚀 Instalação

### Pré-requisitos
- Python 3.10+
- pip

### Setup

1. Clone o repositório:
```bash
git clone https://github.com/alyssonlcss/compute_and_analyze_displacement.git
cd compute_and_analyze_displacement
```

2. Crie um ambiente virtual:
```bash
python -m venv .venv
```

3. Ative o ambiente virtual:
```bash
# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

4. Instale as dependências:
```bash
pip install -r requirements.txt
```

## 📊 Uso

### Executar a análise completa:

```bash
# Opção 1: Executar como módulo
python -m src.main

# Opção 2: Executar diretamente
python src/main.py
```

### Saídas geradas:

| Arquivo | Descrição |
|---------|-----------|
| `result/deslocamento_calculado.csv` | Dados com métricas calculadas |
| `result/medias_por_equipe_dia.csv` | Médias produtivas por equipe/dia |
| `result/medias_Improdutivas_por_equipe_dia.csv` | Médias improdutivas por equipe/dia |
| `result/relatorio_analise_equipes.docx` | Relatório ABNT formatado |

## 📈 Métricas Calculadas

| Métrica | Descrição | Cálculo |
|---------|-----------|---------|
| `TempPrepEquipe_min` | Tempo de preparação | A_Caminho - (PrevLiberada ou Despachada) |
| `TempExe_min` | Tempo de execução | Liberada - No_Local |
| `TempDesl_min` | Tempo de deslocamento | No_Local - A_Caminho |
| `InterReg_min` | Intervalo regulamentar | Fim_Intervalo - Inicio_Intervalo |
| `AtrasLogin_min` | Atraso no login | 1º Login - Inicio_Calendario |

## 🏗️ Arquitetura

O projeto segue os princípios de **Clean Architecture**:

- **Config**: Configurações centralizadas e injetáveis
- **Core**: Modelos de domínio e utilitários puros
- **Services**: Lógica de negócio encapsulada em serviços
- **Reports**: Geração de documentos desacoplada

### Padrões utilizados:

- **Dependency Injection**: Settings injetáveis em todos os serviços
- **Builder Pattern**: DocxBuilder para construção fluente de documentos
- **Pipeline Pattern**: ProcessingPipeline para orquestração
- **Repository Pattern**: DataLoaderService para acesso a dados
- **Single Responsibility**: Cada módulo tem uma responsabilidade única

## 🔧 Configuração

As configurações estão em `src/config/settings.py`:

```python
from src.config import get_settings

settings = get_settings()

# Acessar configurações
print(settings.files.input_file)
print(settings.metrics.tempo_util_meta)
```

## 📝 Metas de Análise

| Métrica | Meta Produtivo | Meta Improdutivo |
|---------|----------------|------------------|
| TempExe_min | 50 min | 20 min |
| InterReg_min | 60 min | 60 min |
| Utilização | 85% de 468 min | 85% de 468 min |

## 📋 Glossário de Métricas (Original)

| Sigla | Descrição |
|-------|-----------|
| HT total | Deslocamento + execução (valor/dia) |
| TR Ordem | Tempo de reparo (valor/ordem) |
| TL Ordem | Tempo de deslocamento (valor/ordem) |
| HT Ordem | Deslocamento + execução (valor/ordem)
| tempo_padrao | Tempo padrão de reparo - expectativa |
| Retorno a base | valor/dia |
| Horas Extras | valor/dia |

## 🧪 Desenvolvimento

### Linting:
```bash
pip install black isort flake8
black src/
isort src/
flake8 src/
```

## 📄 Licença

MIT License

## 👤 Autor

Alysson - [@alyssonlcss](https://github.com/alyssonlcss)