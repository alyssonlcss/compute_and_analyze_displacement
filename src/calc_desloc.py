import os
import pandas as pd
import numpy as np

from gerar_relatorio import gerar_relatorio_abnt2

# === Config ===
SRC_FILE = 'deslocamento.csv'
OUT_DIR = '../result'  # diretório relativo
CSV_OUT = os.path.join(OUT_DIR, 'deslocamento_calculado.csv')
MEDIAS_OUT = os.path.join(OUT_DIR, 'medias_por_equipe_dia.csv')
MEDIAS_IMPRODUTIVAS_OUT = os.path.join(OUT_DIR, 'medias_Improdutivas_por_equipe_dia.csv')

os.makedirs(OUT_DIR, exist_ok=True)

# === Load com codificação Latin-1 ===
df = pd.read_csv(SRC_FILE, dtype=str, encoding='latin1')

# Helper para escolher coluna entre candidatos
def pick_col(candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

# Mapeamento robusto de nomes
col_despachada        = pick_col(['Despachada'])
col_a_caminho         = pick_col(['A_Caminho'])
col_no_local          = pick_col(['No_Local'])
col_liberada          = pick_col(['Liberada'])
col_inicio_intervalo  = pick_col(['Inicio Intervalo','Início Intervalo','Inicio_Intervalo','Início_Intervalo'])
col_fim_intervalo     = pick_col(['Fim Intervalo','Fim_Intervalo'])
col_inicio_calendario = pick_col(['Inicio Calendario','Início Calendario','Inicio_Calendario','Início_Calendario'])
col_primeiro_login    = pick_col(['1º Login','1º LogIn','1º Login Corrigido','1º Login Corrigido'])

# Parse de datas/horários (dd/mm/aaaa HH:MM)
def parse_dt(series):
    return pd.to_datetime(series, dayfirst=True, errors='coerce')

# Cria colunas datetime
if col_despachada:        df['Despachada_dt']        = parse_dt(df[col_despachada])
if col_a_caminho:         df['A_Caminho_dt']         = parse_dt(df[col_a_caminho])
if col_no_local:          df['No_Local_dt']          = parse_dt(df[col_no_local])
if col_liberada:          df['Liberada_dt']          = parse_dt(df[col_liberada])
if col_inicio_intervalo:  df['InicioIntervalo_dt']   = parse_dt(df[col_inicio_intervalo])
if col_fim_intervalo:     df['FimIntervalo_dt']      = parse_dt(df[col_fim_intervalo])
if col_inicio_calendario: df['InicioCalendario_dt']  = parse_dt(df[col_inicio_calendario])

# Primeiro Login (trata '0' e vazio como NaT); senão tenta usar 'Log In'
if col_primeiro_login:
    s = df[col_primeiro_login].replace({'0': np.nan, '': np.nan})
    df['PrimeiroLogin_dt'] = parse_dt(s)
else:
    col_login_alt = pick_col(['Log In','Login'])
    df['PrimeiroLogin_dt'] = parse_dt(df[col_login_alt]) if col_login_alt else pd.NaT

# PrevLiberada / PrevDespachada por Equipe (ordenado por Despachada)
col_equipe = pick_col(['Equipe'])
if col_equipe and 'Despachada_dt' in df:
    df = df.sort_values([col_equipe, 'Despachada_dt']).copy()
    df['PrevLiberada_dt']   = df.groupby(col_equipe)['Liberada_dt']  .shift(1)
    df['PrevDespachada_dt'] = df.groupby(col_equipe)['Despachada_dt'].shift(1)
else:
    df['PrevLiberada_dt']   = pd.NaT
    df['PrevDespachada_dt'] = pd.NaT

# Função para diferença em minutos (CORRIGIDA)
def diff_minutes(a, b):
    """Calcula diferença em minutos entre dois datetimes."""
    if pd.isna(a) or pd.isna(b):
        return np.nan
    return (a - b).total_seconds() / 60.0

# === NOVO CÁLCULO CORRIGIDO para TempPrepEquipe_min ===
temp_prep_values = []
for idx, row in df.iterrows():
    if pd.isna(row['A_Caminho_dt']):
        temp_prep_values.append(np.nan)
        continue
    
    # Calcula diferenças absolutas em minutos
    if pd.notna(row['PrevLiberada_dt']):
        diff_liberada = abs(diff_minutes(row['A_Caminho_dt'], row['PrevLiberada_dt']))
    else:
        diff_liberada = float('inf')
    
    if pd.notna(row['Despachada_dt']):
        diff_despachada = abs(diff_minutes(row['A_Caminho_dt'], row['Despachada_dt']))
    else:
        diff_despachada = float('inf')
    
    # Escolhe a data mais próxima
    if diff_liberada < diff_despachada:
        # Liberada anterior é mais próxima
        if pd.notna(row['PrevLiberada_dt']):
            temp_prep = diff_minutes(row['A_Caminho_dt'], row['PrevLiberada_dt'])
        else:
            temp_prep = np.nan
    else:
        # Despachada atual é mais próxima ou igual
        if pd.notna(row['Despachada_dt']):
            temp_prep = diff_minutes(row['A_Caminho_dt'], row['Despachada_dt'])
        else:
            temp_prep = np.nan
    
    temp_prep_values.append(temp_prep)

df['TempPrepEquipe_min'] = temp_prep_values

# --- Cálculos sem condicional ---
# TempExe = Liberada - No_local
df['TempExe_min']  = df.apply(lambda row: diff_minutes(row['Liberada_dt'], row['No_Local_dt']), axis=1) if 'Liberada_dt' in df and 'No_Local_dt' in df else np.nan
# TempDesl = No_local - A_Caminho
df['TempDesl_min'] = df.apply(lambda row: diff_minutes(row['No_Local_dt'], row['A_Caminho_dt']), axis=1) if 'No_Local_dt' in df and 'A_Caminho_dt' in df else np.nan
# InterReg = Fim_Intervalo - Início_Intervalo
df['InterReg_min'] = df.apply(lambda row: diff_minutes(row['FimIntervalo_dt'], row['InicioIntervalo_dt']), axis=1) if 'FimIntervalo_dt' in df and 'InicioIntervalo_dt' in df else np.nan
# AtrasLogin = 1º Login - InicioCalendario
df['AtrasLogin_min'] = df.apply(lambda row: diff_minutes(row['PrimeiroLogin_dt'], row['InicioCalendario_dt']), axis=1) if 'PrimeiroLogin_dt' in df and 'InicioCalendario_dt' in df else np.nan

# Arredonda para 2 casas
for c in ['TempPrepEquipe_min','TempExe_min','TempDesl_min','InterReg_min','AtrasLogin_min']:
    if c in df.columns:
        df[c] = df[c].round(2)

# === REORDENAÇÃO DAS COLUNAS ===
if col_despachada and col_despachada in df.columns:
    despachada_index = df.columns.get_loc(col_despachada)
    
    calculo_cols = [
        'TempPrepEquipe_min',
        'TempExe_min', 
        'TempDesl_min',
        'InterReg_min',
        'AtrasLogin_min'
    ]
    
    for col in calculo_cols:
        if col in df.columns:
            df = df[[c for c in df.columns if c != col] + [col]]
    
    cols = list(df.columns)
    
    for col in calculo_cols:
        if col in cols:
            cols.remove(col)
    
    if col_despachada in cols:
        despachada_index = cols.index(col_despachada)
        
        for col in reversed(calculo_cols):
            if col in df.columns:
                cols.insert(despachada_index, col)
        
        df = df[cols]

# Salva como CSV (separador vírgula, para Excel)
df.to_csv(CSV_OUT, index=False, encoding='utf-8')
print(f'Arquivo principal gerado:\n- {CSV_OUT}')
print(f'Colunas de cálculo posicionadas antes da coluna "{col_despachada}"')

# ============================================================================
# FUNÇÃO PARA CALCULAR MÉDIAS POR EQUIPE POR DIA
# ============================================================================
def calcular_medias_por_equipe_dia(df_filtrado, tipo='produtivas'):
    """
    Calcula as médias por equipe por dia para cada coluna calculada
    e salva em um novo arquivo CSV.
    """
    print(f"\n" + "="*60)
    print(f"CALCULANDO MÉDIAS {tipo.upper()} POR EQUIPE POR DIA")
    print("="*60)
    
    if df_filtrado.empty:
        print(f"AVISO: Nenhum registro {tipo} encontrado.")
        return None
    
    # Verifica se as colunas necessárias existem
    if col_equipe is None or col_equipe not in df_filtrado.columns:
        print("ERRO: Coluna 'Equipe' não encontrada no dataset.")
        return None
    
    # Tenta encontrar coluna de data (assume que Despachada tem data)
    col_data = col_despachada
    if col_data is None or col_data not in df_filtrado.columns:
        print("ERRO: Coluna de data não encontrada.")
        return None
    
    # Extrai a data da coluna Despachada (ignora hora)
    try:
        df_filtrado['Data'] = pd.to_datetime(df_filtrado[col_data], dayfirst=True, errors='coerce').dt.date
    except Exception as e:
        print(f"ERRO ao extrair datas: {e}")
        return None
    
    # Lista de colunas calculadas para análise
    colunas_calculadas = [
        'TempPrepEquipe_min',
        'TempExe_min', 
        'TempDesl_min',
        'InterReg_min',
        'AtrasLogin_min'
    ]
    
    # Verifica quais colunas existem no dataframe
    colunas_existentes = [col for col in colunas_calculadas if col in df_filtrado.columns]
    
    if not colunas_existentes:
        print("ERRO: Nenhuma coluna calculada encontrada no dataset.")
        return None
    
    print(f"Colunas para cálculo de médias: {', '.join(colunas_existentes)}")
    print(f"Total de registros {tipo}: {len(df_filtrado)}")
    
    # Agrupa por Equipe e Data, calculando a média de cada coluna calculada
    medias_por_dia = df_filtrado.groupby([col_equipe, 'Data'])[colunas_existentes].mean()
    medias_por_dia = medias_por_dia.round(2)
    medias_por_dia = medias_por_dia.reset_index()
    
    # Renomeia as colunas para indicar que são médias
    rename_dict = {col: f'Media_{col}' for col in colunas_existentes}
    medias_por_dia = medias_por_dia.rename(columns=rename_dict)
    
    # Ordena primeiro por Equipe (A-Z) e depois por Data
    medias_por_dia = medias_por_dia.sort_values([col_equipe, 'Data'])
    
    # === Adiciona linha de média de todos os dias para cada equipe ===
    lista_final = []
    equipes_unicas = medias_por_dia[col_equipe].unique()
    
    print(f"\nProcessando {len(equipes_unicas)} equipes...")
    
    for equipe in equipes_unicas:
        dados_equipe = medias_por_dia[medias_por_dia[col_equipe] == equipe].copy()
        lista_final.append(dados_equipe)
        
        media_geral = {}
        for col_original in colunas_existentes:
            col_media = f'Media_{col_original}'
            if col_media in dados_equipe.columns:
                valores = dados_equipe[col_media].dropna()
                if len(valores) > 0:
                    media_geral[col_media] = round(valores.mean(), 2)
                else:
                    media_geral[col_media] = np.nan
        
        linha_media_geral = {
            col_equipe: f"MédiaTodosDias{equipe}",
            'Data': 'GERAL'
        }
        linha_media_geral.update(media_geral)
        
        df_linha_media = pd.DataFrame([linha_media_geral])
        lista_final.append(df_linha_media)
        
        print(f"  - {equipe}: {len(dados_equipe)} dias processados")
    
    if lista_final:
        medias_completas = pd.concat(lista_final, ignore_index=True)
    else:
        medias_completas = pd.DataFrame()
    
    if medias_completas.empty:
        print(f"AVISO: Nenhuma média calculada para registros {tipo}.")
        return None
    
    # Reordena as colunas
    colunas_ordenadas = [col_equipe, 'Data'] + [f'Media_{col}' for col in colunas_existentes]
    colunas_disponiveis = [c for c in colunas_ordenadas if c in medias_completas.columns]
    medias_completas = medias_completas[colunas_disponiveis]
    
    # Estatísticas
    if not medias_completas.empty:
        linhas_media_geral = medias_completas[medias_completas[col_equipe].str.startswith('MédiaTodosDias', na=False)]
        
        print(f"\nEstatísticas {tipo}:")
        print(f"- Total de equipes: {len(equipes_unicas)}")
        print(f"- Dias com registros: {medias_por_dia['Data'].nunique() if not medias_por_dia.empty else 0}")
        print(f"- Registros diários: {len(medias_por_dia)}")
        print(f"- Linhas de 'MédiaTodosDias' adicionadas: {len(linhas_media_geral)}")
        print(f"- Total de linhas no arquivo final: {len(medias_completas)}")
    
    return medias_completas

# ============================================================================
# EXECUTAR O PROCESSAMENTO DE MÉDIAS
# ============================================================================
if __name__ == "__main__":
    
    # === 1. Filtrar dados produtivos e improdutivos ===
    print(f"\n" + "="*60)
    print("FILTRANDO DADOS PRODUTIVOS E IMPRODUTIVOS")
    print("="*60)
    
    # Localizar coluna de status
    col_status = None
    for col in df.columns:
        if col.lower() == 'status':
            col_status = col
            break
    
    if col_status is None:
        status_candidates = ['Status', 'Situação', 'Estado', 'Tipo', 'Classificação', 'Categoria']
        col_status = pick_col(status_candidates)
    
    df_produtivo = df.copy()
    df_improdutivo = pd.DataFrame()
    
    if col_status and col_status in df.columns:
        mask_improdutivo = df[col_status].astype(str).str.strip().str.lower() == 'improdutivo'
        df_improdutivo = df[mask_improdutivo].copy()
        df_produtivo = df[~mask_improdutivo].copy()
        
        print(f"Total de registros no arquivo: {len(df)}")
        print(f"Registros improdutivos encontrados: {len(df_improdutivo)}")
        print(f"Registros produtivos encontrados: {len(df_produtivo)}")
        
    else:
        print(f"AVISO: Coluna 'Status' não encontrada.")
        print(f"Total de registros: {len(df_produtivo)}")
    
    # === 2. Calcular médias para registros PRODUTIVOS ===
    print(f"\n" + "="*60)
    print("PROCESSANDO MÉDIAS PRODUTIVAS")
    print("="*60)
    
    medias_produtivas = calcular_medias_por_equipe_dia(df_produtivo, tipo='produtivas')
    
    if medias_produtivas is not None and not medias_produtivas.empty:
        medias_produtivas.to_csv(MEDIAS_OUT, index=False, encoding='utf-8')
        print(f"\nArquivo de médias produtivas salvo em:\n- {MEDIAS_OUT}")
    else:
        print("AVISO: Não foi possível gerar médias produtivas.")
    
    # === 3. Calcular médias para registros IMPRODUTIVOS ===
    print(f"\n" + "="*60)
    print("PROCESSANDO MÉDIAS IMPRODUTIVAS")
    print("="*60)
    
    medias_improdutivas = calcular_medias_por_equipe_dia(df_improdutivo, tipo='improdutivas')
    
    if medias_improdutivas is not None and not medias_improdutivas.empty:
        medias_improdutivas.to_csv(MEDIAS_IMPRODUTIVAS_OUT, index=False, encoding='utf-8')
        print(f"\nArquivo de médias improdutivas salvo em:\n- {MEDIAS_IMPRODUTIVAS_OUT}")
    else:
        print("AVISO: Não foi possível gerar médias improdutivas.")


    # === 4. Gerar relatório ABNT2 ===
    print(f"\n" + "="*60)
    print("GERANDO RELATÓRIO ABNT2")
    print("="*60)
    
    if medias_produtivas is not None or medias_improdutivas is not None:
        relatorio = gerar_relatorio_abnt2(medias_produtivas, medias_improdutivas, col_equipe)
        print("✓ Relatório gerado com sucesso!")
    else:
        print("✗ Não foi possível gerar relatório (nenhuma média disponível)")


    
    # === Resumo final ===
    print(f"\n" + "="*60)
    print("RESUMO DA EXECUÇÃO")
    print("="*60)
    print(f"1. Arquivo principal gerado: {CSV_OUT}")
    print(f"2. Médias produtivas geradas: {MEDIAS_OUT}")
    print(f"3. Médias improdutivas geradas: {MEDIAS_IMPRODUTIVAS_OUT}")
    print(f"4. Relatório ABNT2 gerado (se aplicável): { 'Sim' if (medias_produtivas is not None or medias_improdutivas is not None) else 'Não' }")

    
    print(f"\nProcessamento concluído!")