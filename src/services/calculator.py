"""
Calculator service for computing displacement metrics.

This module contains the business logic for calculating various time-based
metrics from displacement records.
"""

from typing import Optional, Dict, List
import pandas as pd
import numpy as np
import logging

from ..config import Settings, get_settings
from ..core.utils import DateTimeUtils

logger = logging.getLogger(__name__)


class CalculatorService:
    """
    Service for calculating displacement metrics.
    
    Implements all time-based calculations for team performance analysis,
    including preparation time, execution time, displacement time, and more.
    """
    
    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialize the calculator service.
        
        Args:
            settings: Application settings. If None, uses default settings.
        """
        self._settings = settings or get_settings()
        self._dt_utils = DateTimeUtils()
    
    def process(
        self,
        df: pd.DataFrame,
        columns: Dict[str, Optional[str]]
    ) -> pd.DataFrame:
        """
        Process DataFrame and calculate all metrics.
        
        Args:
            df: Input DataFrame with raw displacement data
            columns: Resolved column name mappings
            
        Returns:
            DataFrame with calculated metrics added
        """
        logger.info("Starting metric calculations")
        
        # Create a copy to avoid modifying original
        result = df.copy()
        
        # Parse datetime columns
        result = self._parse_datetime_columns(result, columns)
        
        # Add previous record references
        result = self._add_previous_references(result, columns)
        
        # Calculate metrics
        result = self._calculate_temp_prep_equipe(result)
        result = self._copy_temp_exe(result, columns)  # Usa TR Ordem do CSV
        result = self._copy_temp_desl(result, columns)  # Usa TL Ordem do CSV
        result = self._copy_tempo_padrao(result, columns)  # Copia tempo_padrao do CSV
        result = self._calculate_jornada(result, columns)  # Calcula Jornada
        result = self._calculate_inter_reg(result)
        result = self._calculate_atras_login(result)
        result = self._calculate_temp_sem_ordem(result, columns)  # Calcula TempSemOrdem por dia
        
        # Round calculated columns
        result = self._round_calculated_columns(result)
        
        # Reorder columns
        result = self._reorder_columns(result, columns)
        
        logger.info("Metric calculations completed")
        
        return result
    
    def _parse_datetime_columns(
        self,
        df: pd.DataFrame,
        columns: Dict[str, Optional[str]]
    ) -> pd.DataFrame:
        """Parse all datetime columns."""
        datetime_mappings = [
            ("despachada", "Despachada_dt"),
            ("a_caminho", "A_Caminho_dt"),
            ("no_local", "No_Local_dt"),
            ("liberada", "Liberada_dt"),
            ("inicio_intervalo", "InicioIntervalo_dt"),
            ("fim_intervalo", "FimIntervalo_dt"),
            ("inicio_calendario", "InicioCalendario_dt"),
        ]
        
        for col_key, dt_col in datetime_mappings:
            col_name = columns.get(col_key)
            if col_name and col_name in df.columns:
                df[dt_col] = self._dt_utils.parse_datetime(df[col_name])
        
        # Handle primeiro_login specially (treats '0' and empty as NaT)
        col_primeiro_login = columns.get("primeiro_login")
        if col_primeiro_login and col_primeiro_login in df.columns:
            s = df[col_primeiro_login].replace({"0": np.nan, "": np.nan})
            df["PrimeiroLogin_dt"] = self._dt_utils.parse_datetime(s)
        else:
            col_login_alt = columns.get("login_alt")
            if col_login_alt and col_login_alt in df.columns:
                df["PrimeiroLogin_dt"] = self._dt_utils.parse_datetime(df[col_login_alt])
            else:
                df["PrimeiroLogin_dt"] = pd.NaT
        
        return df
    
    def _add_previous_references(
        self,
        df: pd.DataFrame,
        columns: Dict[str, Optional[str]]
    ) -> pd.DataFrame:
        """
        Add previous record references per team.
        
        IMPORTANT: Sort by Equipe (A-Z) and A_Caminho (old to new) first,
        as the order is critical for TempPrepEquipe_min calculation.
        """
        col_equipe = columns.get("equipe")
        
        if col_equipe and "A_Caminho_dt" in df.columns:
            # Sort by Equipe (A-Z) and A_Caminho (oldest to newest)
            logger.info("Sorting data by Equipe (A-Z) and A_Caminho (old to new)")
            df = df.sort_values([col_equipe, "A_Caminho_dt"]).copy()
            df["PrevLiberada_dt"] = df.groupby(col_equipe)["Liberada_dt"].shift(1)
            df["PrevDespachada_dt"] = df.groupby(col_equipe)["Despachada_dt"].shift(1)
        elif col_equipe and "Despachada_dt" in df.columns:
            # Fallback to Despachada if A_Caminho not available
            logger.warning("A_Caminho_dt not available, falling back to Despachada_dt for sorting")
            df = df.sort_values([col_equipe, "Despachada_dt"]).copy()
            df["PrevLiberada_dt"] = df.groupby(col_equipe)["Liberada_dt"].shift(1)
            df["PrevDespachada_dt"] = df.groupby(col_equipe)["Despachada_dt"].shift(1)
        else:
            df["PrevLiberada_dt"] = pd.NaT
            df["PrevDespachada_dt"] = pd.NaT
        
        return df
    
    def _calculate_temp_prep_equipe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate TempPrepEquipe_min.
        
        TempPrepEquipe = A_Caminho - max(PrevLiberada, Despachada)
        Se InicioIntervalo e FimIntervalo estiverem dentro desse intervalo, subtrai esse tempo.
        """
        calc_col = self._settings.calculated.temp_prep_equipe
        temp_prep_values = []
        
        for _, row in df.iterrows():
            if pd.isna(row.get("A_Caminho_dt")):
                temp_prep_values.append(np.nan)
                continue
            
            prev_liberada = row.get("PrevLiberada_dt")
            despachada = row.get("Despachada_dt")
            a_caminho = row["A_Caminho_dt"]
            
            # Determine the most recent date between PrevLiberada and Despachada
            if pd.notna(prev_liberada) and pd.notna(despachada):
                reference_dt = max(prev_liberada, despachada)
            elif pd.notna(prev_liberada):
                reference_dt = prev_liberada
            elif pd.notna(despachada):
                reference_dt = despachada
            else:
                temp_prep_values.append(np.nan)
                continue
            

            # Calcula TempPrepEquipe = A_Caminho - reference_dt
            temp_prep = self._dt_utils.diff_minutes(a_caminho, reference_dt)

            # Desconta intervalo se estiver totalmente dentro do período
            inicio_intervalo = row.get("InicioIntervalo_dt")
            fim_intervalo = row.get("FimIntervalo_dt")
            log_msg = (
                f"TempPrepEquipe: ref={reference_dt}, a_caminho={a_caminho}, "
                f"inicio_intervalo={inicio_intervalo}, fim_intervalo={fim_intervalo}, temp_prep={temp_prep}"
            )
            if (
                pd.notna(inicio_intervalo) and pd.notna(fim_intervalo)
                and reference_dt <= inicio_intervalo <= fim_intervalo <= a_caminho
            ):
                intervalo_min = self._dt_utils.diff_minutes(fim_intervalo, inicio_intervalo)
                if pd.notna(intervalo_min):
                    temp_prep -= intervalo_min
                    log_msg += f" | intervalo descontado: {intervalo_min}"
            logger.debug(log_msg)
            temp_prep_values.append(temp_prep)

        df[calc_col] = temp_prep_values
        return df
    
    def _copy_temp_exe(self, df: pd.DataFrame, columns: Dict[str, Optional[str]]) -> pd.DataFrame:
        """Copy TempExe_min from TR Ordem column (already exists in CSV)."""
        calc_col = self._settings.calculated.temp_exe
        col_tr_ordem = columns.get("tr_ordem")
        
        if col_tr_ordem and col_tr_ordem in df.columns:
            # Convert to numeric, handling comma as decimal separator
            df[calc_col] = pd.to_numeric(
                df[col_tr_ordem].astype(str).str.replace(",", "."),
                errors="coerce"
            )
            logger.info(f"TempExe_min copied from '{col_tr_ordem}'")
        else:
            logger.warning("TR Ordem column not found, TempExe_min will be NaN")
            df[calc_col] = np.nan
        
        return df
    
    def _copy_temp_desl(self, df: pd.DataFrame, columns: Dict[str, Optional[str]]) -> pd.DataFrame:
        """Copy TempDesl_min from TL Ordem column (already exists in CSV)."""
        calc_col = self._settings.calculated.temp_desl
        col_tl_ordem = columns.get("tl_ordem")
        
        if col_tl_ordem and col_tl_ordem in df.columns:
            # Convert to numeric, handling comma as decimal separator
            df[calc_col] = pd.to_numeric(
                df[col_tl_ordem].astype(str).str.replace(",", "."),
                errors="coerce"
            )
            logger.info(f"TempDesl_min copied from '{col_tl_ordem}'")
        else:
            logger.warning("TL Ordem column not found, TempDesl_min will be NaN")
            df[calc_col] = np.nan
        
        return df
    
    def _copy_tempo_padrao(self, df: pd.DataFrame, columns: Dict[str, Optional[str]]) -> pd.DataFrame:
        """Copy TempoPadrao_min from tempo_padrao column (already exists in CSV)."""
        calc_col = self._settings.calculated.tempo_padrao
        col_tempo_padrao = columns.get("tempo_padrao")
        
        if col_tempo_padrao and col_tempo_padrao in df.columns:
            # Convert to numeric, handling comma as decimal separator
            df[calc_col] = pd.to_numeric(
                df[col_tempo_padrao].astype(str).str.replace(",", "."),
                errors="coerce"
            )
            logger.info(f"TempoPadrao_min copied from '{col_tempo_padrao}'")
        else:
            logger.warning("tempo_padrao column not found, TempoPadrao_min will be NaN")
            df[calc_col] = np.nan
        
        return df
    
    def _calculate_jornada(self, df: pd.DataFrame, columns: Dict[str, Optional[str]]) -> pd.DataFrame:
        """Calculate Jornada_min = Fim Calendario - Inicio Calendario."""
        calc_col = self._settings.calculated.jornada
        col_fim_calendario = columns.get("fim_calendario")
        
        if col_fim_calendario and col_fim_calendario in df.columns:
            # Parse Fim Calendario
            df["FimCalendario_dt"] = self._dt_utils.parse_datetime(df[col_fim_calendario])
        
        if "FimCalendario_dt" in df.columns and "InicioCalendario_dt" in df.columns:
            df[calc_col] = df.apply(
                lambda row: self._dt_utils.diff_minutes(
                    row["FimCalendario_dt"], row["InicioCalendario_dt"]
                ),
                axis=1
            )
            logger.info("Jornada_min calculated (Fim Calendario - Inicio Calendario)")
        else:
            logger.warning("Fim/Inicio Calendario columns not found, Jornada_min will be NaN")
            df[calc_col] = np.nan
        
        return df
    
    def _calculate_temp_exe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate TempExe_min = Liberada - No_Local."""
        calc_col = self._settings.calculated.temp_exe
        
        if "Liberada_dt" in df.columns and "No_Local_dt" in df.columns:
            df[calc_col] = df.apply(
                lambda row: self._dt_utils.diff_minutes(
                    row["Liberada_dt"], row["No_Local_dt"]
                ),
                axis=1
            )
        else:
            df[calc_col] = np.nan
        
        return df
    
    def _calculate_temp_desl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate TempDesl_min = No_Local - A_Caminho."""
        calc_col = self._settings.calculated.temp_desl
        
        if "No_Local_dt" in df.columns and "A_Caminho_dt" in df.columns:
            df[calc_col] = df.apply(
                lambda row: self._dt_utils.diff_minutes(
                    row["No_Local_dt"], row["A_Caminho_dt"]
                ),
                axis=1
            )
        else:
            df[calc_col] = np.nan
        
        return df
    
    def _calculate_inter_reg(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate InterReg_min = Fim_Intervalo - Inicio_Intervalo."""
        calc_col = self._settings.calculated.inter_reg
        
        if "FimIntervalo_dt" in df.columns and "InicioIntervalo_dt" in df.columns:
            df[calc_col] = df.apply(
                lambda row: self._dt_utils.diff_minutes(
                    row["FimIntervalo_dt"], row["InicioIntervalo_dt"]
                ),
                axis=1
            )
        else:
            df[calc_col] = np.nan
        
        return df
    
    def _calculate_atras_login(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate AtrasLogin_min = PrimeiroLogin - InicioCalendario."""
        calc_col = self._settings.calculated.atras_login
        
        if "PrimeiroLogin_dt" in df.columns and "InicioCalendario_dt" in df.columns:
            df[calc_col] = df.apply(
                lambda row: self._dt_utils.diff_minutes(
                    row["PrimeiroLogin_dt"], row["InicioCalendario_dt"]
                ),
                axis=1
            )
        else:
            df[calc_col] = np.nan
        
        return df
    
    def _calculate_temp_sem_ordem(
        self,
        df: pd.DataFrame,
        columns: Dict[str, Optional[str]]
    ) -> pd.DataFrame:
        """
        Implementa a lógica detalhada para TempSemOrdem conforme especificação do usuário.
        """
        calc_col = self._settings.calculated.temp_sem_ordem
        col_equipe = columns.get("equipe")
        col_despachada = columns.get("despachada")
        col_liberada = columns.get("liberada")
        col_inicio_intervalo = columns.get("inicio_intervalo")
        col_fim_intervalo = columns.get("fim_intervalo")
        col_intervalo = "Intervalo"
        col_logoff_corrigido = "Log Off Corrigido"
        col_hora_ultima_ordem = "Hora Ultima Ordem"
        col_retorno_base = columns.get("retorno_base")

        # Garantir datas em datetime
        for col in [col_despachada, col_liberada, col_inicio_intervalo, col_fim_intervalo, col_logoff_corrigido, col_hora_ultima_ordem]:
            if col and col in df.columns:
                df[col+"_dt"] = self._dt_utils.parse_datetime(df[col])


        for equipe, grupo in df.groupby(col_equipe):
            grupo = grupo.sort_values(col_despachada+"_dt").reset_index(drop=True)
            temp_sem_ordem = [float('nan')] * len(grupo)
            entre_ordem = 0.0
            # Calcula entre_ordem (soma dos intervalos entre Despachada e Liberada, a partir da segunda ordem)
            for i in range(1, len(grupo)):
                despachada = grupo.loc[i, col_despachada+"_dt"]
                liberada = grupo.loc[i-1, col_liberada+"_dt"] if col_liberada+"_dt" in grupo.columns else None
                if pd.notna(despachada) and pd.notna(liberada) and despachada > liberada:
                    entre_ordem += self._dt_utils.diff_minutes(despachada, liberada)

            # fora_ordem
            intervalo = grupo.loc[0, col_intervalo] if col_intervalo in grupo.columns else None
            try:
                intervalo_float = float(str(intervalo).replace(',', '.')) if pd.notna(intervalo) and intervalo != '' else None
            except Exception:
                intervalo_float = None
            logoff_corrigido = grupo.loc[len(grupo)-1, col_logoff_corrigido+"_dt"] if col_logoff_corrigido+"_dt" in grupo.columns else None
            hora_ultima_ordem = grupo.loc[len(grupo)-1, col_hora_ultima_ordem+"_dt"] if col_hora_ultima_ordem+"_dt" in grupo.columns else None
            retorno_base = grupo.loc[len(grupo)-1, col_retorno_base] if col_retorno_base in grupo.columns else 0
            try:
                retorno_base_float = float(str(retorno_base).replace(',', '.')) if pd.notna(retorno_base) and retorno_base != '' else 0.0
            except Exception:
                retorno_base_float = 0.0

            fora_ordem = 0.0
            if pd.notna(logoff_corrigido) and pd.notna(hora_ultima_ordem):
                base_fora_ordem = self._dt_utils.diff_minutes(logoff_corrigido, hora_ultima_ordem)
                if intervalo_float is None or intervalo_float < 0:
                    fora_ordem = base_fora_ordem - retorno_base_float
                else:
                    fora_ordem = base_fora_ordem - retorno_base_float - intervalo_float

            # Primeira ordem do dia: valor da coluna "1º Despacho" convertido para float
            col_primeiro_despacho = "1º Despacho"
            if col_primeiro_despacho in grupo.columns:
                try:
                    temp_sem_ordem[0] = float(str(grupo.loc[0, col_primeiro_despacho]).replace(',', '.'))
                except Exception:
                    temp_sem_ordem[0] = float('nan')
            else:
                temp_sem_ordem[0] = float('nan')
            # Demais ordens: entre_ordem + fora_ordem
            for i in range(1, len(grupo)):
                temp_sem_ordem[i] = entre_ordem + fora_ordem

            df.loc[grupo.index, calc_col] = temp_sem_ordem
        # Garante que a coluna calculada seja float
        df[calc_col] = pd.to_numeric(df[calc_col], errors='coerce')

        return df
    
    def _round_calculated_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Round all calculated columns to 2 decimal places."""
        for col in self._settings.calculated.all_columns:
            if col in df.columns:
                df[col] = df[col].round(2)
        return df
    
    def _reorder_columns(
        self,
        df: pd.DataFrame,
        columns: Dict[str, Optional[str]]
    ) -> pd.DataFrame:
        """Reorder columns to place calculated columns before Despachada."""
        col_despachada = columns.get("despachada")
        
        if not col_despachada or col_despachada not in df.columns:
            return df
        
        calc_cols = self._settings.calculated.all_columns
        cols = [c for c in df.columns if c not in calc_cols]
        
        if col_despachada in cols:
            idx = cols.index(col_despachada)
            existing_calc = [c for c in calc_cols if c in df.columns]
            for col in reversed(existing_calc):
                cols.insert(idx, col)
        
        return df[[c for c in cols if c in df.columns]]
