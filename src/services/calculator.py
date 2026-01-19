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
        
        Uses the MOST RECENT date between:
        - PrevLiberada (previous order's release time for the same team)
        - Despachada (current order's dispatch time)
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
                # Both available: use the most recent one
                reference_dt = max(prev_liberada, despachada)
            elif pd.notna(prev_liberada):
                # Only PrevLiberada available
                reference_dt = prev_liberada
            elif pd.notna(despachada):
                # Only Despachada available
                reference_dt = despachada
            else:
                # Neither available
                temp_prep_values.append(np.nan)
                continue
            
            # Calculate TempPrepEquipe = A_Caminho - reference_dt
            temp_prep = self._dt_utils.diff_minutes(a_caminho, reference_dt)
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
        Calculate TempSemOrdem = Jornada - HD Total - TempPrepEquipe - Intervalo.
        
        This is a per-jornada value based on (Equipe, InicioCalendario_dt, FimCalendario_dt),
        so all records for the same team and jornada will have the same value.
        """
        calc_col = self._settings.calculated.temp_sem_ordem
        col_equipe = columns.get("equipe")
        col_hd_total = columns.get("hd_total")
        col_jornada = self._settings.calculated.jornada
        col_temp_prep = self._settings.calculated.temp_prep_equipe
        col_inter_reg = self._settings.calculated.inter_reg
        
        if not col_equipe or col_equipe not in df.columns:
            logger.warning("Equipe column not found, TempSemOrdem will be NaN")
            df[calc_col] = np.nan
            return df
        
        # Ensure InicioCalendario_dt and FimCalendario_dt exist
        if "InicioCalendario_dt" not in df.columns or "FimCalendario_dt" not in df.columns:
            logger.warning("InicioCalendario_dt or FimCalendario_dt not found, TempSemOrdem will be NaN")
            df[calc_col] = np.nan
            return df
        
        # Parse HD Total (value per jornada, same for all orders of that jornada)
        if col_hd_total and col_hd_total in df.columns:
            df["_HD_Total"] = pd.to_numeric(
                df[col_hd_total].astype(str).str.replace(",", "."),
                errors="coerce"
            )
        else:
            logger.warning("HD Total column not found, using 0")
            df["_HD_Total"] = 0
        
        # Calculate sum of TempPrepEquipe per team per jornada
        group_keys = [col_equipe, "InicioCalendario_dt", "FimCalendario_dt"]
        if col_temp_prep in df.columns:
            sum_temp_prep = df.groupby(group_keys)[col_temp_prep].transform("sum")
        else:
            sum_temp_prep = 0
        
        # Get Intervalo (same value for all orders of same team/jornada, use first non-null)
        if col_inter_reg in df.columns:
            intervalo = df.groupby(group_keys)[col_inter_reg].transform("first").fillna(0)
        else:
            intervalo = 0
        
        # Get Jornada (same value for all orders of same team/jornada)
        if col_jornada in df.columns:
            # TempSemOrdem = Jornada - HD Total - sum(TempPrepEquipe) - Intervalo
            df[calc_col] = df[col_jornada] - df["_HD_Total"] - sum_temp_prep - intervalo
            # Make it the same value for all rows of same team/jornada
            df[calc_col] = df.groupby(group_keys)[calc_col].transform("first")

            # LOG DETALHADO DOS VALORES USADOS NO CÁLCULO
            debug_groups = df.groupby(group_keys).first().reset_index()
            # ...logs de debug removidos...

            logger.info("TempSemOrdem calculated (Jornada - HD Total - TempPrepEquipe - Intervalo) per jornada (Equipe, InicioCalendario_dt, FimCalendario_dt)")
        else:
            logger.warning("Jornada not found, TempSemOrdem will be NaN")
            df[calc_col] = np.nan

        # Clean up temporary columns
        df.drop(columns=["_HD_Total"], inplace=True, errors="ignore")

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
