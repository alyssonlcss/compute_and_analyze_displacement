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
        result = self._calculate_temp_exe(result)
        result = self._calculate_temp_desl(result)
        result = self._calculate_inter_reg(result)
        result = self._calculate_atras_login(result)
        
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
