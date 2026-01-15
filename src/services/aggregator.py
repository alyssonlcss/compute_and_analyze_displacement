"""
Aggregation service for computing team averages.

This module handles the aggregation of calculated metrics by team and date,
producing summary statistics for analysis.
"""

from typing import Optional, Dict, List
import pandas as pd
import numpy as np
import logging

from ..config import Settings, get_settings
from ..core.utils import DateTimeUtils

logger = logging.getLogger(__name__)


class AggregatorService:
    """
    Service for aggregating displacement metrics.
    
    Computes averages per team per day and overall team averages,
    supporting both productive and unproductive record filtering.
    """
    
    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialize the aggregator service.
        
        Args:
            settings: Application settings. If None, uses default settings.
        """
        self._settings = settings or get_settings()
    
    def aggregate(
        self,
        df: pd.DataFrame,
        columns: Dict[str, Optional[str]],
        record_type: str = "produtivas"
    ) -> Optional[pd.DataFrame]:
        """
        Aggregate metrics by team and date.
        
        Args:
            df: DataFrame with calculated metrics
            columns: Resolved column name mappings
            record_type: Type of records ('produtivas' or 'improdutivas')
            
        Returns:
            DataFrame with aggregated averages, or None if aggregation fails
        """
        logger.info(f"Starting aggregation for {record_type} records")
        
        if df.empty:
            logger.warning(f"No {record_type} records to aggregate")
            return None
        
        col_equipe = columns.get("equipe")
        col_despachada = columns.get("despachada")
        
        if not col_equipe or col_equipe not in df.columns:
            logger.error("Column 'Equipe' not found in dataset")
            return None
        
        if not col_despachada or col_despachada not in df.columns:
            logger.error("Date column not found")
            return None
        
        # Extract date
        try:
            df = df.copy()
            df["Data"] = pd.to_datetime(
                df[col_despachada], dayfirst=True, errors="coerce"
            ).dt.date
        except Exception as e:
            logger.error(f"Failed to extract dates: {e}")
            return None
        
        # Get calculated columns that exist
        calc_cols = [
            col for col in self._settings.calculated.all_columns
            if col in df.columns
        ]
        
        if not calc_cols:
            logger.error("No calculated columns found in dataset")
            return None
        
        logger.info(f"Aggregating columns: {', '.join(calc_cols)}")
        logger.info(f"Total {record_type} records: {len(df)}")
        
        # Group and calculate means
        averages = df.groupby([col_equipe, "Data"])[calc_cols].mean()
        averages = averages.round(2).reset_index()
        
        # Rename columns to indicate averages
        rename_map = {col: f"Media_{col}" for col in calc_cols}
        averages = averages.rename(columns=rename_map)
        
        # Sort by team and date
        averages = averages.sort_values([col_equipe, "Data"])
        
        # Add overall averages per team
        averages = self._add_team_totals(averages, col_equipe, calc_cols)
        
        # Log statistics
        self._log_statistics(averages, col_equipe, record_type)
        
        return averages
    
    def _add_team_totals(
        self,
        df: pd.DataFrame,
        col_equipe: str,
        calc_cols: List[str]
    ) -> pd.DataFrame:
        """Add overall average rows for each team."""
        result_frames = []
        teams = df[col_equipe].unique()
        
        logger.info(f"Processing {len(teams)} teams...")
        
        for team in teams:
            team_data = df[df[col_equipe] == team].copy()
            result_frames.append(team_data)
            
            # Calculate overall average for team
            overall_avg = {}
            for col in calc_cols:
                col_media = f"Media_{col}"
                if col_media in team_data.columns:
                    values = team_data[col_media].dropna()
                    overall_avg[col_media] = round(values.mean(), 2) if len(values) > 0 else np.nan
            
            # Create overall row
            overall_row = {
                col_equipe: f"MédiaTodosDias{team}",
                "Data": "GERAL",
            }
            overall_row.update(overall_avg)
            
            result_frames.append(pd.DataFrame([overall_row]))
            
            logger.debug(f"  - {team}: {len(team_data)} days processed")
        
        if result_frames:
            return pd.concat(result_frames, ignore_index=True)
        
        return pd.DataFrame()
    
    def _log_statistics(
        self,
        df: pd.DataFrame,
        col_equipe: str,
        record_type: str
    ) -> None:
        """Log aggregation statistics."""
        if df.empty:
            return
        
        # Count teams (excluding 'MédiaTodosDias' rows)
        regular_rows = df[~df[col_equipe].str.startswith("MédiaTodosDias", na=False)]
        total_rows = df[df[col_equipe].str.startswith("MédiaTodosDias", na=False)]
        
        teams = regular_rows[col_equipe].nunique()
        days = regular_rows["Data"].nunique() if "Data" in regular_rows else 0
        
        logger.info(f"\nStatistics for {record_type}:")
        logger.info(f"- Total teams: {teams}")
        logger.info(f"- Days with records: {days}")
        logger.info(f"- Daily records: {len(regular_rows)}")
        logger.info(f"- 'MédiaTodosDias' rows added: {len(total_rows)}")
        logger.info(f"- Total rows in output: {len(df)}")
    
    def filter_by_status(
        self,
        df: pd.DataFrame,
        columns: Dict[str, Optional[str]]
    ) -> tuple:
        """
        Filter DataFrame into productive and unproductive records.
        
        Args:
            df: DataFrame with calculated metrics
            columns: Resolved column name mappings
            
        Returns:
            Tuple of (productive_df, unproductive_df)
        """
        col_status = columns.get("status")
        
        if not col_status or col_status not in df.columns:
            logger.warning("Status column not found, treating all as productive")
            return df.copy(), pd.DataFrame()
        
        mask = df[col_status].astype(str).str.strip().str.lower() == "improdutivo"
        
        df_unproductive = df[mask].copy()
        df_productive = df[~mask].copy()
        
        logger.info(f"Total records: {len(df)}")
        logger.info(f"Unproductive records: {len(df_unproductive)}")
        logger.info(f"Productive records: {len(df_productive)}")
        
        return df_productive, df_unproductive
