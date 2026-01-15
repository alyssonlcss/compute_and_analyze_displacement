"""
Processing pipeline that orchestrates all services.

This module provides the main processing pipeline that coordinates
data loading, calculation, aggregation, and report generation.
"""

from pathlib import Path
from typing import Optional
import pandas as pd
import logging

from ..config import Settings, get_settings
from ..core.models import ProcessingResult
from .data_loader import DataLoaderService
from .calculator import CalculatorService
from .aggregator import AggregatorService

logger = logging.getLogger(__name__)


class ProcessingPipeline:
    """
    Main processing pipeline for displacement analysis.
    
    Orchestrates all services to process displacement data from
    CSV input to calculated metrics and aggregated reports.
    """
    
    def __init__(self, settings: Optional[Settings] = None):
        """
        Initialize the processing pipeline.
        
        Args:
            settings: Application settings. If None, uses default settings.
        """
        self._settings = settings or get_settings()
        
        # Initialize services
        self._loader = DataLoaderService(self._settings)
        self._calculator = CalculatorService(self._settings)
        self._aggregator = AggregatorService(self._settings)
    
    def run(self, input_path: Optional[Path] = None) -> ProcessingResult:
        """
        Execute the full processing pipeline.
        
        Args:
            input_path: Optional path to input file. Uses default if not provided.
            
        Returns:
            ProcessingResult containing all outputs and statistics
        """
        result = ProcessingResult()
        
        try:
            # Step 1: Load data
            logger.info("=" * 60)
            logger.info("STEP 1: Loading data")
            logger.info("=" * 60)
            
            df = self._loader.load(input_path)
            columns = self._loader.resolved_columns
            result.total_records = len(df)
            
            # Step 2: Calculate metrics
            logger.info("=" * 60)
            logger.info("STEP 2: Calculating metrics")
            logger.info("=" * 60)
            
            df_calculated = self._calculator.process(df, columns)
            result.df_calculated = df_calculated
            
            # Save calculated data
            self._save_dataframe(
                df_calculated,
                self._settings.output_calculated_path,
                "Calculated data"
            )
            
            # Step 3: Filter by status
            logger.info("=" * 60)
            logger.info("STEP 3: Filtering by status")
            logger.info("=" * 60)
            
            df_productive, df_unproductive = self._aggregator.filter_by_status(
                df_calculated, columns
            )
            
            result.productive_records = len(df_productive)
            result.unproductive_records = len(df_unproductive)
            
            # Step 4: Aggregate productive records
            logger.info("=" * 60)
            logger.info("STEP 4: Aggregating productive records")
            logger.info("=" * 60)
            
            if not df_productive.empty:
                result.df_productive_averages = self._aggregator.aggregate(
                    df_productive, columns, "produtivas"
                )
                
                if result.df_productive_averages is not None:
                    self._save_dataframe(
                        result.df_productive_averages,
                        self._settings.output_productive_path,
                        "Productive averages"
                    )
            
            # Step 5: Aggregate unproductive records
            logger.info("=" * 60)
            logger.info("STEP 5: Aggregating unproductive records")
            logger.info("=" * 60)
            
            if not df_unproductive.empty:
                result.df_unproductive_averages = self._aggregator.aggregate(
                    df_unproductive, columns, "improdutivas"
                )
                
                if result.df_unproductive_averages is not None:
                    self._save_dataframe(
                        result.df_unproductive_averages,
                        self._settings.output_unproductive_path,
                        "Unproductive averages"
                    )
            
            # Calculate team count
            col_equipe = columns.get("equipe")
            if col_equipe and col_equipe in df_calculated.columns:
                result.total_teams = df_calculated[col_equipe].nunique()
            
            result.success = True
            result.message = "Processing completed successfully"
            
        except FileNotFoundError as e:
            result.success = False
            result.message = f"File not found: {e}"
            result.processing_errors.append(str(e))
            logger.error(result.message)
            
        except Exception as e:
            result.success = False
            result.message = f"Processing failed: {e}"
            result.processing_errors.append(str(e))
            logger.exception("Pipeline execution failed")
        
        return result
    
    def _save_dataframe(
        self,
        df: pd.DataFrame,
        path: Path,
        description: str
    ) -> None:
        """Save DataFrame to CSV file."""
        try:
            df.to_csv(
                path,
                index=False,
                encoding=self._settings.files.encoding_output
            )
            logger.info(f"{description} saved to: {path}")
        except Exception as e:
            logger.error(f"Failed to save {description}: {e}")
    
    @property
    def loader(self) -> DataLoaderService:
        """Get the data loader service."""
        return self._loader
    
    @property
    def calculator(self) -> CalculatorService:
        """Get the calculator service."""
        return self._calculator
    
    @property
    def aggregator(self) -> AggregatorService:
        """Get the aggregator service."""
        return self._aggregator
