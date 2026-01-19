"""
Excel formatting service for professional-looking spreadsheets.

This module provides utilities to export DataFrames to Excel files
with consistent styling, colors, and formatting.
"""

from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd
import logging

from openpyxl import Workbook
from openpyxl.styles import (
    Font, Fill, PatternFill, Alignment, Border, Side, NamedStyle
)
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

logger = logging.getLogger(__name__)


class ExcelTheme:
    """Color theme for Excel formatting."""
    
    # Header colors
    HEADER_BG = "2F5496"  # Dark blue
    HEADER_FG = "FFFFFF"  # White text
    
    # Alternating row colors
    ROW_EVEN = "D6E3F8"  # Light blue
    ROW_ODD = "FFFFFF"   # White
    
    # Special row colors
    SUMMARY_BG = "FFF2CC"  # Light yellow for GERAL/MédiaTodosDias rows
    SUMMARY_FG = "000000"  # Black text
    
    # Border color
    BORDER_COLOR = "B4C6E7"  # Light blue border


class ExcelStyles:
    """Pre-defined styles for Excel formatting."""
    
    @staticmethod
    def get_header_font() -> Font:
        """Bold white font for headers."""
        return Font(bold=True, color=ExcelTheme.HEADER_FG, size=11)
    
    @staticmethod
    def get_header_fill() -> PatternFill:
        """Dark blue background for headers."""
        return PatternFill(
            start_color=ExcelTheme.HEADER_BG,
            end_color=ExcelTheme.HEADER_BG,
            fill_type="solid"
        )
    
    @staticmethod
    def get_header_alignment() -> Alignment:
        """Center alignment for headers."""
        return Alignment(horizontal="center", vertical="center", wrap_text=True)
    
    @staticmethod
    def get_data_alignment() -> Alignment:
        """Default alignment for data cells."""
        return Alignment(horizontal="center", vertical="center")
    
    @staticmethod
    def get_even_row_fill() -> PatternFill:
        """Light blue background for even rows."""
        return PatternFill(
            start_color=ExcelTheme.ROW_EVEN,
            end_color=ExcelTheme.ROW_EVEN,
            fill_type="solid"
        )
    
    @staticmethod
    def get_odd_row_fill() -> PatternFill:
        """White background for odd rows."""
        return PatternFill(
            start_color=ExcelTheme.ROW_ODD,
            end_color=ExcelTheme.ROW_ODD,
            fill_type="solid"
        )
    
    @staticmethod
    def get_summary_fill() -> PatternFill:
        """Yellow background for summary rows."""
        return PatternFill(
            start_color=ExcelTheme.SUMMARY_BG,
            end_color=ExcelTheme.SUMMARY_BG,
            fill_type="solid"
        )
    
    @staticmethod
    def get_summary_font() -> Font:
        """Bold font for summary rows."""
        return Font(bold=True, color=ExcelTheme.SUMMARY_FG, size=11)
    
    @staticmethod
    def get_thin_border() -> Border:
        """Thin border for cells."""
        side = Side(style="thin", color=ExcelTheme.BORDER_COLOR)
        return Border(left=side, right=side, top=side, bottom=side)
    
    @staticmethod
    def get_number_format() -> str:
        """Number format for decimal values."""
        return "#,##0.00"
    
    @staticmethod
    def get_integer_format() -> str:
        """Number format for integer values."""
        return "#,##0"


class ExcelFormatter:
    """
    Service for exporting DataFrames to professionally formatted Excel files.
    
    Features:
    - Styled headers with colors
    - Alternating row colors (zebra striping)
    - Auto-sized columns
    - Highlighted summary rows (GERAL)
    - Proper number formatting
    """
    
    def __init__(self):
        """Initialize the Excel formatter."""
        self._styles = ExcelStyles()
    
    def export(
        self,
        df: pd.DataFrame,
        path: Path,
        sheet_name: str = "Dados",
        summary_identifier: str = "GERAL",
        freeze_header: bool = True
    ) -> bool:
        """
        Export DataFrame to a formatted Excel file.
        
        Args:
            df: DataFrame to export
            path: Output file path
            sheet_name: Name for the worksheet
            summary_identifier: Text that identifies summary rows (e.g., "GERAL")
            freeze_header: Whether to freeze the header row
            
        Returns:
            True if export was successful, False otherwise
        """
        try:
            logger.info(f"Exporting formatted Excel to: {path}")
            
            # Create workbook and worksheet
            wb = Workbook()
            ws = wb.active
            ws.title = sheet_name
            
            # Write data
            self._write_data(ws, df)
            
            # Apply formatting
            self._format_header(ws, len(df.columns))
            self._format_data_rows(ws, df, summary_identifier)
            self._auto_size_columns(ws, df)
            
            # Freeze header row
            if freeze_header:
                ws.freeze_panes = "A2"
            
            # Save workbook
            wb.save(path)
            logger.info(f"Excel file saved successfully: {path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to export Excel file: {e}")
            return False
    
    def _write_data(self, ws: Worksheet, df: pd.DataFrame) -> None:
        """Write DataFrame data to worksheet."""
        # Write header
        for col_idx, column in enumerate(df.columns, 1):
            ws.cell(row=1, column=col_idx, value=column)
        
        # Write data rows
        for row_idx, row in enumerate(df.values, 2):
            for col_idx, value in enumerate(row, 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                
                # Handle NaN values
                if pd.isna(value):
                    cell.value = ""
                else:
                    cell.value = value
    
    def _format_header(self, ws: Worksheet, num_cols: int) -> None:
        """Apply formatting to header row."""
        header_font = self._styles.get_header_font()
        header_fill = self._styles.get_header_fill()
        header_alignment = self._styles.get_header_alignment()
        border = self._styles.get_thin_border()
        
        for col_idx in range(1, num_cols + 1):
            cell = ws.cell(row=1, column=col_idx)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = border
        
        # Set header row height
        ws.row_dimensions[1].height = 30
    
    def _format_data_rows(
        self,
        ws: Worksheet,
        df: pd.DataFrame,
        summary_identifier: str
    ) -> None:
        """Apply formatting to data rows."""
        even_fill = self._styles.get_even_row_fill()
        odd_fill = self._styles.get_odd_row_fill()
        summary_fill = self._styles.get_summary_fill()
        summary_font = self._styles.get_summary_font()
        data_alignment = self._styles.get_data_alignment()
        border = self._styles.get_thin_border()
        number_format = self._styles.get_number_format()
        integer_format = self._styles.get_integer_format()
        
        num_cols = len(df.columns)
        
        # Identify summary rows
        summary_row_indices = set()
        if "Data" in df.columns:
            data_col_idx = df.columns.get_loc("Data")
            for row_idx, value in enumerate(df["Data"]):
                if str(value) == summary_identifier:
                    summary_row_indices.add(row_idx)
        
        # Also check for MédiaTodosDias pattern in first column
        first_col = df.iloc[:, 0]
        for row_idx, value in enumerate(first_col):
            if "MédiaTodosDias" in str(value):
                summary_row_indices.add(row_idx)
        
        # Apply formatting to each row
        for row_idx in range(len(df)):
            excel_row = row_idx + 2  # Excel rows are 1-indexed, header is row 1
            
            # Determine if this is a summary row
            is_summary = row_idx in summary_row_indices
            
            # Choose fill color
            if is_summary:
                fill = summary_fill
                font = summary_font
            else:
                fill = even_fill if row_idx % 2 == 0 else odd_fill
                font = Font(size=11)
            
            # Apply to each cell in the row
            for col_idx in range(1, num_cols + 1):
                cell = ws.cell(row=excel_row, column=col_idx)
                cell.fill = fill
                cell.font = font
                cell.alignment = data_alignment
                cell.border = border
                
                # Apply number formatting
                col_name = df.columns[col_idx - 1]
                if self._is_numeric_column(col_name, df):
                    if "qtd" in col_name.lower():
                        cell.number_format = integer_format
                    else:
                        cell.number_format = number_format
    
    def _is_numeric_column(self, col_name: str, df: pd.DataFrame) -> bool:
        """Check if a column should have numeric formatting."""
        if col_name not in df.columns:
            return False
        
        # Check if column is numeric
        dtype = df[col_name].dtype
        return pd.api.types.is_numeric_dtype(dtype)
    
    def _auto_size_columns(self, ws: Worksheet, df: pd.DataFrame) -> None:
        """Auto-size column widths based on content."""
        for col_idx, column in enumerate(df.columns, 1):
            # Calculate max width
            max_length = len(str(column))
            
            for row_idx in range(len(df)):
                cell_value = df.iloc[row_idx, col_idx - 1]
                if pd.notna(cell_value):
                    cell_length = len(str(cell_value))
                    max_length = max(max_length, cell_length)
            
            # Add padding and set width
            adjusted_width = min(max_length + 3, 50)  # Cap at 50 chars
            adjusted_width = max(adjusted_width, 10)   # Minimum 10 chars
            
            col_letter = get_column_letter(col_idx)
            ws.column_dimensions[col_letter].width = adjusted_width


# Singleton instance
_formatter_instance: Optional[ExcelFormatter] = None


def get_excel_formatter() -> ExcelFormatter:
    """Get or create the Excel formatter singleton."""
    global _formatter_instance
    if _formatter_instance is None:
        _formatter_instance = ExcelFormatter()
    return _formatter_instance
