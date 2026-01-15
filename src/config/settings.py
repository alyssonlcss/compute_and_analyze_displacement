"""
Application settings and configuration management.

This module provides centralized configuration using the Settings pattern,
enabling easy customization and environment-specific overrides.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
import os


@dataclass(frozen=True)
class PathSettings:
    """File and directory path configurations."""
    
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent)
    src_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent)
    data_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent / "data")
    output_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent / "result")
    
    def __post_init__(self):
        """Ensure output directory exists."""
        self.output_dir.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class FileSettings:
    """Input/output file configurations."""
    
    input_file: str = "deslocamento.csv"
    output_calculated: str = "deslocamento_calculado.csv"
    output_productive_averages: str = "medias_por_equipe_dia.csv"
    output_unproductive_averages: str = "medias_Improdutivas_por_equipe_dia.csv"
    report_file: str = "relatorio_analise_equipes.docx"
    encoding_input: str = "latin1"
    encoding_output: str = "utf-8"


@dataclass(frozen=True)
class ColumnMappings:
    """Column name mappings for robust CSV parsing."""
    
    despachada: List[str] = field(default_factory=lambda: ["Despachada"])
    a_caminho: List[str] = field(default_factory=lambda: ["A_Caminho"])
    no_local: List[str] = field(default_factory=lambda: ["No_Local"])
    liberada: List[str] = field(default_factory=lambda: ["Liberada"])
    inicio_intervalo: List[str] = field(default_factory=lambda: [
        "Inicio Intervalo", "Início Intervalo", "Inicio_Intervalo", "Início_Intervalo"
    ])
    fim_intervalo: List[str] = field(default_factory=lambda: ["Fim Intervalo", "Fim_Intervalo"])
    inicio_calendario: List[str] = field(default_factory=lambda: [
        "Inicio Calendario", "Início Calendario", "Inicio_Calendario", "Início_Calendario"
    ])
    primeiro_login: List[str] = field(default_factory=lambda: [
        "1º Login", "1º LogIn", "1º Login Corrigido"
    ])
    login_alt: List[str] = field(default_factory=lambda: ["Log In", "Login"])
    equipe: List[str] = field(default_factory=lambda: ["Equipe"])
    status: List[str] = field(default_factory=lambda: [
        "status", "Status", "Situação", "Estado", "Tipo", "Classificação", "Categoria"
    ])


@dataclass(frozen=True)
class CalculatedColumns:
    """Names for calculated output columns."""
    
    temp_prep_equipe: str = "TempPrepEquipe_min"
    temp_exe: str = "TempExe_min"
    temp_desl: str = "TempDesl_min"
    inter_reg: str = "InterReg_min"
    atras_login: str = "AtrasLogin_min"
    
    @property
    def all_columns(self) -> List[str]:
        """Return all calculated column names."""
        return [
            self.temp_prep_equipe,
            self.temp_exe,
            self.temp_desl,
            self.inter_reg,
            self.atras_login,
        ]


@dataclass(frozen=True)
class MetricsTargets:
    """Target values for metrics analysis."""
    
    temp_exe_productive: float = 50.0  # minutes
    temp_exe_unproductive: float = 20.0  # minutes
    intervalo_regulamentar: float = 60.0  # minutes
    jornada_total: float = 468.0  # minutes (7h48min)
    utilizacao_meta: float = 0.85  # 85%
    
    @property
    def tempo_util_meta(self) -> float:
        """Calculate target utilization time."""
        return self.jornada_total * self.utilizacao_meta


@dataclass
class Settings:
    """Main application settings container."""
    
    paths: PathSettings = field(default_factory=PathSettings)
    files: FileSettings = field(default_factory=FileSettings)
    columns: ColumnMappings = field(default_factory=ColumnMappings)
    calculated: CalculatedColumns = field(default_factory=CalculatedColumns)
    metrics: MetricsTargets = field(default_factory=MetricsTargets)
    
    @property
    def input_path(self) -> Path:
        """Full path to input file."""
        return self.paths.data_dir / self.files.input_file
    
    @property
    def output_calculated_path(self) -> Path:
        """Full path to calculated output file."""
        return self.paths.output_dir / self.files.output_calculated
    
    @property
    def output_productive_path(self) -> Path:
        """Full path to productive averages file."""
        return self.paths.output_dir / self.files.output_productive_averages
    
    @property
    def output_unproductive_path(self) -> Path:
        """Full path to unproductive averages file."""
        return self.paths.output_dir / self.files.output_unproductive_averages
    
    @property
    def report_path(self) -> Path:
        """Full path to report file."""
        return self.paths.output_dir / self.files.report_file


# Singleton pattern for settings
_settings_instance: Optional[Settings] = None


def get_settings() -> Settings:
    """Get or create the settings singleton instance."""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance
