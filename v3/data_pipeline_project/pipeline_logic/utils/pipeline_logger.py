# pipeline_logger.py

import logging
import pendulum
from typing import Any, Dict, List, Optional
from rich.logging import RichHandler
from rich.console import Console


class PipelineLogger:
    def __init__(self, timezone: str = "UTC", use_color: bool = False,complete_color_off: bool = True):
        self.logger = logging.getLogger("pipeline_logger")
        self.logger.setLevel(logging.DEBUG)

        self.console = Console()
        self.use_color = use_color and not complete_color_off
        self.complete_color_off = complete_color_off        
        self.timezone = timezone

        if not self.logger.handlers:
            console_handler = RichHandler(rich_tracebacks=True, markup=True, show_time=False, show_path=False)
            formatter = logging.Formatter("%(message)s")
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def _get_timestamp(self) -> str:
        return pendulum.now(self.timezone).to_iso8601_string()

    # Public logging methods
    def info(self, message: str = "", log_key: str = "GENERAL", **kwargs: Any) -> None:
        self._log("INFO", message, log_key, multiline=True, **kwargs)

    def warning(self, message: str = "", log_key: str = "GENERAL", **kwargs: Any) -> None:
        self._log("WARNING", message, log_key, multiline=True, **kwargs)

    def error(self, message: str = "", log_key: str = "GENERAL", **kwargs: Any) -> None:
        self._log("ERROR", message, log_key, multiline=True, **kwargs)

    def debug(self, message: str = "", log_key: str = "GENERAL", **kwargs: Any) -> None:
        self._log("DEBUG", message, log_key, multiline=True, **kwargs)

    def critical(self, message: str = "", log_key: str = "GENERAL", **kwargs: Any) -> None:
        self._log("CRITICAL", message, log_key, multiline=True, **kwargs)

    def singleline(self, level: str, message: str = "", log_key: str = "GENERAL", **kwargs: Any) -> None:
        """For single-line logging if needed."""
        self._log(level, message, log_key, multiline=False, **kwargs)

    def _log(
        self,
        level: str,
        message: str,
        log_key: str,
        multiline: bool,
        subject: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        **kwargs: Dict[str, Any]
    ) -> None:
        timestamp = self._get_timestamp()

        if multiline:
            if self.use_color:
                start_separator = f"[bold cyan][ {log_key} ][/bold cyan] " + "=" * 75
                subject_line = f"SUBJECT : [bold]{subject or '-'}[/bold]"
                keywords_line = f"KEYWORDS: [green]{keywords if keywords else '-'}[/green]"
                # MESSAGE stays uncolored
                detail_lines = [f"  [cyan]{k:<16}[/cyan]: {v}" for k, v in kwargs.items()]
            else:
                start_separator = f"[ {log_key} ] " + "=" * 75
                subject_line = f"SUBJECT : {subject or '-'}"
                keywords_line = f"KEYWORDS: {keywords if keywords else '-'}"
                detail_lines = [f"  {k:<16}: {v}" for k, v in kwargs.items()]

            end_separator = "=" * 90

            log_block = [
                start_separator,
                f"[{timestamp}] [{level}]",
                subject_line,
                keywords_line,
                f"MESSAGE : {message}",
                "",
                "OTHER DETAILS:",
                *detail_lines,
                end_separator
            ]

            final_message = "\n".join(log_block)

        else:
            # Single-line log, no colors
            kv_string = " | ".join(f"{key}={value}" for key, value in kwargs.items())
            final_message = f"[{timestamp}] {message}"
            if kv_string:
                final_message += f" | {kv_string}"

        if self.complete_color_off:
            # print with no rich markup parsing
            self.console.print(final_message, markup=False)
        else:
            self.console.print(final_message, markup=self.use_color)

