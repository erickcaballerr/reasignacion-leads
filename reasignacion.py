"""
Handler de reasignacion de leads para RE/MAX Terra (arquitectura batch
blindada, 6 capas).

Cuando Beefast reasigna un lead a otro asesor, actualiza en la hoja MADRE la
columna ASESOR y aumenta el contador de reasignaciones, pero no mueve el lead
entre los libros individuales de cada asesor. Este proceso cierra esa brecha
con garantias de nivel empresarial critico.

Blindaje por capas:

  CAPA 1 - Red y memoria:
    - Lectura de filas origen sin N+1: las filas a mover de un mismo asesor se
      descargan en UNA sola peticion mediante batch_get con rangos discretos.
    - MADRE sin OOM: se descargan solo las columnas necesarias (asesor, id,
      reasignaciones, procesado) por batch_get de columnas, nunca la hoja
      entera.
    - Paginacion estricta: escrituras y borrados masivos se envian en bloques
      de a lo sumo 500 registros para evitar 413 Payload Too Large.

  CAPA 2 - Concurrencia atomica:
    - Candado check-and-set: se escribe un UUID propio, se espera la
      propagacion del cache eventual de Google y se relee; el candado se
      adquiere solo si el UUID persiste.
    - Heartbeat: el candado registra un latido para distinguir un proceso
      activo de un candado huerfano.

  CAPA 3 - Factor humano:
    - Validacion estructural de cabeceras: si la fila 1 de la columna de IDs
      no parece encabezado, se omite ese asesor (StructuralIntegrityError).
    - Sin falsos appends: la insercion localiza la primera fila realmente
      vacia segun la columna de IDs y escribe por rango, sin huecos.

  CAPA 4 - Integridad del dato:
    - Reconciliacion en caliente: antes de marcar procesado, se reubica el ID
      por si hubo borrados de filas durante la corrida.
    - Preservacion total de columnas: las filas se mueven completas, sin
      truncar.
    - Sanitizacion extrema de IDs: numericos, flotantes serializados,
      alfanumericos, UUID y caracteres invisibles.

  CAPA 5 - Seguridad del borrado:
    - Mitigacion de index shift: los borrados se emiten de mayor a menor fila,
      preservando el orden inverso incluso entre bloques paginados.

  CAPA 6 - Auditoria:
    - Marcas de tiempo en America/Mexico_City via zoneinfo (sin librerias
      obsoletas).

Credenciales (variable de entorno):
    GOOGLE_CREDENTIALS  Contenido JSON de la cuenta de servicio de Google.

Uso:
    python reasignacion.py            Ejecucion real.
    python reasignacion.py --dry-run  Simulacion sin escritura.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import unicodedata
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Iterator, Optional, Sequence, TypeVar

try:
    from zoneinfo import ZoneInfo
except ImportError as exc:  # pragma: no cover
    sys.stderr.write(f"Se requiere Python 3.9+ con zoneinfo. Detalle: {exc}\n")
    raise SystemExit(1) from exc

try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.exceptions import APIError, GSpreadException, WorksheetNotFound
    from gspread.utils import rowcol_to_a1
except ImportError as exc:  # pragma: no cover
    sys.stderr.write(
        "Faltan dependencias requeridas. Instale con:\n"
        "    pip install gspread google-auth\n"
        f"Detalle: {exc}\n"
    )
    raise SystemExit(1) from exc


# =========================================================================== #
# Configuracion inmutable
# =========================================================================== #

@dataclass(frozen=True)
class Config:
    """Parametros de ejecucion. Inmutable para impedir mutaciones."""

    google_credentials: str

    madre_sheet_id: str = "1UHu2b3drz-6KhLHnw0EBmbIaggg0RFHIkIPAV4H-OSQ"

    madre_tab: str = "CONTROL INTERNO"
    directorio_tab: str = "Directorio Corporativo"
    asesor_tab: str = "CONTROL INTERNO"
    log_tab: str = "LOG_REASIGNACIONES"
    lock_tab: str = "_LOCK"

    # Indices de columna (base 0) en CONTROL INTERNO.
    col_asesor: int = 3
    col_id: int = 24
    col_reasignaciones: int = 26
    col_procesado: int = 27
    # Ancho minimo garantizado al construir una fila; jamas trunca filas mas
    # anchas (preservacion total de columnas).
    min_cols: int = 28

    # Encabezados del directorio.
    dir_nombre: str = "NOMBRE_ASESOR"
    dir_spreadsheet: str = "ID_SPREADSHEET"

    # Resiliencia de red.
    max_retries: int = 6
    backoff_base_seconds: float = 4.0
    backoff_cap_seconds: float = 60.0

    # Propagacion de Google tras escritura, antes de releer para verificar.
    write_propagation_seconds: float = 4.0
    verify_attempts: int = 4
    verify_interval_seconds: float = 3.0

    # Candado: vigencia, latido y confirmacion check-and-set.
    lock_ttl_minutes: float = 15.0
    heartbeat_interval_seconds: float = 120.0
    lock_confirm_seconds: float = 5.0

    # Paginacion de operaciones masivas.
    batch_chunk_size: int = 500
    # Limite de rangos discretos por batch_get (lectura de filas origen).
    read_chunk_size: int = 100

    timezone_name: str = "America/Mexico_City"

    @classmethod
    def from_environment(cls) -> "Config":
        google_creds = os.environ.get("GOOGLE_CREDENTIALS", "").strip()
        if not google_creds:
            raise ConfigurationError(
                "Falta la variable de entorno GOOGLE_CREDENTIALS."
            )
        return cls(google_credentials=google_creds)


_NULL_ID_TOKENS = frozenset({"", "NAN", "NONE", "NULL", "0"})
_NULL_INT_TOKENS = frozenset({"", "NAN", "NONE", "NULL"})
_FLOAT_ID_RE = re.compile(r"^(\d+)\.0+$")

# Caracteres invisibles a depurar de los identificadores (zero-width, BOM,
# espacios duros).
_INVISIBLE_CHARS = dict.fromkeys(
    [
        0x200B,  # zero width space
        0x200C,  # zero width non-joiner
        0x200D,  # zero width joiner
        0xFEFF,  # zero width no-break space / BOM
        0x00A0,  # no-break space
        0x202F,  # narrow no-break space
    ],
    None,
)

_HEADER_FORBIDDEN_EXACT = frozenset({"", "NAN", "NONE", "NULL", "0"})

_LOG_HEADER = (
    "FECHA",
    "ID BEEFAST",
    "ACCION",
    "ASESOR ANTERIOR",
    "ASESOR NUEVO",
    "DETALLE",
)


# =========================================================================== #
# Jerarquia de excepciones
# =========================================================================== #

class ReassignmentError(Exception):
    """Error base del handler."""


class ConfigurationError(ReassignmentError):
    """Configuracion ausente o invalida."""


class ExternalServiceError(ReassignmentError):
    """Fallo no recuperable contra un servicio externo tras agotar reintentos."""


class WorksheetMissingError(ReassignmentError):
    """La pestaña requerida no existe en el libro indicado."""


class StructuralIntegrityError(ReassignmentError):
    """La hoja perdio su encabezado y no es seguro interpretarla."""


class LockActiveError(ReassignmentError):
    """Otra ejecucion mantiene el candado vigente."""


class LockContendedError(ReassignmentError):
    """Otra instancia gano el candado en la confirmacion check-and-set."""


# =========================================================================== #
# Logging
# =========================================================================== #

logger = logging.getLogger("reasignacion")

T = TypeVar("T")


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root = logging.getLogger("reasignacion")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    root.propagate = False


# =========================================================================== #
# Funciones puras
# =========================================================================== #

def strip_accents(value: object) -> str:
    """Minusculas, sin acentos, con espacios colapsados."""
    text = " ".join(str(value).strip().split()).lower()
    decomposed = unicodedata.normalize("NFD", text)
    return decomposed.encode("ascii", "ignore").decode("utf-8")


def sanitize_id(value: object) -> str:
    """Sanitizacion extrema de un identificador.

    Elimina caracteres invisibles (zero-width, BOM, espacios duros), recorta
    espacios y normaliza flotantes serializados ("12345.0" -> "12345"). No
    altera el caso de identificadores alfanumericos ni UUID.
    """
    text = str(value).translate(_INVISIBLE_CHARS).strip()
    if not text:
        return ""
    match = _FLOAT_ID_RE.match(text)
    if match:
        return match.group(1)
    return text


def is_valid_id(value: object) -> bool:
    """Valido si, tras sanitizar, no es vacio ni cero."""
    canon = sanitize_id(value)
    return canon.upper() not in _NULL_ID_TOKENS


def to_int(value: object) -> int:
    """Convierte a entero de forma tolerante; ausencia equivale a cero."""
    text = str(value).strip()
    if text.upper() in _NULL_INT_TOKENS:
        return 0
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return 0


def ensure_min_width(row: Sequence[str], min_width: int) -> list[str]:
    """Rellena hasta 'min_width' sin truncar filas mas anchas."""
    values = list(row)
    if len(values) < min_width:
        values.extend([""] * (min_width - len(values)))
    return values


def looks_like_header(cell: object) -> bool:
    """Heuristica: una celda de encabezado no es vacia, ni cero, ni un ID puro."""
    text = sanitize_id(cell)
    if text.upper() in _HEADER_FORBIDDEN_EXACT:
        return False
    if text.isdigit():
        return False
    return True


def chunked(items: Sequence[T], size: int) -> Iterator[list[T]]:
    """Divide una secuencia en bloques de a lo sumo 'size' elementos."""
    if size <= 0:
        raise ValueError("El tamano de bloque debe ser positivo.")
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def column_letter(col_index_1based: int) -> str:
    """Convierte un indice de columna (base 1) a su letra A1 (1->A, 27->AA)."""
    letters = ""
    n = col_index_1based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


# =========================================================================== #
# Reloj con zona horaria local
# =========================================================================== #

class Clock:
    """Provee marcas de tiempo en la zona horaria de auditoria."""

    def __init__(self, timezone_name: str) -> None:
        self._tz = ZoneInfo(timezone_name)

    def now(self) -> datetime:
        return datetime.now(self._tz)

    def now_iso(self) -> str:
        return self.now().isoformat(timespec="seconds")

    def parse(self, text: str) -> Optional[datetime]:
        try:
            parsed = datetime.fromisoformat(text)
        except (ValueError, TypeError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=self._tz)
        return parsed


# =========================================================================== #
# Cliente de Google Sheets: reintentos, resolucion de pestañas, lectura en
# bloque y paginacion
# =========================================================================== #

class SheetsClient:
    """Capa unica de acceso a Google Sheets con reintentos, batch_get y chunking."""

    _SCOPES = ("https://www.googleapis.com/auth/spreadsheets",)
    _RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

    def __init__(self, config: Config) -> None:
        self._config = config
        self._client = self._authorize()

    def _authorize(self) -> gspread.Client:
        try:
            info = json.loads(self._config.google_credentials)
        except json.JSONDecodeError as exc:
            raise ConfigurationError(
                "GOOGLE_CREDENTIALS no contiene un JSON valido."
            ) from exc
        try:
            creds = Credentials.from_service_account_info(
                info, scopes=list(self._SCOPES)
            )
            return gspread.authorize(creds)
        except Exception as exc:  # noqa: BLE001
            raise ExternalServiceError(
                f"No fue posible autenticar con Google: {exc}"
            ) from exc

    def call(self, operation: Callable[[], T], description: str) -> T:
        """Ejecuta una operacion de red reintentando fallos transitorios."""
        last_exc: Optional[Exception] = None
        for attempt in range(1, self._config.max_retries + 1):
            try:
                return operation()
            except APIError as exc:
                last_exc = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status in self._RETRYABLE_STATUS and attempt < self._config.max_retries:
                    wait = min(
                        self._config.backoff_base_seconds * (2 ** (attempt - 1)),
                        self._config.backoff_cap_seconds,
                    )
                    logger.warning(
                        "Error transitorio %s al %s. Reintento %d/%d en %.0fs.",
                        status, description, attempt, self._config.max_retries, wait,
                    )
                    time.sleep(wait)
                    continue
                raise ExternalServiceError(
                    f"Error de la API de Google al {description}: {exc}"
                ) from exc
            except GSpreadException as exc:
                raise ExternalServiceError(
                    f"Error de gspread al {description}: {exc}"
                ) from exc
        raise ExternalServiceError(
            f"Se agotaron los reintentos al {description}."
        ) from last_exc

    def open_spreadsheet(self, spreadsheet_id: str) -> gspread.Spreadsheet:
        return self.call(
            lambda: self._client.open_by_key(spreadsheet_id),
            f"abrir el libro {spreadsheet_id}",
        )

    def get_worksheet(
        self, spreadsheet: gspread.Spreadsheet, title: str
    ) -> Optional[gspread.Worksheet]:
        """Devuelve la pestaña por titulo, o None si no existe."""
        try:
            return self.call(
                lambda: spreadsheet.worksheet(title),
                f"abrir la pestania '{title}'",
            )
        except ExternalServiceError as exc:
            if isinstance(exc.__cause__, WorksheetNotFound):
                return None
            raise

    def require_worksheet(
        self, spreadsheet: gspread.Spreadsheet, title: str
    ) -> gspread.Worksheet:
        ws = self.get_worksheet(spreadsheet, title)
        if ws is None:
            raise WorksheetMissingError(
                f"No existe la pestania '{title}' en el libro indicado."
            )
        return ws

    def get_or_create_worksheet(
        self,
        spreadsheet: gspread.Spreadsheet,
        title: str,
        rows: int,
        cols: int,
        header: Optional[Sequence[str]] = None,
    ) -> gspread.Worksheet:
        ws = self.get_worksheet(spreadsheet, title)
        if ws is not None:
            return ws
        ws = self.call(
            lambda: spreadsheet.add_worksheet(title=title, rows=rows, cols=cols),
            f"crear la pestania '{title}'",
        )
        if header is not None:
            self.call(
                lambda: ws.update([list(header)], "A1"),
                f"inicializar la pestania '{title}'",
            )
        return ws

    # -- Lecturas en bloque (batch_get) ----------------------------------- #

    def batch_get(
        self, worksheet: gspread.Worksheet, ranges: Sequence[str], description: str
    ) -> list[list[list[str]]]:
        """Lee multiples rangos discretos en UNA peticion.

        Devuelve, por cada rango solicitado y en el mismo orden, su matriz de
        valores (lista de filas). Rangos vacios devuelven matriz vacia.
        """
        if not ranges:
            return []
        result = self.call(
            lambda: worksheet.batch_get(list(ranges)), description
        )
        normalized: list[list[list[str]]] = []
        for value_range in result:
            # gspread devuelve ValueRange (lista de filas) o estructura vacia.
            normalized.append([list(row) for row in value_range])
        return normalized

    def column_values(
        self, worksheet: gspread.Worksheet, column_index_1based: int, description: str
    ) -> list[str]:
        """Lee una sola columna (minimo trafico y memoria)."""
        return self.call(
            lambda: worksheet.col_values(column_index_1based), description
        )

    def read_records(
        self, worksheet: gspread.Worksheet, description: str
    ) -> list[dict[str, object]]:
        return self.call(worksheet.get_all_records, description)

    def read_cell(
        self, worksheet: gspread.Worksheet, a1: str, description: str
    ) -> str:
        result = self.call(lambda: worksheet.get(a1), description)
        if result and result[0]:
            return str(result[0][0])
        return ""

    # -- Escrituras puntuales --------------------------------------------- #

    def update_range(
        self,
        worksheet: gspread.Worksheet,
        a1_range: str,
        values: Sequence[Sequence[object]],
        description: str,
    ) -> None:
        self.call(
            lambda: worksheet.update([list(r) for r in values], a1_range),
            description,
        )

    def update_cell(
        self,
        worksheet: gspread.Worksheet,
        row_number: int,
        col_number_1based: int,
        value: object,
        description: str,
    ) -> None:
        self.call(
            lambda: worksheet.update_cell(row_number, col_number_1based, value),
            description,
        )

    # -- Escrituras por lote con paginacion -------------------------------- #

    def batch_write_rows(
        self,
        worksheet: gspread.Worksheet,
        row_payloads: Sequence[tuple[int, Sequence[str]]],
        description: str,
    ) -> None:
        """Escribe varias filas, paginando para no exceder limites de payload."""
        if not row_payloads:
            return
        total_chunks = 0
        for chunk in chunked(row_payloads, self._config.batch_chunk_size):
            total_chunks += 1
            data = []
            for row_number, values in chunk:
                end_col = max(len(values), 1)
                start = rowcol_to_a1(row_number, 1)
                end = rowcol_to_a1(row_number, end_col)
                data.append({"range": f"{start}:{end}", "values": [list(values)]})
            self.call(
                lambda payload=data: worksheet.batch_update(
                    payload, value_input_option="RAW"
                ),
                f"{description} (bloque {total_chunks})",
            )

    def batch_delete_rows(
        self,
        spreadsheet: gspread.Spreadsheet,
        worksheet: gspread.Worksheet,
        row_numbers: Sequence[int],
        description: str,
    ) -> None:
        """Elimina varias filas en orden inverso, paginando los requests."""
        if not row_numbers:
            return
        ordered = sorted(set(row_numbers), reverse=True)
        sheet_id = worksheet.id
        total_chunks = 0
        for chunk in chunked(ordered, self._config.batch_chunk_size):
            total_chunks += 1
            requests = [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": rn - 1,
                            "endIndex": rn,
                        }
                    }
                }
                for rn in chunk
            ]
            self.call(
                lambda payload=requests: spreadsheet.batch_update(
                    {"requests": payload}
                ),
                f"{description} (bloque {total_chunks})",
            )

    def append_log_rows(
        self, worksheet: gspread.Worksheet, rows: Sequence[Sequence[str]], description: str
    ) -> None:
        """Agrega filas de auditoria, paginando por si son muchas."""
        if not rows:
            return
        total_chunks = 0
        for chunk in chunked(rows, self._config.batch_chunk_size):
            total_chunks += 1
            self.call(
                lambda payload=chunk: worksheet.append_rows(
                    [list(r) for r in payload], value_input_option="RAW"
                ),
                f"{description} (bloque {total_chunks})",
            )


# =========================================================================== #
# Modelo de dominio (inmutable)
# =========================================================================== #

@dataclass(frozen=True)
class Advisor:
    name: str
    spreadsheet_id: str

    @property
    def key(self) -> str:
        return strip_accents(self.name)


@dataclass(frozen=True)
class LeadPlacement:
    advisor_key: str
    rows: tuple[int, ...]

    @property
    def primary_row(self) -> int:
        return self.rows[0]

    @property
    def duplicate_rows(self) -> tuple[int, ...]:
        return self.rows[1:]


@dataclass(frozen=True)
class PendingReassignment:
    madre_row: int
    lead_id: str
    target_key: str
    target_name_raw: str
    reassignment_count: int


class Action:
    MOVED = "MOVIDO"
    ALREADY_OK = "YA_CORRECTO"
    PENDING = "PENDIENTE"
    ERROR = "ERROR"
    SKIPPED = "OMITIDO"
    SIMULATED = "SIMULACION"
    DEDUP = "DEDUPLICADO"
    CORRUPT = "CORRUPCION"


@dataclass
class LogBuffer:
    clock: Clock
    entries: list[list[str]] = field(default_factory=list)

    def add(
        self,
        lead_id: str,
        action: str,
        previous_advisor: str,
        new_advisor: str,
        detail: str,
    ) -> None:
        self.entries.append(
            [self.clock.now_iso(), lead_id, action, previous_advisor, new_advisor, detail]
        )


@dataclass
class RunStats:
    moved: int = 0
    already_ok: int = 0
    pending: int = 0
    errors: int = 0
    skipped: int = 0
    deduplicated: int = 0
    corrupt_sheets: int = 0

    def summary_lines(self, dry_run: bool) -> list[str]:
        title = "RESUMEN (SIMULACION)" if dry_run else "RESUMEN"
        return [
            "-" * 58,
            f"  {title}",
            "-" * 58,
            f"  Movidos        : {self.moved}",
            f"  Ya correctos   : {self.already_ok}",
            f"  Pendientes     : {self.pending} (se reintentaran)",
            f"  Errores        : {self.errors} (se reintentaran)",
            f"  Omitidos       : {self.skipped}",
            f"  Duplicados rem.: {self.deduplicated}",
            f"  Hojas corruptas: {self.corrupt_sheets}",
            "-" * 58,
        ]


# =========================================================================== #
# Indice de leads (lectura minima por columna + validacion de encabezado)
# =========================================================================== #

class LeadIndex:
    """Indice {ID -> ubicacion} construido leyendo solo la columna de IDs."""

    def __init__(self) -> None:
        self._by_id: dict[str, LeadPlacement] = {}

    def register_column(self, advisor: Advisor, id_column: Sequence[str]) -> None:
        rows_by_id: dict[str, list[int]] = {}
        for offset, raw in enumerate(id_column[1:], start=2):
            canon = sanitize_id(raw)
            if not is_valid_id(canon):
                continue
            rows_by_id.setdefault(canon, []).append(offset)

        for lead_id, rows in rows_by_id.items():
            if lead_id in self._by_id:
                continue
            self._by_id[lead_id] = LeadPlacement(
                advisor_key=advisor.key, rows=tuple(rows)
            )

    def locate(self, lead_id: str) -> Optional[LeadPlacement]:
        return self._by_id.get(sanitize_id(lead_id))


# =========================================================================== #
# Repositorio de la hoja MADRE (lectura por columnas, sin OOM)
# =========================================================================== #

class MadreRepository:
    def __init__(self, config: Config, sheets: SheetsClient, clock: Clock) -> None:
        self._config = config
        self._sheets = sheets
        self._clock = clock
        self._spreadsheet = sheets.open_spreadsheet(config.madre_sheet_id)
        self._control_ws: Optional[gspread.Worksheet] = None
        self._lock_token: Optional[str] = None

    def _control(self) -> gspread.Worksheet:
        if self._control_ws is None:
            self._control_ws = self._sheets.require_worksheet(
                self._spreadsheet, self._config.madre_tab
            )
        return self._control_ws

    # -- Candado check-and-set con heartbeat ------------------------------ #

    def acquire_lock(self) -> None:
        """Adquiere el candado con confirmacion check-and-set.

        Estructura (fila 1): A1=inicio, B1=latido, C1=token (UUID).
        Escribe el UUID, espera la propagacion del cache eventual de Google y
        relee: solo si el token persiste, el candado es nuestro.
        """
        ws = self._sheets.get_worksheet(self._spreadsheet, self._config.lock_tab)
        if ws is None:
            ws = self._sheets.get_or_create_worksheet(
                self._spreadsheet, self._config.lock_tab, rows=1, cols=3
            )
        else:
            existing = self._sheets.batch_get(ws, ["A1:C1"], "leer el candado")
            row = existing[0][0] if existing and existing[0] else []
            heartbeat = ""
            if row:
                heartbeat = row[1] if len(row) > 1 and row[1] else row[0]
            if self._lock_is_fresh(heartbeat):
                raise LockActiveError("Hay una ejecucion en curso (candado vigente).")

        token = uuid.uuid4().hex
        stamp = self._clock.now_iso()
        self._sheets.update_range(
            ws, "A1:C1", [[stamp, stamp, token]], "escribir intento de candado"
        )

        time.sleep(self._config.lock_confirm_seconds)
        observed = self._sheets.read_cell(ws, "C1", "confirmar el candado")
        if observed != token:
            raise LockContendedError(
                "Otra instancia tomo el candado durante la confirmacion."
            )
        self._lock_token = token

    def heartbeat(self) -> None:
        if self._lock_token is None:
            return
        ws = self._sheets.get_worksheet(self._spreadsheet, self._config.lock_tab)
        if ws is None:
            return
        try:
            self._sheets.update_cell(
                ws, 1, 2, self._clock.now_iso(), "actualizar el latido del candado"
            )
        except ExternalServiceError:
            logger.warning("No fue posible actualizar el latido del candado.")

    def _lock_is_fresh(self, stamp: str) -> bool:
        parsed = self._clock.parse(stamp)
        if parsed is None:
            return False
        age_min = (self._clock.now() - parsed).total_seconds() / 60.0
        return age_min < self._config.lock_ttl_minutes

    def release_lock(self) -> None:
        if self._lock_token is None:
            return
        ws = self._sheets.get_worksheet(self._spreadsheet, self._config.lock_tab)
        if ws is None:
            return
        try:
            observed = self._sheets.read_cell(ws, "C1", "verificar token al liberar")
            if observed and observed != self._lock_token:
                logger.warning(
                    "El candado fue tomado por otra instancia; no se libera."
                )
                return
            self._sheets.update_range(
                ws, "A1:C1", [["", "", ""]], "liberar el candado"
            )
        except ExternalServiceError:
            logger.warning("No fue posible liberar el candado; vencera por TTL.")
        finally:
            self._lock_token = None

    # -- Directorio -------------------------------------------------------- #

    def load_directory(self) -> dict[str, Advisor]:
        ws = self._sheets.require_worksheet(self._spreadsheet, self._config.directorio_tab)
        records = self._sheets.read_records(ws, "leer el directorio")
        directory: dict[str, Advisor] = {}
        for record in records:
            name = str(record.get(self._config.dir_nombre, "")).strip()
            ss_id = str(record.get(self._config.dir_spreadsheet, "")).strip()
            if not name or not ss_id:
                continue
            advisor = Advisor(name=name, spreadsheet_id=ss_id)
            directory.setdefault(advisor.key, advisor)
        logger.info("Asesores en el directorio: %d", len(directory))
        return directory

    # -- Deteccion de pendientes (lectura por columnas, sin OOM) ---------- #

    def detect_pending(
        self, directory: dict[str, Advisor], log: LogBuffer
    ) -> list[PendingReassignment]:
        """Detecta pendientes leyendo SOLO las columnas necesarias.

        En lugar de descargar la hoja entera (riesgo de OOM en hojas pesadas),
        se solicitan por batch_get unicamente las cuatro columnas relevantes:
        asesor, id, reasignaciones y procesado.
        """
        cfg = self._config
        ws = self._control()

        col_asesor_letter = column_letter(cfg.col_asesor + 1)
        col_id_letter = column_letter(cfg.col_id + 1)
        col_reas_letter = column_letter(cfg.col_reasignaciones + 1)
        col_proc_letter = column_letter(cfg.col_procesado + 1)

        ranges = [
            f"{col_asesor_letter}:{col_asesor_letter}",
            f"{col_id_letter}:{col_id_letter}",
            f"{col_reas_letter}:{col_reas_letter}",
            f"{col_proc_letter}:{col_proc_letter}",
        ]
        columns = self._sheets.batch_get(ws, ranges, "leer columnas de MADRE")

        def flat(matrix: list[list[str]]) -> list[str]:
            return [row[0] if row else "" for row in matrix]

        asesores = flat(columns[0]) if len(columns) > 0 else []
        ids = flat(columns[1]) if len(columns) > 1 else []
        reasignaciones = flat(columns[2]) if len(columns) > 2 else []
        procesados = flat(columns[3]) if len(columns) > 3 else []

        n = max(len(asesores), len(ids), len(reasignaciones), len(procesados))
        logger.info("Filas en la hoja MADRE: %d", max(n - 1, 0))

        # Validacion estructural del encabezado de la columna de IDs.
        header = ids[0] if ids else ""
        if not looks_like_header(header):
            raise StructuralIntegrityError(
                "El encabezado de la columna de IDs en MADRE no es valido; "
                "posible borrado de la fila de encabezados."
            )

        def at(seq: list[str], i: int) -> str:
            return seq[i] if i < len(seq) else ""

        # Conteo de duplicados sobre la columna de IDs.
        counts: dict[str, int] = {}
        for raw in ids[1:]:
            lead_id = sanitize_id(raw)
            if is_valid_id(lead_id):
                counts[lead_id] = counts.get(lead_id, 0) + 1
        duplicates = frozenset(k for k, v in counts.items() if v > 1)
        if duplicates:
            logger.warning(
                "Identificadores duplicados en MADRE (se omitiran): %s",
                sorted(duplicates)[:10],
            )

        pending: list[PendingReassignment] = []
        for i in range(1, n):  # fila 1 es encabezado; filas de datos desde i=1
            offset = i + 1  # numero de fila real (base 1, encabezado en 1)
            lead_id = sanitize_id(at(ids, i))
            advisor_raw = str(at(asesores, i)).strip()
            count = to_int(at(reasignaciones, i))
            processed = to_int(at(procesados, i))

            if count <= processed:
                continue
            if not is_valid_id(lead_id):
                continue
            if lead_id in duplicates:
                log.add(lead_id, Action.SKIPPED, "", advisor_raw, "ID duplicado en MADRE")
                continue
            if not advisor_raw:
                log.add(lead_id, Action.ERROR, "", "", "Asesor destino vacio en MADRE")
                continue

            target_key = strip_accents(advisor_raw)
            if target_key not in directory:
                log.add(
                    lead_id, Action.ERROR, "", advisor_raw,
                    "Asesor destino ausente del directorio",
                )
                continue

            pending.append(
                PendingReassignment(
                    madre_row=offset,
                    lead_id=lead_id,
                    target_key=target_key,
                    target_name_raw=advisor_raw,
                    reassignment_count=count,
                )
            )

        logger.info("Reasignaciones pendientes y validas: %d", len(pending))
        return pending

    def mark_processed_reconciled(self, item: PendingReassignment) -> None:
        """Marca AB := AA reubicando la fila por si hubo index shift."""
        cfg = self._config
        ws = self._control()
        id_column = self._sheets.column_values(
            ws, cfg.col_id + 1, "reubicar el lead en MADRE"
        )
        if id_column and not looks_like_header(id_column[0]):
            logger.error(
                "Encabezado de MADRE invalido al reconciliar %s; no se marca.",
                item.lead_id,
            )
            return
        target = item.lead_id
        matches = [
            offset
            for offset, raw in enumerate(id_column[1:], start=2)
            if sanitize_id(raw) == target
        ]
        if len(matches) != 1:
            logger.warning(
                "No se marca AB para %s: %d coincidencias en MADRE.",
                target, len(matches),
            )
            return
        current_row = matches[0]
        self._sheets.update_cell(
            ws, current_row, cfg.col_procesado + 1, item.reassignment_count,
            f"marcar procesada la fila {current_row}",
        )

    # -- Auditoria --------------------------------------------------------- #

    def append_log(self, log: LogBuffer) -> None:
        if not log.entries:
            return
        try:
            ws = self._sheets.get_or_create_worksheet(
                self._spreadsheet,
                self._config.log_tab,
                rows=2000,
                cols=len(_LOG_HEADER),
                header=_LOG_HEADER,
            )
            self._sheets.append_log_rows(ws, log.entries, "escribir la auditoria")
        except ExternalServiceError as exc:
            logger.warning("No fue posible escribir la auditoria: %s", exc)


# =========================================================================== #
# Repositorio de libros de asesores (batch_get de filas, sin N+1)
# =========================================================================== #

class AdvisorRepository:
    def __init__(self, config: Config, sheets: SheetsClient) -> None:
        self._config = config
        self._sheets = sheets
        self._ws_cache: dict[str, gspread.Worksheet] = {}
        self._ss_cache: dict[str, gspread.Spreadsheet] = {}

    def _spreadsheet(self, advisor: Advisor) -> gspread.Spreadsheet:
        ss = self._ss_cache.get(advisor.spreadsheet_id)
        if ss is None:
            ss = self._sheets.open_spreadsheet(advisor.spreadsheet_id)
            self._ss_cache[advisor.spreadsheet_id] = ss
        return ss

    def worksheet(self, advisor: Advisor) -> gspread.Worksheet:
        """Pestaña configurada del asesor. Nunca cae a otra hoja."""
        cached = self._ws_cache.get(advisor.spreadsheet_id)
        if cached is not None:
            return cached
        ss = self._spreadsheet(advisor)
        ws = self._sheets.get_worksheet(ss, self._config.asesor_tab)
        if ws is None:
            raise WorksheetMissingError(
                f"El libro de {advisor.name} no tiene la pestania "
                f"'{self._config.asesor_tab}'."
            )
        self._ws_cache[advisor.spreadsheet_id] = ws
        return ws

    def validated_id_column(self, advisor: Advisor) -> list[str]:
        """Lee la columna de IDs y valida su encabezado."""
        ws = self.worksheet(advisor)
        column = self._sheets.column_values(
            ws, self._config.col_id + 1, f"leer IDs de {advisor.name}"
        )
        header = column[0] if column else ""
        if not looks_like_header(header):
            raise StructuralIntegrityError(
                f"El libro de {advisor.name} parece sin encabezado en la "
                f"columna de IDs (celda: {header!r}); se omite por seguridad."
            )
        return column

    def fetch_rows(
        self, advisor: Advisor, row_numbers: Sequence[int]
    ) -> dict[int, list[str]]:
        """Descarga varias filas completas en UNA peticion (sin N+1).

        Usa batch_get con un rango discreto por fila (A{n}:{n}). Para
        conjuntos grandes, los rangos se paginan en bloques. Devuelve un mapa
        {numero_de_fila -> valores}, preservando todas las columnas.
        """
        if not row_numbers:
            return {}
        ws = self.worksheet(advisor)
        ordered = sorted(set(row_numbers))
        result: dict[int, list[str]] = {}
        for block in chunked(ordered, self._config.read_chunk_size):
            ranges = [f"A{rn}:{rn}" for rn in block]
            matrices = self._sheets.batch_get(
                ws, ranges, f"leer {len(block)} fila(s) de {advisor.name}"
            )
            for rn, matrix in zip(block, matrices):
                result[rn] = matrix[0] if matrix else []
        return result

    def first_empty_row(self, advisor: Advisor, id_column: Sequence[str]) -> int:
        """Primera fila realmente vacia segun la columna de IDs ya validada."""
        for offset, raw in enumerate(id_column[1:], start=2):
            if not str(raw).strip():
                return offset
        return len(id_column) + 1

    def batch_insert(
        self, advisor: Advisor, rows: Sequence[Sequence[str]], id_column: Sequence[str]
    ) -> list[int]:
        """Inserta varias filas (paginado) en filas vacias contiguas."""
        if not rows:
            return []
        start_row = self.first_empty_row(advisor, id_column)
        payloads: list[tuple[int, Sequence[str]]] = []
        used_rows: list[int] = []
        for index, values in enumerate(rows):
            row_number = start_row + index
            payloads.append((row_number, values))
            used_rows.append(row_number)
        ws = self.worksheet(advisor)
        self._sheets.batch_write_rows(
            ws, payloads, f"insertar {len(rows)} lead(s) en {advisor.name}"
        )
        return used_rows

    def batch_delete(self, advisor: Advisor, row_numbers: Sequence[int]) -> None:
        """Elimina varias filas (paginado) en orden inverso seguro."""
        if not row_numbers:
            return
        ss = self._spreadsheet(advisor)
        ws = self.worksheet(advisor)
        self._sheets.batch_delete_rows(
            ss, ws, row_numbers,
            f"eliminar {len(set(row_numbers))} fila(s) de {advisor.name}",
        )

    def verify_present(self, advisor: Advisor, lead_ids: Sequence[str]) -> bool:
        """Confirma que todos los IDs esten presentes, tolerando propagacion."""
        targets = {sanitize_id(x) for x in lead_ids}
        ws = self.worksheet(advisor)
        for attempt in range(1, self._config.verify_attempts + 1):
            column = self._sheets.column_values(
                ws, self._config.col_id + 1, f"verificar en {advisor.name}"
            )
            present = {sanitize_id(raw) for raw in column[1:]}
            if targets.issubset(present):
                return True
            if attempt < self._config.verify_attempts:
                time.sleep(self._config.verify_interval_seconds)
        return False


# =========================================================================== #
# Plan de movimientos agrupado
# =========================================================================== #

@dataclass
class MoveOrder:
    item: PendingReassignment
    origin_key: str
    placement: LeadPlacement
    full_row: tuple[str, ...]


@dataclass
class TargetBatch:
    advisor: Advisor
    orders: list[MoveOrder] = field(default_factory=list)


@dataclass
class OriginBatch:
    advisor: Advisor
    rows: list[int] = field(default_factory=list)


@dataclass
class _Classified:
    """Resultado intermedio de clasificar los pendientes antes de leer filas."""

    movable_by_origin: dict[str, list[PendingReassignment]] = field(default_factory=dict)
    placements: dict[int, LeadPlacement] = field(default_factory=dict)
    to_mark: list[PendingReassignment] = field(default_factory=list)


# =========================================================================== #
# Orquestador
# =========================================================================== #

class ReassignmentHandler:
    def __init__(
        self,
        config: Config,
        madre: MadreRepository,
        advisors: AdvisorRepository,
        clock: Clock,
        dry_run: bool,
    ) -> None:
        self._config = config
        self._madre = madre
        self._advisors = advisors
        self._clock = clock
        self._dry_run = dry_run
        self._id_columns: dict[str, list[str]] = {}

    @contextmanager
    def _lock(self) -> Iterator[None]:
        acquired = False
        if not self._dry_run:
            self._madre.acquire_lock()
            acquired = True
        try:
            yield
        finally:
            if acquired:
                self._madre.release_lock()

    # -- Indice (lectura minima + validacion de encabezado) --------------- #

    def _build_index(
        self, directory: dict[str, Advisor], log: LogBuffer, stats: RunStats
    ) -> LeadIndex:
        index = LeadIndex()
        total = len(directory)
        for position, advisor in enumerate(directory.values(), start=1):
            logger.info("  Indexando %d/%d: %s", position, total, advisor.name)
            try:
                id_column = self._advisors.validated_id_column(advisor)
            except WorksheetMissingError as exc:
                logger.error("Asesor omitido (sin pestania valida): %s", exc)
                log.add("", Action.ERROR, advisor.name, "", str(exc))
                continue
            except StructuralIntegrityError as exc:
                logger.error("Asesor omitido (hoja corrupta): %s", exc)
                log.add("", Action.CORRUPT, advisor.name, "", str(exc))
                stats.corrupt_sheets += 1
                continue
            except ExternalServiceError as exc:
                logger.warning("No se pudo indexar a %s: %s", advisor.name, exc)
                continue
            self._id_columns[advisor.key] = id_column
            index.register_column(advisor, id_column)
        return index

    # -- Clasificacion (sin leer filas todavia) --------------------------- #

    def _classify(
        self,
        pending: Sequence[PendingReassignment],
        directory: dict[str, Advisor],
        index: LeadIndex,
        log: LogBuffer,
        stats: RunStats,
    ) -> _Classified:
        """Clasifica cada pendiente resolviendo los casos triviales.

        Agrupa los movimientos reales por asesor origen, SIN leer aun las
        filas (esa lectura se hace luego en bloque, una peticion por origen).
        """
        result = _Classified()

        for item in pending:
            target = directory[item.target_key]
            placement = index.locate(item.lead_id)

            if placement is not None and placement.advisor_key == item.target_key:
                logger.info("Lead %s ya pertenece a %s.", item.lead_id, target.name)
                result.to_mark.append(item)
                if placement.duplicate_rows:
                    stats.deduplicated += len(placement.duplicate_rows)
                    log.add(
                        item.lead_id, Action.DEDUP, target.name, target.name,
                        f"{len(placement.duplicate_rows)} duplicado(s) local(es)",
                    )
                    # Las filas duplicadas en el destino se programaran como
                    # borrado del propio destino mas adelante.
                    result.placements[id(item)] = placement
                log.add(
                    item.lead_id, Action.ALREADY_OK, target.name, target.name,
                    "El lead ya estaba en el asesor destino",
                )
                stats.already_ok += 1
                continue

            if placement is None:
                logger.warning(
                    "Lead %s no encontrado en ninguna hoja. Se reintentara.",
                    item.lead_id,
                )
                log.add(
                    item.lead_id, Action.PENDING, "", item.target_name_raw,
                    "El lead aun no existe en ninguna hoja",
                )
                stats.pending += 1
                continue

            origin = directory.get(placement.advisor_key)
            if origin is None:
                logger.warning(
                    "Lead %s en asesor fuera de directorio. Se reintentara.",
                    item.lead_id,
                )
                log.add(
                    item.lead_id, Action.ERROR, placement.advisor_key,
                    item.target_name_raw, "Origen fuera del directorio",
                )
                stats.errors += 1
                continue

            if self._dry_run:
                logger.info(
                    "Simulacion: lead %s se moveria de %s a %s.",
                    item.lead_id, origin.name, target.name,
                )
                log.add(
                    item.lead_id, Action.SIMULATED, origin.name, target.name,
                    "Sin cambios (simulacion)",
                )
                stats.moved += 1
                continue

            result.movable_by_origin.setdefault(origin.key, []).append(item)
            result.placements[id(item)] = placement

        return result

    # -- Construccion de lotes (lectura de filas en bloque, sin N+1) ------- #

    def _build_batches(
        self,
        classified: _Classified,
        directory: dict[str, Advisor],
        log: LogBuffer,
        stats: RunStats,
    ) -> tuple[dict[str, TargetBatch], dict[str, OriginBatch]]:
        target_batches: dict[str, TargetBatch] = {}
        origin_batches: dict[str, OriginBatch] = {}

        # Programar borrado de duplicados locales en destinos 'ya correctos'.
        for item in classified.to_mark:
            placement = classified.placements.get(id(item))
            if placement is None or not placement.duplicate_rows:
                continue
            advisor = directory[item.target_key]
            batch = origin_batches.setdefault(advisor.key, OriginBatch(advisor=advisor))
            batch.rows.extend(placement.duplicate_rows)

        # Movimientos reales: una lectura batch_get por asesor origen.
        for origin_key, items in classified.movable_by_origin.items():
            origin = directory[origin_key]
            rows_needed = [
                classified.placements[id(item)].primary_row for item in items
            ]
            try:
                fetched = self._advisors.fetch_rows(origin, rows_needed)
            except (ExternalServiceError, WorksheetMissingError) as exc:
                logger.error("Lectura batch de %s fallida: %s", origin.name, exc)
                for item in items:
                    log.add(
                        item.lead_id, Action.ERROR, origin.name, item.target_name_raw,
                        f"Lectura batch de origen fallida: {exc}"[:250],
                    )
                    stats.errors += 1
                continue

            for item in items:
                placement = classified.placements[id(item)]
                target = directory[item.target_key]
                full_row = fetched.get(placement.primary_row, [])
                if not any(str(c).strip() for c in full_row):
                    # La fila vino vacia: el dato cambio bajo nuestros pies.
                    logger.warning(
                        "Fila %d de %s vacia al releer; se reintentara %s.",
                        placement.primary_row, origin.name, item.lead_id,
                    )
                    log.add(
                        item.lead_id, Action.PENDING, origin.name, target.name,
                        "La fila de origen aparecio vacia al releer",
                    )
                    stats.pending += 1
                    continue

                order = MoveOrder(
                    item=item,
                    origin_key=origin.key,
                    placement=placement,
                    full_row=tuple(full_row),
                )
                target_batches.setdefault(
                    target.key, TargetBatch(advisor=target)
                ).orders.append(order)

                o_batch = origin_batches.setdefault(
                    origin.key, OriginBatch(advisor=origin)
                )
                o_batch.rows.append(placement.primary_row)
                if placement.duplicate_rows:
                    o_batch.rows.extend(placement.duplicate_rows)
                    stats.deduplicated += len(placement.duplicate_rows)

        return target_batches, origin_batches

    # -- Ejecucion de lotes ----------------------------------------------- #

    def _execute(
        self,
        target_batches: dict[str, TargetBatch],
        origin_batches: dict[str, OriginBatch],
        to_mark: list[PendingReassignment],
        log: LogBuffer,
        stats: RunStats,
    ) -> None:
        for target_batch in target_batches.values():
            advisor = target_batch.advisor
            orders = target_batch.orders
            rows_to_write = [o.full_row for o in orders]
            lead_ids = [o.item.lead_id for o in orders]

            id_column = self._id_columns.get(advisor.key)
            if id_column is None:
                try:
                    id_column = self._advisors.validated_id_column(advisor)
                except (ExternalServiceError, WorksheetMissingError, StructuralIntegrityError) as exc:
                    logger.error("Destino %s inutilizable: %s", advisor.name, exc)
                    for o in orders:
                        self._cancel_order(o, origin_batches)
                        log.add(
                            o.item.lead_id, Action.ERROR, "", advisor.name,
                            f"Destino inutilizable: {exc}"[:250],
                        )
                        stats.errors += 1
                    continue

            try:
                self._advisors.batch_insert(advisor, rows_to_write, id_column)
            except (ExternalServiceError, WorksheetMissingError) as exc:
                logger.error("Insercion fallida en %s: %s", advisor.name, exc)
                for o in orders:
                    self._cancel_order(o, origin_batches)
                    log.add(
                        o.item.lead_id, Action.ERROR, "", advisor.name,
                        f"Insercion batch fallida: {exc}"[:250],
                    )
                    stats.errors += 1
                continue

            time.sleep(self._config.write_propagation_seconds)

            if not self._advisors.verify_present(advisor, lead_ids):
                logger.error(
                    "No se verifico la insercion batch en %s.", advisor.name
                )
                for o in orders:
                    self._cancel_order(o, origin_batches)
                    log.add(
                        o.item.lead_id, Action.ERROR, "", advisor.name,
                        "No se verifico la insercion batch en destino",
                    )
                    stats.errors += 1
                continue

            for o in orders:
                logger.info(
                    "Lead %s insertado en %s.", o.item.lead_id, advisor.name
                )
                log.add(o.item.lead_id, Action.MOVED, "", advisor.name, "Insertado")
                to_mark.append(o.item)
                stats.moved += 1

        for origin_batch in origin_batches.values():
            advisor = origin_batch.advisor
            try:
                self._advisors.batch_delete(advisor, origin_batch.rows)
                logger.info(
                    "Eliminadas %d fila(s) de %s.",
                    len(set(origin_batch.rows)), advisor.name,
                )
            except (ExternalServiceError, WorksheetMissingError) as exc:
                logger.error("Borrado fallido en %s: %s", advisor.name, exc)
                log.add(
                    "", Action.ERROR, advisor.name, "",
                    f"Borrado batch fallido: {exc}"[:250],
                )

        seen: set[int] = set()
        for item in to_mark:
            if id(item) in seen:
                continue
            seen.add(id(item))
            try:
                self._madre.mark_processed_reconciled(item)
            except ExternalServiceError as exc:
                logger.error(
                    "No se pudo marcar AB para %s: %s", item.lead_id, exc
                )

    def _cancel_order(
        self, order: MoveOrder, origin_batches: dict[str, OriginBatch]
    ) -> None:
        """Revierte la programacion de borrado de una orden fallida."""
        batch = origin_batches.get(order.origin_key)
        if batch is None:
            return
        for row in order.placement.rows:
            if row in batch.rows:
                batch.rows.remove(row)

    # -- Heartbeat entre fases -------------------------------------------- #

    def _beat(self, last_beat: float) -> float:
        now = time.monotonic()
        if not self._dry_run and (now - last_beat) >= self._config.heartbeat_interval_seconds:
            self._madre.heartbeat()
            return now
        return last_beat

    def run(self) -> RunStats:
        stats = RunStats()
        log = LogBuffer(clock=self._clock)
        with self._lock():
            last_beat = time.monotonic()
            directory = self._madre.load_directory()
            pending = self._madre.detect_pending(directory, log)

            if not pending:
                logger.info("No hay reasignaciones pendientes.")
                self._madre.append_log(log)
                return stats

            index = self._build_index(directory, log, stats)
            last_beat = self._beat(last_beat)

            classified = self._classify(pending, directory, index, log, stats)
            last_beat = self._beat(last_beat)

            if self._dry_run:
                logger.info(
                    "Simulacion: %d entrada(s) de auditoria; "
                    "%d origen(es) con movimientos.",
                    len(log.entries), len(classified.movable_by_origin),
                )
                return stats

            target_batches, origin_batches = self._build_batches(
                classified, directory, log, stats
            )
            last_beat = self._beat(last_beat)

            self._execute(
                target_batches, origin_batches, list(classified.to_mark), log, stats
            )
            self._madre.append_log(log)

        return stats


# =========================================================================== #
# Punto de entrada
# =========================================================================== #

def main(argv: Optional[Sequence[str]] = None) -> int:
    configure_logging()
    args = list(argv) if argv is not None else sys.argv[1:]
    dry_run = "--dry-run" in args

    mode = "SIMULACION (sin escritura)" if dry_run else "EJECUCION REAL"
    logger.info("Iniciando handler de reasignacion | modo: %s", mode)

    try:
        config = Config.from_environment()
        clock = Clock(config.timezone_name)
        sheets = SheetsClient(config)
        madre = MadreRepository(config, sheets, clock)
        advisors = AdvisorRepository(config, sheets)
        handler = ReassignmentHandler(config, madre, advisors, clock, dry_run)
        stats = handler.run()
    except LockActiveError as exc:
        logger.info("Ejecucion omitida: %s", exc)
        return 0
    except LockContendedError as exc:
        logger.info("Ejecucion abortada por contencion de candado: %s", exc)
        return 0
    except ConfigurationError as exc:
        logger.error("Error de configuracion: %s", exc)
        return 2
    except StructuralIntegrityError as exc:
        logger.error("Integridad estructural comprometida: %s", exc)
        return 1
    except ReassignmentError as exc:
        logger.error("La reasignacion fallo: %s", exc)
        return 1
    except Exception:  # noqa: BLE001
        logger.exception("Error inesperado durante la reasignacion.")
        return 1

    for line in stats.summary_lines(dry_run):
        logger.info(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
