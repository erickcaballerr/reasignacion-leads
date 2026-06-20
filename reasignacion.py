"""
Handler de reasignacion de leads para RE/MAX Terra.

Cuando Beefast reasigna un lead a otro asesor, actualiza en la hoja MADRE
la columna ASESOR y aumenta en uno el contador de reasignaciones, pero no
mueve el lead entre las hojas individuales de cada asesor. Este proceso
cierra esa brecha de forma idempotente y a prueba de fallos.

Flujo:
    1. Toma un candado de ejecucion para impedir corridas solapadas.
    2. Lee el directorio de asesores (nombre -> id de su libro).
    3. Detecta en MADRE las filas cuyo contador de reasignaciones supera al
       contador de reasignaciones ya procesadas (AA > AB).
    4. Construye en memoria un indice {ID BEEFAST -> ubicacion} leyendo cada
       libro de asesor una sola vez (lectura O(asesores), no O(pendientes x
       asesores)).
    5. Para cada pendiente: agrega la fila al asesor destino, verifica la
       insercion con reintentos, elimina del asesor origen y marca la
       reasignacion como procesada (AB := AA) en MADRE.
    6. Registra cada operacion en la hoja de auditoria y libera el candado.

Garantias de diseno:
    - Idempotencia: ejecutar repetidamente converge al mismo estado.
    - No perdida: el orden agregar -> verificar -> eliminar asegura que un
      lead nunca desaparezca; el peor caso es un duplicado transitorio que
      una corrida posterior reconcilia.
    - Resiliencia: toda llamada de red reintenta ante limites de tasa (429)
      y errores transitorios (5xx) con espera incremental.
    - Robustez de datos: identificadores no numericos o en cero se ignoran;
      duplicados en MADRE se registran y omiten; contadores ausentes valen
      cero; filas mas cortas de lo esperado se normalizan.
    - Aislamiento de fallos: un asesor inaccesible o un lead irresoluble no
      detiene el lote; se registra y se reintenta en la siguiente corrida.

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
import sys
import time
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterator, Optional, Sequence, TypeVar

try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.exceptions import APIError, GSpreadException, WorksheetNotFound
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
    """Parametros de ejecucion. Inmutable para evitar mutaciones accidentales."""

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
    total_cols: int = 28

    # Encabezados del directorio.
    dir_nombre: str = "NOMBRE_ASESOR"
    dir_spreadsheet: str = "ID_SPREADSHEET"

    # Control de trafico y resiliencia de red.
    api_pause_seconds: float = 1.2
    max_retries: int = 6
    backoff_base_seconds: float = 4.0
    backoff_cap_seconds: float = 60.0

    # Latencia de propagacion de Google tras una escritura, antes de releer.
    write_propagation_seconds: float = 4.0
    verify_attempts: int = 4
    verify_interval_seconds: float = 3.0

    # Vigencia del candado de ejecucion.
    lock_ttl_minutes: float = 10.0

    @classmethod
    def from_environment(cls) -> "Config":
        google_creds = os.environ.get("GOOGLE_CREDENTIALS", "").strip()
        if not google_creds:
            raise ConfigurationError(
                "Falta la variable de entorno GOOGLE_CREDENTIALS."
            )
        return cls(google_credentials=google_creds)


# Conjuntos de tokens que representan ausencia de valor.
_NULL_ID_TOKENS = frozenset({"", "NAN", "NONE", "NULL", "0"})
_NULL_INT_TOKENS = frozenset({"", "NAN", "NONE", "NULL"})

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


class LockActiveError(ReassignmentError):
    """Otra ejecucion mantiene el candado vigente."""


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
# Funciones puras de normalizacion
# =========================================================================== #

def strip_accents(value: object) -> str:
    """Normaliza a minusculas, sin acentos y con espacios colapsados."""
    text = " ".join(str(value).strip().split()).lower()
    decomposed = unicodedata.normalize("NFD", text)
    return decomposed.encode("ascii", "ignore").decode("utf-8")


def is_valid_id(value: object) -> bool:
    """Un identificador es valido si es estrictamente numerico y mayor que cero."""
    text = str(value).strip()
    if text.upper() in _NULL_ID_TOKENS:
        return False
    return text.isdigit() and int(text) > 0


def normalize_id(value: object) -> str:
    """Forma canonica de un identificador para comparaciones e indexado."""
    return str(value).strip()


def to_int(value: object) -> int:
    """Convierte a entero de forma tolerante; ausencia equivale a cero."""
    text = str(value).strip()
    if text.upper() in _NULL_INT_TOKENS:
        return 0
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return 0


def pad_row(row: Sequence[str], width: int) -> list[str]:
    """Garantiza que una fila tenga exactamente 'width' columnas."""
    values = list(row[:width])
    if len(values) < width:
        values.extend([""] * (width - len(values)))
    return values


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# =========================================================================== #
# Cliente de Google Sheets con reintentos y resolucion segura de pestañas
# =========================================================================== #

class SheetsClient:
    """Acceso a Google Sheets con reintentos uniformes.

    Centraliza dos responsabilidades que antes estaban dispersas y eran
    fuente de errores: el reintento ante fallos transitorios y la resolucion
    de pestañas (que distingue 'no existe' de 'fallo de red' sin depender de
    inspeccionar causas anidadas).
    """

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
        """Ejecuta una operacion de red reintentando fallos transitorios.

        Reintenta ante codigos 429 y 5xx con espera exponencial acotada.
        Cualquier otro APIError o error de gspread se considera definitivo y
        se reempaqueta como ExternalServiceError, preservando la causa.
        """
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
                # Errores de gspread que no son APIError (p. ej. parsing).
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
        """Devuelve la pestaña por titulo, o None si no existe.

        Distingue limpiamente 'no existe' (None) de 'fallo de red' (excepcion)
        sin que el llamador tenga que inspeccionar causas anidadas.
        """
        try:
            return self.call(
                lambda: spreadsheet.worksheet(title),
                f"abrir la pestania '{title}'",
            )
        except ExternalServiceError as exc:
            if isinstance(exc.__cause__, WorksheetNotFound):
                return None
            raise

    def get_or_create_worksheet(
        self,
        spreadsheet: gspread.Spreadsheet,
        title: str,
        rows: int,
        cols: int,
        header: Optional[Sequence[str]] = None,
    ) -> gspread.Worksheet:
        """Obtiene la pestaña o la crea (con encabezado opcional) si falta."""
        worksheet = self.get_worksheet(spreadsheet, title)
        if worksheet is not None:
            return worksheet
        worksheet = self.call(
            lambda: spreadsheet.add_worksheet(title=title, rows=rows, cols=cols),
            f"crear la pestania '{title}'",
        )
        if header is not None:
            self.call(
                lambda: worksheet.update([list(header)], "A1"),
                f"inicializar la pestania '{title}'",
            )
        return worksheet

    def first_worksheet(self, spreadsheet: gspread.Spreadsheet) -> gspread.Worksheet:
        return self.call(
            lambda: spreadsheet.get_worksheet(0),
            "abrir la primera pestania",
        )

    def read_values(
        self, worksheet: gspread.Worksheet, description: str
    ) -> list[list[str]]:
        return self.call(worksheet.get_all_values, description)

    def read_records(
        self, worksheet: gspread.Worksheet, description: str
    ) -> list[dict[str, object]]:
        return self.call(worksheet.get_all_records, description)


# =========================================================================== #
# Modelo de dominio (estructuras inmutables)
# =========================================================================== #

@dataclass(frozen=True)
class Advisor:
    """Asesor del directorio corporativo."""

    name: str
    spreadsheet_id: str

    @property
    def key(self) -> str:
        return strip_accents(self.name)


@dataclass(frozen=True)
class LeadLocation:
    """Ubicacion de un lead dentro del libro de un asesor."""

    advisor: Advisor
    row_number: int


@dataclass(frozen=True)
class PendingReassignment:
    """Reasignacion detectada en MADRE, pendiente de aplicar."""

    madre_row: int
    lead_id: str
    target_key: str
    target_name_raw: str
    reassignment_count: int
    row_values: tuple[str, ...]


class Action:
    """Catalogo de acciones registrables en la auditoria."""

    MOVED = "MOVIDO"
    ALREADY_OK = "YA_CORRECTO"
    PENDING = "PENDIENTE"
    ERROR = "ERROR"
    SKIPPED = "OMITIDO"
    SIMULATED = "SIMULACION"


@dataclass
class LogBuffer:
    """Acumula entradas de auditoria para escritura en lote."""

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
            [utc_now_iso(), lead_id, action, previous_advisor, new_advisor, detail]
        )


@dataclass
class RunStats:
    """Contadores del resultado de una corrida."""

    moved: int = 0
    already_ok: int = 0
    pending: int = 0
    errors: int = 0
    skipped: int = 0

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
            "-" * 58,
        ]


# =========================================================================== #
# Indice en memoria de leads por asesor
# =========================================================================== #

class LeadIndex:
    """Indice {ID BEEFAST -> ubicacion} construido de una sola lectura.

    Evita el patron O(pendientes x asesores): en lugar de buscar cada lead en
    los 26 libros, se leen los 26 libros una vez y el cruce posterior es O(1).
    """

    def __init__(self) -> None:
        self._by_id: dict[str, LeadLocation] = {}

    def register(
        self, advisor: Advisor, rows: Sequence[Sequence[str]], id_col: int
    ) -> None:
        for offset, row in enumerate(rows[1:], start=2):
            if len(row) <= id_col:
                continue
            lead_id = normalize_id(row[id_col])
            if not is_valid_id(lead_id):
                continue
            # La primera aparicion prevalece; conflictos internos de una hoja
            # se ignoran a este nivel (los duplicados en MADRE se filtran
            # aparte). setdefault evita sobrescribir.
            self._by_id.setdefault(
                lead_id, LeadLocation(advisor=advisor, row_number=offset)
            )

    def locate(self, lead_id: str) -> Optional[LeadLocation]:
        return self._by_id.get(normalize_id(lead_id))


# =========================================================================== #
# Repositorio de la hoja MADRE
# =========================================================================== #

class MadreRepository:
    """Encapsula toda lectura/escritura sobre el libro MADRE."""

    def __init__(self, config: Config, sheets: SheetsClient) -> None:
        self._config = config
        self._sheets = sheets
        self._spreadsheet = sheets.open_spreadsheet(config.madre_sheet_id)
        self._control_ws: Optional[gspread.Worksheet] = None

    # -- Pestaña principal ------------------------------------------------- #

    def _control_worksheet(self) -> gspread.Worksheet:
        if self._control_ws is None:
            ws = self._sheets.get_worksheet(self._spreadsheet, self._config.madre_tab)
            if ws is None:
                raise ReassignmentError(
                    f"No existe la pestania '{self._config.madre_tab}' en MADRE."
                )
            self._control_ws = ws
        return self._control_ws

    # -- Candado ----------------------------------------------------------- #

    def acquire_lock(self) -> None:
        """Toma el candado o aborta si hay uno vigente.

        El candado es una pestaña con una marca temporal en A1. Si la marca
        es reciente, otra ejecucion esta en curso. Marcas ilegibles o
        vencidas se sobrescriben.
        """
        ws = self._sheets.get_worksheet(self._spreadsheet, self._config.lock_tab)
        if ws is not None:
            values = self._sheets.read_values(ws, "leer el candado")
            stamp = values[0][0] if values and values[0] else ""
            if self._lock_is_fresh(stamp):
                raise LockActiveError("Hay una ejecucion en curso (candado vigente).")
        else:
            ws = self._sheets.get_or_create_worksheet(
                self._spreadsheet, self._config.lock_tab, rows=1, cols=1
            )
        self._sheets.call(
            lambda: ws.update([[utc_now_iso()]], "A1"), "tomar el candado"
        )

    def _lock_is_fresh(self, stamp: str) -> bool:
        if not stamp:
            return False
        try:
            stamped = datetime.fromisoformat(stamp)
        except ValueError:
            return False
        if stamped.tzinfo is None:
            stamped = stamped.replace(tzinfo=timezone.utc)
        age_minutes = (datetime.now(timezone.utc) - stamped).total_seconds() / 60.0
        return age_minutes < self._config.lock_ttl_minutes

    def release_lock(self) -> None:
        ws = self._sheets.get_worksheet(self._spreadsheet, self._config.lock_tab)
        if ws is None:
            return
        try:
            self._sheets.call(
                lambda: ws.update([[""]], "A1"), "liberar el candado"
            )
        except ExternalServiceError:
            logger.warning("No fue posible liberar el candado; vencera por TTL.")

    # -- Directorio -------------------------------------------------------- #

    def load_directory(self) -> dict[str, Advisor]:
        ws = self._sheets.get_worksheet(self._spreadsheet, self._config.directorio_tab)
        if ws is None:
            raise ReassignmentError(
                f"No existe la pestania '{self._config.directorio_tab}'."
            )
        records = self._sheets.read_records(ws, "leer el directorio")
        directory: dict[str, Advisor] = {}
        for record in records:
            name = str(record.get(self._config.dir_nombre, "")).strip()
            ss_id = str(record.get(self._config.dir_spreadsheet, "")).strip()
            if not name or not ss_id:
                continue
            advisor = Advisor(name=name, spreadsheet_id=ss_id)
            # Si dos filas comparten nombre normalizado, la primera prevalece.
            directory.setdefault(advisor.key, advisor)
        logger.info("Asesores en el directorio: %d", len(directory))
        return directory

    # -- Deteccion de pendientes ------------------------------------------ #

    def detect_pending(
        self, directory: dict[str, Advisor]
    ) -> tuple[list[PendingReassignment], LogBuffer]:
        ws = self._control_worksheet()
        values = self._sheets.read_values(ws, "leer la hoja MADRE")
        logger.info("Filas en la hoja MADRE: %d", max(len(values) - 1, 0))

        duplicates = self._duplicate_ids(values)
        if duplicates:
            logger.warning(
                "Identificadores duplicados en MADRE (se omitiran): %s",
                sorted(duplicates)[:10],
            )

        cfg = self._config
        pending: list[PendingReassignment] = []
        log = LogBuffer()

        for offset, raw in enumerate(values[1:], start=2):
            row = pad_row(raw, cfg.total_cols)
            lead_id = normalize_id(row[cfg.col_id])
            advisor_raw = str(row[cfg.col_asesor]).strip()
            count = to_int(row[cfg.col_reasignaciones])
            processed = to_int(row[cfg.col_procesado])

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
                    row_values=tuple(row),
                )
            )

        logger.info("Reasignaciones pendientes y validas: %d", len(pending))
        return pending, log

    def _duplicate_ids(self, values: Sequence[Sequence[str]]) -> frozenset[str]:
        counts: dict[str, int] = {}
        col = self._config.col_id
        for row in values[1:]:
            if len(row) > col:
                lead_id = normalize_id(row[col])
                if is_valid_id(lead_id):
                    counts[lead_id] = counts.get(lead_id, 0) + 1
        return frozenset(lead_id for lead_id, n in counts.items() if n > 1)

    def mark_processed(self, item: PendingReassignment) -> None:
        ws = self._control_worksheet()
        self._sheets.call(
            lambda: ws.update_cell(
                item.madre_row, self._config.col_procesado + 1, item.reassignment_count
            ),
            f"marcar procesada la fila {item.madre_row}",
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
            self._sheets.call(
                lambda: ws.append_rows(log.entries, value_input_option="RAW"),
                "escribir la auditoria",
            )
        except ExternalServiceError as exc:
            logger.warning("No fue posible escribir la auditoria: %s", exc)


# =========================================================================== #
# Repositorio de los libros de asesores
# =========================================================================== #

class AdvisorRepository:
    """Encapsula lectura/escritura sobre los libros individuales."""

    def __init__(self, config: Config, sheets: SheetsClient) -> None:
        self._config = config
        self._sheets = sheets

    def _worksheet(self, advisor: Advisor) -> gspread.Worksheet:
        spreadsheet = self._sheets.open_spreadsheet(advisor.spreadsheet_id)
        ws = self._sheets.get_worksheet(spreadsheet, self._config.asesor_tab)
        if ws is None:
            ws = self._sheets.first_worksheet(spreadsheet)
        return ws

    def read_rows(self, advisor: Advisor) -> Optional[list[list[str]]]:
        try:
            ws = self._worksheet(advisor)
            return self._sheets.read_values(ws, f"leer el libro de {advisor.name}")
        except ExternalServiceError as exc:
            logger.warning("No se pudo leer el libro de %s: %s", advisor.name, exc)
            return None

    def append_lead(self, advisor: Advisor, row_values: Sequence[str]) -> None:
        ws = self._worksheet(advisor)
        payload = pad_row(row_values, self._config.total_cols)
        self._sheets.call(
            lambda: ws.append_row(payload, value_input_option="RAW"),
            f"agregar el lead a {advisor.name}",
        )

    def delete_row(self, advisor: Advisor, row_number: int) -> None:
        ws = self._worksheet(advisor)
        self._sheets.call(
            lambda: ws.delete_rows(row_number),
            f"eliminar la fila {row_number} de {advisor.name}",
        )

    def contains_lead(self, advisor: Advisor, lead_id: str) -> bool:
        ws = self._worksheet(advisor)
        values = self._sheets.read_values(ws, f"verificar en {advisor.name}")
        target = normalize_id(lead_id)
        col = self._config.col_id
        for row in values[1:]:
            if len(row) > col and normalize_id(row[col]) == target:
                return True
        return False

    def verify_present(self, advisor: Advisor, lead_id: str) -> bool:
        """Confirma la presencia de un lead reintentando ante la latencia de
        propagacion de Google tras una escritura."""
        for attempt in range(1, self._config.verify_attempts + 1):
            if self.contains_lead(advisor, lead_id):
                return True
            if attempt < self._config.verify_attempts:
                time.sleep(self._config.verify_interval_seconds)
        return False


# =========================================================================== #
# Orquestador
# =========================================================================== #

class ReassignmentHandler:
    """Coordina la deteccion y aplicacion de reasignaciones."""

    def __init__(
        self,
        config: Config,
        madre: MadreRepository,
        advisors: AdvisorRepository,
        dry_run: bool,
    ) -> None:
        self._config = config
        self._madre = madre
        self._advisors = advisors
        self._dry_run = dry_run

    @contextmanager
    def _lock(self) -> Iterator[None]:
        """Gestiona el candado de forma segura aun ante excepciones."""
        acquired = False
        if not self._dry_run:
            self._madre.acquire_lock()
            acquired = True
        try:
            yield
        finally:
            if acquired:
                self._madre.release_lock()

    def _build_index(self, directory: dict[str, Advisor]) -> LeadIndex:
        index = LeadIndex()
        total = len(directory)
        for position, advisor in enumerate(directory.values(), start=1):
            logger.info("  Indexando %d/%d: %s", position, total, advisor.name)
            rows = self._advisors.read_rows(advisor)
            if rows is not None:
                index.register(advisor, rows, self._config.col_id)
            time.sleep(self._config.api_pause_seconds)
        return index

    def _process(
        self,
        item: PendingReassignment,
        directory: dict[str, Advisor],
        index: LeadIndex,
        log: LogBuffer,
        stats: RunStats,
    ) -> None:
        target = directory[item.target_key]
        current = index.locate(item.lead_id)

        # Caso 1: ya esta en el asesor correcto -> solo marcar procesado.
        if current is not None and current.advisor.key == item.target_key:
            logger.info("Lead %s ya pertenece a %s.", item.lead_id, target.name)
            if not self._dry_run:
                self._madre.mark_processed(item)
            log.add(
                item.lead_id, Action.ALREADY_OK, target.name, target.name,
                "El lead ya estaba en el asesor destino",
            )
            stats.already_ok += 1
            return

        # Caso 2: el lead aun no existe en ninguna hoja -> reintentar luego.
        if current is None:
            logger.warning(
                "Lead %s no encontrado en ninguna hoja. Se reintentara.",
                item.lead_id,
            )
            log.add(
                item.lead_id, Action.PENDING, "", item.target_name_raw,
                "El lead aun no existe en ninguna hoja",
            )
            stats.pending += 1
            return

        # Caso 3: simulacion.
        if self._dry_run:
            logger.info(
                "Simulacion: lead %s se moveria de %s a %s.",
                item.lead_id, current.advisor.name, target.name,
            )
            log.add(
                item.lead_id, Action.SIMULATED, current.advisor.name, target.name,
                "Sin cambios (simulacion)",
            )
            stats.moved += 1
            return

        # Caso 4: mover de verdad.
        self._move(item, current, target, log, stats)

    def _move(
        self,
        item: PendingReassignment,
        current: LeadLocation,
        target: Advisor,
        log: LogBuffer,
        stats: RunStats,
    ) -> None:
        """Mueve el lead con la garantia de no perdida.

        Orden estricto e innegociable: agregar al destino, esperar la
        propagacion, verificar, eliminar del origen y recien entonces marcar
        procesado. Si algo falla, no se marca procesado y la fila permanece
        elegible para la siguiente corrida; el lead jamas desaparece.
        """
        origin = current.advisor
        try:
            self._advisors.append_lead(target, list(item.row_values))
            time.sleep(self._config.write_propagation_seconds)

            if not self._advisors.verify_present(target, item.lead_id):
                raise ExternalServiceError(
                    "No se verifico la insercion en el asesor destino."
                )

            self._advisors.delete_row(origin, current.row_number)
            self._madre.mark_processed(item)

            logger.info(
                "Lead %s movido de %s a %s.",
                item.lead_id, origin.name, target.name,
            )
            log.add(item.lead_id, Action.MOVED, origin.name, target.name, "OK")
            stats.moved += 1

        except ExternalServiceError as exc:
            logger.error("Error moviendo el lead %s: %s", item.lead_id, exc)
            log.add(
                item.lead_id, Action.ERROR, origin.name, target.name, str(exc)[:250]
            )
            stats.errors += 1

    def run(self) -> RunStats:
        stats = RunStats()
        with self._lock():
            directory = self._madre.load_directory()
            pending, log = self._madre.detect_pending(directory)

            if not pending:
                logger.info("No hay reasignaciones pendientes.")
                self._madre.append_log(log)
                return stats

            index = self._build_index(directory)

            for item in pending:
                logger.info(
                    "Procesando lead %s -> %s",
                    item.lead_id, item.target_name_raw,
                )
                self._process(item, directory, index, log, stats)

            if self._dry_run:
                logger.info(
                    "Simulacion: se habrian registrado %d entradas de auditoria.",
                    len(log.entries),
                )
            else:
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
        sheets = SheetsClient(config)
        madre = MadreRepository(config, sheets)
        advisors = AdvisorRepository(config, sheets)
        handler = ReassignmentHandler(config, madre, advisors, dry_run)
        stats = handler.run()
    except LockActiveError as exc:
        logger.info("Ejecucion omitida: %s", exc)
        return 0
    except ConfigurationError as exc:
        logger.error("Error de configuracion: %s", exc)
        return 2
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
