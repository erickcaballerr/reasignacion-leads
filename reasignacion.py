"""
Handler de reasignacion de leads para RE/MAX Terra.

Cuando Beefast reasigna un lead a otro asesor, actualiza en la hoja MADRE
la columna ASESOR y aumenta en uno el contador de reasignaciones, pero no
mueve el lead entre las hojas individuales de cada asesor. Este proceso
cierra esa brecha:

    1. Detecta en la hoja MADRE las filas cuya cantidad de reasignaciones
       supera el contador de reasignaciones ya procesadas (AA > AB).
    2. Para cada una, determina el asesor destino y el identificador del
       lead (ID BEEFAST).
    3. Localiza el lead en la hoja del asesor que lo posee actualmente.
    4. Lo agrega a la hoja del asesor destino, verifica la insercion y solo
       entonces lo elimina del asesor de origen.
    5. Marca la reasignacion como procesada (AB := AA) en la hoja MADRE.
    6. Registra el resultado de cada operacion en una hoja de auditoria.

Diseno defensivo:
    - Identificadores no numericos o en cero se ignoran.
    - Identificadores duplicados en la hoja MADRE se registran y se omiten.
    - El contador procesado se normaliza (valores vacios equivalen a cero).
    - El proceso escanea la hoja completa; no depende de disparadores por
      edicion, dado que Beefast inserta filas en lugar de editarlas.
    - Si el asesor destino no existe en el directorio, o el lead aun no
      esta en ninguna hoja, no se marca como procesado y se reintenta en la
      siguiente ejecucion.
    - Un candado de ejecucion evita corridas solapadas.
    - Las llamadas a la API reintentan ante limites de tasa.
    - El orden agregar-verificar-eliminar garantiza que un lead nunca se
      pierda: en el peor caso queda duplicado, jamas ausente.

Para acelerar el proceso, las hojas de los asesores se leen una sola vez al
inicio y se construye un indice en memoria; el cruce posterior es inmediato.

Credenciales (variables de entorno):
    GOOGLE_CREDENTIALS  Contenido JSON de la cuenta de servicio de Google.

Uso:
    python reasignacion.py            Ejecucion real.
    python reasignacion.py --dry-run  Simulacion sin escritura.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, TypeVar

try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.exceptions import APIError, WorksheetNotFound
except ImportError as exc:  # pragma: no cover
    sys.stderr.write(
        "Faltan dependencias requeridas. Instale con:\n"
        "    pip install gspread google-auth\n"
        f"Detalle: {exc}\n"
    )
    raise SystemExit(1) from exc


# --------------------------------------------------------------------------- #
# Configuracion
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    """Parametros de ejecucion del handler de reasignacion."""

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

    # Columnas del directorio.
    dir_nombre: str = "NOMBRE_ASESOR"
    dir_spreadsheet: str = "ID_SPREADSHEET"

    # Control de trafico hacia la API.
    api_pause_seconds: float = 0.3
    max_retries: int = 5
    backoff_base_seconds: float = 5.0

    # Vigencia del candado de ejecucion, en minutos.
    lock_ttl_minutes: float = 10.0

    @classmethod
    def from_environment(cls) -> "Config":
        import os

        google_creds = os.environ.get("GOOGLE_CREDENTIALS", "").strip()
        if not google_creds:
            raise ConfigurationError(
                "Falta la variable de entorno GOOGLE_CREDENTIALS."
            )
        return cls(google_credentials=google_creds)


NULOS = frozenset({"", "NAN", "NONE", "0"})
ESTATUS_LOG_HEADER = [
    "FECHA",
    "ID BEEFAST",
    "ACCION",
    "ASESOR ANTERIOR",
    "ASESOR NUEVO",
    "DETALLE",
]


# --------------------------------------------------------------------------- #
# Excepciones
# --------------------------------------------------------------------------- #

class ReassignmentError(Exception):
    """Error base del handler de reasignacion."""


class ConfigurationError(ReassignmentError):
    """Configuracion ausente o invalida."""


class ExternalServiceError(ReassignmentError):
    """Fallo no recuperable contra un servicio externo."""


class LockActiveError(ReassignmentError):
    """Otra ejecucion mantiene el candado activo."""


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #

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


def strip_accents(value: str) -> str:
    import unicodedata

    text = str(value).strip().lower()
    return unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("utf-8")


def is_valid_id(value: object) -> bool:
    """Un identificador es valido si es numerico y mayor que cero."""
    text = str(value).strip()
    if text.upper() in NULOS:
        return False
    return text.isdigit() and int(text) > 0


def to_int(value: object) -> int:
    """Convierte a entero de forma tolerante; vacio o invalido equivale a 0."""
    text = str(value).strip()
    if text.upper() in {"", "NAN", "NONE"}:
        return 0
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return 0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Cliente de Google Sheets con reintentos
# --------------------------------------------------------------------------- #

class SheetsClient:
    """Envoltura sobre gspread con reintentos ante limites de tasa."""

    _SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

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
            creds = Credentials.from_service_account_info(info, scopes=self._SCOPES)
            return gspread.authorize(creds)
        except Exception as exc:  # noqa: BLE001
            raise ExternalServiceError(
                f"No fue posible autenticar con Google: {exc}"
            ) from exc

    def with_retries(self, operation: Callable[[], T], description: str) -> T:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self._config.max_retries + 1):
            try:
                return operation()
            except APIError as exc:
                status = getattr(exc.response, "status_code", None)
                last_exc = exc
                if status == 429 and attempt < self._config.max_retries:
                    wait = self._config.backoff_base_seconds * attempt
                    logger.warning(
                        "Limite de tasa de Google al %s. "
                        "Reintentando en %.0fs (intento %d/%d).",
                        description, wait, attempt, self._config.max_retries,
                    )
                    time.sleep(wait)
                    continue
                raise ExternalServiceError(
                    f"Error de la API de Google al {description}: {exc}"
                ) from exc
        raise ExternalServiceError(
            f"Se agotaron los reintentos al {description}."
        ) from last_exc

    @property
    def client(self) -> gspread.Client:
        return self._client


# --------------------------------------------------------------------------- #
# Modelo de dominio
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Advisor:
    """Asesor del directorio corporativo."""

    name: str
    spreadsheet_id: str

    @property
    def normalized_name(self) -> str:
        return strip_accents(self.name)


@dataclass(frozen=True)
class LeadLocation:
    """Ubicacion de un lead dentro de la hoja de un asesor."""

    advisor: Advisor
    row_number: int
    row_values: list[str]


@dataclass(frozen=True)
class PendingReassignment:
    """Reasignacion detectada en la hoja MADRE, pendiente de aplicar."""

    madre_row: int
    lead_id: str
    target_advisor_name: str
    reassignment_count: int
    row_values: list[str]


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
    pending: int = 0
    errors: int = 0
    skipped: int = 0

    def summary_lines(self, dry_run: bool) -> list[str]:
        title = "RESUMEN (SIMULACION)" if dry_run else "RESUMEN"
        return [
            "-" * 56,
            f"  {title}",
            "-" * 56,
            f"  Movidos      : {self.moved}",
            f"  Pendientes   : {self.pending} (se reintentaran)",
            f"  Errores      : {self.errors} (se reintentaran)",
            f"  Omitidos     : {self.skipped}",
            "-" * 56,
        ]


# --------------------------------------------------------------------------- #
# Indice de leads en hojas de asesores
# --------------------------------------------------------------------------- #

class LeadIndex:
    """Indice en memoria de la ubicacion de cada lead por asesor.

    Se construye leyendo cada hoja de asesor una sola vez. Asocia cada
    ID BEEFAST con su ubicacion, evitando busquedas repetidas sobre las 26
    hojas para cada reasignacion.
    """

    def __init__(self) -> None:
        self._by_id: dict[str, LeadLocation] = {}

    def register(self, advisor: Advisor, rows: list[list[str]], id_col: int) -> None:
        for offset, row in enumerate(rows[1:], start=2):
            if len(row) <= id_col:
                continue
            lead_id = str(row[id_col]).strip()
            if not is_valid_id(lead_id):
                continue
            # La primera aparicion gana; las duplicadas se ignoran a este
            # nivel (los duplicados en MADRE se filtran por separado).
            self._by_id.setdefault(
                lead_id, LeadLocation(advisor=advisor, row_number=offset, row_values=row)
            )

    def locate(self, lead_id: str) -> Optional[LeadLocation]:
        return self._by_id.get(str(lead_id).strip())


# --------------------------------------------------------------------------- #
# Logica de reasignacion
# --------------------------------------------------------------------------- #

class ReassignmentHandler:
    """Orquesta la deteccion y aplicacion de reasignaciones de leads."""

    def __init__(self, config: Config, sheets: SheetsClient, dry_run: bool) -> None:
        self._config = config
        self._sheets = sheets
        self._dry_run = dry_run
        self._madre = sheets.with_retries(
            lambda: sheets.client.open_by_key(config.madre_sheet_id),
            "abrir el libro MADRE",
        )

    # -- Candado de ejecucion --------------------------------------------- #

    def _acquire_lock(self) -> None:
        try:
            worksheet = self._sheets.with_retries(
                lambda: self._madre.worksheet(self._config.lock_tab),
                "leer el candado",
            )
            values = self._sheets.with_retries(
                worksheet.get_all_values, "leer el candado"
            )
            if values and values[0]:
                try:
                    stamped = datetime.fromisoformat(values[0][0])
                    age_min = (
                        datetime.now(timezone.utc) - stamped
                    ).total_seconds() / 60
                    if age_min < self._config.lock_ttl_minutes:
                        raise LockActiveError(
                            f"Candado activo hace {age_min:.1f} min."
                        )
                except ValueError:
                    pass  # marca ilegible: se sobrescribe
        except ExternalServiceError as exc:
            if isinstance(exc.__cause__, WorksheetNotFound):
                worksheet = self._sheets.with_retries(
                    lambda: self._madre.add_worksheet(
                        title=self._config.lock_tab, rows=1, cols=1
                    ),
                    "crear el candado",
                )
            else:
                raise

        self._sheets.with_retries(
            lambda: worksheet.update([[utc_now_iso()]], "A1"),
            "tomar el candado",
        )

    def _release_lock(self) -> None:
        try:
            worksheet = self._sheets.with_retries(
                lambda: self._madre.worksheet(self._config.lock_tab),
                "liberar el candado",
            )
            self._sheets.with_retries(
                lambda: worksheet.update([[""]], "A1"), "liberar el candado"
            )
        except ReassignmentError:
            logger.warning("No fue posible liberar el candado.")

    # -- Carga de datos ---------------------------------------------------- #

    def _load_directory(self) -> dict[str, Advisor]:
        worksheet = self._sheets.with_retries(
            lambda: self._madre.worksheet(self._config.directorio_tab),
            "abrir el directorio",
        )
        records = self._sheets.with_retries(
            worksheet.get_all_records, "leer el directorio"
        )
        directory: dict[str, Advisor] = {}
        for record in records:
            name = str(record.get(self._config.dir_nombre, "")).strip()
            ss_id = str(record.get(self._config.dir_spreadsheet, "")).strip()
            if name and ss_id:
                advisor = Advisor(name=name, spreadsheet_id=ss_id)
                directory[advisor.normalized_name] = advisor
        logger.info("Asesores en el directorio: %d", len(directory))
        return directory

    def _build_lead_index(self, directory: dict[str, Advisor]) -> LeadIndex:
        """Lee cada hoja de asesor una vez y construye el indice de leads."""
        index = LeadIndex()
        total = len(directory)
        for position, advisor in enumerate(directory.values(), start=1):
            logger.info(
                "  Indexando %d/%d: %s", position, total, advisor.name
            )
            rows = self._read_advisor_rows(advisor)
            if rows is not None:
                index.register(advisor, rows, self._config.col_id)
            time.sleep(self._config.api_pause_seconds)
        return index

    def _read_advisor_rows(self, advisor: Advisor) -> Optional[list[list[str]]]:
        try:
            spreadsheet = self._sheets.with_retries(
                lambda: self._sheets.client.open_by_key(advisor.spreadsheet_id),
                f"abrir la hoja de {advisor.name}",
            )
            try:
                worksheet = spreadsheet.worksheet(self._config.asesor_tab)
            except WorksheetNotFound:
                worksheet = spreadsheet.get_worksheet(0)
            return self._sheets.with_retries(
                worksheet.get_all_values, f"leer la hoja de {advisor.name}"
            )
        except ReassignmentError as exc:
            logger.warning("No se pudo leer la hoja de %s: %s", advisor.name, exc)
            return None

    def _detect_pending(self) -> tuple[list[PendingReassignment], set[str], LogBuffer]:
        """Detecta reasignaciones pendientes y duplicados en la hoja MADRE."""
        worksheet = self._sheets.with_retries(
            lambda: self._madre.worksheet(self._config.madre_tab),
            "abrir la hoja MADRE",
        )
        values = self._sheets.with_retries(
            worksheet.get_all_values, "leer la hoja MADRE"
        )
        logger.info("Filas en la hoja MADRE: %d", len(values) - 1)

        duplicates = self._find_duplicate_ids(values)
        if duplicates:
            logger.warning(
                "Identificadores duplicados en MADRE (se omitiran): %s",
                sorted(duplicates)[:10],
            )

        pending: list[PendingReassignment] = []
        log = LogBuffer()
        cfg = self._config

        for offset, row in enumerate(values[1:], start=2):
            normalized = list(row) + [""] * (cfg.total_cols - len(row))
            lead_id = str(normalized[cfg.col_id]).strip()
            advisor = str(normalized[cfg.col_asesor]).strip()
            count = to_int(normalized[cfg.col_reasignaciones])
            processed = to_int(normalized[cfg.col_procesado])

            if count <= processed:
                continue
            if not is_valid_id(lead_id):
                continue
            if lead_id in duplicates:
                log.add(lead_id, "OMITIDO", "", advisor, "ID duplicado en MADRE")
                continue

            pending.append(
                PendingReassignment(
                    madre_row=offset,
                    lead_id=lead_id,
                    target_advisor_name=advisor,
                    reassignment_count=count,
                    row_values=normalized[: cfg.total_cols],
                )
            )

        logger.info("Reasignaciones pendientes: %d", len(pending))
        return pending, duplicates, log

    def _find_duplicate_ids(self, values: list[list[str]]) -> set[str]:
        counts: dict[str, int] = {}
        for row in values[1:]:
            if len(row) > self._config.col_id:
                lead_id = str(row[self._config.col_id]).strip()
                if is_valid_id(lead_id):
                    counts[lead_id] = counts.get(lead_id, 0) + 1
        return {lead_id for lead_id, total in counts.items() if total > 1}

    # -- Aplicacion -------------------------------------------------------- #

    def _process_one(
        self,
        item: PendingReassignment,
        directory: dict[str, Advisor],
        lead_index: LeadIndex,
        madre_ws: gspread.Worksheet,
        log: LogBuffer,
        stats: RunStats,
    ) -> None:
        target_key = strip_accents(item.target_advisor_name)
        target = directory.get(target_key)

        if target is None:
            logger.warning(
                "Asesor destino '%s' no esta en el directorio. Se reintentara.",
                item.target_advisor_name,
            )
            log.add(
                item.lead_id, "ERROR", "", item.target_advisor_name,
                "Asesor destino ausente del directorio",
            )
            stats.errors += 1
            return

        current = lead_index.locate(item.lead_id)

        if current is not None and current.advisor.normalized_name == target_key:
            logger.info(
                "Lead %s ya pertenece a %s. Se marca como procesado.",
                item.lead_id, target.name,
            )
            self._mark_processed(madre_ws, item)
            log.add(
                item.lead_id, "YA_CORRECTO", target.name, target.name,
                "El lead ya estaba en el asesor destino",
            )
            return

        if current is None:
            logger.warning(
                "Lead %s no se encontro en ninguna hoja. Se reintentara.",
                item.lead_id,
            )
            log.add(
                item.lead_id, "PENDIENTE", "", item.target_advisor_name,
                "El lead aun no existe en ninguna hoja",
            )
            stats.pending += 1
            return

        if self._dry_run:
            logger.info(
                "Simulacion: lead %s se moveria de %s a %s.",
                item.lead_id, current.advisor.name, target.name,
            )
            log.add(
                item.lead_id, "SIMULACION", current.advisor.name, target.name,
                "Sin cambios (simulacion)",
            )
            stats.moved += 1
            return

        self._move_lead(item, current, target, madre_ws, log, stats)

    def _move_lead(
        self,
        item: PendingReassignment,
        current: LeadLocation,
        target: Advisor,
        madre_ws: gspread.Worksheet,
        log: LogBuffer,
        stats: RunStats,
    ) -> None:
        """Mueve el lead garantizando que nunca se pierda.

        Orden estricto: primero se agrega al destino, se verifica la
        insercion, y solo entonces se elimina del origen. Ante cualquier
        fallo, el lead permanece (a lo sumo duplicado, nunca ausente) y la
        reasignacion no se marca como procesada para reintentarse despues.
        """
        try:
            target_ws = self._open_advisor_worksheet(target)

            payload = list(item.row_values)[: self._config.total_cols]
            self._sheets.with_retries(
                lambda: target_ws.append_row(payload, value_input_option="RAW"),
                f"agregar el lead a {target.name}",
            )
            time.sleep(self._config.api_pause_seconds)

            if not self._verify_present(target, item.lead_id):
                raise ExternalServiceError(
                    "No se pudo verificar la insercion en el asesor destino."
                )

            origin_ws = self._open_advisor_worksheet(current.advisor)
            self._sheets.with_retries(
                lambda: origin_ws.delete_rows(current.row_number),
                f"eliminar el lead de {current.advisor.name}",
            )
            time.sleep(self._config.api_pause_seconds)

            self._mark_processed(madre_ws, item)

            logger.info(
                "Lead %s movido de %s a %s.",
                item.lead_id, current.advisor.name, target.name,
            )
            log.add(
                item.lead_id, "MOVIDO", current.advisor.name, target.name, "OK"
            )
            stats.moved += 1

        except ReassignmentError as exc:
            logger.error("Error moviendo el lead %s: %s", item.lead_id, exc)
            log.add(
                item.lead_id, "ERROR", current.advisor.name, target.name, str(exc)[:200]
            )
            stats.errors += 1

    def _open_advisor_worksheet(self, advisor: Advisor) -> gspread.Worksheet:
        spreadsheet = self._sheets.with_retries(
            lambda: self._sheets.client.open_by_key(advisor.spreadsheet_id),
            f"abrir la hoja de {advisor.name}",
        )
        try:
            return spreadsheet.worksheet(self._config.asesor_tab)
        except WorksheetNotFound:
            return spreadsheet.get_worksheet(0)

    def _verify_present(self, advisor: Advisor, lead_id: str) -> bool:
        worksheet = self._open_advisor_worksheet(advisor)
        values = self._sheets.with_retries(
            worksheet.get_all_values, f"verificar el lead en {advisor.name}"
        )
        for row in values[1:]:
            if len(row) > self._config.col_id and str(
                row[self._config.col_id]
            ).strip() == lead_id:
                return True
        return False

    def _mark_processed(
        self, madre_ws: gspread.Worksheet, item: PendingReassignment
    ) -> None:
        self._sheets.with_retries(
            lambda: madre_ws.update_cell(
                item.madre_row,
                self._config.col_procesado + 1,
                item.reassignment_count,
            ),
            "marcar la reasignacion como procesada",
        )

    def _write_log(self, log: LogBuffer) -> None:
        if not log.entries:
            return
        try:
            try:
                worksheet = self._sheets.with_retries(
                    lambda: self._madre.worksheet(self._config.log_tab),
                    "abrir la hoja de log",
                )
            except ExternalServiceError as exc:
                if isinstance(exc.__cause__, WorksheetNotFound):
                    worksheet = self._sheets.with_retries(
                        lambda: self._madre.add_worksheet(
                            title=self._config.log_tab, rows=1000, cols=6
                        ),
                        "crear la hoja de log",
                    )
                    self._sheets.with_retries(
                        lambda: worksheet.update([ESTATUS_LOG_HEADER], "A1"),
                        "inicializar la hoja de log",
                    )
                else:
                    raise
            self._sheets.with_retries(
                lambda: worksheet.append_rows(
                    log.entries, value_input_option="RAW"
                ),
                "escribir el log",
            )
        except ReassignmentError as exc:
            logger.warning("No fue posible escribir el log: %s", exc)

    # -- Punto de entrada -------------------------------------------------- #

    def run(self) -> RunStats:
        stats = RunStats()

        if not self._dry_run:
            self._acquire_lock()

        try:
            directory = self._load_directory()
            pending, _, log = self._detect_pending()

            if not pending:
                logger.info("No hay reasignaciones pendientes.")
                if not self._dry_run:
                    self._write_log(log)
                return stats

            lead_index = self._build_lead_index(directory)

            madre_ws = self._sheets.with_retries(
                lambda: self._madre.worksheet(self._config.madre_tab),
                "abrir la hoja MADRE",
            )

            for item in pending:
                logger.info(
                    "Procesando lead %s -> %s",
                    item.lead_id, item.target_advisor_name,
                )
                self._process_one(
                    item, directory, lead_index, madre_ws, log, stats
                )

            if not self._dry_run:
                self._write_log(log)
            else:
                logger.info(
                    "Simulacion: se habrian registrado %d entradas de log.",
                    len(log.entries),
                )

        finally:
            if not self._dry_run:
                self._release_lock()

        return stats


# --------------------------------------------------------------------------- #
# Punto de entrada
# --------------------------------------------------------------------------- #

def main(argv: Optional[list[str]] = None) -> int:
    configure_logging()
    args = argv if argv is not None else sys.argv[1:]
    dry_run = "--dry-run" in args

    mode = "SIMULACION (sin escritura)" if dry_run else "EJECUCION REAL"
    logger.info("Iniciando handler de reasignacion | modo: %s", mode)

    try:
        config = Config.from_environment()
        sheets = SheetsClient(config)
        handler = ReassignmentHandler(config, sheets, dry_run)
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
