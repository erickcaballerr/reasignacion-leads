"""
reasignacion.py — Handler de reasignación de leads (versión blindada)
═══════════════════════════════════════════════════════════════════════
Cuando Beefast reasigna un lead a otro asesor, actualiza la columna D
(ASESOR) y suma +1 a la columna AA (CANTIDAD DE REASIGNACIONES) en la
hoja MADRE — pero NO mueve el lead entre las hojas de los asesores.

Este script cierra ese hueco:
  1. Detecta filas en MADRE donde AA > AB (reasignación pendiente)
  2. Lee el nuevo asesor (col D) y el ID BEEFAST (col Y)
  3. Encuentra en qué hoja de asesor está actualmente el lead
  4. Lo AGREGA al nuevo asesor, verifica, y SOLO ENTONCES lo borra del viejo
  5. Marca AB = AA en MADRE
  6. Registra todo en la pestaña LOG_REASIGNACIONES

LAS 10 DEFENSAS:
  1. Ignora IDs en 0, vacíos o no numéricos
  2. Detecta IDs duplicados en MADRE → no los toca, los registra
  3. Normaliza AB (nan/vacío = 0)
  4. Escanea toda la hoja (no depende de onEdit; Beefast inserta filas)
  5. Si el lead no existe aún → no marca AB, reintenta después
  6. Si el asesor nuevo no está en Directorio → registra error, sigue
  7. Lock para evitar ejecuciones solapadas
  8. Reintento con backoff ante rate limit de Google
  9. Agrega-verifica-borra: el lead nunca se pierde (peor caso: duplicado)
 10. Log de auditoría completo en pestaña dedicada

CREDENCIALES (variables de entorno / secretos de GitHub):
  EASYBROKER_API_KEY  (no se usa aquí pero se mantiene por consistencia)
  GOOGLE_CREDENTIALS  → contenido del credentials.json
═══════════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import logging
import unicodedata
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# ═══════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════

GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")
DRY_RUN            = "--dry-run" in sys.argv

MADRE_SHEET_ID = "1UHu2b3drz-6KhLHnw0EBmbIaggg0RFHIkIPAV4H-OSQ"  # Plan de Trabajo Madre

MADRE_TAB       = "CONTROL INTERNO"
DIRECTORIO_TAB  = "Directorio Corporativo"
LOG_TAB         = "LOG_REASIGNACIONES"
ASESOR_TAB      = "CONTROL INTERNO"   # nombre de la pestaña dentro de cada hoja de asesor

# Posición de columnas (0-based) en CONTROL INTERNO
COL_ASESOR   = 3    # D
COL_ID       = 24   # Y  (ID BEEFAST)
COL_AA       = 26   # AA (CANTIDAD DE REASIGNACIONES)
COL_AB       = 27   # AB (REASIGNACION_PROCESADA)
TOTAL_COLS   = 28

# Directorio: columnas
DIR_NOMBRE      = "NOMBRE_ASESOR"
DIR_SPREADSHEET = "ID_SPREADSHEET"

DELAY = 1.0   # pausa base entre llamadas (segundos)


# ═══════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════

def normalizar(texto: str) -> str:
    """Quita acentos y espacios para comparar nombres sin importar tildes."""
    s = str(texto).strip().lower()
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("utf-8")


def id_valido(valor) -> bool:
    """DEFENSA 1: un ID es válido solo si es numérico y distinto de 0."""
    s = str(valor).strip()
    if s in ("", "nan", "none", "0"):
        return False
    return s.isdigit() and int(s) > 0


def a_entero(valor) -> int:
    """DEFENSA 3: normaliza AA/AB. Cualquier cosa no numérica = 0."""
    s = str(valor).strip()
    if s in ("", "nan", "none"):
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def con_reintento(funcion, *args, intentos=4, **kwargs):
    """DEFENSA 8: reintenta ante rate limit (429) con espera creciente."""
    log = logging.getLogger(__name__)
    for intento in range(1, intentos + 1):
        try:
            return funcion(*args, **kwargs)
        except APIError as e:
            codigo = getattr(e.response, "status_code", None)
            if codigo == 429 and intento < intentos:
                espera = 5 * intento
                log.warning(f"    Rate limit de Google. Esperando {espera}s (intento {intento}/{intentos})...")
                time.sleep(espera)
            else:
                raise
    return None


# ═══════════════════════════════════════════════════════════════════
#  GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════════

def conectar():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if not GOOGLE_CREDENTIALS:
        print("❌ Falta el secreto GOOGLE_CREDENTIALS.")
        sys.exit(1)
    try:
        cred_dict = json.loads(GOOGLE_CREDENTIALS)
        creds = Credentials.from_service_account_info(cred_dict, scopes=scopes)
        return gspread.authorize(creds)
    except json.JSONDecodeError:
        print("❌ GOOGLE_CREDENTIALS no es JSON válido.")
        sys.exit(1)


# ── Lock: DEFENSA 7 ───────────────────────────────────────────────

def hay_lock_activo(madre_sh) -> bool:
    """Verifica si otra ejecución está corriendo (lock < 10 min de antigüedad)."""
    log = logging.getLogger(__name__)
    try:
        meta = con_reintento(madre_sh.worksheet, "_LOCK")
        valores = con_reintento(meta.get_all_values)
        if valores and len(valores) > 0 and valores[0]:
            ts_str = valores[0][0]
            try:
                ts = datetime.fromisoformat(ts_str)
                edad_min = (datetime.now() - ts).total_seconds() / 60
                if edad_min < 10:
                    log.warning(f"  Lock activo (hace {edad_min:.1f} min). Otra ejecución en curso. Saliendo.")
                    return True
            except ValueError:
                pass
    except gspread.WorksheetNotFound:
        pass
    return False


def poner_lock(madre_sh):
    try:
        try:
            ws = con_reintento(madre_sh.worksheet, "_LOCK")
        except gspread.WorksheetNotFound:
            ws = con_reintento(madre_sh.add_worksheet, title="_LOCK", rows=1, cols=1)
        con_reintento(ws.update, [[datetime.now().isoformat()]], "A1")
    except Exception as e:
        logging.getLogger(__name__).warning(f"  No se pudo poner lock: {e}")


def quitar_lock(madre_sh):
    try:
        ws = con_reintento(madre_sh.worksheet, "_LOCK")
        con_reintento(ws.update, [[""]], "A1")
    except Exception:
        pass


# ── Log: DEFENSA 10 ───────────────────────────────────────────────

def escribir_log(madre_sh, filas_log):
    """Agrega entradas a la pestaña LOG_REASIGNACIONES."""
    if not filas_log:
        return
    log = logging.getLogger(__name__)
    try:
        try:
            ws = con_reintento(madre_sh.worksheet, LOG_TAB)
        except gspread.WorksheetNotFound:
            ws = con_reintento(madre_sh.add_worksheet, title=LOG_TAB, rows=1000, cols=6)
            con_reintento(ws.update, [["FECHA", "ID BEEFAST", "ACCIÓN", "ASESOR ANTERIOR", "ASESOR NUEVO", "DETALLE"]], "A1")
        con_reintento(ws.append_rows, filas_log, value_input_option="RAW")
    except Exception as e:
        log.warning(f"  No se pudo escribir log: {e}")


# ═══════════════════════════════════════════════════════════════════
#  BÚSQUEDA EN HOJAS DE ASESORES
# ═══════════════════════════════════════════════════════════════════

def buscar_lead_en_asesor(gc, spreadsheet_id, id_beefast):
    """Busca el ID BEEFAST en la hoja de un asesor.
    Devuelve (worksheet, numero_fila, fila_datos) o (None, None, None).
    """
    try:
        sh = con_reintento(gc.open_by_key, spreadsheet_id)
        ws = con_reintento(sh.worksheet, ASESOR_TAB)
    except gspread.WorksheetNotFound:
        # Si no existe esa pestaña, intentar la primera
        try:
            ws = con_reintento(sh.get_worksheet, 0)
        except Exception:
            return None, None, None
    except Exception:
        return None, None, None

    try:
        valores = con_reintento(ws.get_all_values)
    except Exception:
        return None, None, None

    for i, fila in enumerate(valores[1:], start=2):
        if len(fila) > COL_ID and str(fila[COL_ID]).strip() == str(id_beefast).strip():
            return ws, i, fila

    return None, None, None


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-7s  %(message)s",
                        datefmt="%H:%M:%S")
    log = logging.getLogger(__name__)

    modo = "🔍 DRY RUN" if DRY_RUN else "🚀 MODO REAL"
    print(f"\n{modo} — Handler de reasignación\n{'─'*55}")

    gc       = conectar()
    madre_sh = con_reintento(gc.open_by_key, MADRE_SHEET_ID)

    # ── DEFENSA 7: lock ──────────────────────────────────────────
    if not DRY_RUN and hay_lock_activo(madre_sh):
        return
    if not DRY_RUN:
        poner_lock(madre_sh)

    filas_log = []

    try:
        # ── Leer Directorio Corporativo ──────────────────────────
        log.info("Leyendo Directorio Corporativo...")
        dir_ws   = con_reintento(madre_sh.worksheet, DIRECTORIO_TAB)
        dir_rows = con_reintento(dir_ws.get_all_records)
        # Mapa: nombre normalizado → spreadsheet_id
        directorio = {}
        for r in dir_rows:
            nombre = str(r.get(DIR_NOMBRE, "")).strip()
            ss_id  = str(r.get(DIR_SPREADSHEET, "")).strip()
            if nombre and ss_id:
                directorio[normalizar(nombre)] = {"nombre": nombre, "id": ss_id}
        log.info(f"  Asesores en directorio: {len(directorio)}")

        # ── Leer MADRE ───────────────────────────────────────────
        log.info(f"Leyendo MADRE ('{MADRE_TAB}')...")
        madre_ws   = con_reintento(madre_sh.worksheet, MADRE_TAB)
        madre_vals = con_reintento(madre_ws.get_all_values)
        log.info(f"  Filas en MADRE: {len(madre_vals) - 1}")

        # ── DEFENSA 2: detectar IDs duplicados ───────────────────
        conteo_ids = {}
        for fila in madre_vals[1:]:
            if len(fila) > COL_ID:
                idv = str(fila[COL_ID]).strip()
                if id_valido(idv):
                    conteo_ids[idv] = conteo_ids.get(idv, 0) + 1
        ids_duplicados = {k for k, v in conteo_ids.items() if v > 1}
        if ids_duplicados:
            log.warning(f"  ⚠ {len(ids_duplicados)} IDs duplicados detectados (se omitirán): {list(ids_duplicados)[:5]}")

        # ── Detectar reasignaciones pendientes (AA > AB) ─────────
        pendientes = []
        for i, fila in enumerate(madre_vals[1:], start=2):
            if len(fila) <= COL_AB:
                # fila más corta de lo esperado, rellenar mentalmente
                fila = fila + [""] * (TOTAL_COLS - len(fila))
            idv     = str(fila[COL_ID]).strip()
            asesor  = str(fila[COL_ASESOR]).strip()
            aa      = a_entero(fila[COL_AA])
            ab      = a_entero(fila[COL_AB])

            if aa <= ab:
                continue  # ya procesada o sin reasignación

            # DEFENSA 1
            if not id_valido(idv):
                continue

            # DEFENSA 2
            if idv in ids_duplicados:
                filas_log.append([datetime.now().isoformat(), idv, "OMITIDO", "", asesor, "ID duplicado en MADRE"])
                continue

            pendientes.append({"fila_madre": i, "id": idv, "asesor_nuevo": asesor, "aa": aa, "fila_datos": fila})

        log.info(f"  Reasignaciones pendientes: {len(pendientes)}")

        if not pendientes:
            log.info("Nada que procesar. ✓")
            return

        # ── Procesar cada reasignación ───────────────────────────
        n_movidos = n_errores = n_omitidos = 0

        for p in pendientes:
            idv          = p["id"]
            asesor_nuevo = p["asesor_nuevo"]
            fila_madre   = p["fila_madre"]
            log.info(f"\n  Procesando ID {idv} → {asesor_nuevo}")

            # DEFENSA 6: asesor nuevo en directorio?
            clave = normalizar(asesor_nuevo)
            if clave not in directorio:
                log.warning(f"    ⚠ Asesor '{asesor_nuevo}' no está en Directorio. Reintentará luego.")
                filas_log.append([datetime.now().isoformat(), idv, "ERROR", "", asesor_nuevo, "Asesor no está en Directorio Corporativo"])
                n_errores += 1
                continue  # NO se actualiza AB → se reintenta

            destino = directorio[clave]

            # Buscar dónde está actualmente el lead (en todas las hojas)
            ubicacion_actual = None
            for clave_asesor, info in directorio.items():
                ws, num_fila, fila_datos = buscar_lead_en_asesor(gc, info["id"], idv)
                time.sleep(DELAY)
                if ws is not None:
                    ubicacion_actual = {"asesor": info["nombre"], "clave": clave_asesor,
                                        "ws": ws, "fila": num_fila, "id_ss": info["id"]}
                    break

            # ¿Ya está en el asesor correcto?
            if ubicacion_actual and ubicacion_actual["clave"] == clave:
                log.info(f"    Ya está en {asesor_nuevo}. Solo marco AB.")
                if not DRY_RUN:
                    con_reintento(madre_ws.update_cell, fila_madre, COL_AB + 1, p["aa"])
                filas_log.append([datetime.now().isoformat(), idv, "YA_CORRECTO", asesor_nuevo, asesor_nuevo, "El lead ya estaba en el asesor correcto"])
                continue

            # DEFENSA 5: lead no existe en ninguna hoja
            if ubicacion_actual is None:
                log.warning(f"    ⚠ Lead no encontrado en ninguna hoja. Reintentará luego.")
                filas_log.append([datetime.now().isoformat(), idv, "PENDIENTE", "", asesor_nuevo, "Lead aún no existe en ninguna hoja (Forwarder no corrió?)"])
                n_omitidos += 1
                continue  # NO se actualiza AB

            asesor_viejo = ubicacion_actual["asesor"]

            if DRY_RUN:
                log.info(f"    [DRY RUN] Movería de {asesor_viejo} → {asesor_nuevo}")
                filas_log.append([datetime.now().isoformat(), idv, "DRY_RUN", asesor_viejo, asesor_nuevo, "Simulación"])
                n_movidos += 1
                continue

            # ── DEFENSA 9: AGREGAR primero, verificar, LUEGO borrar ──
            try:
                # 1. Agregar al nuevo asesor (fila completa)
                destino_sh = con_reintento(gc.open_by_key, destino["id"])
                try:
                    destino_ws = con_reintento(destino_sh.worksheet, ASESOR_TAB)
                except gspread.WorksheetNotFound:
                    destino_ws = con_reintento(destino_sh.get_worksheet, 0)

                fila_completa = p["fila_datos"][:TOTAL_COLS]
                con_reintento(destino_ws.append_row, fila_completa, value_input_option="RAW")
                time.sleep(DELAY)

                # 2. Verificar que se agregó
                _, verif_fila, _ = buscar_lead_en_asesor(gc, destino["id"], idv)
                time.sleep(DELAY)
                if verif_fila is None:
                    raise RuntimeError("No se pudo verificar el lead en el asesor nuevo tras agregarlo")

                # 3. Solo ahora, borrar del viejo
                con_reintento(ubicacion_actual["ws"].delete_rows, ubicacion_actual["fila"])
                time.sleep(DELAY)

                # 4. Marcar AB = AA en MADRE
                con_reintento(madre_ws.update_cell, fila_madre, COL_AB + 1, p["aa"])

                log.info(f"    ✓ Movido: {asesor_viejo} → {asesor_nuevo}")
                filas_log.append([datetime.now().isoformat(), idv, "MOVIDO", asesor_viejo, asesor_nuevo, "OK"])
                n_movidos += 1

            except Exception as e:
                log.error(f"    ✗ Error moviendo {idv}: {e}")
                filas_log.append([datetime.now().isoformat(), idv, "ERROR", asesor_viejo, asesor_nuevo, str(e)[:200]])
                n_errores += 1
                # NO se actualiza AB → se reintenta la próxima vez

        # ── Resumen ──────────────────────────────────────────────
        print(f"\n{'═'*55}")
        print(f"  RESUMEN {'(DRY RUN)' if DRY_RUN else ''}")
        print(f"{'═'*55}")
        print(f"  ✓ Movidos    : {n_movidos}")
        print(f"  ⚠ Pendientes : {n_omitidos}  (reintentarán)")
        print(f"  ✗ Errores    : {n_errores}  (reintentarán)")
        print(f"{'═'*55}\n")

    finally:
        # ── Escribir log y quitar lock SIEMPRE ──────────────────
        if not DRY_RUN:
            escribir_log(madre_sh, filas_log)
            quitar_lock(madre_sh)
        else:
            log.info(f"[DRY RUN] Se habrían registrado {len(filas_log)} entradas en el log")


if __name__ == "__main__":
    main()
