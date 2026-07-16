#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
s3_upload.py — Subir videos pesados al bucket del Observatorio (AWS S3).

Uso pensado para los MIEMBROS del equipo. Cada persona sube sus videos con
su propio perfil de credenciales de AWS. El script:

  1. Identifica automáticamente quién eres (tu usuario IAM) mediante STS,
     de modo que tus videos se guardan en tu propia carpeta:
         s3://<bucket>/uploads/<tu-usuario>/...
     Esta carpeta la impone la política de IAM: no puedes escribir en la de
     otra persona, y tampoco puedes borrar nada una vez subido.

  2. Registra "quién y cuándo" de forma automática: el usuario queda en la
     ruta y en los metadatos; la fecha/hora queda en los metadatos y en la
     marca LastModified de S3.

  3. Mide la duración de cada video con `ffprobe` (parte de ffmpeg) para que
     el dashboard del dueño pueda sumar las horas totales subidas.

  4. Sube archivos grandes de forma eficiente y reanudable mediante
     transferencia multiparte, con barra de progreso.

Funciona igual en Windows y macOS (solo requiere Python + boto3 + ffmpeg,
todo incluido en environment.yml).

Dos formas de subir (puedes combinarlas en la misma llamada)
------------------------------------------------------------
    # OPCIÓN 1 · un ARCHIVO de video -> sube ese video
    python s3_upload.py mi_video.mp4

    # OPCIÓN 2 · una CARPETA -> sube automáticamente TODOS los videos que haya dentro
    python s3_upload.py ./grabaciones
    #   (añade --recursivo para incluir también las subcarpetas)
    python s3_upload.py ./grabaciones --recursivo

Ejemplos
--------
    # Varios videos y/o carpetas a la vez, con bucket y perfil explícitos
    python s3_upload.py clase1.mov ./grabaciones --bucket observatorio-videos-gicc --profile juan

    # Simular la subida sin enviar nada (para revisar qué haría)
    python s3_upload.py mi_video.mp4 --dry-run
"""

from __future__ import annotations

import argparse
import mimetypes
import os
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
    from botocore.exceptions import (
        BotoCoreError,
        ClientError,
        NoCredentialsError,
        ProfileNotFound,
    )
except ImportError:
    sys.exit(
        "ERROR: falta la librería boto3.\n"
        "Crea el entorno con:  conda env create -f environment.yml\n"
        "y actívalo con:       conda activate s3-observatorio"
    )

try:
    from tqdm import tqdm
except ImportError:  # la barra de progreso es opcional; degradamos con elegancia
    tqdm = None


# --------------------------------------------------------------------------- #
#  Configuración por defecto (se puede sobreescribir por CLI o entorno)
# --------------------------------------------------------------------------- #
DEFAULT_BUCKET = os.environ.get("S3_BUCKET", "")
DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")
DEFAULT_PROFILE = os.environ.get("AWS_PROFILE") or None
DEFAULT_PREFIX = os.environ.get("S3_PREFIX", "uploads").strip("/")

# Extensiones de video aceptadas (en minúsculas).
EXTENSIONES_VIDEO = {
    ".mp4", ".mov", ".mkv", ".avi", ".m4v",
    ".webm", ".flv", ".wmv", ".mpg", ".mpeg", ".ts", ".3gp",
}

# Tipos de contenido para que S3 sirva el archivo correctamente.
CONTENT_TYPES = {
    ".mp4": "video/mp4",
    ".m4v": "video/x-m4v",
    ".mov": "video/quicktime",
    ".mkv": "video/x-matroska",
    ".avi": "video/x-msvideo",
    ".webm": "video/webm",
    ".flv": "video/x-flv",
    ".wmv": "video/x-ms-wmv",
    ".mpg": "video/mpeg",
    ".mpeg": "video/mpeg",
    ".ts": "video/mp2t",
    ".3gp": "video/3gpp",
}

# Umbral y tamaño de parte para la transferencia multiparte (64 MB).
# Con esto los videos grandes se dividen en partes y se suben en paralelo.
UMBRAL_MULTIPARTE = 64 * 1024 * 1024
TAMANO_PARTE = 64 * 1024 * 1024
MAX_CONCURRENCIA = 4


# --------------------------------------------------------------------------- #
#  Utilidades
# --------------------------------------------------------------------------- #
def humano(num_bytes: float) -> str:
    """Convierte un número de bytes a una cadena legible (KB, MB, GB...)."""
    for unidad in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:3.1f} {unidad}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def duracion_hms(segundos: float | None) -> str:
    """Formatea segundos como H:MM:SS (o 'desconocida')."""
    if not segundos or segundos <= 0:
        return "desconocida"
    s = int(round(segundos))
    return f"{s // 3600}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def nombre_seguro(nombre: str) -> str:
    """Limpia el nombre de archivo para usarlo como clave de S3 sin sorpresas."""
    base = os.path.basename(nombre)
    limpio = []
    for c in base:
        if c.isalnum() or c in ("-", "_", ".", " "):
            limpio.append(c)
        else:
            limpio.append("_")
    return "".join(limpio).strip().replace(" ", "_") or "video"


def obtener_duracion_segundos(ruta: Path) -> float | None:
    """Devuelve la duración del video en segundos usando ffprobe, o None."""
    try:
        salida = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(ruta),
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        valor = salida.stdout.strip()
        return float(valor) if valor else None
    except (FileNotFoundError, ValueError, subprocess.SubprocessError):
        # ffprobe no está instalado o el archivo no se pudo analizar.
        return None


def content_type_de(ruta: Path) -> str:
    """Devuelve el Content-Type adecuado para el video."""
    ext = ruta.suffix.lower()
    if ext in CONTENT_TYPES:
        return CONTENT_TYPES[ext]
    adivinado, _ = mimetypes.guess_type(str(ruta))
    return adivinado or "application/octet-stream"


class BarraProgreso:
    """Callback de progreso para boto3, seguro entre hilos (multiparte)."""

    def __init__(self, total: int, descripcion: str):
        self._lock = threading.Lock()
        self._visto = 0
        self._total = total
        if tqdm is not None:
            self._pbar = tqdm(
                total=total, unit="B", unit_scale=True, unit_divisor=1024,
                desc=descripcion, leave=True,
            )
        else:
            self._pbar = None
            print(f"  Subiendo {descripcion} ({humano(total)})...", flush=True)

    def __call__(self, bytes_transferidos: int):
        with self._lock:
            self._visto += bytes_transferidos
            if self._pbar is not None:
                self._pbar.update(bytes_transferidos)

    def cerrar(self):
        if self._pbar is not None:
            self._pbar.close()


# --------------------------------------------------------------------------- #
#  Núcleo
# --------------------------------------------------------------------------- #
def resolver_usuario(session: "boto3.Session", override: str | None) -> str:
    """
    Determina el usuario IAM autenticado mediante STS. Ese nombre define la
    carpeta destino y DEBE coincidir con ${aws:username} de la política de IAM.
    """
    if override:
        if not override.isascii():
            sys.exit("ERROR: el nombre de usuario debe ser ASCII (así lo son los "
                     "usuarios de IAM).")
        print(
            f"AVISO: usando usuario forzado '{override}'. Si no coincide con "
            "tu usuario IAM real, S3 rechazará la subida (AccessDenied).",
            file=sys.stderr,
        )
        return override

    try:
        arn = session.client("sts").get_caller_identity()["Arn"]
    except (ClientError, BotoCoreError, NoCredentialsError) as exc:
        sys.exit(
            "ERROR: no se pudo identificar tu usuario en AWS.\n"
            f"Detalle: {exc}\n"
            "¿Configuraste tus credenciales?  Ejecuta:  aws configure --profile TU_PERFIL"
        )

    # La política de IAM se basa en la variable ${aws:username}, que SOLO existe
    # para usuarios de IAM. Un rol asumido, AWS SSO / IAM Identity Center, un
    # usuario federado o la cuenta root no la exponen, así que la subida por
    # carpeta no funcionaría: abortamos con un mensaje claro en vez de dejar
    # que S3 devuelva un AccessDenied confuso.
    #   arn:aws:iam::123456789012:user/juan.perez         -> usuario IAM (OK)
    #   arn:aws:sts::123456789012:assumed-role/rol/sesion -> rol asumido (no soportado)
    if ":user/" not in arn:
        sys.exit(
            "ERROR: este script requiere credenciales de un USUARIO de IAM.\n"
            "Tu identidad actual es: " + arn + "\n"
            "Los roles asumidos, AWS SSO / IAM Identity Center, usuarios federados\n"
            "o la cuenta root no permiten la subida por carpeta de este diseño.\n"
            "Configura un perfil con las claves de tu usuario IAM:\n"
            "  aws configure --profile TU_USUARIO"
        )
    usuario = arn.split(":user/", 1)[1].split("/")[-1]
    if not usuario.isascii():
        sys.exit(f"ERROR: nombre de usuario IAM no ASCII no soportado: {usuario!r}")
    return usuario


def construir_clave(prefijo: str, usuario: str, ruta: Path, momento: datetime,
                    usadas: set[str] | None = None) -> str:
    """
    Construye una clave de S3 ÚNICA: uploads/<usuario>/<AAAAMMDD-HHMMSS>_<archivo>.

    Si se pasa `usadas`, garantiza que la clave no colisione con otra ya generada
    en esta misma corrida y la registra en el conjunto. Al subir una CARPETA puede
    haber videos DISTINTOS con el mismo nombre (p. ej. `dia1/intro.mp4` y
    `dia2/intro.mp4`) que, subidos en el mismo segundo, producirían la misma clave
    y se sobrescribirían en S3; en ese caso se añade un sufijo incremental
    (`_2`, `_3`, …) antes de la extensión para conservar ambos videos.
    """
    marca = momento.strftime("%Y%m%d-%H%M%S")
    nombre = f"{marca}_{nombre_seguro(ruta.name)}"
    clave = f"{prefijo}/{usuario}/{nombre}"
    if usadas is not None:
        raiz, ext = os.path.splitext(nombre)  # nombre no lleva '/', seguro en Windows/macOS
        n = 2
        while clave in usadas:
            clave = f"{prefijo}/{usuario}/{raiz}_{n}{ext}"
            n += 1
        usadas.add(clave)
    return clave


def subir_archivo(
    s3, bucket: str, prefijo: str, usuario: str, ruta: Path,
    transfer_config: TransferConfig, dry_run: bool, usadas: set[str],
) -> tuple[bool, float | None]:
    """Sube un archivo. Devuelve (exito, duracion_segundos)."""
    try:
        tamano = ruta.stat().st_size
    except OSError as exc:
        # El archivo pudo borrarse o volverse ilegible tras listarlo; lo omitimos
        # sin abortar el resto del lote.
        print(f"\n• {ruta.name}\n    ✗ No se pudo leer el archivo: {exc}", file=sys.stderr)
        return False, None
    ahora_utc = datetime.now(timezone.utc)
    ahora_local = datetime.now().astimezone()
    duracion = obtener_duracion_segundos(ruta)
    clave = construir_clave(prefijo, usuario, ruta, ahora_utc, usadas)

    print(f"\n• {ruta.name}")
    print(f"    tamaño   : {humano(tamano)}")
    print(f"    duración : {duracion_hms(duracion)}")
    print(f"    destino  : s3://{bucket}/{clave}")

    # Los metadatos de S3 deben ser ASCII: codificamos el nombre original.
    metadatos = {
        "uploader": usuario,
        "upload-timestamp-utc": ahora_utc.isoformat(),
        "upload-timestamp-local": ahora_local.isoformat(),
        "original-filename": quote(ruta.name),
        "client-tool": "s3_upload.py",
    }
    if duracion is not None:
        metadatos["duration-seconds"] = f"{duracion:.3f}"

    if dry_run:
        print("    (dry-run: no se subió nada)")
        return True, duracion

    extra_args = {"Metadata": metadatos, "ContentType": content_type_de(ruta)}
    barra = BarraProgreso(tamano, ruta.name)
    try:
        s3.upload_file(
            Filename=str(ruta),
            Bucket=bucket,
            Key=clave,
            ExtraArgs=extra_args,
            Config=transfer_config,
            Callback=barra,
        )
    except ClientError as exc:
        barra.cerrar()
        codigo = exc.response.get("Error", {}).get("Code", "")
        if codigo in ("AccessDenied", "AccessDeniedException", "403"):
            print(
                "    ✗ ACCESO DENEGADO. Solo puedes subir a tu propia carpeta "
                f"'{prefijo}/{usuario}/'. Verifica que tu usuario IAM y tu perfil "
                "sean los correctos.",
                file=sys.stderr,
            )
        else:
            print(f"    ✗ Error de S3 ({codigo}): {exc}", file=sys.stderr)
        return False, duracion
    except (BotoCoreError, OSError) as exc:
        barra.cerrar()
        print(f"    ✗ Error al subir: {exc}", file=sys.stderr)
        return False, duracion

    barra.cerrar()
    print("    ✓ Subido correctamente")
    return True, duracion


def reunir_archivos(rutas: list[str], recursivo: bool) -> list[Path]:
    """
    Expande cada entrada de la línea de comandos a la lista de videos a subir.

    Admite las dos formas de subir del script, y puedes mezclarlas:
      · Opción 1 — un ARCHIVO de video: se sube ese archivo.
      · Opción 2 — una CARPETA: se suben automáticamente todos los videos que
        contenga (con `recursivo=True`, también los de sus subcarpetas).

    Si un mismo video llega por más de una vía (p. ej. una carpeta y además el
    archivo suelto), se sube una sola vez.
    """
    resultado: list[Path] = []
    vistos: set[Path] = set()  # evita subir dos veces el mismo archivo

    def agregar(video: Path) -> bool:
        """Registra un video; devuelve False si ya estaba en la lista."""
        try:
            clave = video.resolve()
        except OSError:
            clave = video
        if clave in vistos:
            return False
        vistos.add(clave)
        resultado.append(video)
        return True

    for entrada in rutas:
        p = Path(entrada).expanduser()
        if p.is_dir():
            # Opción 2: una carpeta -> todos los videos que haya dentro.
            patron = "**/*" if recursivo else "*"
            encontrados = 0  # videos que hay en la carpeta
            nuevos = 0       # de esos, los que aún no estaban en la lista
            for hijo in sorted(p.glob(patron)):
                if hijo.is_file() and hijo.suffix.lower() in EXTENSIONES_VIDEO:
                    encontrados += 1
                    if agregar(hijo):
                        nuevos += 1
            alcance = "incluye subcarpetas" if recursivo else "sin subcarpetas"
            if nuevos:
                print(f"Carpeta '{p.name}': {nuevos} video(s) para subir ({alcance}).")
            elif encontrados:
                # Tenía videos, pero todos ya estaban en la lista (deduplicados).
                print(f"Carpeta '{p.name}': sus {encontrados} video(s) ya estaban "
                      "en la lista.")
            elif recursivo:
                print(f"AVISO: la carpeta '{p.name}' no contiene videos ({alcance}).",
                      file=sys.stderr)
            else:
                print(f"AVISO: la carpeta '{p.name}' no contiene videos sueltos; "
                      "usa --recursivo para buscar también en subcarpetas.",
                      file=sys.stderr)
        elif p.is_file():
            # Opción 1: un archivo de video suelto.
            if p.suffix.lower() in EXTENSIONES_VIDEO:
                agregar(p)
            else:
                print(
                    f"AVISO: se omite '{p.name}' (no parece un video: {p.suffix})",
                    file=sys.stderr,
                )
        else:
            print(f"AVISO: no existe la ruta '{entrada}', se omite.", file=sys.stderr)
    return resultado


def crear_argumentos() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Sube videos pesados a la carpeta personal del usuario en AWS S3. "
            "Acepta dos formas de carga: (1) el nombre de un archivo de video, o "
            "(2) el nombre de una carpeta, en cuyo caso sube todos los videos que "
            "contenga (con --recursivo, también los de las subcarpetas)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "rutas", nargs="+", metavar="ARCHIVO_O_CARPETA",
        help="Uno o más videos y/o carpetas. Opción 1: un archivo de video "
             "(mi_video.mp4). Opción 2: una carpeta (./grabaciones), que sube "
             "todos los videos que haya dentro. Puedes combinar varios.",
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET,
                        help="Nombre del bucket S3 (o variable S3_BUCKET).")
    parser.add_argument("--region", default=DEFAULT_REGION,
                        help="Región de AWS (o variable AWS_REGION).")
    parser.add_argument("--profile", default=DEFAULT_PROFILE,
                        help="Perfil de credenciales de AWS CLI (o variable AWS_PROFILE).")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX,
                        help="Prefijo raíz dentro del bucket (por defecto 'uploads').")
    parser.add_argument("--usuario", default=None,
                        help="Forzar el nombre de usuario/carpeta (avanzado; normalmente "
                             "se detecta solo con STS).")
    parser.add_argument("--recursivo", action="store_true",
                        help="Buscar videos también en subcarpetas.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostrar qué se subiría sin subir nada.")
    return parser


def main() -> int:
    args = crear_argumentos().parse_args()

    if not args.bucket:
        sys.exit(
            "ERROR: no se indicó el bucket.\n"
            "Usa  --bucket NOMBRE  o define la variable de entorno S3_BUCKET "
            "(ver config.example.env)."
        )

    archivos = reunir_archivos(args.rutas, args.recursivo)
    if not archivos:
        sys.exit("No se encontraron videos para subir.")

    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
    except ProfileNotFound as exc:
        sys.exit(f"ERROR: {exc}\nRevisa tu perfil con:  aws configure list-profiles")

    usuario = resolver_usuario(session, args.usuario)
    prefijo = args.prefix.strip("/")
    s3 = session.client("s3")
    transfer_config = TransferConfig(
        multipart_threshold=UMBRAL_MULTIPARTE,
        multipart_chunksize=TAMANO_PARTE,
        max_concurrency=MAX_CONCURRENCIA,
        use_threads=True,
    )

    print("=" * 68)
    print("  Observatorio de Videos AWS S3 — Subida de videos")
    print("=" * 68)
    print(f"  Usuario detectado : {usuario}")
    print(f"  Bucket            : {args.bucket}  (región {args.region})")
    print(f"  Carpeta destino   : {prefijo}/{usuario}/")
    print(f"  Videos a subir    : {len(archivos)}")
    if args.dry_run:
        print("  MODO              : DRY-RUN (no se subirá nada)")
    print("=" * 68)

    exitos = 0
    horas_totales = 0.0
    claves_usadas: set[str] = set()  # evita que dos videos homónimos colisionen en S3
    for ruta in archivos:
        ok, dur = subir_archivo(
            s3, args.bucket, prefijo, usuario, ruta, transfer_config, args.dry_run,
            claves_usadas,
        )
        if ok:
            exitos += 1
            if dur:
                horas_totales += dur / 3600.0

    print("\n" + "=" * 68)
    print(f"  Resumen: {exitos}/{len(archivos)} videos subidos correctamente.")
    print(f"  Horas de video procesadas: {horas_totales:.2f} h")
    print("=" * 68)
    return 0 if exitos == len(archivos) else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        sys.exit("\nInterrumpido por el usuario.")
