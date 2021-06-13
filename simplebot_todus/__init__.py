import io
import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
from threading import Event, Semaphore, Thread
from urllib.parse import quote_plus

import multivolumefile
import py7zr
import simplebot
from deltachat import Message
from simplebot.bot import DeltaBot, Replies

from .db import DBManager
from .todus.client import ToDusClient
from .todus.errors import AbortError
from .util import download_file, download_ytvideo, get_db, is_ytlink, parse_phone

__version__ = "1.0.0"

part_size = 1024 * 1024 * 15
queue_size = 50
pool = ThreadPoolExecutor(max_workers=10)
petitions = dict()
downloading = set()
db: DBManager


class Download:
    def __init__(self, addr: str) -> None:
        self.addr = addr
        self.step = -2.0
        self.parts = 0
        self.size = 0
        self.canceled = Event()
        self.client = ToDusClient()

    def abort(self) -> None:
        self.canceled.set()
        self.client.abort()

    def __repr__(self) -> str:
        return f"<{self.addr} {self.step}/{self.parts}>"


@simplebot.hookimpl
def deltabot_init(bot: DeltaBot) -> None:
    global db
    db = get_db(bot)


@simplebot.filter
def filter_messages(bot: DeltaBot, message: Message, replies: Replies) -> None:
    """Process ToDus verification codes."""
    if message.chat.is_group():
        return
    try:
        code = int(message.text)
    except ValueError:
        return
    acc = db.get_account(message.get_sender_contact().addr)
    if acc:
        if acc["password"]:
            replies.add(text="❌ Ya verificaste tu número de teléfono")
            return
        try:
            password = ToDusClient().validate_code(acc["phone"], str(code))
            db.set_password(acc["addr"], password)
            replies.add(
                text=f"☑️ Tu cuenta ha sido verificada! ya puedes comenzar a pedir contenido.\n\nContraseña:\n{password}"
            )
        except Exception as ex:
            bot.logger.exception(ex)
            replies.add(text=f"❌ Falló la verificación: {ex}")
        return


@simplebot.command
def s3_login(bot: DeltaBot, payload: str, message: Message, replies: Replies) -> None:
    """Verificar tu número de teléfono. Ejemplo: /s3_login 5355555"""
    addr = message.get_sender_contact().addr
    acc = db.get_account(addr)
    if acc:
        replies.add(
            text="❌ Ya estás registrado, debes darte baja primero con /s3_logout"
        )
        return
    try:
        phone = parse_phone(payload)
        db.add_account(addr, phone)
        ToDusClient().request_code(phone)
        replies.add(text="Debes recibir un código SMS, envíalo aquí")
    except Exception as ex:
        bot.logger.exception(ex)
        replies.add(
            text=f"❌ Ocurrió un error, verifica que pusiste el número correctamente. {ex}"
        )


@simplebot.command
def s3_login2(bot: DeltaBot, payload: str, message: Message, replies: Replies) -> None:
    """Iniciar sessión con tu número de teléfono y contraseña. Ejemplo: /s3_login2 5355555 ay21XjB8i7Uyz"""
    addr = message.get_sender_contact().addr
    acc = db.get_account(addr)
    if acc:
        replies.add(
            text="❌ Ya estás registrado, debes darte baja primero con /s3_logout"
        )
        return
    try:
        phone, password = payload.rsplit(maxsplit=1)
        phone = parse_phone(phone)
        ToDusClient().login(phone, password)
        db.add_account(addr, phone, password)
        replies.add(
            text=f"☑️ Tu cuenta ha sido verificada! ya puedes comenzar a pedir contenido.\n\nContraseña:\n{password}"
        )
    except Exception as ex:
        bot.logger.exception(ex)
        replies.add(
            text=f"❌ Ocurrió un error, verifica que pusiste el número y contraseña correctamente. {ex}"
        )


@simplebot.command
def s3_logout(bot: DeltaBot, message: Message, replies: Replies) -> None:
    """Darte baja del bot y olvidar tu cuenta."""
    addr = message.get_sender_contact().addr
    if addr in petitions:
        replies.add(
            text="❌ Tienes una petición pendiente en cola, espera a que tu descarga termine para darte baja.",
            quote=message,
        )
        return
    acc = db.get_account(addr)
    if acc:
        db.delete_account(addr)
        replies.add(
            text="🗑️ Tu cuenta ha sido desvinculada.\n\n**⚠️ATENCIÓN:** No se estén dando de baja y logueando otra vez constantemente si no quieren que ToDus bloquee su cuenta. No pueden la misma cuenta de ToDus en varios dispositivos por eso la app del ToDus les dejará de funcionar, tienen que o dejar de usar la apk o usar alguna que les deje establecer el password (el token que les envía el bot cuando inician sesión)"
        )
    else:
        replies.add(text="No estás registrado")


@simplebot.command
def s3_status(bot: DeltaBot, payload: str, message: Message, replies: Replies) -> None:
    """Muestra el estado de tu descarga."""
    addr = message.get_sender_contact().addr
    in_queue = addr in petitions
    d = None
    for download in list(downloading):
        if download.addr == addr:
            d = download
            break
    if d and d.parts:
        step = max(int(d.step), 0)
        percent = step / d.parts
        progress = ("🟩" * round(10 * percent)).ljust(10, "⬜")
        text = f"⬇️ Tu petición se está descargando!\n\n{progress}\n**{step}/{d.parts} ({d.size//1024:,}KB)**"
    elif in_queue:
        text = "⏳ Tu petición está pendiente en cola, espera tu turno."
    else:
        text = "❌ No tienes ninguna petición pendiente en cola."
    replies.add(text=text)


@simplebot.command
def s3_get(bot: DeltaBot, payload: str, message: Message, replies: Replies) -> None:
    """Obtén un archivo de internet como enlace de descarga gratis de s3, debes estar registrado para usar este comando."""
    addr = message.get_sender_contact().addr
    acc = db.get_account(addr)
    if acc and acc["password"]:
        if not payload:
            replies.add(
                text="❌ Ehhh... no me pasaste la URL de internet que quieres descargar, por ejemplo: /s3_get https://fsf.org",
                quote=message,
            )
        elif addr in petitions:
            replies.add(
                text="❌ Ya tienes una petición pendiente en cola, espera a que tu descarga termine, solo puedes hacer una petición a la vez.",
                quote=message,
            )
        elif len(petitions) >= queue_size:
            replies.add(
                text="⏸️ Ya hay muchas peticiones pendientes en cola, intenta más tarde.",
                quote=message,
            )
        else:
            petitions[addr] = payload
            pool.submit(_process_request, bot, message, addr, acc, payload)
            replies.add(
                text="⏳ Tu petición ha sido puesta en la cola de descargas, por favor, espera.",
                quote=message,
            )
    else:
        replies.add(text="❌ No estás registrado", quote=message)


@simplebot.command
def s3_cancel(message: Message, replies: Replies) -> None:
    """Cancela la descarga de tu petición."""
    addr = message.get_sender_contact().addr
    download = None
    for d in list(downloading):
        if d.addr == addr:
            download = d
    if download:
        download.abort()
    else:
        replies.add(
            text="❌ No tienes ninguna descarga en curso. Si tienes una petición en la cola, debes esperar a que tu descarga comience para cancelarla",
            quote=message,
        )


@simplebot.command
def s3_pass(message: Message, replies: Replies) -> None:
    """Obtén el password de tu sessión registrada."""
    acc = db.get_account(message.get_sender_contact().addr)
    if acc and acc["password"]:
        replies.add(text=acc["password"])
    else:
        replies.add(text="❌ No estás registrado", quote=message)


@simplebot.command
def s3_token(message: Message, replies: Replies) -> None:
    """Obtén un token temporal que sirve para autenticarse en el servidor de s3 con otras apps que lo soporten."""
    acc = db.get_account(message.get_sender_contact().addr)
    if acc and acc["password"]:
        replies.add(text=ToDusClient().login(acc["phone"], acc["password"]))
    else:
        replies.add(text="❌ No estás registrado", quote=message)


def _process_request(
    bot: DeltaBot, msg: Message, addr: str, acc: dict, url: str
) -> None:
    bot.logger.debug("Processing petition: %s - %s", addr, url)
    d = Download(addr)
    downloading.add(d)
    cancel_err = ValueError("Descarga cancelada.")
    try:
        is_admin = bot.is_admin(addr)
        if is_ytlink(url):
            filename, data, size = download_ytvideo(url, is_admin)
        else:
            filename, data, size = download_file(url, is_admin)
        bot.logger.debug(f"Downloaded {size//1024:,}KB: {url}")

        if d.canceled.is_set():
            raise cancel_err

        d.size = size
        d.step += 1  # step == -1
        with TemporaryDirectory() as tempdir:
            with multivolumefile.open(
                os.path.join(tempdir, filename + ".7z"),
                "wb",
                volume=part_size,
            ) as vol:
                with py7zr.SevenZipFile(
                    vol, "w", filters=[{"id": py7zr.FILTER_COPY}]
                ) as a:
                    a.writestr(data, filename)
            del data
            parts = sorted(os.listdir(tempdir))
            urls = []
            d.parts = len(parts)
            d.step += 1  # step == 0
            for i, name in enumerate(parts, 1):
                if d.canceled.is_set():
                    raise cancel_err
                bot.logger.debug("Uploading %s/%s: %s", i, d.parts, url)
                with open(os.path.join(tempdir, name), "rb") as file:
                    part = file.read()
                try:
                    token = d.client.login(acc["phone"], acc["password"])
                    d.step += 0.5
                    urls.append(d.client.upload_file(token, part, len(part)))
                except AbortError:
                    raise cancel_err
                except Exception as ex:
                    bot.logger.exception(ex)
                    time.sleep(15)
                    try:
                        token = d.client.login(acc["phone"], acc["password"])
                        if d.step.is_integer():
                            d.step += 0.5
                        urls.append(d.client.upload_file(token, part, len(part)))
                    except AbortError:
                        raise cancel_err
                    except Exception as ex:
                        bot.logger.exception(ex)
                        raise ValueError(
                            f"Failed to upload part {i} ({len(part):,}B): {ex}"
                        )
                d.step += 0.5
        txt = "\n".join(f"{down_url}\t{name}" for down_url, name in zip(urls, parts))
        replies = Replies(msg, logger=bot.logger)
        replies.add(
            text=f"{filename} **({size//1024:,}KB)**",
            filename=filename + ".txt",
            bytefile=io.BytesIO(txt.encode()),
            quote=msg,
        )
        replies.send_reply_messages()
    except Exception as ex:
        bot.logger.exception(ex)
        replies = Replies(msg, logger=bot.logger)
        replies.add(text=f"❌ La descarga falló. {ex}", quote=msg)
        replies.send_reply_messages()
    finally:
        downloading.discard(d)
        del petitions[addr]
