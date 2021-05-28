import io
import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
from threading import Semaphore, Thread
from urllib.parse import quote_plus

import multivolumefile
import py7zr
import simplebot
from deltachat import Message
from simplebot.bot import DeltaBot, Replies

from .db import DBManager
from .todus.client import ToDusClient
from .util import download_file, download_ytvideo, get_db, is_ytlink, parse_phone

__version__ = "1.0.0"

part_size = 1024 * 1024 * 15
queue_size = 50
pool = ThreadPoolExecutor(max_workers=10)
petitions = dict()
downloading = set()
db: DBManager


@simplebot.hookimpl
def deltabot_init(bot: DeltaBot) -> None:
    global db
    db = get_db(bot)


@simplebot.filter
def filter_messages(bot: DeltaBot, message: Message, replies: Replies) -> None:
    """Process ToDus verification codes."""
    if message.chat.is_group():
        return
    acc = db.get_account(message.get_sender_contact().addr)
    if acc:
        if acc["password"]:
            replies.add(text="❌ Ya verificaste tu número de teléfono")
            return
        try:
            code = int(message.text)
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
def s3_logout(bot: DeltaBot, message: Message, replies: Replies) -> None:
    """Darte baja del bot y olvidar tu cuenta."""
    addr = message.get_sender_contact().addr
    acc = db.get_account(addr)
    if acc:
        db.delete_account(addr)
        replies.add(
            text="🗑️ Tu cuenta ha sido desvinculada.\n\n**⚠️ATENCIÓN:** No se estén dando de baja y logueando otra vez constantemente si no quieren que ToDus bloquee su cuenta. No pueden la misma cuenta de ToDus en varios dispositivos por eso la app del ToDus les dejará de funcionar, tienen que o dejar de usar la apk o usar alguna que les deje establecer el password (el token que les envía el bot cuando inician sesión)"
        )
    else:
        replies.add(text="No estás registrado")


@simplebot.command
def s3_get(bot: DeltaBot, payload: str, message: Message, replies: Replies) -> None:
    """Obten un archivo de internet como enlace de descarga gratis de s3, debes estar registrado para usar este comando."""
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


def _process_request(
    bot: DeltaBot, msg: Message, addr: str, acc: dict, url: str
) -> None:
    bot.logger.debug("Processing petition: %s - %s", addr, url)
    downloading.add(addr)
    try:
        is_admin = bot.is_admin(addr)
        if is_ytlink(url):
            filename, data, size = download_ytvideo(url, is_admin)
        else:
            filename, data, size = download_file(url, is_admin)
        bot.logger.debug(f"Downloaded {size//1024:,}KB: {url}")
        with TemporaryDirectory() as tempdir:
            with multivolumefile.open(
                os.path.join(tempdir, filename + ".7z"),
                "wb",
                volume=part_size,
            ) as vol:
                with py7zr.SevenZipFile(vol, "w", filters=[{"id": py7zr.FILTER_COPY}]) as a:
                    a.writestr(data, filename)
            del data
            parts = sorted(os.listdir(tempdir))
            parts_count = len(parts)
            urls = []
            client = ToDusClient()
            for i, name in enumerate(parts, 1):
                bot.logger.debug("Uploading %s/%s: %s", i, parts_count, url)
                with open(os.path.join(tempdir, name), "rb") as file:
                    part = file.read()
                try:
                    token = client.login(acc["phone"], acc["password"])
                    urls.append(client.upload_file(token, part, len(part)))
                except Exception as ex:
                    bot.logger.exception(ex)
                    time.sleep(15)
                    try:
                        token = client.login(acc["phone"], acc["password"])
                        urls.append(client.upload_file(token, part, len(part)))
                    except Exception as ex:
                        bot.logger.exception(ex)
                        raise ValueError(
                            f"Failed to upload part {i} ({len(part):,}B): {ex}"
                        )
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
        downloading.discard(addr)
        del petitions[addr]
