import io
import mimetypes
import os
import queue
import re
import time
from tempfile import TemporaryDirectory
from threading import Semaphore, Thread
from urllib.parse import quote_plus

import multivolumefile
import py7zr
import requests
import simplebot
from deltachat import Message
from simplebot.bot import DeltaBot, Replies

from .db import DBManager
from .todus.client import ToDusClient

__version__ = "1.0.0"
HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
}
max_size = 1024 * 1024 * 200
part_size = 1024 * 1024 * 15
download_queue: queue.Queue = queue.Queue(100)
db: DBManager


@simplebot.hookimpl
def deltabot_init(bot: DeltaBot) -> None:
    global db
    db = _get_db(bot)


@simplebot.hookimpl
def deltabot_start(bot: DeltaBot) -> None:
    Thread(target=_process_queue, args=(bot,), daemon=True).start()


@simplebot.filter
def filter_messages(bot: DeltaBot, message: Message, replies: Replies) -> None:
    """Process ToDus verification codes."""
    if message.chat.is_group():
        return
    acc = db.get_account(message.get_sender_contact().addr)
    if acc:
        if acc["password"]:
            replies.add(text="ERROR: ya verificaste tu número de teléfono")
            return
        try:
            code = int(message.text)
            password = _get_client().validate_code(acc["phone"], str(code))
            db.set_password(acc["addr"], password)
            replies.add(
                text=f"Tu cuenta ha sido verificada! ya puedes comenzar a pedir contenido.\n\nContraseña:\n{password}"
            )
        except Exception as ex:
            bot.logger.exception(ex)
            replies.add(text=f"Falló la verificación: {ex}")
        return


@simplebot.command
def s3_login(bot: DeltaBot, payload: str, message: Message, replies: Replies) -> None:
    """Verificar tu número de teléfono. Ejemplo: /s3_login 5355555"""
    addr = message.get_sender_contact().addr
    acc = db.get_account(addr)
    if acc:
        replies.add(
            text="ERROR: ya estás registrado, debes darte baja primero con /s3_logout"
        )
        return
    try:
        phone = _parse_phone(payload)
        db.add_account(addr, phone)
        _get_client().request_code(phone)
        replies.add(text="Debes recibir un código SMS, envíalo aquí")
    except Exception as ex:
        bot.logger.exception(ex)
        replies.add(
            text=f"Ocurrió un error, verifica que pusiste el número correctamente. {ex}"
        )


@simplebot.command
def s3_logout(bot: DeltaBot, message: Message, replies: Replies) -> None:
    """Darte baja del bot y olvidar tu cuenta."""
    addr = message.get_sender_contact().addr
    acc = db.get_account(addr)
    if acc:
        db.delete_account(addr)
        replies.add(
            text="Tu cuenta ha sido desvinculada.\n\nATENCIÓN RETRASADOS MENTALES: No se estén dando de baja y logueando otra vez constantemente si no quieren que ToDus bloquee su cuenta. No pueden la misma cuenta de ToDus en varios dispositivos por eso la app del ToDus les dejará de funcionar, tienen que o dejar de usar la apk o usar alguna que les deje establecer el password (el token que les envía el bot cuando inician sesión)"
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
                text="Ehhh... no me pasaste la URL de internet que quieres descargar, por ejemplo: /s3_get https://fsf.org"
            )
            return
        try:
            download_queue.put((message, payload), block=False)
            replies.add(
                text="Tu petición ha sido puesta en la cola de descargas, espera."
            )
        except queue.Full:
            replies.add(
                text="Ya hay muchas peticiones pendientes en cola, intenta más tarde."
            )
    else:
        replies.add(text="No estás registrado")


def _process_queue(bot: DeltaBot) -> None:
    sem = Semaphore(10)
    while True:
        msg, url = download_queue.get()
        with sem:
            pass
        Thread(target=_process_request, args=(bot, msg, url, sem), daemon=True).start()
        time.sleep(1)


def _process_request(bot: DeltaBot, msg: Message, url: str, sem: Semaphore) -> None:
    with sem:
        addr = msg.get_sender_contact().addr
        acc = db.get_account(addr)
        if acc and acc["password"]:
            try:
                filename, data, size = _download_file(url)
                bot.logger.debug(f"Downloaded {size//1024:,}KB: {url}")
                with TemporaryDirectory() as tempdir:
                    with multivolumefile.open(
                        os.path.join(tempdir.name, filename + ".7z"),
                        "wb",
                        volume=part_size,
                    ) as vol:
                        with py7zr.SevenZipFile(vol, "w") as a:
                            a.writestr(data, filename)
                    del data
                    parts = sorted(os.listdir(tempdir.name))
                    parts_count = len(parts)
                    urls = []
                    client = _get_client()
                    for i, name in enumerate(parts, 1):
                        bot.logger.debug("Uploading %s/%s: %s", i, parts_count, url)
                        token = client.login(acc["phone"], acc["password"])
                        with open(os.path.join(tempdir.name, name), "rb") as file:
                            part = file.read()
                        urls.append(client.upload_file(token, part, len(part)))
                txt = "\n".join(
                    f"{down_url}\t{name}" for down_url, name in zip(urls, parts)
                )
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
                msg.chat.send_text(f"La descarga falló. {ex}")


def _get_client() -> ToDusClient:
    return ToDusClient()


def _parse_phone(phone: str) -> str:
    phone = phone.lstrip("+").replace(" ", "")
    return "53" + re.match(r"(53)?(\d{8})", phone).group(2)


def _get_db(bot: DeltaBot) -> DBManager:
    path = os.path.join(os.path.dirname(bot.account.db_path), __name__)
    if not os.path.exists(path):
        os.makedirs(path)
    return DBManager(os.path.join(path, "sqlite.db"))


def _download_file(url: str) -> tuple:
    if "://" not in url:
        url = "http://" + url
    with requests.get(url, headers=HEADERS, stream=True, timeout=15) as r:
        r.raise_for_status()
        size = 0
        data = b""
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            size += len(chunk)
            if size > max_size:
                raise ValueError("File too big")
            data += chunk
        return (get_filename(r) or "file", data, size)


def get_filename(r) -> str:
    d = r.headers.get("content-disposition")
    if d is not None and re.findall("filename=(.+)", d):
        fname = re.findall("filename=(.+)", d)[0].strip('"')
    else:
        fname = r.url.split("/")[-1].split("?")[0].split("#")[0]

    if "." in fname:
        return fname

    ctype = r.headers.get("content-type", "").split(";")[0].strip().lower()
    if ctype == "text/plain":
        ext = ".txt"
    elif ctype == "image/jpeg":
        ext = ".jpg"
    else:
        ext = mimetypes.guess_extension(ctype) or ""
    return (fname or "file") + ext
