import functools
import logging
import mimetypes
import os
import re
from tempfile import TemporaryDirectory

import requests
import youtube_dl
from simplebot.bot import DeltaBot

from .db import DBManager
from .errors import FileTooBig

session = requests.Session()
session.headers.update(
    {
        "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
    }
)
session.request = functools.partial(session.request, timeout=15)


def is_ytlink(url: str) -> bool:
    return url.startswith(
        (
            "https://www.youtube.com/watch?v=",
            "https://m.youtube.com/watch?v=",
            "https://youtu.be/",
        )
    )


def parse_phone(phone: str) -> str:
    phone = phone.lstrip("+").replace(" ", "")
    return "53" + re.match(r"(53)?(\d{8})", phone).group(2)


def get_db(bot: DeltaBot) -> DBManager:
    path = os.path.join(os.path.dirname(bot.account.db_path), __name__.split(".")[0])
    if not os.path.exists(path):
        os.makedirs(path)
    return DBManager(os.path.join(path, "sqlite.db"))


def download_ytvideo(url: str, max_size: int, is_admin: bool) -> tuple:
    with TemporaryDirectory() as tempdir:
        opts = {
            "format": "best" if is_admin else f"best[filesize<{max_size}]",
            "max_downloads": 1,
            "socket_timeout": 15,
            "outtmpl": tempdir + "/%(title)s.%(ext)s",
        }
        with youtube_dl.YoutubeDL(opts) as yt:
            yt.download([url])
        files = os.listdir(tempdir)
        if len(files) > 1:
            raise FileTooBig()
        filename = files[0]
        data = b""
        size = 0
        chunk_size = 1024 * 1024 * 5
        with open(os.path.join(tempdir, filename), "rb") as f:
            chunk = f.read(chunk_size)
            while chunk:
                size += len(chunk)
                if not is_admin and size > max_size:
                    raise FileTooBig()
                data += chunk
                chunk = f.read(chunk_size)
    return (filename, data, size)


def download_file(url: str, max_size: int, is_admin: bool) -> tuple:
    if "://" not in url:
        url = "http://" + url
    with session.get(url, stream=True) as r:
        r.raise_for_status()
        data = b""
        size = 0
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            size += len(chunk)
            if not is_admin and size > max_size:
                raise FileTooBig()
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
