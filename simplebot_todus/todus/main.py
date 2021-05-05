import argparse
import logging
import os
import sys
from urllib.parse import quote_plus, unquote_plus

from . import __version__
from .client import ToDusClient
from .s3 import get_real_url

logging.basicConfig(format="%(levelname)s - %(message)s", level=logging.INFO)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=__name__.split(".")[0],
        description="ToDus Client",
    )
    parser.add_argument(
        "-n",
        "--number",
        dest="number",
        metavar="PHONE-NUMBER",
        help="account's phone number",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--config-folder",
        dest="folder",
        type=str,
        default="",
        help="folder where account configuration will be saved/loaded",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=__version__,
        help="show program's version number and exit.",
    )

    subparsers = parser.add_subparsers(dest="command")

    login_parser = subparsers.add_parser(name="login", help="authenticate in server")

    up_parser = subparsers.add_parser(name="upload", help="upload file")
    up_parser.add_argument("file", nargs="+", help="file to upload")

    down_parser = subparsers.add_parser(name="download", help="download file")
    down_parser.add_argument("url", nargs="+", help="url to download or txt file path")

    return parser


def register(client: ToDusClient, phone: str) -> str:
    client.request_code(phone)
    pin = input("Enter PIN:").strip()
    password = client.validate_code(phone, pin)
    logging.debug("PASSWORD: %s", password)
    return password


def get_password(phone: str, folder: str) -> str:
    path = os.path.join(folder, phone + ".cfg")
    if os.path.exists(path):
        with open(path) as file:
            return file.read().split("=", maxsplit=1)[-1].strip()
    return ""


def set_password(phone: str, password: str, folder: str) -> None:
    with open(os.path.join(folder, phone + ".cfg"), "w") as file:
        file.write("password=" + password)


def main() -> None:
    parser = get_parser()
    args = parser.parse_args()
    client = ToDusClient()
    password = get_password(args.number, args.folder)
    if not password and args.command != "loging":
        print("ERROR: account not authenticated, login first.")
        return
    if args.command == "upload":
        token = client.login(args.number, password)
        logging.debug("Token: '%s'", token)
        for path in args.file:
            with open(path, "rb") as file:
                data = file.read()
            logging.info("Uploading: %s", path)
            url = client.upload_file(token, data, len(data))
            url += "?name=" + quote_plus(os.path.basename(path))
            logging.info("URL: %s", url)
    elif args.command == "download":
        token = client.login(args.number, password)
        logging.debug("Token: '%s'", token)
        while args.url:
            url = args.url.pop(0)
            if os.path.exists(url):
                with open(url) as fp:
                    args.url = [
                        "{}?name={}".format(*line.strip().split(maxsplit=1))
                        for line in fp.readlines()
                    ] + args.url
                    continue
            logging.info("Downloading: %s", url)
            url, name = url.split("?name=", maxsplit=1)
            name = unquote_plus(name)
            size = client.download_file(token, url, name)
            logging.debug("File Size: %s", size // 1024)
    elif args.command == "login":
        set_password(args.number, register(client, args.number), args.folder)
    else:
        parser.print_usage()
