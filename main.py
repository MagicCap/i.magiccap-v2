# This code is a part of MagicCap which is a MPL-2.0 licensed project.
# Copyright (C) Jake Gealer <jake@gealer.email> 2019.

import sentry_sdk
from sentry_sdk.integrations.sanic import SanicIntegration
from sanic import Sanic, response
from sanic.request import Request
from sanic.exceptions import NotFound
from sanic_cors import CORS
from rethinkdb import RethinkDB
import os
import random
import string
import aiobotocore
import botocore
# Imports go here.

r = RethinkDB()
# Defines the RethinkDB driver.


class SanicS3Stream:
    def __init__(self, response):
        self.s3_response = response

    async def __call__(self, response):
        async with self.s3_response['Body'] as stream:
            async for chunk in stream.iter_chunks():
                await response.write(chunk[0])


class RethinkSanic(Sanic):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = None
        self.client = None
        r.set_loop_type("asyncio")
        self.register_listener(self._connect_rethinkdb_and_s3, "before_server_start")

    async def create_db_if_not_exists(self, db):
        try:
            await r.db_create(db).run(self.conn)
        except BaseException:
            pass

    async def create_table_if_not_exists(self, table):
        try:
            await r.table_create(table).run(self.conn)
        except BaseException:
            pass

    async def create_index_if_not_exists(self, table, *args, **kwargs):
        try:
            await r.table(table).index_create(*args, **kwargs).run(self.conn)
        except BaseException:
            pass

    async def create_s3_client(self):
        session = aiobotocore.get_session(loop=self.loop)
        kwargs = {}

        def set_if_exists(k, as_):
            try:
                kwargs[as_] = os.environ[k]
            except KeyError:
                pass

        set_if_exists("AWS_ENDPOINT", "endpoint_url")
        set_if_exists("AWS_SECRET_ACCESS_KEY", "aws_secret_access_key")
        set_if_exists("AWS_ACCESS_KEY_ID", "aws_access_key_id")
        set_if_exists("AWS_REGION", "region_name")

        self.client = session.create_client("s3", **kwargs)

    @staticmethod
    async def _connect_rethinkdb_and_s3(app, loop):
        app.conn = await r.connect(
            host=os.environ.get("RETHINKDB_HOSTNAME") or "127.0.0.1",
            user=os.environ.get("RETHINKDB_USER") or "admin",
            password=os.environ.get("RETHINKDB_PASSWORD") or ""
        )

        await app.create_db_if_not_exists("magiccap")
        app.conn.use("magiccap")

        await app.create_table_if_not_exists("uploads")
        await app.create_table_if_not_exists("installs")

        await app.create_s3_client()


app = RethinkSanic(__name__)
# Defines the app.

CORS(app)
# Allows CORS.

sentry_sdk.init(
    dsn=os.environ['SENTRY_DSN'],
    integrations=[SanicIntegration()]
)
# Loads in Sentry.


@app.route("/")
async def root_redirect(_):
    """Redirects to magiccap.me."""
    return response.redirect("https://magiccap.me")


@app.route("/upload", methods=["POST"])
async def upload(req: Request):
    """Used to upload to i.magiccap"""
    try:
        auth_header = req.headers['Authorization']
    except KeyError:
        return response.json({
            "error": "No authorization header present."
        }, status=400)

    auth_header_split = auth_header.split(" ")
    if len(auth_header_split) != 2:
        return response.json({
            "error": "Invalid authorization header present."
        }, status=400)

    install = await r.table("installs").get(auth_header_split[1]).run(app.conn)
    if not install:
        return response.json({
            "error": "Invalid installation ID."
        }, status=400)

    try:
        file_ = req.files['data'][0]
    except KeyError:
        return response.json({
            "error": "No data found."
        }, status=400)

    filename = "".join([random.choice(string.ascii_lowercase) for i in range(8)])
    ext = file_.name.split(".").pop().lower()

    await app.client.put_object(Bucket=os.environ['AWS_BUCKET'], Key=f"{filename}.{ext}", Body=file_.body, ContentType=file_.type)

    return response.json({
        "url": f"https://i.magiccap.me/{filename}.{ext}"
    })


@app.route("/<item>")
async def image_view(req, item):
    """Loads a image view."""
    session = aiobotocore.get_session(loop=app.loop)

    try:
        get_res = await app.client.get_object(Bucket=os.environ['AWS_BUCKET'], Key=item)
        return response.stream(SanicS3Stream(get_res), content_type=get_res['ContentType'])
    except botocore.exceptions.ClientError:
        return response.text("Not found.", status=404)


@app.exception(NotFound)
async def ignore_404s(_, exception):
    """I don't want Sentry emails about this."""
    return response.text("Not found.", status=404)


if __name__ == "__main__":
    app.run(port=8000, host="0.0.0.0")
# Starts the app.
