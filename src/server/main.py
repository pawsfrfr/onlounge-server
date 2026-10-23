import asyncio
import random
import string

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

rooms = {}
rooms_lock = asyncio.Lock()

CODE_CHARS = string.ascii_uppercase.replace("I", "").replace("O", "") + "23456789"


def generate_code():
    return "".join(random.choices(CODE_CHARS, k=6))


async def broadcast_room(code):
    room = rooms.get(code)
    if not room:
        return

    state = {
        "type": "room_update",
        "code": code,
        "status": room["status"],
        "players": [
            {
                "id": pid,
                "name": player["name"],
                "ready": player["ready"],
                "is_host": pid == room["host"],
            }
            for pid, player in room["players"].items()
        ],
    }

    for ws in list(room["sockets"].values()):
        await ws.send_json(state)


async def remove_player(code, pid):
    if not code or not pid:
        return

    async with rooms_lock:
        room = rooms.get(code)

        if not room or pid not in room["players"]:
            return

        room["players"].pop(pid)
        room["sockets"].pop(pid, None)

        if room["host"] == pid:
            if room["players"]:
                room["host"] = next(iter(room["players"]))
            else:
                del rooms[code]
                return

        await broadcast_room(code)


@app.get("/")
async def home():
    return {"status": "onLounge server is running"}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    code = None
    pid = None

    try:
        while True:
            message = await ws.receive_json()
            action = message.get("action")

            if action == "create":
                async with rooms_lock:
                    if code:
                        continue

                    while True:
                        new_code = generate_code()
                        if new_code not in rooms:
                            break

                    code = new_code
                    pid = "".join(
                        random.choices(string.ascii_letters + string.digits, k=16)
                    )

                    name = str(message.get("name", "Player"))[:20] or "Player"

                    rooms[code] = {
                        "host": pid,
                        "players": {
                            pid: {"name": name, "ready": False}
                        },
                        "sockets": {pid: ws},
                        "status": "waiting",
                    }

                    await ws.send_json({
                        "type": "created",
                        "code": code,
                        "player_id": pid,
                    })
                    await broadcast_room(code)

            elif action == "join":
                async with rooms_lock:
                    if code:
                        continue

                    requested_code = str(message.get("code", "")).strip().upper()
                    room = rooms.get(requested_code)

                    if not room:
                        await ws.send_json({
                            "type": "error",
                            "message": "Room not found",
                        })
                        continue

                    if room["status"] != "waiting":
                        await ws.send_json({
                            "type": "error",
                            "message": "Game already started",
                        })
                        continue

                    if len(room["players"]) >= 8:
                        await ws.send_json({
                            "type": "error",
                            "message": "Room is full",
                        })
                        continue

                    code = requested_code
                    pid = "".join(
                        random.choices(string.ascii_letters + string.digits, k=16)
                    )
                    name = str(message.get("name", "Player"))[:20] or "Player"

                    room["players"][pid] = {"name": name, "ready": False}
                    room["sockets"][pid] = ws

                    await ws.send_json({
                        "type": "joined",
                        "code": code,
                        "player_id": pid,
                    })
                    await broadcast_room(code)

            elif action == "ready":
                async with rooms_lock:
                    room = rooms.get(code)

                    if room and pid in room["players"]:
                        room["players"][pid]["ready"] = bool(
                            message.get("ready", False)
                        )
                        await broadcast_room(code)

            elif action == "start":
                async with rooms_lock:
                    room = rooms.get(code)

                    if room and room["host"] == pid:
                        room["status"] = "playing"
                        await broadcast_room(code)

            elif action == "leave":
                await remove_player(code, pid)
                code = None
                pid = None

    except WebSocketDisconnect:
        await remove_player(code, pid)


# Run with:
# uv run uvicorn main:app --reload

